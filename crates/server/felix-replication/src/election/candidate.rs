//! A follower standing for election once it suspects its leader: rules 1 to
//! 4 of "Electing without the control plane" in `docs/replication-design.md`,
//! `Elect` in `docs/formal/FelixShard.tla`.
//!
//! Only the steps before the fence live here: the pre-vote, the wait for its
//! place, and the ballot. Once the ballot is on disk the standing goes into
//! [`Elections`], the routing feed names this broker leader at the new
//! generation, and the shard opens through the promotion path with a
//! candidate's fence.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

use felix_broker::{Broker, LogKind, StreamLog};
use felix_common::membership::ELECTION_GENERATION_WINDOW;
use felix_router::{Route, ShardKey, ShardKind, ShardRouter};
use felix_storage::disk_log::{GenerationCheck, Membership, ReplicaSet};
use felix_wire::internal::{InternalMessage, PreVote, PreVoteRefusal, ReplicaLog};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{Elected, Elections, Recorded};
use crate::peer::PeerRequester;
use crate::quorum::QuorumMarks;
use crate::suspicion::{Suspects, SuspicionConfig};

/// The wait per place in the set's order, so the replica furthest ahead
/// usually stands alone.
const PLACE_DELAY: Duration = Duration::from_millis(300);
/// A failed round backs off a random time between these, the top doubling
/// on each failure up to [`BACKOFF_TOP`].
const BACKOFF_FLOOR: Duration = Duration::from_millis(200);
const BACKOFF_FIRST_TOP: Duration = Duration::from_millis(800);
const BACKOFF_TOP: Duration = Duration::from_secs(3);
/// How long a shard that cannot stand waits before it is looked at again.
const INELIGIBLE_FOR: Duration = Duration::from_secs(1);
/// How long a standing may take to open before it is given up, unless its
/// fence or catch-up is still running. Its ballot stays on disk, so the next
/// try stands a generation higher.
const OPEN_WITHIN: Duration = Duration::from_secs(3);
/// How long a pre-vote answer is waited for. A silent voter counts as no.
/// A dial still running when it ends goes on, so the next round finds the
/// connection.
const PRE_VOTE_WITHIN: Duration = Duration::from_secs(1);
/// How often a standing is checked for having opened.
const OPEN_POLL: Duration = Duration::from_millis(50);
/// Shards standing at once, so a broker that lost many leaders together
/// does not ask its peers for every vote at the same instant.
const STANDING_AT_ONCE: usize = 64;
/// How often eligibility and stepped-down standings are looked at between
/// changes to the suspect list.
const TICK: Duration = Duration::from_millis(250);
/// How often followed shards are checked for a leader making no progress.
/// Coarse against the bound, which is minutes, and it reads every quiet
/// shard's log.
const SHARD_SCAN: Duration = Duration::from_secs(5);
/// The least wait after a failed round on a shard suspected alone. Its
/// leader's broker answers, so a voter that still hears it refuses every
/// round; retrying at the broker backoff would only load the voters.
const SHARD_RETRY: Duration = Duration::from_secs(5);
/// Of [`STANDING_AT_ONCE`], how many rounds may stand on a shard's suspicion
/// alone, so shards of a broker that really died never queue behind them.
const SHARD_AT_ONCE: usize = 16;
/// How long a ballot naming this broker that no standing here wrote holds
/// it off standing. The control plane named this broker and the routes have
/// not caught up yet, which takes a reconcile; after a restart it may be a
/// standing this process no longer remembers, so it is not held off forever.
const NAMED_HERE_FOR: Duration = Duration::from_secs(30);

/// What one round of standing came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Round {
    /// The shard opened at the generation this broker stood at.
    Opened,
    /// Refused, split, or never opened: back off and try again.
    Failed,
    /// Not a shard this broker may stand for right now.
    Ineligible,
}

/// Everything a round needs, shared by every standing.
struct Ctx<R> {
    requester: Arc<R>,
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
    marks: Arc<QuorumMarks>,
    elections: Arc<Elections>,
    suspects: Arc<Suspects>,
    /// Why each shard last could not stand, so the reason is logged once
    /// when it changes rather than every round.
    held_back: std::sync::Mutex<HashMap<ShardKey, (&'static str, String)>>,
    /// When each shard was first held back for following a newer leader,
    /// by the generation it follows. See [`Ctx::defers`].
    deferring: std::sync::Mutex<HashMap<ShardKey, (u64, Instant)>>,
    /// See [`SuspicionConfig::shard_suspect_after`]; zero is off.
    shard_after: Duration,
    /// Since when each followed shard has had no word from its leader, by
    /// what the clock started on. See [`Ctx::shard_stuck`].
    quiet: std::sync::Mutex<HashMap<ShardKey, Quiet>>,
    /// The shards the last scan found stuck.
    stuck: std::sync::Mutex<HashSet<ShardKey>>,
    /// The generation each shard last stood at here.
    stood: std::sync::Mutex<HashMap<ShardKey, u64>>,
    /// When a ballot naming this broker that no standing here wrote was
    /// first seen, by generation. See [`Ctx::named_here`].
    named_here: std::sync::Mutex<HashMap<ShardKey, (u64, Instant)>>,
    /// Test-only: while this file exists, a pre-vote waits before it asks,
    /// so a test can release two candidates' rounds together.
    pre_vote_gate: Option<std::path::PathBuf>,
}

/// What a shard's quiet clock started on. Any change restarts it: a new
/// route is the control plane acting, a new ballot is a fence taken.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Quiet {
    generation: u64,
    leader: String,
    accepted: u64,
    since: Instant,
}

impl<R> Ctx<R> {
    /// Whether this copy suspects `key` itself, though its leader's broker
    /// may answer pings: it took a generation (`accepted`, at or above the
    /// route's) that has not started here, and for the shard bound it has
    /// neither stored a batch nor taken a fence from the route's leader.
    ///
    /// An idle shard is never stuck: its generation started. Nor is a copy
    /// behind the route, which its leader has yet to fence; the copies it
    /// fenced decide. The clock restarts when the route or the ballot
    /// changes, so with the control plane up its own rename comes first.
    ///
    /// A route naming this broker is stuck when its own ballot is above the
    /// route's generation and nothing elected holds that ballot: the driver's
    /// open at the route's generation is refused for good, and with the
    /// control plane down nobody else renames the shard. Its followers took
    /// its fence, so it is the only one that can stand.
    fn shard_stuck(&self, key: &ShardKey, route: &Route, log: &StreamLog, now: Instant) -> bool {
        if self.shard_after.is_zero() {
            return false;
        }
        let local = self.router.local_node_id();
        let accepted = log.accepted_generation();
        let blocked = if route.leader.node_id == local {
            accepted > route.generation
                && log.accepted_leader().as_deref() == Some(local)
                && self
                    .elections
                    .get(key)
                    .is_none_or(|elected| elected.generation < accepted)
        } else {
            let started = log
                .generations()
                .iter()
                .any(|epoch| epoch.generation == accepted);
            accepted >= route.generation && !started
        };
        // A ballot the control plane named this broker at holds it off while
        // the routes catch up, as in `stand`. Asked on every scan so that
        // hold runs alongside the bound rather than after it.
        let named = blocked && route.leader.node_id == local && self.named_here(key, accepted);
        let mut quiet = self.quiet.lock().expect("quiet lock");
        if accepted == 0 || !blocked {
            quiet.remove(key);
            return false;
        }
        let on = Quiet {
            generation: route.generation,
            leader: route.leader.node_id.clone(),
            accepted,
            since: now,
        };
        let since = match quiet.get(key) {
            Some(known)
                if Quiet {
                    since: known.since,
                    ..on.clone()
                } == *known =>
            {
                known.since
            }
            _ => {
                quiet.insert(key.clone(), on);
                now
            }
        };
        let since = self
            .elections
            .last_heard(key)
            .map_or(since, |heard| heard.max(since));
        !named && now.saturating_duration_since(since) >= self.shard_after
    }

    /// Recheck every followed shard whose leader's broker this one does not
    /// suspect, and keep [`Ctx::stuck`] current.
    async fn scan_stuck(&self, suspected: &BTreeSet<String>) {
        let local = self.router.local_node_id();
        let now = Instant::now();
        let mut followed = HashSet::new();
        let mut stuck = HashSet::new();
        for (key, route) in self.router.snapshot().iter() {
            if !holds_copy(route, local) {
                continue;
            }
            followed.insert(key.clone());
            if suspected.contains(&route.leader.node_id) {
                continue;
            }
            // A shard heard within the bound is not stuck whatever its log
            // says, and under load that is every busy shard: no log read.
            let heard = self
                .elections
                .last_heard(key)
                .is_some_and(|heard| now.saturating_duration_since(heard) < self.shard_after);
            if heard {
                continue;
            }
            // Only a log already open: a copy not open cannot be stuck, and
            // opening every quiet shard's log here would recover them all on
            // this task after a restart. `stand` reads it properly.
            let log = match key.kind {
                ShardKind::Stream => self.broker.durable_storage().and_then(|storage| {
                    storage.opened_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                }),
                ShardKind::Cache => shard_log(&self.broker, key).await,
            };
            let Some(log) = log else {
                continue;
            };
            if self.shard_stuck(key, route, &log, now) {
                stuck.insert(key.clone());
            }
        }
        self.quiet
            .lock()
            .expect("quiet lock")
            .retain(|key, _| followed.contains(key));
        self.elections.forget_heard_except(&followed);
        let mut known = self.stuck.lock().expect("stuck lock");
        for key in stuck.difference(&known) {
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                after_ms = self.shard_after.as_millis() as u64,
                "this shard's leader has made no progress here; standing for it is allowed",
            );
        }
        *known = stuck;
    }

    fn ineligible(&self, key: &ShardKey, why: &'static str) -> Round {
        self.held_back(key, why, "");
        Round::Ineligible
    }

    /// Whether this copy still defers to the newer leader whose fence it took
    /// at `generation`, unless that leader's pre-vote answer says it stood
    /// down. Without a bound once that leader's start record is here: it
    /// opened and reached this copy. Otherwise only for the elections' defer
    /// bound, so a leader that never opens does not hold the shard forever.
    fn defers(&self, key: &ShardKey, generation: u64, started: bool) -> bool {
        if started {
            return true;
        }
        let now = Instant::now();
        let mut deferring = self.deferring.lock().expect("deferring lock");
        let since = match deferring.get(key) {
            Some((at, since)) if *at == generation => *since,
            _ => {
                deferring.insert(key.clone(), (generation, now));
                now
            }
        };
        now.saturating_duration_since(since) < self.elections.defer_for()
    }

    /// Whether this copy's ballot at `accepted` names this broker without a
    /// standing here having written it, so the control plane named this
    /// broker leader there and it is opening the shard. Bounded by
    /// [`NAMED_HERE_FOR`].
    fn named_here(&self, key: &ShardKey, accepted: u64) -> bool {
        if self.stood.lock().expect("stood lock").get(key) == Some(&accepted) {
            return false;
        }
        let now = Instant::now();
        let mut named = self.named_here.lock().expect("named_here lock");
        let since = match named.get(key) {
            Some((at, since)) if *at == accepted => *since,
            _ => {
                named.insert(key.clone(), (accepted, now));
                now
            }
        };
        now.saturating_duration_since(since) < NAMED_HERE_FOR
    }

    fn held_back(&self, key: &ShardKey, why: &'static str, answers: &str) {
        let seen = (why, answers.to_string());
        let changed = self
            .held_back
            .lock()
            .expect("held_back lock")
            .insert(key.clone(), seen.clone())
            != Some(seen);
        if changed {
            tracing::info!(
                stream = %key.stream,
                shard = key.shard,
                why,
                answers,
                "not standing for election",
            );
        }
    }
}

/// Stand for every shard whose leader this broker suspects, until
/// `shutdown`. `None` when the broker does not check sets, has no
/// registry of elections, or the config turns the detector off.
pub fn spawn<R: PeerRequester + Send + Sync + 'static>(
    requester: Arc<R>,
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
    marks: Arc<QuorumMarks>,
    suspects: Arc<Suspects>,
    config: SuspicionConfig,
    shutdown: CancellationToken,
) -> Option<tokio::task::JoinHandle<()>> {
    if config.suspect_after.is_zero() || !requester.offers_election() {
        return None;
    }
    let elections = Arc::clone(marks.elections()?);
    let ctx = Arc::new(Ctx {
        requester,
        broker,
        router,
        marks,
        elections,
        suspects,
        held_back: Default::default(),
        deferring: Default::default(),
        shard_after: if config.suspects_shards() {
            config.shard_suspect_after
        } else {
            Duration::ZERO
        },
        quiet: Default::default(),
        stuck: Default::default(),
        stood: Default::default(),
        named_here: Default::default(),
        pre_vote_gate: pre_vote_gate(),
    });
    Some(tokio::spawn(run(ctx, shutdown)))
}

async fn run<R: PeerRequester + Send + Sync + 'static>(
    ctx: Arc<Ctx<R>>,
    shutdown: CancellationToken,
) {
    let mut suspected = ctx.suspects.subscribe();
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut rounds: JoinSet<(ShardKey, Round)> = JoinSet::new();
    // By whether the round stands on the shard's suspicion alone.
    let mut in_flight: HashMap<ShardKey, bool> = HashMap::new();
    let mut backoff: HashMap<ShardKey, Backoff> = HashMap::new();
    let mut scanned: Option<Instant> = None;
    // The generation each shard was last fenced again from, so it is once.
    let mut restood: HashMap<ShardKey, u64> = HashMap::new();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = suspected.changed() => {}
            _ = ticker.tick() => {}
            Some(done) = rounds.join_next() => {
                if let Ok((key, round)) = done {
                    let shard_only = in_flight.remove(&key).unwrap_or(false);
                    match round {
                        Round::Opened => {
                            backoff.remove(&key);
                            ctx.held_back.lock().expect("held_back lock").remove(&key);
                        }
                        Round::Failed => {
                            let backoff = backoff.entry(key).or_default();
                            backoff.failed();
                            if shard_only {
                                backoff.at_least(SHARD_RETRY);
                            }
                        }
                        Round::Ineligible => backoff.entry(key).or_default().wait(INELIGIBLE_FOR),
                    }
                }
                continue;
            }
        }
        step_down_promised_away(&ctx).await;
        if !ctx
            .marks
            .fleet_supports(felix_common::fleet::REPLICA_ELECTION)
        {
            continue;
        }
        let names = ctx.suspects.current();
        let now = Instant::now();
        backoff.retain(|_, backoff| !backoff.idle(now));
        if !ctx.shard_after.is_zero()
            && scanned.is_none_or(|at| now.saturating_duration_since(at) >= SHARD_SCAN)
        {
            ctx.scan_stuck(&names).await;
            scanned = Some(Instant::now());
        }
        if !ctx.shard_after.is_zero() {
            restood.retain(|key, _| ctx.elections.get(key).is_some());
            for (key, generation) in ctx.elections.collided_and_refused(ctx.shard_after) {
                if in_flight.len() >= STANDING_AT_ONCE
                    || in_flight.contains_key(&key)
                    || restood.get(&key) == Some(&generation)
                {
                    continue;
                }
                restood.insert(key.clone(), generation);
                in_flight.insert(key.clone(), true);
                let ctx = Arc::clone(&ctx);
                rounds.spawn(async move {
                    let round = restand(&ctx, &key, generation).await;
                    (key, round)
                });
            }
        }
        let stuck = ctx.stuck.lock().expect("stuck lock").clone();
        if names.is_empty() && stuck.is_empty() {
            // The next outage says again why a shard does not stand.
            ctx.held_back.lock().expect("held_back lock").clear();
            continue;
        }
        let local = ctx.router.local_node_id();
        let snapshot = ctx.router.snapshot();
        // Shards of a suspected broker first: a real outage is not held up
        // behind shards suspected alone, which get a smaller share.
        let mut order: Vec<_> = snapshot.iter().collect();
        order.sort_by_key(|(_, route)| !names.contains(&route.leader.node_id));
        for (key, route) in order {
            if in_flight.len() >= STANDING_AT_ONCE {
                break;
            }
            let broker_suspected = names.contains(&route.leader.node_id);
            if !holds_copy(route, local)
                || !(broker_suspected || stuck.contains(key))
                || in_flight.contains_key(key)
                || backoff.get(key).is_some_and(|backoff| backoff.until > now)
            {
                continue;
            }
            if !broker_suspected
                && in_flight.values().filter(|shard_only| **shard_only).count() >= SHARD_AT_ONCE
            {
                continue;
            }
            in_flight.insert(key.clone(), !broker_suspected);
            let ctx = Arc::clone(&ctx);
            let key = key.clone();
            let route = route.clone();
            rounds.spawn(async move {
                let round = stand(&ctx, &key, &route).await;
                (key, round)
            });
        }
    }
}

/// Whether this broker holds a copy of `route`'s shard: a follower, or the
/// leader the route names, which [`Ctx::shard_stuck`] checks for standing
/// against its own ballot.
fn holds_copy(route: &Route, local: &str) -> bool {
    route.leader.node_id == local
        || route
            .replicas
            .iter()
            .any(|replica| replica.node_id == local)
}

/// `FELIX_TEST_PRE_VOTE_GATE`, honoured in debug builds only.
fn pre_vote_gate() -> Option<std::path::PathBuf> {
    if !cfg!(debug_assertions) {
        return None;
    }
    std::env::var_os("FELIX_TEST_PRE_VOTE_GATE").map(std::path::PathBuf::from)
}

/// Withdraw a standing whose generation this broker has since promised to
/// another leader (rule 5). The driver stops leading it; this drops the
/// route that named it leader.
async fn step_down_promised_away<R>(ctx: &Ctx<R>) {
    for (key, generation) in ctx.elections.standing() {
        if let Some(log) = shard_log(&ctx.broker, &key).await
            && log.accepted_generation() > generation
        {
            ctx.elections.withdraw(&key, generation);
        }
    }
}

/// One round for `key`, whose leader in `route` this broker suspects.
async fn stand<R: PeerRequester + Send + Sync + 'static>(
    ctx: &Ctx<R>,
    key: &ShardKey,
    route: &Route,
) -> Round {
    let local = ctx.router.local_node_id();
    let Some(log) = eligible_log(ctx, key).await else {
        return ctx.ineligible(
            key,
            "the shard does not acknowledge on its followers, or its log is not open here",
        );
    };
    let suspected = |ctx: &Ctx<R>| {
        ctx.suspects.current().contains(&route.leader.node_id)
            || ctx.shard_stuck(key, route, &log, Instant::now())
    };
    if !suspected(ctx) {
        return ctx.ineligible(key, "the leader is neither suspected nor stuck here");
    }
    // This copy took another leader's fence above the route, and nothing
    // says that leader is gone: standing over it would only take its
    // follower away, leaving it a generation it can never serve at. It
    // stands anyway once that leader answers the pre-vote as one that is
    // neither fencing nor leading, or, if its start record never came,
    // once the elections' defer bound has passed.
    let accepted = log.accepted_generation();
    let follows_newer = log
        .accepted_leader()
        .filter(|leader| {
            accepted > route.generation
                && **leader != *local
                && !ctx.suspects.current().contains(&**leader)
        })
        .map(|leader| leader.to_string());
    let follows_newer = follows_newer.filter(|_| {
        let started = log
            .generations()
            .iter()
            .any(|epoch| epoch.generation == accepted);
        ctx.defers(key, accepted, started)
    });
    let Standing {
        kept,
        read,
        leaving,
        generation,
    } = match standing_on(ctx, key, route, &log) {
        Ok(standing) => standing,
        Err(round) => return round,
    };
    // The control plane named this broker leader above the route this round
    // read, and the lifecycle took that ballot before the routes caught up.
    // The shard is led here: standing would raise this copy's own ballot past
    // the generation it serves, and it would never count a mark there again.
    if accepted > route.generation
        && log
            .accepted_leader()
            .is_some_and(|leader| *leader == *local)
        && ctx.named_here(key, accepted)
    {
        return ctx.ineligible(key, "this broker was named leader at a newer generation");
    }
    let first = pre_vote(ctx, key, &log, &kept, generation).await;
    if let Some(leader) = &follows_newer {
        let stood_down = first.as_ref().is_ok_and(|first| first.stood_down(leader));
        if !stood_down {
            return ctx.ineligible(key, "this copy follows a newer leader it does not suspect");
        }
        tracing::info!(
            stream = %key.stream,
            shard = key.shard,
            generation = accepted,
            leader = %leader,
            "the newer leader this copy follows is not leading it; standing over it",
        );
    }
    let first = match first {
        Ok(first) if first.majority() => first,
        Ok(lost) => {
            ctx.held_back(key, "the pre-vote found no majority", &lost.answers());
            return Round::Failed;
        }
        Err(why) => {
            ctx.held_back(key, "the pre-vote found no majority", &why);
            return Round::Failed;
        }
    };
    if first.place > 0 {
        tokio::time::sleep(PLACE_DELAY * first.place).await;
        // A voter ahead that stood meanwhile has its ballot on disk, but its
        // fence may not have reached this broker yet under load. Standing
        // anyway splits the vote at one generation and both candidates lose
        // it. Asking again sees the ballot whatever the fence's latency. Only
        // a refusal counts: a voter silent this time proves nothing.
        let again = pre_vote(ctx, key, &log, &kept, generation).await;
        if let Some(changed) = again
            .as_ref()
            .ok()
            .and_then(|again| first.granted.iter().find(|node| again.refused(node)))
        {
            ctx.held_back(
                key,
                "a voter that said yes refused when asked again",
                changed,
            );
            return Round::Failed;
        }
    }
    // Someone may have stood meanwhile, the leader come back, or the routes
    // named a new leader, perhaps this broker.
    let still = suspected(ctx)
        && ctx.router.snapshot().get(key).is_some_and(|now| {
            now.leader.node_id == route.leader.node_id && now.generation == route.generation
        })
        && log.accepted_generation() < generation
        && ctx
            .elections
            .get(key)
            .is_none_or(|elected| elected.generation < generation);
    if !still {
        return Round::Failed;
    }
    // The ballot is on disk before anything names this broker leader. A set
    // this copy took while the round ran (a fence or a batch below the new
    // generation) would leave the checks above stale, so the ballot is taken
    // only on the set they read, under the lock every set raise takes. A
    // learner flip from `admit_member` fails the round on purpose: the
    // tally's voting members changed.
    match log.accept_generation_on_set(generation, local, read).await {
        Ok(Some(GenerationCheck::Raised)) => {}
        Ok(None) => {
            ctx.held_back(key, "the kept replica set changed while the round ran", "");
            return Round::Failed;
        }
        _ => return Round::Failed,
    }
    ctx.stood
        .lock()
        .expect("stood lock")
        .insert(key.clone(), generation);
    tracing::info!(
        stream = %key.stream,
        shard = key.shard,
        generation,
        leader = %route.leader.node_id,
        broker_suspected = ctx.suspects.current().contains(&route.leader.node_id),
        "standing for election: the leader is unreachable or stuck and a majority agreed",
    );
    ctx.elections.record(
        key.clone(),
        Elected {
            leader: local.to_string(),
            generation,
            set: kept,
            leaving,
            incarnation: crate::incarnation::of(&ctx.broker, key),
        },
    );
    open_or_withdraw(ctx, key, &log, generation, OPEN_WITHIN).await
}

/// Fence `key` one generation up as the leader this broker already is by
/// election at `generation`, where the control plane's route names another
/// leader at that same generation and a copy of the set follows neither.
/// With the control plane silent nothing else renames the shard, and that
/// copy refuses this leader for good. No pre-vote: its voters hear this
/// broker, and nobody is deposed. The ballot and fence rules are a
/// standing's.
async fn restand<R>(ctx: &Ctx<R>, key: &ShardKey, generation: u64) -> Round {
    let local = ctx.router.local_node_id();
    let Some(log) = eligible_log(ctx, key).await else {
        return ctx.ineligible(key, "the shard's log is not open here");
    };
    let Some(route) = ctx.router.snapshot().get(key).cloned() else {
        return Round::Ineligible;
    };
    let leads = route.leader.node_id == local
        && route.generation == generation
        && log.accepted_generation() == generation
        && opened(&log, generation)
        && ctx.elections.stands_at(key, generation);
    if !leads {
        return Round::Ineligible;
    }
    let Standing {
        kept,
        read,
        leaving,
        generation: next,
    } = match standing_on(ctx, key, &route, &log) {
        Ok(standing) => standing,
        Err(round) => return round,
    };
    // As in `stand`: the ballot only on the set `standing_on` checked.
    match log.accept_generation_on_set(next, local, read).await {
        Ok(Some(GenerationCheck::Raised)) => {}
        Ok(None) => {
            ctx.held_back(key, "the kept replica set changed while the round ran", "");
            return Round::Failed;
        }
        _ => return Round::Failed,
    }
    ctx.stood
        .lock()
        .expect("stood lock")
        .insert(key.clone(), next);
    tracing::info!(
        stream = %key.stream,
        shard = key.shard,
        generation = next,
        "fencing again one generation up: the control plane named another leader at this \
         election's generation, and a copy follows neither",
    );
    ctx.elections.record(
        key.clone(),
        Elected {
            leader: local.to_string(),
            generation: next,
            set: kept,
            leaving,
            incarnation: crate::incarnation::of(&ctx.broker, key),
        },
    );
    // Given up, it costs more than a standing: this broker's own ballot is
    // now past the generation it was serving, so the shard has no leader
    // until another bound passes. A fence that stalls for a moment, a copy
    // briefly unreachable, is waited out for the defer bound instead.
    open_or_withdraw(ctx, key, &log, next, ctx.elections.defer_for()).await
}

/// What a standing for `key` fences: the set its copy keeps, the set kept
/// beside it, and the generation above both the copy's ballot and `route`.
struct Standing {
    kept: Membership,
    /// The set the checks read. The ballot is taken only while the copy
    /// still keeps exactly this.
    read: ReplicaSet,
    leaving: Option<Membership>,
    generation: u64,
}

/// The standing this copy may fence for `key`, or why it may not stand.
fn standing_on<R>(
    ctx: &Ctx<R>,
    key: &ShardKey,
    route: &Route,
    log: &StreamLog,
) -> Result<Standing, Round> {
    let local = ctx.router.local_node_id();
    let Some(kept) = log.replica_set() else {
        return Err(ctx.ineligible(key, "this copy keeps no replica set"));
    };
    // A change pending on the kept set is left to the control plane: an
    // elected route names one set, so the followers' would not know which
    // set to leave. Costs availability only. A set being left with the same
    // copies is the one a move's cut-over leaves, its successor flagged a
    // learner; that is stood on, and fenced jointly.
    if kept
        .leaving
        .as_ref()
        .is_some_and(|leaving| !same_copies(&kept.current, leaving))
    {
        return Err(ctx.ineligible(key, "the kept replica set has a change pending"));
    }
    // Earlier sets are kept only while a fence leaving them never opened, so a
    // majority of one may hold an acknowledged write no later set does. The
    // candidate's fence leaves one set, so it is left to the control plane.
    if !kept.earlier.is_empty() {
        return Err(ctx.ineligible(key, "the kept replica set keeps earlier sets"));
    }
    // A start record at the kept set's generation is written only by the
    // leader that kept the set being left, once its joint fence took a
    // majority of it. Fencing that set again would need its dead members too.
    // A later start proves less: a leader the control plane named may have
    // fenced the kept set alone.
    let leaving = kept.leaving.clone().filter(|_| {
        !log.generations()
            .iter()
            .any(|epoch| epoch.generation == kept.current.generation)
    });
    // Its fence would stay pending, after the ballot took a generation.
    if leaving.is_some() && !ctx.marks.fleet_supports(felix_common::fleet::JOINT_FENCE) {
        return Err(ctx.ineligible(
            key,
            "a set is kept beside this one and joint fences are off",
        ));
    }
    if kept.current.generation == 0 {
        return Err(ctx.ineligible(key, "this copy keeps no replica set"));
    }
    // Only a voter of the set it keeps stands (`Elect`: f \in mine[f],
    // ~NonVoter(f)).
    let votes = kept
        .current
        .members
        .iter()
        .any(|member| member.node == local && !member.learner);
    if !votes || log.member_is_wiped(local) {
        return Err(ctx.ineligible(key, "this copy does not vote in its kept set"));
    }
    // A candidate fences the set it keeps and the one kept beside it. If the
    // control plane records another set, a majority of that one could hold
    // what this fence never reaches, so it does not stand at all (#1806).
    // That also means the elected leader counts the kept set alone: the
    // control plane holds it with nothing pending.
    let recorded = ctx
        .elections
        .recorded(key)
        .unwrap_or_else(|| Recorded::of_route(route));
    if !recorded.is(&kept.current) {
        return Err(ctx.ineligible(
            key,
            "the control plane records another set, or a change pending",
        ));
    }

    // Above the route's generation too: the control plane may have named
    // one this replica was never fenced at, and standing there would collide
    // with that leader's.
    let generation = log.accepted_generation().max(route.generation) + 1;
    // The control plane adopts an election only this far past its own
    // generation, so a standing beyond it could never be handed over and its
    // route would never drop.
    if generation
        > recorded
            .generation
            .saturating_add(ELECTION_GENERATION_WINDOW)
    {
        return Err(ctx.ineligible(key, "past the generations the control plane adopts"));
    }
    Ok(Standing {
        read: kept.clone(),
        kept: kept.current,
        leaving,
        generation,
    })
}
/// Wait for this broker's standing for `key` at `generation` to open, and
/// withdraw it if it does not within `open_within` of its route's
/// publication.
async fn open_or_withdraw<R>(
    ctx: &Ctx<R>,
    key: &ShardKey,
    log: &StreamLog,
    generation: u64,
    open_within: Duration,
) -> Round {
    let local = ctx.router.local_node_id();
    // The open bound runs from when the route naming this standing is
    // published: the driver fences only published routes, and a routing feed
    // waiting on an unanswering control plane can hold it back for seconds.
    // Withdrawing before then only burns the generation. Unpublished for the
    // defer bound, it is given up anyway.
    let recorded_at = Instant::now();
    let mut published_at: Option<Instant> = None;
    loop {
        if opened(log, generation) {
            return Round::Opened;
        }
        let ours = ctx
            .elections
            .get(key)
            .is_some_and(|elected| elected.generation == generation && elected.leader == local);
        if !ours || log.accepted_generation() > generation {
            ctx.elections.withdraw(key, generation);
            return Round::Failed;
        }
        // A fence or catch-up still running is let finish: giving up now
        // would cost a generation, and the next try would need one more.
        let now = Instant::now();
        if published_at.is_none()
            && ctx.router.snapshot().get(key).is_some_and(|route| {
                route.generation == generation && route.leader.node_id == local
            })
        {
            published_at = Some(now);
        }
        let due = match published_at {
            Some(at) => now >= at + open_within,
            None => now >= recorded_at + ctx.elections.defer_for(),
        };
        if due && !ctx.elections.is_fencing(key, generation) {
            tracing::info!(
                stream = %key.stream,
                shard = key.shard,
                generation,
                "the standing did not open in time; withdrawing it",
            );
            ctx.elections.withdraw(key, generation);
            return Round::Failed;
        }
        tokio::time::sleep(OPEN_POLL).await;
    }
}

/// Whether `leaving` names the same members on the same copies as `current`,
/// so the two differ in learner flags at most.
fn same_copies(current: &Membership, leaving: &Membership) -> bool {
    current.members.len() == leaving.members.len()
        && current.members.iter().all(|member| {
            leaving
                .members
                .iter()
                .any(|left| left.node == member.node && left.storage == member.storage)
        })
}

/// The shard's own log, if the shard may elect: acknowledged at quorum, by
/// the followers' answers rather than a lease.
async fn eligible_log<R>(ctx: &Ctx<R>, key: &ShardKey) -> Option<StreamLog> {
    let by_followers = match key.kind {
        ShardKind::Stream => {
            ctx.marks.acks_by_followers()
                && ctx
                    .marks
                    .fleet_supports(felix_common::fleet::LEASE_FREE_READS)
        }
        ShardKind::Cache => ctx.marks.caches_ack_by_followers(),
    };
    if !by_followers || !crate::quorum::acknowledges_at_quorum(&ctx.broker, key).await {
        return None;
    }
    shard_log(&ctx.broker, key).await
}

async fn shard_log(broker: &Broker, key: &ShardKey) -> Option<StreamLog> {
    let kind = match key.kind {
        ShardKind::Stream => LogKind::Stream,
        ShardKind::Cache => LogKind::Cache,
    };
    let shard = crate::incarnation::shard_ref(broker, key, 0);
    broker
        .shard_log_for(
            kind,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            None,
            crate::incarnation::expected(broker, key.kind, &shard),
        )
        .await
}

/// The answers to one pre-vote.
#[derive(Debug, PartialEq, Eq)]
struct PreVoted {
    /// Voters ahead of this broker: those that refused because their log is
    /// ahead, and those that granted with a lower node id, which may hold an
    /// equal log.
    place: u32,
    /// The other voters that said yes.
    granted: BTreeSet<String>,
    /// The other voters that said no, and why.
    refused: Vec<(String, PreVoteRefusal)>,
    /// Voters in the set, this broker included.
    voters: usize,
}

impl PreVoted {
    /// A majority of the set said yes, this broker included.
    fn majority(&self) -> bool {
        (self.granted.len() + 1) * 2 > self.voters
    }

    fn refused(&self, node: &str) -> bool {
        self.refused.iter().any(|(refuser, _)| refuser == node)
    }

    /// Whether `node` answered as one neither fencing nor leading the shard:
    /// a yes, or a refusal it only reaches past that check. Silence is not
    /// an answer.
    fn stood_down(&self, node: &str) -> bool {
        self.granted.contains(node)
            || self
                .refused
                .iter()
                .any(|(refuser, why)| refuser == node && *why == PreVoteRefusal::LogBehind)
    }

    /// What the voters answered, for the log.
    fn answers(&self) -> String {
        let refused: Vec<String> = self
            .refused
            .iter()
            .map(|(node, refusal)| format!("{node}: {refusal:?}"))
            .collect();
        format!(
            "{} of {} voters with this broker, {} answered, refused by [{}]",
            self.granted.len() + 1,
            self.voters,
            self.granted.len() + self.refused.len(),
            refused.join(", ")
        )
    }
}

/// Ask the other voters of `set` whether they would take `generation`. A
/// voter that does not answer in time is neither.
async fn pre_vote<R: PeerRequester + Send + Sync + 'static>(
    ctx: &Ctx<R>,
    key: &ShardKey,
    log: &StreamLog,
    set: &Membership,
    generation: u64,
) -> Result<PreVoted, String> {
    let local = ctx.router.local_node_id();
    let end = log
        .tail_offset()
        .await
        .map_err(|err| format!("could not read the log's end: {err}"))?;
    let request = PreVote {
        correlation_id: 0,
        shard: crate::incarnation::shard_ref(&ctx.broker, key, generation),
        log: match key.kind {
            ShardKind::Stream => ReplicaLog::Stream,
            ShardKind::Cache => ReplicaLog::Cache,
        },
        last_generation: crate::replica::last_generation(&log.generations(), end),
        log_end: end,
        set_generation: set.generation,
    };
    let voters: Vec<&str> = set
        .members
        .iter()
        .filter(|member| !member.learner)
        .map(|member| member.node.as_str())
        .collect();
    let rounds = futures::future::join_all(
        voters
            .iter()
            .filter(|node| **node != local)
            .filter_map(|node| Some((*node, ctx.router.node_addr(node)?)))
            .map(|(node, addr)| {
                let message = InternalMessage::PreVote(request.clone());
                // The dial is warmed apart from the request: a first request to
                // a voter dials it, and on a slow link that can take longer
                // than the wait. Cancelled with the request, every later round
                // would start it over and time out the same way. The pool
                // bounds the dial and dials a peer once at a time.
                let requester = Arc::clone(&ctx.requester);
                let to = node.to_string();
                tokio::spawn(async move {
                    let _ = requester.capabilities(&to, addr).await;
                });
                let asked = ctx.requester.request(node, addr, message);
                async move {
                    match tokio::time::timeout(PRE_VOTE_WITHIN, asked).await {
                        Ok(Ok(InternalMessage::PreVoteAnswer(answer))) => {
                            Some((node, answer.refusal))
                        }
                        _ => None,
                    }
                }
            }),
    );
    if let Some(gate) = ctx.pre_vote_gate.as_ref().filter(|gate| gate.exists()) {
        tracing::info!(
            stream = %key.stream,
            shard = key.shard,
            "holding the pre-vote at the test gate",
        );
        while gate.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    let answers = rounds.await;
    let mut voted = PreVoted {
        place: 0,
        granted: BTreeSet::new(),
        refused: Vec::new(),
        voters: voters.len(),
    };
    for (node, refusal) in answers.into_iter().flatten() {
        match refusal {
            None => {
                voted.granted.insert(node.to_string());
                if node < local {
                    voted.place += 1;
                }
            }
            Some(refusal) => {
                if refusal == PreVoteRefusal::LogBehind {
                    voted.place += 1;
                }
                voted.refused.push((node.to_string(), refusal));
            }
        }
    }
    Ok(voted)
}

/// Whether the shard opened at `generation`: its start record is written.
fn opened(log: &StreamLog, generation: u64) -> bool {
    log.generations()
        .iter()
        .any(|epoch| epoch.generation == generation)
}

/// When a shard may next stand.
#[derive(Debug)]
struct Backoff {
    failures: u32,
    until: Instant,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            failures: 0,
            until: Instant::now(),
        }
    }
}

impl Backoff {
    /// A random wait from [`BACKOFF_FLOOR`] to a top that doubles with each
    /// failure, so two candidates that split a round rarely split the next.
    fn failed(&mut self) {
        let top = BACKOFF_FIRST_TOP
            .saturating_mul(1 << self.failures.min(8))
            .min(BACKOFF_TOP);
        self.failures += 1;
        let span = top.saturating_sub(BACKOFF_FLOOR).as_millis() as u64;
        let jitter = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish()
            % (span + 1);
        self.until = Instant::now() + BACKOFF_FLOOR + Duration::from_millis(jitter);
    }

    fn wait(&mut self, wait: Duration) {
        self.until = Instant::now() + wait;
    }

    fn at_least(&mut self, wait: Duration) {
        self.until = self.until.max(Instant::now() + wait);
    }

    /// Long past its wait, so forgetting it changes nothing.
    fn idle(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.until) > BACKOFF_TOP * 4
    }
}

#[cfg(test)]
mod tests;

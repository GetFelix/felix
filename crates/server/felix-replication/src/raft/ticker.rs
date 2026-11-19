//! The node ticker: the one timer behind every group on a broker.
//!
//! Each group is built with openraft's heartbeat and election timers off.
//! This task asks a leader for a heartbeat check once per heartbeat, at the
//! short or the idle gap, and turns a follower's election timer on once its
//! wait for the leader has run out. A follower that hears its leader is
//! never asked. See
//! `docs/per-shard-raft.md`, "One timer for every group".
//!
//! Turning that timer on is the only way a member here starts an election,
//! so the ticker also bounds how many groups on the broker are in one at
//! once ([`Elections`]). See "Many groups electing at once" there.

use std::collections::{HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use felix_common::random::HashMap;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::group::{Group, Timers};
use super::reach::{CEILING, Peer};
use super::runtime::{ElectionLoad, Registry};

/// Times the ticker ended on a panic and was started again. Any increase
/// is a bug: no group on the broker was ticked in between.
const TICKER_RESTARTS_TOTAL: &str = "felix_broker_raft_ticker_restarts_total";

/// Members here let stand whose election is not over.
const ELECTIONS_IN_FLIGHT: &str = "felix_broker_raft_elections_in_flight";

/// Members here whose wait for a leader ran out and that wait for a place.
const ELECTIONS_WAITING: &str = "felix_broker_raft_elections_waiting";

/// Run the ticker until `stop`, starting it again if it panics.
///
/// Without a ticker no leader here heartbeats and no follower here starts a
/// new election, and nothing else would show it. A restart puts both back within a
/// heartbeat, where failing the runtime would stop every group for a bug
/// that may be in one. The restarts are logged and counted for whoever
/// watches the broker.
pub(crate) async fn supervise(
    registry: Arc<Registry>,
    every: Duration,
    timers: Timers,
    stop: CancellationToken,
) {
    loop {
        let ticker = run(Arc::clone(&registry), every, timers, stop.clone());
        let Err(panic) = AssertUnwindSafe(ticker).catch_unwind().await else {
            return;
        };
        let reason = panic
            .downcast_ref::<&str>()
            .map(|reason| reason.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "no message".into());
        let restarts = registry.ticker_restarts.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::counter!(TICKER_RESTARTS_TOTAL).increment(1);
        tracing::error!(%reason, restarts, "the Raft ticker panicked and is being started again");
        // A ticker that panics every time must not spin.
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = tokio::time::sleep(Duration::from_millis(timers.heartbeat)) => {}
        }
    }
}

async fn run(registry: Arc<Registry>, every: Duration, timers: Timers, stop: CancellationToken) {
    let mut interval = tokio::time::interval(every);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut looks = Looks::default();
    let mut back = Back::default();
    let mut elections = Elections::new(&timers, Arc::clone(&registry.vote_saves));
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _ = interval.tick() => {}
        }
        #[cfg(test)]
        if registry.trip_ticker.swap(false, Ordering::Relaxed) {
            panic!("tripped by a test");
        }
        registry.tail_budget.report();
        back.retry(&registry, Duration::from_millis(timers.heartbeat));
        let due = looks.due(registry.ticked(), &timers);
        if timers.max_elections_at_once > 0 {
            let now = timers.now();
            elections.settle(now);
            let wanting = due.iter().filter(|group| group.wants_to_stand());
            elections.queue(wanting.cloned(), now);
            elections.admit(now);
            elections.report(&registry);
        }
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = tick_all(&due, Instant::now() + every) => {}
        }
    }
}

/// Retries peer members that answer again, ahead of openraft's backoff.
///
/// A broker back from an outage gets one walk of every group here that
/// leads a member on it, except the groups whose member there still
/// refuses: those are not open yet, and each is retried on its own member's
/// first answer instead. A broker is walked at most once per `CEILING` and
/// never twice at once, so a flapping or struggling peer is not called more
/// often than openraft's backoff would call it at its ceiling.
#[derive(Default)]
struct Back {
    walks: HashMap<Arc<str>, Walk>,
}

#[derive(Default)]
struct Walk {
    last: Option<Instant>,
    busy: Arc<AtomicBool>,
    /// When a group last heartbeat to the broker while it was down.
    scouted: Option<Instant>,
    scouting: Arc<AtomicBool>,
}

/// Retry commands sent to the groups' cores at once.
const RESETS_AT_ONCE: usize = 64;

impl Back {
    fn retry(&mut self, registry: &Registry, heartbeat: Duration) {
        let reach = &registry.reach;
        #[cfg(test)]
        if reach.no_wake.load(Ordering::Relaxed) {
            return;
        }
        let peers = reach.peers();
        self.walks.retain(|broker, _| peers.contains_key(broker));
        let now = Instant::now();
        for (broker, peer) in peers.iter() {
            let walk = self.walks.entry(Arc::clone(broker)).or_default();
            scout(registry, peer, walk, now, heartbeat);
            if walk.busy.load(Ordering::Acquire) {
                continue;
            }
            let whole = walk.last.is_none_or(|at| now - at >= CEILING) && peer.take_back();
            let opened = peer.take_opened();
            let groups: Vec<Arc<Group>> = if whole {
                walk.last = Some(now);
                #[cfg(test)]
                reach.walks.fetch_add(1, Ordering::Relaxed);
                registry
                    .ticked()
                    .iter()
                    .filter(|group| !group.refused_on(broker))
                    .cloned()
                    .collect()
            } else {
                opened
                    .iter()
                    .filter_map(|id| registry.get(&id.shard).filter(|group| group.id == *id))
                    .collect()
            };
            if groups.is_empty() {
                continue;
            }
            walk.busy.store(true, Ordering::Release);
            let busy = Arc::clone(&walk.busy);
            let broker = Arc::clone(broker);
            tokio::spawn(async move {
                futures::stream::iter(groups)
                    .for_each_concurrent(RESETS_AT_ONCE, |group| {
                        let broker = Arc::clone(&broker);
                        async move { group.retry_now(&broker).await }
                    })
                    .await;
                busy.store(false, Ordering::Release);
            });
        }
        reach.forget_idle();
    }
}

/// When the ticker next has to read each group's due time.
///
/// Reading it means touching the group, and with thousands of groups that
/// is most of what an interval costs. Anything other than a timer check
/// that moves a group's due time moves it at least a heartbeat ahead of the
/// moment it runs (`Pace::wrote`, `Pace::followed`), so a group read now
/// and found not due cannot become due sooner than that.
#[derive(Default)]
struct Looks {
    groups: Arc<Vec<Arc<Group>>>,
    /// By position in `groups`, on the runtime's clock.
    at: Vec<u64>,
}

impl Looks {
    /// The groups whose timer check is due now.
    fn due(&mut self, groups: Arc<Vec<Arc<Group>>>, timers: &Timers) -> Vec<Arc<Group>> {
        if !Arc::ptr_eq(&self.groups, &groups) {
            // A group was opened or closed, and positions moved.
            self.at.clear();
            self.at.resize(groups.len(), 0);
            self.groups = groups;
        }
        let now = timers.now();
        let mut due = Vec::new();
        for (group, look) in self.groups.iter().zip(&mut self.at) {
            if now < *look {
                continue;
            }
            let at = group.due_at();
            if now >= at {
                // The check sets a new due time: read it next interval.
                due.push(Arc::clone(group));
            } else {
                *look = at.min(now + timers.heartbeat);
            }
        }
        due
    }
}

/// While a broker is down, one group that leads here heartbeats to it each
/// short gap. Idle groups heartbeat it only every idle gap, and the walk
/// that retries them all waits for the broker's first answer, so this keeps
/// that answer within about a heartbeat of the broker coming back. It costs
/// one group's heartbeat per dead broker, whatever the number of groups.
fn scout(registry: &Registry, peer: &Peer, walk: &mut Walk, now: Instant, heartbeat: Duration) {
    if !peer.is_down()
        || walk.scouting.load(Ordering::Acquire)
        || walk.scouted.is_some_and(|at| now - at < heartbeat)
    {
        return;
    }
    walk.scouted = Some(now);
    let Some(group) = peer
        .scout()
        .and_then(|id| registry.get(&id.shard).filter(|group| group.id == id))
    else {
        return;
    };
    walk.scouting.store(true, Ordering::Release);
    let scouting = Arc::clone(&walk.scouting);
    tokio::spawn(async move {
        group.heartbeat_now().await;
        scouting.store(false, Ordering::Release);
    });
}

/// One timer check on each of `groups`, until `deadline`.
///
/// The checks are polled together in this task. A check waits while its
/// group's queue is full, so asking the groups in turn would let one busy
/// group hold up the rest. A check still waiting at the deadline is dropped,
/// never queued behind the next interval's: openraft reads the clock when it
/// runs a check, so the next one does the same work.
async fn tick_all(groups: &[Arc<Group>], deadline: Instant) {
    if groups.is_empty() {
        return;
    }
    let mut waiting: FuturesUnordered<_> = groups.iter().map(|group| group.tick()).collect();
    let all = async { while waiting.next().await.is_some() {} };
    let _ = tokio::time::timeout_at(deadline, all).await;
}

/// What the election bound needs from a member. A trait so its rules can be
/// tested without openraft.
pub(super) trait Campaigner {
    /// Whether it may still stand, having heard no leader and granted no
    /// vote since `since`.
    fn may_stand(&self, since: u64) -> bool;
    /// Turn its election timer on, if it may still stand.
    fn arm(&self, since: u64) -> bool;
    /// Turn its election timer off.
    fn disarm(&self);
    /// Whether the election it was let into is over.
    fn settled(&self) -> bool;
    /// Vote and pre-vote requests sent so far, and how many are unanswered.
    fn asks(&self) -> (u64, u64);
    /// A candidate whose real vote requests have not gone out yet: its
    /// pre-vote round is answered and it is saving its own vote.
    fn saving_its_vote(&self) -> bool;
    /// Its group took writes when it last heard its leader.
    fn urgent(&self) -> bool;
    fn set_waiting(&self, waiting: bool);
    /// Its place among the members that came due together.
    fn order(&self) -> u64;
}

impl Campaigner for Group {
    fn may_stand(&self, since: u64) -> bool {
        Group::may_stand(self, since)
    }
    fn arm(&self, since: u64) -> bool {
        Group::arm(self, since)
    }
    fn disarm(&self) {
        self.hold_elections();
    }
    fn settled(&self) -> bool {
        Group::settled(self)
    }
    fn asks(&self) -> (u64, u64) {
        Group::asks(self)
    }
    fn saving_its_vote(&self) -> bool {
        Group::saving_its_vote(self)
    }
    fn urgent(&self) -> bool {
        Group::urgent(self)
    }
    fn set_waiting(&self, waiting: bool) {
        Group::set_waiting(self, waiting);
    }
    fn order(&self) -> u64 {
        Group::order(self)
    }
}

/// The lowest the limit falls to, unless the configured bound is lower.
/// Below this a broker that lost hundreds of leaders would take minutes to
/// elect them all.
const FLOOR: usize = 8;

/// A vote save this many times slower than one that had the disk to itself
/// is slow.
const SLOWER: u64 = 4;

/// The bound on elections at once on this broker.
///
/// A member whose wait for its leader ran out gets its election timer
/// turned on only while fewer than `limit` members here have theirs on;
/// the rest wait. Members of groups that took writes go ahead of the
/// others the first time they queue; a member that lost goes to the back.
/// openraft still decides when a member stands and whether it wins. A
/// place is given back when the member leads, hears a leader or grants a
/// vote, or lost: every request of its round answered without it leading
/// (seen on two looks, and not while it saves its own vote), no stand
/// within `election_max`, or no result within `election_min` of standing.
/// One that lost has its timer turned off and waits again at the back.
///
/// `limit` starts at [`super::RuntimeConfig::max_elections_at_once`] and
/// follows this broker's vote saves while its elections are losing (at
/// least as many lost as won lately). Then a save over [`SLOWER`] times
/// what one takes with the disk to itself cuts it to what the disk would
/// have saved in time, never below [`FLOOR`]. Slow saves while elections
/// win are a slow disk, not one the elections overload, and cut nothing.
/// An interval with no cut gives back a place for each election that won
/// in it, and at least one.
pub(super) struct Elections<M: Campaigner = Group> {
    most: usize,
    floor: usize,
    limit: usize,
    urgent: VecDeque<Waiting<M>>,
    queue: VecDeque<Waiting<M>>,
    held: Vec<Held<M>>,
    /// Queued or held, by address.
    known: HashSet<usize>,
    /// Before any save had the disk to itself, a save slower than this (in
    /// microseconds) is slow: a quarter of `election_min`.
    slow_at_first: u64,
    /// No save faster than this (microseconds) is slow, whatever the
    /// baseline: a twentieth of `election_min`.
    never_slow: u64,
    /// The longest an armed member keeps its place before it stands.
    stand: u64,
    /// The longest a member that stood keeps its place.
    round: u64,
    /// No member is let in while the saves queued here would take longer
    /// than this to drain (microseconds): a quarter of `election_min`.
    backlog: u64,
    /// When `limit` last grew while elections were losing.
    grown: u64,
    saves: Arc<VoteSaves>,
    lowered: Lowered,
    outcomes: Outcomes,
    peak: usize,
    /// Totals since the ticker started, for `ElectionLoad`.
    totals: (u64, u64, u64),
    reported: ElectionLoad,
}

struct Waiting<M> {
    member: Arc<M>,
    since: u64,
}

struct Held<M> {
    member: Arc<M>,
    since: u64,
    /// Its requests sent before it was let in.
    asks: u64,
    stood: Option<u64>,
    /// Every request of its round was answered when last looked at.
    answered: bool,
}

/// Elections here that won and lost lately, halved every `election_max`.
#[derive(Default)]
struct Outcomes {
    won: u64,
    lost: u64,
    since: u64,
}

impl Outcomes {
    fn add(&mut self, now: u64, window: u64, won: u64, lost: u64) {
        if now >= self.since + window {
            self.won /= 2;
            self.lost /= 2;
            self.since = now;
        }
        self.won += won;
        self.lost += lost;
    }

    fn losing(&self) -> bool {
        self.lost > 0 && self.lost >= self.won
    }
}

/// The last time `limit` was lowered.
#[derive(Default)]
struct Lowered {
    from: usize,
    took: u64,
}

/// How long this broker's vote saves take, waits for the disk included. In
/// microseconds, on the runtime's clock in milliseconds for `since`.
#[derive(Default)]
pub(crate) struct VoteSaves {
    /// When `limit` was last lowered.
    since: AtomicU64,
    /// The slowest save that began after `since`, under the limit as it is.
    slowest: AtomicU64,
    /// The slowest that began before it, under the limit as it was.
    slowest_before: AtomicU64,
    /// Saves under way here, and saves begun, to tell one that had the
    /// disk to itself.
    saving: AtomicU64,
    begun: AtomicU64,
    /// A moving average of the saves that had the disk to themselves, or 0
    /// before the first.
    alone: AtomicU64,
    /// A moving average of the microseconds between two saves ending while
    /// others were still under way: what the disk gets through, whatever
    /// its width. 0 before the first.
    gap: AtomicU64,
    /// When the last save ended, in microseconds on the same clock.
    last_end: AtomicU64,
}

/// The ticket of a save that began while another was under way.
const NOT_ALONE: u64 = u64::MAX;

impl VoteSaves {
    fn began(&self) -> u64 {
        let others = self.saving.fetch_add(1, Ordering::Relaxed);
        let id = self.begun.fetch_add(1, Ordering::Relaxed);
        if others == 0 { id } else { NOT_ALONE }
    }

    /// How long the saves under way and `more` yet to come take to drain,
    /// in microseconds. 0 before the disk's pace is known.
    fn drain(&self, more: u64) -> u64 {
        let pending = self.saving.load(Ordering::Relaxed) + more;
        pending.saturating_mul(self.gap.load(Ordering::Relaxed))
    }

    /// The save with `ticket` ended at `now` ms, `at` microseconds on the
    /// runtime's clock; it took `took` microseconds.
    fn ended(&self, ticket: u64, now: u64, at: u64, took: Option<u64>) {
        let others = self.saving.fetch_sub(1, Ordering::Relaxed) - 1;
        let last = self.last_end.swap(at, Ordering::Relaxed);
        if others > 0 && last > 0 {
            let gap = at.saturating_sub(last).max(1);
            let average = match self.gap.load(Ordering::Relaxed) {
                0 => gap,
                average => ((average * 7 + gap) / 8).max(1),
            };
            self.gap.store(average, Ordering::Relaxed);
        }
        let Some(took) = took else {
            return;
        };
        let took = took.max(1);
        // Nothing began beside it either.
        if ticket != NOT_ALONE && self.begun.load(Ordering::Relaxed) == ticket + 1 {
            let alone = self.alone.load(Ordering::Relaxed);
            let alone = match alone {
                0 => took,
                alone => (alone * 7 + took) / 8,
            };
            self.alone.store(alone.max(1), Ordering::Relaxed);
        }
        let began = now.saturating_sub(took / 1000);
        let slowest = match began >= self.since.load(Ordering::Relaxed) {
            true => &self.slowest,
            false => &self.slowest_before,
        };
        slowest.fetch_max(took, Ordering::Relaxed);
    }
}

/// [`VoteSaves`] as one member's log tells it.
pub(crate) struct VoteClock {
    pub(crate) saves: Arc<VoteSaves>,
    pub(crate) timers: Timers,
    /// This member's saves under way.
    pub(crate) own: Arc<AtomicU64>,
}

impl felix_storage::raft_log::VoteTimer for VoteClock {
    fn began(&self) -> u64 {
        self.own.fetch_add(1, Ordering::Relaxed);
        self.saves.began()
    }
    fn ended(&self, ticket: u64, took: Option<Duration>) {
        self.own.fetch_sub(1, Ordering::Relaxed);
        let took = took.map(|took| took.as_micros() as u64);
        let at = self.timers.started.elapsed().as_micros() as u64;
        self.saves.ended(ticket, self.timers.now(), at.max(1), took);
    }
}

fn address<M>(member: &Arc<M>) -> usize {
    Arc::as_ptr(member) as *const () as usize
}

impl<M: Campaigner> Elections<M> {
    fn new(timers: &Timers, saves: Arc<VoteSaves>) -> Self {
        let most = timers.max_elections_at_once;
        Self {
            most,
            floor: most.min(FLOOR.max(most / 8)),
            limit: most,
            urgent: VecDeque::new(),
            queue: VecDeque::new(),
            held: Vec::new(),
            known: HashSet::new(),
            slow_at_first: timers.election_min * 1000 / 4,
            never_slow: timers.election_min * 1000 / 20,
            stand: timers.election_max,
            round: timers.election_min,
            backlog: timers.election_min * 1000 / 4,
            grown: 0,
            saves,
            lowered: Lowered::default(),
            outcomes: Outcomes::default(),
            peak: 0,
            totals: (0, 0, 0),
            reported: ElectionLoad::default(),
        }
    }

    /// Queue the members in `wanting` that are not queued or held yet:
    /// urgent ones ahead of the rest, each in their order.
    fn queue(&mut self, wanting: impl Iterator<Item = Arc<M>>, now: u64) {
        let mut new: Vec<(bool, Arc<M>)> = wanting
            .filter(|member| !self.known.contains(&address(member)))
            .map(|member| (member.urgent(), member))
            .collect();
        new.sort_by_key(|(_, member)| member.order());
        for (urgent, member) in new {
            self.known.insert(address(&member));
            member.set_waiting(true);
            let waiting = Waiting { member, since: now };
            match urgent {
                true => self.urgent.push_back(waiting),
                false => self.queue.push_back(waiting),
            }
        }
    }

    /// Let members in from the front of the queues while places are free.
    fn admit(&mut self, now: u64) {
        while self.held.len() < self.limit {
            // Rounds that gave up leave their saves queued, so places alone
            // do not bound the disk's queue. Each member let in is about to
            // add one.
            let queued = self.saves.drain(self.held.len() as u64);
            if !self.held.is_empty() && queued > self.backlog {
                break;
            }
            let Some(Waiting { member, since }) =
                self.urgent.pop_front().or_else(|| self.queue.pop_front())
            else {
                break;
            };
            member.set_waiting(false);
            if !member.arm(since) {
                self.known.remove(&address(&member));
                continue;
            }
            let asks = member.asks().0;
            self.held.push(Held {
                member,
                since: now,
                asks,
                stood: None,
                answered: false,
            });
        }
        self.peak = self.peak.max(self.held.len());
    }

    /// Give back the places of elections that are over, drop waiting
    /// members that heard a leader, and move `limit`.
    fn settle(&mut self, now: u64) {
        let mut lost = Vec::new();
        let mut won = 0;
        let (stand, round) = (self.stand, self.round);
        let known = &mut self.known;
        self.held.retain_mut(|held| {
            if held.member.settled() {
                known.remove(&address(&held.member));
                won += 1;
                return false;
            }
            let (asks, out) = held.member.asks();
            if held.stood.is_none() && asks > held.asks {
                held.stood = Some(now);
            }
            // A candidate saving its vote has nothing out, but its vote
            // requests go out once the save ends: its round is not over.
            let answered = out == 0 && held.stood.is_some() && !held.member.saving_its_vote();
            let over = match held.stood {
                None => now >= held.since + stand,
                Some(stood) => (held.answered && answered) || now >= stood + round,
            };
            held.answered = answered;
            if over {
                // One that never stood tells nothing about the disk.
                lost.push((Arc::clone(&held.member), held.stood.is_some()));
            }
            !over
        });
        let rounds = lost.iter().filter(|(_, stood)| *stood).count() as u64;
        self.outcomes.add(now, self.stand, won as u64, rounds);
        self.totals.0 += won as u64;
        self.totals.1 += rounds;
        for (member, _) in lost {
            // Its round may still be won: openraft takes the answers
            // either way. It just does not start another before its turn.
            member.disarm();
            member.set_waiting(true);
            self.queue.push_back(Waiting { member, since: now });
        }
        for queue in [&mut self.urgent, &mut self.queue] {
            queue.retain(|waiting| {
                let keep = waiting.member.may_stand(waiting.since);
                if !keep {
                    waiting.member.set_waiting(false);
                    known.remove(&address(&waiting.member));
                }
                keep
            });
        }
        self.follow_the_disk(now, won);
    }

    /// Cut `limit` on a slow vote save while elections lose, or give
    /// places back.
    fn follow_the_disk(&mut self, now: u64, won: usize) {
        let slow = match self.saves.alone.load(Ordering::Relaxed) {
            0 => self.slow_at_first,
            alone => (alone * SLOWER).max(self.never_slow),
        };
        let floor = self.floor;
        // To what the disk would have saved inside `slow`.
        let fits = |from: usize, took: u64| ((from as u64 * slow / took) as usize).max(floor);
        let slowest = self.saves.slowest.swap(0, Ordering::Relaxed);
        let before = self.saves.slowest_before.swap(0, Ordering::Relaxed);
        let losing = self.outcomes.losing();
        let was = self.limit;
        if losing && slowest > slow {
            self.lowered = Lowered {
                from: self.limit,
                took: slowest,
            };
            self.limit = self.limit.min(fits(self.limit, slowest));
            self.saves.since.store(now, Ordering::Relaxed);
        } else if losing && before > slow {
            // Queued under the limit as it was. Slower than the save it was
            // lowered for, that limit was further off than it looked.
            if before > self.lowered.took {
                self.lowered.took = before;
                self.limit = self.limit.min(fits(self.lowered.from, before));
            }
        } else {
            self.grow(now, won, losing);
        }
        self.totals.2 += u64::from(self.limit < was);
    }

    /// Give places back: one for each election that won, and at least
    /// one a tick. While elections lose, one for each `election_min`.
    fn grow(&mut self, now: u64, won: usize, losing: bool) {
        if !losing {
            self.limit = (self.limit + won.max(1)).min(self.most);
        } else if now >= self.grown + self.round {
            self.grown = now;
            self.limit = (self.limit + 1).min(self.most);
        }
    }

    fn load(&self) -> ElectionLoad {
        ElectionLoad {
            in_flight: self.held.len(),
            waiting: self.urgent.len() + self.queue.len(),
            limit: self.limit,
            peak: self.peak,
            won: self.totals.0,
            lost: self.totals.1,
            cuts: self.totals.2,
            saving: self.saves.saving.load(Ordering::Relaxed),
        }
    }

    fn report(&mut self, registry: &Registry) {
        let load = self.load();
        if load != self.reported {
            metrics::gauge!(ELECTIONS_IN_FLIGHT).set(load.in_flight as f64);
            metrics::gauge!(ELECTIONS_WAITING).set(load.waiting as f64);
            registry.set_election_load(load);
            self.reported = load;
        }
    }
}

#[cfg(test)]
mod tests;

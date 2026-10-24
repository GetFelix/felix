//! The running driver's shards: each passes on its own, and an exchange with a
//! slow follower outlives the pass that started it.
//!
//! A pass that waited for every shard, and a shard's pass that waited for
//! every follower, put the slowest peer on the broker in front of every
//! quorum mark it publishes. One paused destination, not even counted toward
//! any quorum, held every `Quorum` publish for a dial timeout. So a shard
//! starts its next pass as soon as its last one ends, whatever other shards
//! are doing, and a pass that has published its mark waits on the followers
//! still answering only until the shard's next pass is wanted. Then it hands
//! their exchanges back, and the driver waits for them instead.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use felix_broker::Broker;
use felix_router::{ShardKey, ShardRouter};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;

use super::shard::{
    AuxCursors, Exchange, RideAlong, ShardCursors, ShardPass, Shipped, Stragglers, replicate_shard,
};
use super::{
    DRAIN_RETRY, FENCE_RETRY, SHARD_CONCURRENCY, WriteFence, alone_report, fence_backoff,
    fence_one, open_fenced, take_work, watch_key,
};
use crate::halted::HaltedReplica;
use crate::peer::PeerRequester;
use crate::promotion::Outcome;
use crate::promotion::PromotionGate;
use crate::quorum::QuorumMarks;
use crate::reporter::Reporter;
use crate::status::{FenceStatus, ShardStatus, ShardStatusBoard, follower_status};
use crate::{FollowerCursor, MoveThrottle, Rebuilds};

/// Everything a pass borrows from the driver task, for as long as it runs.
pub(super) struct Context<'a, R> {
    pub(super) requester: &'a R,
    pub(super) broker: &'a Arc<Broker>,
    pub(super) router: &'a ShardRouter,
    pub(super) fence: &'a dyn WriteFence,
    pub(super) gate: &'a dyn PromotionGate,
    pub(super) marks: &'a QuorumMarks,
    pub(super) status: &'a ShardStatusBoard,
    pub(super) reporter: Option<&'a Reporter>,
    pub(super) rebuilds: &'a Rebuilds,
    pub(super) throttle: &'a MoveThrottle,
    /// The tick: how often a shard led here alone repeats its report, which
    /// the control plane believes only for a while.
    pub(super) refresh: std::time::Duration,
}

impl<R> Clone for Context<'_, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R> Copy for Context<'_, R> {}

/// What woke the driver.
pub(super) enum Event<'a> {
    /// An append, a route change, or the tick: every shard gets a pass.
    Wake,
    /// Shards fenced here and not yet drained, and shards promoted here and
    /// not yet opened, get another pass.
    Retry,
    /// A shard's pass ended; the scan it started under.
    Passed(u64, Box<ShardPass<'a>>),
    /// A promoted shard's fence attempt ended.
    Fenced(FenceAttempt),
    /// An exchange a pass handed back has ended.
    Answered(Answered),
    /// A report for a shard led here alone was read and queued.
    Reported(Reported),
}

/// A shard led here alone, reported at `generation` with `tail`.
pub(super) struct Reported {
    key: ShardKey,
    generation: u64,
    incarnation: u64,
    tail: u64,
}

/// The last report sent for a shard led here alone. The tail is a floor only
/// for the same log, so it is kept per incarnation too: a stream recreated at
/// the deleted one's generation starts a new log.
struct Alone {
    generation: u64,
    incarnation: u64,
    tail: u64,
    at: tokio::time::Instant,
}

/// One fence attempt for a shard promoted here, ended.
pub(super) struct FenceAttempt {
    key: ShardKey,
    generation: u64,
    /// The shard is open now.
    opened: bool,
    /// The other replicas in the route the attempt fenced.
    replicas: Vec<String>,
    /// Who took it, when the attempt stopped short. `None` once a majority
    /// did: the attempt stops listening then.
    took: Option<Vec<String>>,
    why: Option<String>,
}

/// An exchange handed back by a pass, ended.
pub(super) struct Answered {
    key: ShardKey,
    generation: u64,
    incarnation: u64,
    cursor: FollowerCursor,
    /// Boxed: three more cursors make every event as large as this one.
    rode: Box<RideAlong>,
    cut: bool,
    /// Every answer came from the copy the cursor describes.
    checked: bool,
}

/// What the driver hands a shard's pass under way: that the next pass is
/// wanted, and the answers of exchanges an earlier pass left behind.
#[derive(Default)]
pub(super) struct NextPass {
    pub(super) wanted: tokio::sync::Notify,
    answered: parking_lot::Mutex<Vec<Answered>>,
}

impl std::fmt::Debug for NextPass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextPass")
            .field("answered", &self.answered.lock().len())
            .finish_non_exhaustive()
    }
}

impl NextPass {
    /// The answers measured at `generation` and `incarnation`, for the pass
    /// to count. Any others stay for the driver to merge.
    pub(super) fn take(&self, generation: u64, incarnation: u64) -> Vec<Shipped> {
        let mut answered = self.answered.lock();
        let (taken, kept) = std::mem::take(&mut *answered)
            .into_iter()
            .partition(|answer| {
                answer.generation == generation && answer.incarnation == incarnation
            });
        *answered = kept;
        taken
            .into_iter()
            .map(|answer: Answered| Shipped {
                cursor: answer.cursor,
                rode: *answer.rode,
                cut: answer.cut,
                checked: answer.checked,
            })
            .collect()
    }
}

/// The driver's view of one shard it leads.
#[derive(Default)]
struct ShardState {
    /// A pass is under way, holding the shard's cursors; told when the next
    /// one is wanted, so it stops waiting on followers that have not answered.
    running: Option<Arc<NextPass>>,
    /// Asked for a pass while one was under way, or while too many were.
    again: bool,
    /// Exchanges that ended while a pass held the cursors.
    held: Vec<Answered>,
    /// Promoted here, and a majority has not taken the fence yet.
    fence_pending: bool,
    /// No fence before this. Every append wakes a scan, so without it a shard
    /// the broker keeps closed after its fence is fenced again at the append
    /// rate of every other shard.
    fence_after: Option<tokio::time::Instant>,
    /// Fence attempts in a row that left the shard closed, and the generation
    /// they were at: a new promotion starts from [`FENCE_RETRY`] again.
    fence_failures: (u64, u32),
    /// What its last pass left.
    last: Option<LastPass>,
}

/// What a shard's last pass left, for the driver's summary.
struct LastPass {
    scan: u64,
    halted: Vec<HaltedReplica>,
    lag: Option<u64>,
    behind: bool,
    drain_pending: bool,
}

impl LastPass {
    /// A shard with nobody to ship to: nothing to wait for.
    fn idle(scan: u64) -> Self {
        Self {
            scan,
            halted: Vec::new(),
            lag: None,
            behind: false,
            drain_pending: false,
        }
    }
}

type PassFuture<'a> = BoxFuture<'a, (u64, ShardPass<'a>)>;

pub(super) struct Shards<'a, R> {
    cx: Context<'a, R>,
    pub(super) scan: u64,
    streams: HashMap<ShardKey, ShardCursors>,
    group: HashMap<ShardKey, ShardCursors>,
    dead_letters: HashMap<ShardKey, ShardCursors>,
    counters: HashMap<ShardKey, ShardCursors>,
    states: HashMap<ShardKey, ShardState>,
    /// Followers with an exchange still under way, per shard, and whether
    /// each may hold a rebuild slot. Kept apart from the shard's state: a
    /// shard lost and led again must not ship to a follower that an exchange
    /// from its earlier leadership is still talking to.
    busy: HashMap<ShardKey, HashMap<String, bool>>,
    pub(super) passes: FuturesUnordered<PassFuture<'a>>,
    /// Fence attempts for shards promoted here, which ship nothing until
    /// one succeeds.
    pub(super) fences: FuturesUnordered<BoxFuture<'a, FenceAttempt>>,
    pub(super) exchanges: FuturesUnordered<BoxFuture<'a, Answered>>,
    /// Shards led here with no other copy, by when each last reported.
    alone: HashMap<ShardKey, Alone>,
    /// When each shard this broker gave up was last probed.
    probed: HashMap<ShardKey, tokio::time::Instant>,
    pub(super) reports: FuturesUnordered<BoxFuture<'a, Reported>>,
}

impl<'a, R: PeerRequester + Send + Sync> Shards<'a, R> {
    pub(super) fn new(cx: Context<'a, R>) -> Self {
        Self {
            cx,
            scan: 0,
            streams: HashMap::new(),
            group: HashMap::new(),
            dead_letters: HashMap::new(),
            counters: HashMap::new(),
            states: HashMap::new(),
            busy: HashMap::new(),
            passes: FuturesUnordered::new(),
            fences: FuturesUnordered::new(),
            exchanges: FuturesUnordered::new(),
            alone: HashMap::new(),
            probed: HashMap::new(),
            reports: FuturesUnordered::new(),
        }
    }

    /// Whether `route` names this broker as the leader, and it leads.
    fn leads(&self, key: &ShardKey, route: &felix_router::Route) -> bool {
        route.leader.node_id == self.cx.router.local_node_id()
            && !self.cx.fence.withheld(&watch_key(key), route.generation)
    }

    /// Give every shard led here a pass, and forget the ones that are not.
    pub(super) fn scan(&mut self) {
        self.scan += 1;
        let table = self.cx.router.snapshot();
        let live: Vec<ShardKey> = table
            .iter()
            .filter(|(key, route)| self.leads(key, route))
            .map(|(key, _)| key.clone())
            .collect();
        let given_up: Vec<(ShardKey, felix_router::Route)> = table
            .iter()
            .filter(|(key, route)| {
                route.leader.node_id == self.cx.router.local_node_id()
                    && self.cx.fence.gave_up_at(&watch_key(key)) == Some(route.generation)
            })
            .map(|(key, route)| (key.clone(), route.clone()))
            .collect();
        self.probed
            .retain(|key, _| given_up.iter().any(|(given, _)| given == key));
        for (key, route) in given_up {
            self.probe(key, route);
        }
        for key in &live {
            self.start(key);
        }

        // A shard this broker no longer leads keeps no cursors: they would be
        // a belief about a follower under a leadership that has ended. One
        // with a pass under way is dropped when the pass ends.
        let led: HashSet<&ShardKey> = live.iter().collect();
        self.states
            .retain(|key, state| state.running.is_some() || led.contains(key));
        self.alone.retain(|key, _| led.contains(key));
        for map in [
            &mut self.streams,
            &mut self.group,
            &mut self.dead_letters,
            &mut self.counters,
        ] {
            map.retain(|key, _| led.contains(key));
        }
        // A shard this broker no longer leads stops promising a quorum.
        // Dropping the mark ends any publish still waiting on it, rather than
        // leaving it to run out its timeout for an answer that can no longer
        // come.
        let led_keys: Vec<crate::ShardKey> = live.iter().map(watch_key).collect();
        self.cx.marks.retain(&led_keys);
        let led_keys: HashSet<&crate::ShardKey> = led_keys.iter().collect();
        self.cx.status.retain(|key| led_keys.contains(key));

        // Slots in use are whatever the cursors still say is rebuilding. A
        // cursor discarded on a generation change or a lost shard took its
        // slot with it, and nothing else would give it back. Only counted
        // with no pass holding cursors; an exchange still under way counts
        // if its follower went in halted or rebuilding.
        if self.passes.is_empty() {
            let at_rest = [
                &self.streams,
                &self.group,
                &self.dead_letters,
                &self.counters,
            ]
            .iter()
            .flat_map(|map| map.values())
            .flat_map(|entry| entry.followers.iter())
            .filter(|cursor| cursor.rebuilding)
            .count();
            let under_way = self
                .busy
                .values()
                .flat_map(|nodes| nodes.values())
                .filter(|may_rebuild| **may_rebuild)
                .count();
            self.cx.rebuilds.set_in_flight(at_rest + under_way);
        }
    }

    /// How soon to retry the shards waiting on something other than a wake:
    /// a drained report the move is held on, or a promotion's fence.
    pub(super) fn retry_in(&self) -> Option<std::time::Duration> {
        let idle = || self.states.values().filter(|state| state.running.is_none());
        if idle().any(|state| state.last.as_ref().is_some_and(|last| last.drain_pending)) {
            Some(DRAIN_RETRY)
        } else if idle().any(|state| state.fence_pending) {
            // The soonest a fence is due. A shard with none set has its
            // fence still to start, so it goes at the usual spacing.
            let now = tokio::time::Instant::now();
            idle()
                .filter(|state| state.fence_pending)
                .map(|state| {
                    state
                        .fence_after
                        .map_or(FENCE_RETRY, |after| after.saturating_duration_since(now))
                })
                .min()
        } else {
            None
        }
    }

    /// Another pass for every shard [`Self::retry_in`] was waiting on.
    pub(super) fn retry(&mut self) {
        let waiting: Vec<ShardKey> = self
            .states
            .iter()
            .filter(|(_, state)| {
                state.running.is_none()
                    && (state.fence_pending
                        || state.last.as_ref().is_some_and(|last| last.drain_pending))
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in waiting {
            self.start(&key);
        }
    }

    /// Start a pass for `key`, or ask for one once the pass under way ends.
    fn start(&mut self, key: &ShardKey) {
        let table = self.cx.router.snapshot();
        let Some(route) = table.get(key).filter(|route| self.leads(key, route)) else {
            return;
        };
        let state = self.states.entry(key.clone()).or_default();
        if let Some(next) = &state.running {
            state.again = true;
            next.wanted.notify_one();
            return;
        }
        if self.passes.len() + self.fences.len() >= SHARD_CONCURRENCY {
            state.again = true;
            return;
        }
        state.again = false;
        // Promoted and not yet fenced: nothing ships until it is, because it
        // may yet take a tail from a follower that shipping would truncate,
        // and no mark moves for it either.
        if self.cx.gate.awaiting(&watch_key(key)) == Some(route.generation) {
            if state
                .fence_after
                .is_some_and(|after| tokio::time::Instant::now() < after)
            {
                return;
            }
            state.running = Some(Arc::default());
            let cx = self.cx;
            let (key, route) = (key.clone(), route.clone());
            let local = cx.router.local_node_id();
            let replicas: Vec<String> = route
                .replicas
                .iter()
                .filter(|replica| replica.node_id != local)
                .map(|replica| replica.node_id.clone())
                .collect();
            self.fences.push(Box::pin(async move {
                let (key, generation, outcome) =
                    fence_one(cx.requester, cx.broker, local, key, route, cx.marks).await;
                let (took, why) = match &outcome {
                    Outcome::Pending { why, took } => (Some(took.clone()), Some(why.clone())),
                    Outcome::Fenced { .. } | Outcome::Lease { .. } => (None, None),
                };
                let opened = open_fenced(cx.gate, &key, generation, outcome).await;
                let why = why.or_else(|| {
                    (!opened).then(|| "the fence was taken but the shard stayed closed".into())
                });
                FenceAttempt {
                    key,
                    generation,
                    opened,
                    replicas,
                    took,
                    why,
                }
            }));
            return;
        }
        let Some((entry, aux)) = take_work(
            key,
            route,
            &mut self.streams,
            &mut self.group,
            &mut self.dead_letters,
            &mut self.counters,
        ) else {
            state.last = Some(LastPass::idle(self.scan));
            self.report_alone(key, route.generation);
            self.cx.status.put(
                watch_key(key),
                ShardStatus {
                    generation: route.generation,
                    tail: None,
                    followers: Vec::new(),
                    fence: None,
                    drain_pending: false,
                    behind: false,
                },
            );
            return;
        };
        self.alone.remove(key);
        let next = Arc::new(NextPass::default());
        let state = self.states.entry(key.clone()).or_default();
        state.running = Some(Arc::clone(&next));
        let busy: HashSet<String> = self
            .busy
            .get(key)
            .map(|nodes| nodes.keys().cloned().collect())
            .unwrap_or_default();
        self.passes
            .push(self.pass(key.clone(), route.clone(), entry, aux, busy, next));
    }

    /// Report a shard led here alone at a new generation, and again each tick
    /// so the report stays fresh. Every append wakes a scan, so not on each.
    fn report_alone(&mut self, key: &ShardKey, generation: u64) {
        let Some(reporter) = self.cx.reporter else {
            return;
        };
        let now = tokio::time::Instant::now();
        let incarnation = crate::incarnation::of(self.cx.broker, key);
        let floor = match self.alone.get(key) {
            Some(last) if last.generation == generation && last.incarnation == incarnation => {
                // Half, so a tick landing a moment early does not skip one.
                if now.duration_since(last.at) < self.cx.refresh / 2 {
                    return;
                }
                last.tail
            }
            _ => 0,
        };
        self.alone.insert(
            key.clone(),
            Alone {
                generation,
                incarnation,
                tail: floor,
                at: now,
            },
        );
        let broker = self.cx.broker;
        let key = key.clone();
        self.reports.push(Box::pin(async move {
            let report = alone_report(broker, &key, generation, floor)
                .await
                .at_incarnation(incarnation);
            let tail = report.tail;
            reporter.submit(report);
            Reported {
                key,
                generation,
                incarnation,
                tail,
            }
        }));
    }

    /// Probe the followers of a shard this broker gave up, each tick, and
    /// report what they hold. See `crate::reclaim`.
    fn probe(&mut self, key: ShardKey, route: felix_router::Route) {
        let Some(reporter) = self.cx.reporter else {
            return;
        };
        let now = tokio::time::Instant::now();
        if self
            .probed
            .get(&key)
            .is_some_and(|at| now.duration_since(*at) < self.cx.refresh / 2)
        {
            return;
        }
        self.probed.insert(key.clone(), now);
        let cx = self.cx;
        self.reports.push(Box::pin(async move {
            let local = cx.router.local_node_id();
            if let Some(probe) =
                crate::reclaim::probe(cx.requester, cx.broker, local, &key, &route).await
            {
                cx.fence
                    .probed(&watch_key(&key), route.generation, probe.reclaimable);
                reporter.submit(probe.report);
            }
            Reported {
                key,
                generation: route.generation,
                incarnation: 0,
                tail: 0,
            }
        }));
    }

    /// Keep a shard's reported tail, so the next report does not go back.
    pub(super) fn reported(&mut self, reported: Reported) {
        if let Some(last) = self.alone.get_mut(&reported.key)
            && last.generation == reported.generation
            && last.incarnation == reported.incarnation
        {
            last.tail = last.tail.max(reported.tail);
        }
    }

    fn pass(
        &self,
        key: ShardKey,
        route: felix_router::Route,
        entry: ShardCursors,
        aux: AuxCursors,
        busy: HashSet<String>,
        next: Arc<NextPass>,
    ) -> PassFuture<'a> {
        let cx = self.cx;
        let scan = self.scan;
        Box::pin(async move {
            let pass = replicate_shard(
                cx.requester,
                cx.broker,
                cx.fence,
                cx.marks,
                cx.reporter,
                cx.rebuilds,
                cx.throttle,
                key,
                route,
                entry,
                aux,
                &busy,
                Stragglers::HandOff { next },
            )
            .await;
            (scan, pass)
        })
    }

    /// Take back a finished pass's cursors, and hold on to the exchanges it
    /// left under way.
    pub(super) fn fold(&mut self, scan: u64, pass: ShardPass<'a>) {
        let key = pass.key.clone();
        let led = self
            .cx
            .router
            .snapshot()
            .get(&key)
            .is_some_and(|route| self.leads(&key, route));
        if !self.states.contains_key(&key) {
            return;
        }
        if !led {
            // Lost while the pass ran. Its exchanges go with it.
            self.states.remove(&key);
            return;
        }
        let ShardPass {
            generation,
            incarnation,
            exchanges,
            straggling,
            cursors,
            aux,
            halted,
            lag,
            tail,
            copying,
            again,
            drain_pending,
            behind,
            ..
        } = pass;
        let followers = cursors
            .followers
            .iter()
            .map(|cursor| {
                let learner = cursors.learner.as_deref() == Some(cursor.node_id.as_str());
                follower_status(cursor, tail, learner)
            })
            .collect();
        self.cx.status.put(
            watch_key(&key),
            ShardStatus {
                generation,
                tail,
                followers,
                fence: None,
                drain_pending,
                behind,
            },
        );
        self.streams.insert(key.clone(), cursors);
        self.group.insert(key.clone(), aux.group);
        self.dead_letters.insert(key.clone(), aux.dead_letters);
        self.counters.insert(key.clone(), aux.counters);
        let state = self.states.get_mut(&key).expect("checked above");
        if let Some(next) = state.running.take() {
            state.held.append(&mut next.answered.lock());
        }
        state.last = Some(LastPass {
            scan,
            halted,
            lag,
            behind,
            drain_pending,
        });
        let mut again = std::mem::take(&mut state.again) || copying || again;
        let held = std::mem::take(&mut state.held);
        for answered in held {
            again |= self.merge(answered);
        }

        let busy = self.busy.entry(key.clone()).or_default();
        busy.extend(straggling);
        for exchange in exchanges {
            self.exchanges.push(Box::pin(answered(
                key.clone(),
                generation,
                incarnation,
                exchange,
            )));
        }

        if again {
            self.start(&key);
        }
        // Shards turned away while too many passes were under way.
        let waiting: Vec<ShardKey> = self
            .states
            .iter()
            .filter(|(_, state)| state.again && state.running.is_none())
            .map(|(key, _)| key.clone())
            .collect();
        for key in waiting {
            self.start(&key);
        }
    }

    /// A promoted shard's fence attempt ended. Open, it ships on its next
    /// pass; not yet, it is tried again after [`fence_backoff`].
    pub(super) fn fenced(&mut self, attempt: FenceAttempt) {
        let FenceAttempt {
            key,
            generation,
            opened,
            replicas,
            took,
            why,
        } = attempt;
        let scan = self.scan;
        let Some(state) = self.states.get_mut(&key) else {
            return;
        };
        if let Some(next) = state.running.take() {
            state.held.append(&mut next.answered.lock());
        }
        state.fence_pending = !opened;
        let failures = match state.fence_failures {
            _ if opened => 0,
            (at, failures) if at == generation => failures.saturating_add(1),
            _ => 1,
        };
        state.fence_failures = (generation, failures);
        state.fence_after =
            (!opened).then(|| tokio::time::Instant::now() + fence_backoff(failures));
        state.last = Some(LastPass::idle(scan));
        // Open, the shard's next pass replaces this with its cursors.
        let fence = (!opened).then(|| {
            let took = took.unwrap_or_default();
            FenceStatus {
                pending: replicas
                    .into_iter()
                    .filter(|node| !took.contains(node))
                    .collect(),
                took,
                attempts: failures,
                retry_at: state.fence_after,
                why,
            }
        });
        self.cx.status.put(
            watch_key(&key),
            ShardStatus {
                generation,
                tail: None,
                followers: Vec::new(),
                fence,
                drain_pending: false,
                behind: false,
            },
        );
        if opened || std::mem::take(&mut state.again) {
            self.start(&key);
        }
    }

    /// An exchange a pass handed back has ended: put its cursor back, and
    /// pass again if it moved.
    pub(super) fn apply(&mut self, answered: Answered) {
        if let Some(nodes) = self.busy.get_mut(&answered.key) {
            nodes.remove(&answered.cursor.node_id);
            if nodes.is_empty() {
                self.busy.remove(&answered.key);
            }
        }
        let key = answered.key.clone();
        let Some(state) = self.states.get_mut(&key) else {
            return;
        };
        if let Some(next) = &state.running {
            // The pass under way may be waiting for a majority this answer
            // completes, and counts it; see the wait in `replicate_shard`.
            next.answered.lock().push(answered);
            next.wanted.notify_one();
            return;
        }
        if self.merge(answered) {
            self.start(&key);
        }
    }

    /// Put an ended exchange's cursor where its stale copy is. True when the
    /// follower moved, a copy was cut at its slice, or the follower answered
    /// for more than the mark counts: each is a pass's worth of work to do now
    /// rather than at the next wake. A follower that did not answer is left
    /// for the next wake, or a peer that fails fast would be dialled in a loop.
    ///
    /// The last case is a level follower asked again for an answer the mark
    /// can count, which moves no cursor. Only a pass counts an answer, and
    /// with every publisher waiting on the mark no append is coming to start
    /// one.
    fn merge(&mut self, answered: Answered) -> bool {
        let Answered {
            key,
            generation,
            incarnation,
            cursor,
            rode,
            cut,
            checked,
        } = answered;
        let mark = self
            .cx
            .marks
            .offset_at(&watch_key(&key), incarnation, generation)
            .unwrap_or(0);
        let counts_for_more = checked && cursor.halted.is_none() && cursor.confirmed > mark;
        let at = (generation, incarnation);
        let mut moved = put_back(self.streams.get_mut(&key), at, cursor) || counts_for_more;
        for (entries, cursor) in [
            (&mut self.group, rode.group),
            (&mut self.dead_letters, rode.dead_letters),
            (&mut self.counters, rode.counters),
        ] {
            if let Some(cursor) = cursor {
                moved |= put_back(entries.get_mut(&key), at, cursor);
            }
        }
        moved || cut
    }

    /// What the driver publishes after each event: every halted follower,
    /// the worst lag, the scan every shard has passed since, and whether any
    /// shard is left with no follower holding its whole log.
    pub(super) fn summary(&self) -> (Vec<HaltedReplica>, Option<u64>, u64, bool) {
        let lasts = || self.states.values().filter_map(|state| state.last.as_ref());
        let halted = lasts().flat_map(|last| last.halted.clone()).collect();
        let worst_lag = lasts().filter_map(|last| last.lag).max();
        let finished = self
            .states
            .values()
            .map(|state| state.last.as_ref().map_or(0, |last| last.scan))
            .min()
            .unwrap_or(self.scan);
        let behind = lasts().any(|last| last.behind);
        (halted, worst_lag, finished, behind)
    }
}

/// Put an ended exchange's cursor in place of its stale copy, if the cursors
/// are still at the exchange's generation and incarnation. True when the
/// follower moved.
fn put_back(
    entry: Option<&mut ShardCursors>,
    (generation, incarnation): (u64, u64),
    cursor: FollowerCursor,
) -> bool {
    let Some(slot) = entry
        .filter(|entry| entry.generation == generation && entry.incarnation == incarnation)
        .and_then(|entry| {
            entry
                .followers
                .iter_mut()
                .find(|held| held.node_id == cursor.node_id)
        })
    else {
        return false;
    };
    let moved = slot.next_offset != cursor.next_offset || slot.halted != cursor.halted;
    // The route may have moved the follower to a new address meanwhile.
    let addr = slot.addr;
    *slot = cursor;
    slot.addr = addr;
    moved
}

async fn answered(
    key: ShardKey,
    generation: u64,
    incarnation: u64,
    exchange: Exchange<'_>,
) -> Answered {
    let Shipped {
        cursor,
        rode,
        cut,
        checked,
    } = exchange.await;
    Answered {
        key,
        generation,
        incarnation,
        cursor,
        rode: Box::new(rode),
        cut,
        checked,
    }
}

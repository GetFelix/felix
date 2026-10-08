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

use super::shard::{AuxCursors, Exchange, ShardCursors, ShardPass, Stragglers, replicate_shard};
use super::{
    DRAIN_RETRY, FENCE_RETRY, SHARD_CONCURRENCY, WriteFence, fence_backoff, fence_one, open_fenced,
    take_work, watch_key,
};
use crate::halted::HaltedReplica;
use crate::peer::PeerRequester;
use crate::promotion::PromotionGate;
use crate::quorum::QuorumMarks;
use crate::reporter::Reporter;
use crate::{FollowerCursor, MoveThrottle, Rebuilds};

/// Everything a pass borrows from the driver task, for as long as it runs.
pub(super) struct Context<'a, R> {
    pub(super) requester: &'a R,
    pub(super) broker: &'a Arc<Broker>,
    pub(super) router: &'a ShardRouter,
    pub(super) fence: &'a dyn WriteFence,
    pub(super) gate: &'a dyn PromotionGate,
    pub(super) marks: &'a QuorumMarks,
    pub(super) reporter: Option<&'a Reporter>,
    pub(super) rebuilds: &'a Rebuilds,
    pub(super) throttle: &'a MoveThrottle,
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
    /// A promoted shard's fence attempt ended: the generation it was for,
    /// and whether the shard is open now.
    Fenced((ShardKey, u64, bool)),
    /// An exchange a pass handed back has ended.
    Answered(Answered),
}

/// An exchange handed back by a pass, ended.
pub(super) struct Answered {
    key: ShardKey,
    generation: u64,
    cursor: FollowerCursor,
    cut: bool,
}

/// The driver's view of one shard it leads.
#[derive(Default)]
struct ShardState {
    /// A pass is under way, holding the shard's cursors; told when the next
    /// one is wanted, so it stops waiting on followers that have not answered.
    running: Option<Arc<tokio::sync::Notify>>,
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
    pub(super) fences: FuturesUnordered<BoxFuture<'a, (ShardKey, u64, bool)>>,
    pub(super) exchanges: FuturesUnordered<BoxFuture<'a, Answered>>,
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
        }
    }

    /// Give every shard led here a pass, and forget the ones that are not.
    pub(super) fn scan(&mut self) {
        self.scan += 1;
        let table = self.cx.router.snapshot();
        let local = self.cx.router.local_node_id();
        let live: Vec<ShardKey> = table
            .iter()
            .filter(|(_, route)| route.leader.node_id == local)
            .map(|(key, _)| key.clone())
            .collect();
        for key in &live {
            self.start(key);
        }

        // A shard this broker no longer leads keeps no cursors: they would be
        // a belief about a follower under a leadership that has ended. One
        // with a pass under way is dropped when the pass ends.
        let led: HashSet<&ShardKey> = live.iter().collect();
        self.states
            .retain(|key, state| state.running.is_some() || led.contains(key));
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
        self.cx
            .marks
            .retain(&live.iter().map(watch_key).collect::<Vec<_>>());

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
        let Some(route) = table
            .get(key)
            .filter(|route| route.leader.node_id == self.cx.router.local_node_id())
        else {
            return;
        };
        let state = self.states.entry(key.clone()).or_default();
        if let Some(next_wanted) = &state.running {
            state.again = true;
            next_wanted.notify_one();
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
            state.running = Some(Arc::new(tokio::sync::Notify::new()));
            let cx = self.cx;
            let (key, route) = (key.clone(), route.clone());
            self.fences.push(Box::pin(async move {
                let (key, generation, outcome) = fence_one(
                    cx.requester,
                    cx.broker,
                    cx.router.local_node_id(),
                    key,
                    route,
                    cx.marks,
                )
                .await;
                let opened = open_fenced(cx.gate, &key, generation, outcome).await;
                (key, generation, opened)
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
            return;
        };
        let next_wanted = Arc::new(tokio::sync::Notify::new());
        state.running = Some(Arc::clone(&next_wanted));
        let busy: HashSet<String> = self
            .busy
            .get(key)
            .map(|nodes| nodes.keys().cloned().collect())
            .unwrap_or_default();
        self.passes
            .push(self.pass(key.clone(), route.clone(), entry, aux, busy, next_wanted));
    }

    fn pass(
        &self,
        key: ShardKey,
        route: felix_router::Route,
        entry: ShardCursors,
        aux: AuxCursors,
        busy: HashSet<String>,
        next_wanted: Arc<tokio::sync::Notify>,
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
                Stragglers::HandOff { next_wanted },
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
            .is_some_and(|route| route.leader.node_id == self.cx.router.local_node_id());
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
            exchanges,
            straggling,
            cursors,
            aux,
            halted,
            lag,
            copying,
            drain_pending,
            behind,
            ..
        } = pass;
        self.streams.insert(key.clone(), cursors);
        self.group.insert(key.clone(), aux.group);
        self.dead_letters.insert(key.clone(), aux.dead_letters);
        self.counters.insert(key.clone(), aux.counters);
        let state = self.states.get_mut(&key).expect("checked above");
        state.running = None;
        state.last = Some(LastPass {
            scan,
            halted,
            lag,
            behind,
            drain_pending,
        });
        let mut again = std::mem::take(&mut state.again) || copying;
        let held = std::mem::take(&mut state.held);
        for answered in held {
            again |= self.merge(answered);
        }

        let busy = self.busy.entry(key.clone()).or_default();
        busy.extend(straggling);
        for exchange in exchanges {
            self.exchanges
                .push(Box::pin(answered(key.clone(), generation, exchange)));
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
    pub(super) fn fenced(&mut self, (key, generation, opened): (ShardKey, u64, bool)) {
        let scan = self.scan;
        let Some(state) = self.states.get_mut(&key) else {
            return;
        };
        state.running = None;
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
        if let Some(next_wanted) = &state.running {
            // The pass under way may be waiting for a majority this answer
            // completes; see the wait in `replicate_shard`.
            next_wanted.notify_one();
            state.held.push(answered);
            return;
        }
        if self.merge(answered) {
            self.start(&key);
        }
    }

    /// Put an ended exchange's cursor where its stale copy is. True when the
    /// follower moved, or a copy was cut at its slice: either way there is a
    /// pass's worth of work to do now rather than at the next wake. A
    /// follower that did not answer is left for the next wake, or a peer that
    /// fails fast would be dialled in a loop.
    fn merge(&mut self, answered: Answered) -> bool {
        let Some(entry) = self
            .streams
            .get_mut(&answered.key)
            .filter(|entry| entry.generation == answered.generation)
        else {
            return false;
        };
        let Some(slot) = entry
            .followers
            .iter_mut()
            .find(|held| held.node_id == answered.cursor.node_id)
        else {
            return false;
        };
        let moved = slot.next_offset != answered.cursor.next_offset
            || slot.halted != answered.cursor.halted;
        // The route may have moved the follower to a new address meanwhile.
        let addr = slot.addr;
        *slot = answered.cursor;
        slot.addr = addr;
        moved || answered.cut
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

async fn answered(key: ShardKey, generation: u64, exchange: Exchange<'_>) -> Answered {
    let (cursor, cut) = exchange.await;
    Answered {
        key,
        generation,
        cursor,
        cut,
    }
}

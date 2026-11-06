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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use felix_broker::{Appended, AppendedShards, Broker};
use felix_common::membership::RefusedGeneration;
use felix_router::{ShardKey, ShardRouter};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;

use super::shard::{
    AuxCursors, Exchange, RideAlong, ShardCursors, ShardPass, Shipped, Stragglers, replicate_shard,
};
use super::{
    DRAIN_RETRY, FENCE_CONCURRENCY, FENCE_RETRY, REFRESH_SHARE, SHARD_CONCURRENCY, WriteFence,
    alone_report, fence_backoff, fence_one, open_fenced, take_work, watch_key,
};
use crate::halted::HaltedReplica;
use crate::metrics::DriverMetrics;
use crate::peer::PeerRequester;
use crate::promotion::Outcome;
use crate::promotion::PromotionGate;
use crate::quorum::QuorumMarks;
use crate::reporter::Reporter;
use crate::status::{FenceStatus, FollowerStatus, ShardStatus, ShardStatusBoard, follower_status};
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

/// Why a scan ran.
#[derive(Clone, Copy)]
pub(super) enum WakeKind {
    Append,
    Tick,
    Routes,
}

impl WakeKind {
    fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Tick => "tick",
            Self::Routes => "routes",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Append => 0,
            Self::Tick => 1,
            Self::Routes => 2,
        }
    }
}

/// Where a shard waits for a slot. See [`Shards::fill`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Queue {
    /// An append since the shard was last asked: a write is waiting on this
    /// pass now, and the pass is short.
    Appended,
    /// A write waiting on the mark, a shard new here or just opened, a move,
    /// or an answer that moved a follower: work a client may be waiting on.
    Urgent,
    /// Nothing new: the tick's pass, or a follower to try again.
    Refresh,
    /// A promoted shard's fence. Its own slots, so new shards' fences
    /// cannot crowd out shipping.
    Fence,
}

impl Queue {
    fn of(appended: bool, urgent: bool) -> Self {
        if appended {
            Self::Appended
        } else if urgent {
            Self::Urgent
        } else {
            Self::Refresh
        }
    }

    /// Lower starts sooner; a shard queued again moves up, never down.
    fn rank(self) -> u8 {
        match self {
            Self::Appended => 0,
            Self::Urgent => 1,
            Self::Refresh => 2,
            Self::Fence => 3,
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Urgent => 0,
            Self::Refresh => 1,
            Self::Fence => 2,
            Self::Appended => 3,
        }
    }
}

/// What woke the driver.
pub(super) enum Event<'a> {
    /// An append, a route change, or the tick. A route change or the tick
    /// passes every shard; an append passes the shards appended to and
    /// those whose last pass left work. Only an append cuts a pass under way
    /// short.
    Wake(WakeKind),
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
    /// So many replicas promised the generation to another node that no
    /// majority was left to take it, or the set it leaves is not known here.
    refused: Option<RefusedGeneration>,
}

/// An exchange handed back by a pass, ended.
pub(super) struct Answered {
    key: ShardKey,
    pub(super) generation: u64,
    pub(super) incarnation: u64,
    pub(super) cursor: FollowerCursor,
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
    /// An append woke the driver since the pass last looked.
    pub(super) appended: std::sync::atomic::AtomicBool,
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
    /// Asked for a pass while one was under way.
    again: bool,
    /// That ask was urgent ([`Queue::Urgent`]).
    again_urgent: bool,
    /// That ask came with an append ([`Queue::Appended`]).
    again_appended: bool,
    /// Waiting for a slot: in which queue, since when, and the ticket its
    /// queue entry carries.
    queued: Option<(Queue, tokio::time::Instant, u64)>,
    /// The bit an append to this shard sets in [`AppendedShards`].
    slot: Option<u16>,
    /// The last pass left the mark below the tail, or no follower holding
    /// all of it: a write may be waiting on the next pass.
    owed: bool,
    /// Exchanges that ended while a pass held the cursors.
    held: Vec<Answered>,
    /// Promoted here, and a majority has not taken the fence yet.
    fence_pending: bool,
    /// No fence before this. Appends wake scans, so without it a shard
    /// the broker keeps closed after its fence is fenced again at the append
    /// rate of every other shard.
    fence_after: Option<tokio::time::Instant>,
    /// Fence attempts in a row that left the shard closed, and the generation
    /// they were at: a new promotion starts from [`FENCE_RETRY`] again.
    fence_failures: (u64, u32),
    /// The last fence attempt found the generation promised to another node
    /// by too many replicas to open, or could not learn the set it leaves.
    /// Reported each tick, as a refused open is.
    fence_refused: Option<RefusedGeneration>,
    /// After the shard opened, a follower refused it for a newer generation
    /// and no majority is left without it
    /// ([`crate::follower::refused_without_majority`]). Reported each tick.
    ship_refused: Option<RefusedGeneration>,
    /// The last pass found this broker's own ballot past the generation it
    /// leads. Reported each tick, as a refused open is.
    ballot_refused: Option<RefusedGeneration>,
    /// The ballot past the led generation the passes have seen, and since
    /// when. Reported only once it outlasts [`super::BALLOT_ABOVE_FOR`].
    ballot_above_since: Option<(u64, tokio::time::Instant)>,
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
    /// The tail the pass read.
    tail: Option<u64>,
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
            tail: None,
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
    /// Shards waiting for a pass slot, oldest first, each with the ticket it
    /// was queued with. An entry whose ticket the shard's state no longer
    /// holds is stale and skipped: a shard that started or moved queue and
    /// then queued again waits at the back, not in its old place.
    appended: VecDeque<(ShardKey, u64)>,
    urgent: VecDeque<(ShardKey, u64)>,
    refresh: VecDeque<(ShardKey, u64)>,
    /// Shards waiting for a fence slot.
    fencing: VecDeque<(ShardKey, u64)>,
    /// The last ticket handed out.
    tickets: u64,
    /// Shards with `queued` set, per [`Queue`].
    queued_in: [usize; 4],
    /// Shards with `queued` set.
    waiting: usize,
    /// Urgent shards started in a row while a refresh waited.
    refresh_passed_over: usize,
    /// Appended shards started in a row while an urgent one waited.
    urgent_passed_over: usize,
    /// The table the last full scan read. An append wake scans in full only
    /// when the table has changed since.
    scanned_table: Option<Arc<felix_router::RoutingTable>>,
    /// Shards whose route names this broker as leader, by the bit an append
    /// to them sets, as of [`Self::scanned_table`].
    led_slots: HashMap<u16, Vec<ShardKey>>,
    /// Of those, the ones moving.
    moving: Vec<ShardKey>,
    /// Of those, the ones this broker does not lead yet (withheld, or an
    /// open refused). Each append wake checks them again.
    not_led: HashSet<ShardKey>,
    metrics: DriverMetrics,
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
            appended: VecDeque::new(),
            urgent: VecDeque::new(),
            refresh: VecDeque::new(),
            fencing: VecDeque::new(),
            tickets: 0,
            queued_in: [0; 4],
            waiting: 0,
            refresh_passed_over: 0,
            urgent_passed_over: 0,
            scanned_table: None,
            led_slots: HashMap::new(),
            moving: Vec::new(),
            not_led: HashSet::new(),
            metrics: DriverMetrics::new(),
        }
    }

    /// Whether `route` names this broker as the leader, and it leads.
    fn leads(&self, key: &ShardKey, route: &felix_router::Route) -> bool {
        route.leader.node_id == self.cx.router.local_node_id()
            && !self.cx.fence.withheld(&watch_key(key), route.generation)
            && self.refused_open(key, route).is_none()
    }

    /// The refused open that keeps this broker from leading `route`'s
    /// generation, if it refused that one. An older refusal says nothing
    /// about it.
    fn refused_open(
        &self,
        key: &ShardKey,
        route: &felix_router::Route,
    ) -> Option<RefusedGeneration> {
        if route.leader.node_id != self.cx.router.local_node_id() {
            return None;
        }
        self.cx
            .fence
            .refused_at(&watch_key(key))
            .filter(|refused| refused.generation == route.generation)
    }

    /// What to report as refused at `route`'s generation: an open refused
    /// there, a fence refused by a promise to another node
    /// ([`crate::promotion::Outcome::Refused`]), or a joint fence that cannot
    /// learn the set it leaves. The shard still fences again after the last
    /// two, in case the set has changed since.
    fn refusal(&self, key: &ShardKey, route: &felix_router::Route) -> Option<RefusedGeneration> {
        self.refused_open(key, route).or_else(|| {
            let state = self.states.get(key)?;
            state
                .fence_refused
                .clone()
                .or_else(|| state.ship_refused.clone())
                .or_else(|| state.ballot_refused.clone())
                .filter(|refused| {
                    refused.generation == route.generation
                        && route.leader.node_id == self.cx.router.local_node_id()
                })
        })
    }

    /// Give the shards led here that want one a pass, and forget the ones no
    /// longer led here.
    ///
    /// An append wake with the table unchanged since the last full scan
    /// visits only the shards it can ask ([`Self::scan_appended`]). Appends
    /// wake the driver many times a second, and a walk of the whole table on
    /// each took most of the task every pass is polled on, so a write's pass
    /// waited behind scans (#1987).
    pub(super) fn scan(&mut self, wake: WakeKind) {
        self.scan += 1;
        self.metrics.scanned(wake.index());
        self.cx.status.scanned(self.scan);
        // Taken on every scan: one that passes every shard covers the bits.
        let appended = self.cx.broker.appended_shards().take();
        let table = self.cx.router.snapshot();
        let unchanged = self
            .scanned_table
            .as_ref()
            .is_some_and(|scanned| Arc::ptr_eq(scanned, &table));
        if matches!(wake, WakeKind::Append) && unchanged {
            self.scan_appended(&table, &appended);
        } else {
            self.scan_all(wake, &table, &appended);
        }
        self.count_rebuilds();
    }

    /// An append wake's scan: the shards appended to, owed a mark or moving,
    /// and the ones not led at the last full scan that may be led now. These
    /// are the only shards [`Self::asks`] passes on an append. Forgetting
    /// shards no longer led, and the reports the tick repeats, wait for the
    /// next full scan; a route change always gets one.
    fn scan_appended(&mut self, table: &felix_router::RoutingTable, appended: &Appended) {
        let mut visit: Vec<ShardKey> = appended
            .slots()
            .filter_map(|slot| self.led_slots.get(&slot))
            .flatten()
            .cloned()
            .collect();
        visit.extend(
            self.states
                .iter()
                .filter(|(_, state)| state.owed)
                .map(|(key, _)| key.clone()),
        );
        visit.extend(self.moving.iter().cloned());
        visit.extend(
            self.not_led
                .iter()
                .filter(|key| !self.states.contains_key(*key))
                .cloned(),
        );
        let mut asked: HashSet<ShardKey> = HashSet::with_capacity(visit.len());
        for key in visit {
            if asked.contains(&key) {
                continue;
            }
            let Some(route) = table.get(&key).filter(|route| self.leads(&key, route)) else {
                continue;
            };
            if let Some((urgent, cuts)) = self.asks(&key, route, WakeKind::Append, appended) {
                self.want(&key, urgent, cuts);
                self.cx
                    .status
                    .note_wake(&watch_key(&key), WakeKind::Append.name());
                asked.insert(key);
            }
        }
        // As in a full scan: a pass waiting on a slow follower gives its slot
        // up once shards wait for one.
        if self.queued_for_pass() {
            for (key, state) in &self.states {
                if let Some(next) = &state.running
                    && !asked.contains(key)
                {
                    next.wanted.notify_one();
                }
            }
        }
        self.fill();
    }

    /// Every shard in `table`: ask the ones led here, report the ones refused
    /// or given up, forget the ones no longer led, and index the table for
    /// the append wakes that follow.
    fn scan_all(
        &mut self,
        wake: WakeKind,
        table: &Arc<felix_router::RoutingTable>,
        appended: &Appended,
    ) {
        let local = self.cx.router.local_node_id();
        self.led_slots.clear();
        self.moving.clear();
        let entries: Vec<_> = table.iter().collect();
        for &(key, route) in &entries {
            if route.leader.node_id != local {
                continue;
            }
            let slot = AppendedShards::slot(&key.tenant_id, &key.namespace, &key.stream, key.shard);
            self.led_slots.entry(slot).or_default().push(key.clone());
            if route.draining || route.pending.is_some() {
                self.moving.push(key.clone());
            }
        }
        self.scanned_table = Some(Arc::clone(table));
        let live: Vec<ShardKey> = entries
            .iter()
            .copied()
            .filter(|(key, route)| self.leads(key, route))
            .map(|(key, _)| key.clone())
            .collect();
        let given_up: Vec<(ShardKey, felix_router::Route)> = entries
            .iter()
            .copied()
            .filter(|(key, route)| {
                route.leader.node_id == self.cx.router.local_node_id()
                    && self.cx.fence.gave_up_at(&watch_key(key)) == Some(route.generation)
            })
            .map(|(key, route)| (key.clone(), route.clone()))
            .collect();
        let refused: Vec<(ShardKey, RefusedGeneration)> = entries
            .iter()
            .copied()
            .filter_map(|(key, route)| Some((key.clone(), self.refusal(key, route)?)))
            .collect();
        self.probed.retain(|key, _| {
            given_up.iter().any(|(given, _)| given == key)
                || refused.iter().any(|(refused, _)| refused == key)
        });
        for (key, route) in given_up {
            self.probe(key, route);
        }
        for (key, refused) in refused {
            self.report_refusal(key, refused);
        }
        for key in &live {
            let Some(route) = table.get(key) else {
                continue;
            };
            match self.asks(key, route, wake, appended) {
                Some((urgent, cuts)) => {
                    self.want(key, urgent, cuts);
                    self.cx.status.note_wake(&watch_key(key), wake.name());
                }
                // A pass that has published its mark and waits on a slow
                // follower holds its slot until told the next pass is wanted;
                // with shards waiting, that is now.
                None if self.queued_for_pass() => {
                    if let Some(next) = self.states.get(key).and_then(|s| s.running.as_ref()) {
                        next.wanted.notify_one();
                    }
                }
                None => {}
            }
        }

        // A shard this broker no longer leads keeps no cursors: they would be
        // a belief about a follower under a leadership that has ended. One
        // with a pass under way is dropped when the pass ends.
        let led: HashSet<&ShardKey> = live.iter().collect();
        self.not_led = self
            .led_slots
            .values()
            .flatten()
            .filter(|key| !led.contains(key))
            .cloned()
            .collect();
        let queued_in = &mut self.queued_in;
        let mut dropped = 0;
        self.states.retain(|key, state| {
            let keep = state.running.is_some() || led.contains(key);
            if !keep && let Some((queue, ..)) = state.queued {
                queued_in[queue.index()] -= 1;
                dropped += 1;
            }
            keep
        });
        self.waiting -= dropped;
        self.metrics.waiting(self.waiting);
        self.fill();
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
    }

    /// Whether a shard waits for a pass slot.
    fn queued_for_pass(&self) -> bool {
        [Queue::Appended, Queue::Urgent, Queue::Refresh]
            .iter()
            .any(|queue| self.queued_in[queue.index()] > 0)
    }

    /// Entries in the pass queues, stale ones included.
    #[cfg(test)]
    pub(super) fn queue_entries(&self) -> usize {
        self.appended.len() + self.urgent.len() + self.refresh.len()
    }

    /// Queue `key` as the scan would, for tests that place entries exactly.
    #[cfg(test)]
    pub(super) fn queue_for_test(&mut self, key: &ShardKey, queue: Queue) {
        self.enqueue(key, queue);
    }

    /// Take `key` out of its queue as a start does, leaving its old entries.
    #[cfg(test)]
    pub(super) fn leave_queue_for_test(&mut self, key: &ShardKey) {
        self.dequeue(key);
    }

    /// Whether a pass is under way for `key`.
    #[cfg(test)]
    pub(super) fn runs(&self, key: &ShardKey) -> bool {
        self.states.get(key).is_some_and(|s| s.running.is_some())
    }

    /// Whether a shard waits in the refresh queue.
    #[cfg(test)]
    pub(super) fn refresh_waits(&self) -> bool {
        self.queued_in[Queue::Refresh.index()] > 0
    }

    fn count_rebuilds(&mut self) {
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
        // Not one queued for a slot: it starts when one frees, and retrying
        // it before then would wake the driver in a loop.
        let idle = || {
            self.states
                .iter()
                .filter(|(_, state)| state.running.is_none() && state.queued.is_none())
        };
        if idle().any(|(_, state)| state.last.as_ref().is_some_and(|last| last.drain_pending)) {
            Some(DRAIN_RETRY)
        } else if idle().any(|(_, state)| state.fence_pending) {
            // The soonest a fence is due. A shard with none set has its
            // fence still to start, so it goes at the usual spacing.
            let now = tokio::time::Instant::now();
            let table = self.cx.router.snapshot();
            idle()
                .filter(|(key, state)| state.fence_pending && self.fences_at_route(key, &table))
                .map(|(_, state)| {
                    state
                        .fence_after
                        .map_or(FENCE_RETRY, |after| after.saturating_duration_since(now))
                })
                .min()
        } else {
            None
        }
    }

    /// Whether [`Self::want`] would fence `key` now: this broker leads its
    /// route and the gate awaits the route's generation. A pending fence
    /// that fails this has nothing to retry until the route or the gate
    /// moves, and counting it would make [`Self::retry_in`] answer zero
    /// forever once its backoff has passed.
    fn fences_at_route(&self, key: &ShardKey, table: &felix_router::RoutingTable) -> bool {
        table.get(key).is_some_and(|route| {
            self.leads(key, route)
                && self.cx.gate.awaiting(&watch_key(key)) == Some(route.generation)
        })
    }

    /// Another pass for every shard [`Self::retry_in`] was waiting on.
    pub(super) fn retry(&mut self) {
        let table = self.cx.router.snapshot();
        let waiting: Vec<ShardKey> = self
            .states
            .iter()
            .filter(|(key, state)| {
                state.running.is_none()
                    && ((state.fence_pending && self.fences_at_route(key, &table))
                        || state.last.as_ref().is_some_and(|last| last.drain_pending))
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in waiting {
            self.want(&key, true, false);
        }
    }

    /// Whether a scan woken by `wake` asks `key` for a pass: `Some((urgent,
    /// cuts))` if so. See [`Self::want`].
    ///
    /// Woken by an append, a scan asks only the shards appended to, the ones
    /// owed a mark, and the ones moving. The rest wait for the tick, which
    /// passes every shard: it refreshes the report and retries a follower that
    /// is gone. A shard left with work enqueued itself when its pass ended,
    /// and a busy follower's answer that moves its cursor asks for a pass.
    /// Asked on every append anywhere, a shard with a dead follower would take
    /// a slot from the shards with writes waiting on them (#1944).
    fn asks(
        &mut self,
        key: &ShardKey,
        route: &felix_router::Route,
        wake: WakeKind,
        appended: &Appended,
    ) -> Option<(bool, bool)> {
        let append = matches!(wake, WakeKind::Append);
        let Some(state) = self.states.get_mut(key) else {
            return Some((true, false));
        };
        let slot = *state.slot.get_or_insert_with(|| {
            AppendedShards::slot(&key.tenant_id, &key.namespace, &key.stream, key.shard)
        });
        let dirty = appended.contains(slot);
        // A move's drained report holds its clients, so it goes first too.
        let moving = route.draining || route.pending.is_some();
        if dirty || state.owed || moving {
            return Some((true, dirty));
        }
        (!append).then_some((false, false))
    }

    /// Start a pass for `key`, or ask for one once the pass under way ends,
    /// or queue it for a slot. `urgent` puts it in [`Queue::Urgent`]. `cuts`
    /// is whether the wake may be new data for the pass under way to stop
    /// waiting for; the pass checks this shard's tail before it does. It also
    /// puts the shard in [`Queue::Appended`].
    fn want(&mut self, key: &ShardKey, urgent: bool, cuts: bool) {
        let table = self.cx.router.snapshot();
        let Some(route) = table.get(key).filter(|route| self.leads(key, route)) else {
            return;
        };
        let state = self.states.entry(key.clone()).or_default();
        if let Some(next) = &state.running {
            state.again = true;
            state.again_urgent |= urgent;
            state.again_appended |= cuts;
            if cuts {
                next.appended
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            next.wanted.notify_one();
            return;
        }
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
            if self.fences.len() >= FENCE_CONCURRENCY {
                self.enqueue(key, Queue::Fence);
                return;
            }
            self.dequeue(key);
            let state = self.states.get_mut(key).expect("inserted above");
            state.again = false;
            state.again_urgent = false;
            state.again_appended = false;
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
                // Held until the shard has opened.
                let (key, generation, outcome, fencing) =
                    fence_one(cx.requester, cx.broker, cx.router, key, route, cx.marks).await;
                let (took, why) = match &outcome {
                    Outcome::Pending { why, took } | Outcome::Refused { why, took, .. } => {
                        (Some(took.clone()), Some(why.clone()))
                    }
                    Outcome::SetUnknown { why } => (Some(Vec::new()), Some(why.clone())),
                    Outcome::Fenced { .. } | Outcome::Lease { .. } => (None, None),
                };
                let refused = match &outcome {
                    Outcome::Refused { promised_to, .. } => Some(RefusedGeneration {
                        generation,
                        promised_to: promised_to.clone(),
                    }),
                    Outcome::SetUnknown { .. } => Some(RefusedGeneration::set_unknown(generation)),
                    _ => None,
                };
                let opened = open_fenced(cx.gate, &key, generation, outcome).await;
                // Survives the adoption that drops the standing, which the
                // set-keeping rule reads (see `opened_elected`).
                if opened
                    && fencing.is_some()
                    && let Some(elections) = cx.marks.elections()
                {
                    elections.opened(&key, generation);
                }
                drop(fencing);
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
                    refused,
                }
            }));
            return;
        }
        if self.passes.len() >= SHARD_CONCURRENCY {
            self.enqueue(key, Queue::of(cuts, urgent));
            return;
        }
        self.dequeue(key);
        let state = self.states.get_mut(key).expect("inserted above");
        state.again = false;
        state.again_urgent = false;
        state.again_appended = false;
        let Some((entry, aux)) = take_work(
            key,
            route,
            &mut self.streams,
            &mut self.group,
            &mut self.dead_letters,
            &mut self.counters,
        ) else {
            state.last = Some(LastPass::idle(self.scan));
            state.owed = false;
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
        let launched = entry
            .followers
            .iter()
            .filter(|cursor| !busy.contains(&cursor.node_id))
            .map(|cursor| (cursor.node_id.clone(), cursor.next_offset))
            .collect();
        let mark = self.cx.marks.offset_at(
            &watch_key(key),
            crate::incarnation::of(self.cx.broker, key),
            route.generation,
        );
        self.cx
            .status
            .pass_started(&watch_key(key), self.scan, mark, launched);
        self.passes
            .push(self.pass(key.clone(), route.clone(), entry, aux, busy, next));
    }

    /// Queue `key` for a slot, unless it already waits in a queue that
    /// starts it no later.
    fn enqueue(&mut self, key: &ShardKey, queue: Queue) {
        let Some(state) = self.states.get_mut(key) else {
            return;
        };
        // A first pass is slow while the followers open their copies. Written
        // to already, a bulk of new shards would otherwise fill the appended
        // queue and put an append's short pass behind them again.
        let queue = if queue == Queue::Appended && state.last.is_none() {
            Queue::Urgent
        } else {
            queue
        };
        let since = match state.queued {
            // Moved up, or out of the fence queue once its fence is done
            // with, so it waits for the slot it now needs.
            Some((held, since, _)) if held != Queue::Fence && queue.rank() < held.rank() => {
                self.queued_in[held.index()] -= 1;
                since
            }
            Some((held @ Queue::Fence, since, _)) if queue != Queue::Fence => {
                self.queued_in[held.index()] -= 1;
                since
            }
            Some(_) => return,
            None => {
                self.waiting += 1;
                tokio::time::Instant::now()
            }
        };
        self.tickets += 1;
        state.queued = Some((queue, since, self.tickets));
        self.queued_in[queue.index()] += 1;
        match queue {
            Queue::Appended => &mut self.appended,
            Queue::Urgent => &mut self.urgent,
            Queue::Refresh => &mut self.refresh,
            Queue::Fence => &mut self.fencing,
        }
        .push_back((key.clone(), self.tickets));
        self.metrics.waiting(self.waiting);
    }

    /// `key` is starting: whatever queue entry it has is now stale. Records
    /// how long it waited.
    fn dequeue(&mut self, key: &ShardKey) {
        if let Some((queue, since, _)) = self
            .states
            .get_mut(key)
            .and_then(|state| state.queued.take())
        {
            self.queued_in[queue.index()] -= 1;
            self.waiting -= 1;
            self.metrics.waiting(self.waiting);
            self.metrics.waited(queue.index(), since.elapsed());
        }
    }

    /// Hand free slots to the shards waiting for one, in the order they
    /// asked within each queue: appended ones first, then urgent, then
    /// refresh, but never more than [`REFRESH_SHARE`] in a row from a queue
    /// ahead while one behind waits, so a steady stream of appends elsewhere
    /// cannot hold a shard back for good.
    ///
    /// Appended goes ahead of urgent because a fresh append's pass is short
    /// and a client waits on it now. Urgent also holds every shard new here
    /// or just opened, and a bulk create puts hundreds there at once, each
    /// pass slow while its followers open their copies. A lease renewal
    /// behind them in one queue waited seconds (#1987).
    fn fill(&mut self) {
        // With nothing left in a queue every entry in it is stale.
        for (queue, entries) in [
            (Queue::Appended, &mut self.appended),
            (Queue::Urgent, &mut self.urgent),
            (Queue::Refresh, &mut self.refresh),
            (Queue::Fence, &mut self.fencing),
        ] {
            if self.queued_in[queue.index()] == 0 {
                entries.clear();
            }
        }
        while self.passes.len() < SHARD_CONCURRENCY {
            let waits = |queue: Queue| self.queued_in[queue.index()] > 0;
            let (appended_waits, urgent_waits, refresh_waits) = (
                waits(Queue::Appended),
                waits(Queue::Urgent),
                waits(Queue::Refresh),
            );
            let queue = if refresh_waits
                && ((!appended_waits && !urgent_waits) || self.refresh_passed_over >= REFRESH_SHARE)
            {
                Queue::Refresh
            } else if urgent_waits && (!appended_waits || self.urgent_passed_over >= REFRESH_SHARE)
            {
                Queue::Urgent
            } else {
                Queue::Appended
            };
            let Some((key, ticket)) = match queue {
                Queue::Appended => &mut self.appended,
                Queue::Urgent => &mut self.urgent,
                _ => &mut self.refresh,
            }
            .pop_front() else {
                break;
            };
            if !self.waits_in(&key, queue, ticket) {
                continue;
            }
            self.refresh_passed_over = match queue {
                Queue::Refresh => 0,
                _ if refresh_waits => self.refresh_passed_over + 1,
                _ => 0,
            };
            // Kept across a refresh start: reset there, a refresh due every
            // fifth start would keep the urgent queue waiting for good.
            self.urgent_passed_over = match queue {
                Queue::Appended if urgent_waits => self.urgent_passed_over + 1,
                Queue::Refresh if urgent_waits => self.urgent_passed_over,
                _ => 0,
            };
            // Before `want`, which may return without starting it: a shard
            // left marked queued but in no queue would never queue again.
            self.dequeue(&key);
            self.want(&key, queue != Queue::Refresh, false);
        }
        while self.fences.len() < FENCE_CONCURRENCY {
            let Some((key, ticket)) = self.fencing.pop_front() else {
                break;
            };
            if self.waits_in(&key, Queue::Fence, ticket) {
                self.dequeue(&key);
                self.want(&key, true, false);
            }
        }
    }

    /// Whether `key` still waits in `queue` under `ticket`, rather than
    /// having started or moved since it was queued there.
    fn waits_in(&self, key: &ShardKey, queue: Queue, ticket: u64) -> bool {
        self.states.get(key).is_some_and(|state| {
            state.running.is_none()
                && state
                    .queued
                    .is_some_and(|(held, _, at)| held == queue && at == ticket)
        })
    }

    /// Report a shard led here alone at a new generation, and again each tick
    /// so the report stays fresh. Appends wake scans, so not on each.
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

    /// Tell the control plane, each tick, that this broker refused to lead a
    /// shard at the generation it names. See `crate::reclaim::refusal_report`.
    fn report_refusal(&mut self, key: ShardKey, refused: RefusedGeneration) {
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
        let incarnation = crate::incarnation::of(self.cx.broker, &key);
        reporter.submit(crate::reclaim::refusal_report(&key, refused).at_incarnation(incarnation));
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
        let prior = cx.router.prior_set(&key);
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
                prior,
            )
            .await;
            (scan, pass)
        })
    }

    /// Take back a finished pass's cursors, and hold on to the exchanges it
    /// left under way.
    pub(super) fn fold(&mut self, scan: u64, pass: ShardPass<'a>) {
        let key = pass.key.clone();
        let table = self.cx.router.snapshot();
        let route = table.get(&key);
        let led = route.is_some_and(|route| self.leads(&key, route));
        if !self.states.contains_key(&key) {
            self.cx.status.forget(&watch_key(&key));
            self.fill();
            return;
        }
        if !led {
            // Lost while the pass ran. Its exchanges go with it.
            if let Some((queue, ..)) = self.states.remove(&key).and_then(|state| state.queued) {
                self.queued_in[queue.index()] -= 1;
                self.waiting -= 1;
                self.metrics.waiting(self.waiting);
            }
            // Still named leader, so led again without a route change: the
            // append wakes check it as they check one not led at a full scan.
            if route.is_some_and(|route| route.leader.node_id == self.cx.router.local_node_id()) {
                self.not_led.insert(key.clone());
            }
            self.cx.status.forget(&watch_key(&key));
            self.fill();
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
            ballot_above,
            ..
        } = pass;
        // Busy from earlier passes too: a follower this pass launched nobody
        // for is still being waited on.
        let unanswered = |node: &str| {
            straggling.iter().any(|(held, _)| held == node)
                || self
                    .busy
                    .get(&key)
                    .is_some_and(|nodes| nodes.contains_key(node))
        };
        let followers = cursors
            .followers
            .iter()
            .map(|cursor| {
                let learner =
                    cursor.learner || cursors.learner.as_deref() == Some(cursor.node_id.as_str());
                let busy = unanswered(&cursor.node_id);
                FollowerStatus {
                    busy,
                    // A straggler's cursor is the one it was launched with.
                    busy_at: busy.then_some(cursor.next_offset),
                    ..follower_status(cursor, tail, learner)
                }
            })
            .collect();
        let outstanding = cursors
            .followers
            .iter()
            .filter(|cursor| unanswered(&cursor.node_id))
            .map(|cursor| (cursor.node_id.clone(), cursor.next_offset))
            .collect();
        let mark = self
            .cx
            .marks
            .offset_at(&watch_key(&key), incarnation, generation);
        // Only while some follower is answering: with every one of them out
        // of reach, a pass on each append could not raise the mark, and the
        // tick retries them (#1944). One that refused a batch for its routing
        // view and then took the fence sent again is in reach: the next pass
        // brings it level.
        let reachable = cursors
            .followers
            .iter()
            .any(|cursor| cursor.halted.is_none() && (!cursor.stalled || cursor.refused));
        let owed =
            reachable && (behind || tail.is_some_and(|tail| mark.is_some_and(|mark| mark < tail)));
        self.cx
            .status
            .pass_ended(&watch_key(&key), scan, mark, outstanding);
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
        let ship_refused = crate::follower::refused_without_majority(
            &cursors.followers,
            cursors.learner.as_deref(),
        );
        self.streams.insert(key.clone(), cursors);
        self.group.insert(key.clone(), aux.group);
        self.dead_letters.insert(key.clone(), aux.dead_letters);
        self.counters.insert(key.clone(), aux.counters);
        let state = self.states.get_mut(&key).expect("checked above");
        if let Some(next) = state.running.take() {
            state.held.append(&mut next.answered.lock());
        }
        state.owed = owed;
        let report = ship_refused.is_some() && state.ship_refused.is_none();
        state.ship_refused = ship_refused.map(|ballot| {
            if report {
                tracing::warn!(
                    stream = %key.stream,
                    shard = key.shard,
                    generation,
                    ballot,
                    "a follower took a newer generation and no majority is left without it; \
                     reporting the generation refused",
                );
            }
            RefusedGeneration::set_unknown(generation)
        });
        let now = tokio::time::Instant::now();
        state.ballot_above_since = ballot_above.map(|ballot| match state.ballot_above_since {
            Some((seen, since)) if seen == ballot => (ballot, since),
            _ => (ballot, now),
        });
        // A promotion's ballot lands before its route; only one that stays
        // means a standing here was given up.
        let ballot_above = state
            .ballot_above_since
            .filter(|(_, since)| now.duration_since(*since) >= super::BALLOT_ABOVE_FOR)
            .map(|(ballot, _)| ballot);
        if let Some(ballot) = ballot_above
            && state.ballot_refused.is_none()
        {
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                generation,
                ballot,
                "this broker's own ballot is past the generation it leads, so no write \
                 there can be acknowledged; reporting the generation refused",
            );
        }
        state.ballot_refused = ballot_above.map(|_| RefusedGeneration::set_unknown(generation));
        state.last = Some(LastPass {
            scan,
            halted,
            lag,
            behind,
            drain_pending,
            tail,
        });
        let appended = std::mem::take(&mut state.again_appended);
        let mut urgent = std::mem::take(&mut state.again_urgent) || copying || again || owed;
        let mut again = std::mem::take(&mut state.again) || copying || again;
        let held = std::mem::take(&mut state.held);
        for answered in held {
            let moved = self.merge(answered);
            again |= moved;
            urgent |= moved;
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

        // Behind the shards already waiting, so a shard with steady work
        // gives its slot up rather than keeping it pass after pass.
        if again {
            self.enqueue(&key, Queue::of(appended, urgent));
        }
        if let Some(refused) = self.states.get(&key).and_then(|s| s.ship_refused.clone()) {
            self.report_refusal(key, refused);
        }
        self.fill();
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
            refused,
        } = attempt;
        let scan = self.scan;
        let Some(state) = self.states.get_mut(&key) else {
            self.fill();
            return;
        };
        state.fence_refused = refused.clone();
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
        let appended = std::mem::take(&mut state.again_appended);
        if opened || std::mem::take(&mut state.again) {
            self.want(&key, true, appended);
        }
        if let Some(refused) = refused {
            self.report_refusal(key, refused);
        }
        self.fill();
    }

    /// An answer from `node` for an exchange launched at `next_offset`, which
    /// is where the shard's cursor for it is taken to be: the answer moves
    /// nothing.
    #[cfg(test)]
    pub(super) fn answer_from(
        &mut self,
        key: &ShardKey,
        node: &str,
        next_offset: u64,
        checked: bool,
    ) -> Answered {
        let entry = self.streams.get_mut(key).expect("cursors");
        let cursor = entry
            .followers
            .iter_mut()
            .find(|cursor| cursor.node_id == node)
            .expect("follower");
        cursor.next_offset = next_offset;
        cursor.confirmed = next_offset;
        cursor.stalled = false;
        let cursor = cursor.clone();
        Answered {
            key: key.clone(),
            generation: entry.generation,
            incarnation: entry.incarnation,
            cursor,
            rode: Box::default(),
            cut: false,
            checked,
        }
    }

    /// An answer carrying `node`'s held cursor exactly as it is.
    #[cfg(test)]
    pub(super) fn answer_held(&self, key: &ShardKey, node: &str, checked: bool) -> Answered {
        let entry = self.streams.get(key).expect("cursors");
        let cursor = entry
            .followers
            .iter()
            .find(|cursor| cursor.node_id == node)
            .expect("follower")
            .clone();
        Answered {
            key: key.clone(),
            generation: entry.generation,
            incarnation: entry.incarnation,
            cursor,
            rode: Box::default(),
            cut: false,
            checked,
        }
    }

    /// Halt `node`'s held cursor, as an earlier pass would have.
    #[cfg(test)]
    pub(super) fn halt_follower(&mut self, key: &ShardKey, node: &str) {
        let entry = self.streams.get_mut(key).expect("cursors");
        let cursor = entry
            .followers
            .iter_mut()
            .find(|cursor| cursor.node_id == node)
            .expect("follower");
        cursor.halted = Some(crate::follower::Halt::Diverged);
    }

    #[cfg(test)]
    pub(super) fn broker(&self) -> &Broker {
        self.cx.broker
    }

    /// Mark `node` as having an exchange from an earlier pass under way.
    #[cfg(test)]
    pub(super) fn hold_busy(&mut self, key: &ShardKey, node: &str) {
        self.busy
            .entry(key.clone())
            .or_default()
            .insert(node.to_string(), false);
    }

    /// An exchange a pass handed back has ended: put its cursor back, and
    /// pass again if it moved.
    pub(super) fn apply(&mut self, answered: Answered) {
        self.cx
            .status
            .exchange_ended(&watch_key(&answered.key), &answered.cursor.node_id);
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
            self.cx.status.note_wake(&watch_key(&key), "busy answer");
            next.answered.lock().push(answered);
            next.wanted.notify_one();
            return;
        }
        self.cx
            .status
            .note_wake(&watch_key(&key), "straggler answer");
        if self.merge(answered) {
            self.want(&key, true, false);
        }
    }

    /// Put an ended exchange's cursor where its stale copy is. True when the
    /// follower moved, a copy was cut at its slice, the follower answered for
    /// more than the mark counts, or it answered from behind the tail the last
    /// pass read: each is a pass's worth of work to do now
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

        // A pass that found this follower busy shipped it nothing, and the
        // append that started that pass was its only wake. Without another
        // pass the records past its cursor wait for the tick.
        let tail = self
            .states
            .get(&key)
            .and_then(|state| state.last.as_ref())
            .and_then(|last| last.tail);
        let current = self
            .streams
            .get(&key)
            .is_some_and(|e| (e.generation, e.incarnation) == (generation, incarnation));
        let counts_for_more = current && checked && cursor.counts() && cursor.confirmed > mark;
        let owed = current
            && cursor.halted.is_none()
            && !cursor.stalled
            && tail.is_some_and(|tail| cursor.next_offset < tail);
        // A straggler's refusal for its routing view leaves the shard owed,
        // as a pass that took it would: the next pass is what resolves it.
        // Only while the mark is short of the tail, or every level shard of
        // a follower stuck behind would take a pass on each append.
        let behind = self
            .states
            .get(&key)
            .and_then(|state| state.last.as_ref())
            .is_some_and(|last| last.behind);
        let refused_behind = current
            && cursor.halted.is_none()
            && cursor.refused
            && tail.is_some_and(|tail| cursor.next_offset < tail && (mark < tail || behind));
        if refused_behind && let Some(state) = self.states.get_mut(&key) {
            state.owed = true;
        }
        let at = (generation, incarnation);
        let mut moved = put_back(self.streams.get_mut(&key), at, cursor) || counts_for_more || owed;
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
    // A learner answer cleared counts the follower toward the marks again,
    // which a waiting write may need.
    let moved = slot.next_offset != cursor.next_offset
        || slot.halted != cursor.halted
        || (slot.learner && !cursor.learner);
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

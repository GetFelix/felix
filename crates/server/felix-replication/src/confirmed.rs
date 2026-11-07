//! A shard's assignment as the control plane confirms it.
//!
//! A copy with no promise on disk answers a leader only as the control plane
//! backs it (see `ReplicaHandler::refuse_unbacked`). The routing view is
//! seeded from whichever control-plane member answered, which can lag, so
//! such a copy also asks for a read the control plane's leader confirmed
//! (#1332).
//!
//! The read never runs on the peer lane ([`ConfirmedReads`]): a fence
//! waiting on a control-plane round trip would outlast the leader's request
//! timeout and hold one of its stream's control slots meanwhile (#1791).
//!
//! Reads that wait for a free request go out together, up to [`MAX_BATCH`]
//! in one control-plane request, so the shards kept fresh scale with the
//! batch and not with the round trip alone. Each answer in a batch is still a
//! read the control plane's leader confirmed after the request arrived, which
//! is after the shard's own read began, so it is as fresh as a single read.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::{ArcSwap, ArcSwapOption};
// Tokio's clock, so paused-time tests drive the refresh; outside them it is
// the std clock.
use tokio::time::Instant;

use felix_router::ShardKey;

/// What a confirmed read found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirmed {
    /// The control plane names `leader` at `generation`.
    Assigned { generation: u64, leader: String },
    /// The control plane has no assignment for the shard.
    Unassigned,
    /// The control plane does not serve confirmed reads (an older build, or
    /// a Raft leader that predates them). The routing view answers alone, as
    /// it did before.
    NotServed,
    /// No confirmed answer arrived. Nothing is known, so nothing is answered.
    Unavailable,
}

/// Reads a shard's assignment no older than any write the control plane
/// acknowledged before the call.
#[async_trait::async_trait]
pub trait ConfirmedRoutes: Send + Sync {
    async fn confirmed(&self, key: &ShardKey) -> Confirmed;

    /// [`Self::confirmed`] for every shard in `keys` in one request, the
    /// answers in `keys`' order. `None` when the control plane has no such
    /// request; each shard is then read alone.
    async fn confirmed_many(&self, keys: &[ShardKey]) -> Option<Vec<Confirmed>> {
        let _ = keys;
        None
    }
}

/// How long an answer is used under elections. A learner checks every batch
/// it takes; short, since only a fresh read sees the control plane move past
/// a generation.
pub(crate) const FRESH_UNDER_ELECTIONS: Duration = Duration::from_millis(500);

/// How long an answer is used without elections. Here age is not what makes
/// an answer safe: answers live in memory, so every one was read after this
/// process started, after any wipe, and names the newest generation the copy
/// lost a promise for or a later one. That is all `Forget` under
/// `PromiseFromRoutes` in `docs/formal/FelixShard.tla` assumes. The model
/// seeds that view at the wipe and never refreshes it, so it already takes a
/// leader deposed after the answer was read; for a learner that is
/// `CatchUpFromDeposed`. TLC finds neither unsafe. The bound is for
/// liveness: a `NotServed` answer is asked again within 10 s once the
/// control plane is upgraded.
pub(crate) const FRESH: Duration = Duration::from_secs(10);

/// How long after a refused request its shard's answer is kept fresh for
/// the retry, at least. Longer than a leader's longest wait between fence
/// attempts (`FENCE_RETRY_MAX`, 2 s), so a retry spaced out by backoff finds
/// a fresh answer instead of being refused again. A leader that retries on
/// its driver tick waits longer; see [`ConfirmedReads::new`].
const WANTED_FOR: Duration = Duration::from_secs(5);

/// The longest a refused request's retry is waited for when retries keep
/// coming later than expected. A leader's queued fences can put a retry
/// several ticks after its refusal; the wait grows to twice the gap between
/// refusals, up to this, so such a retry is answered instead of refused
/// forever.
const MAX_OWED_FOR: Duration = Duration::from_secs(60);

/// Reads a routing change can start before any request, so a bulk placement
/// does not start hundreds. Past it, a shard is read on its first request.
const PREFETCH_PER_CHANGE: usize = 64;

/// Control-plane requests in flight at once across all shards, so a bulk
/// create does not send the control plane hundreds at the same moment.
const MAX_CONCURRENT_READS: usize = 16;

/// Most shards one control-plane request reads. Reads queue while every
/// request is in flight, and the next request takes up to this many of them.
const MAX_BATCH: usize = 64;

/// Past this many shards, ones nobody asked about for [`FRESH`] are dropped
/// when the next shard is added.
const PRUNE_ABOVE: usize = 1024;

/// Confirmed reads, made in the background so that no peer request waits on
/// the control plane. One task per shard reads while requests for it keep
/// coming, refreshing the answer before it goes stale.
///
/// A request costs a lock-free map load, a few atomics and an `ArcSwap`
/// load: no lock, no allocation, nothing awaited. Only the first request
/// for a shard allocates, copying the map to add it.
pub(crate) struct ConfirmedReads {
    /// ahash: the key is three strings, hashed on every request.
    slots: ArcSwap<HashMap<Arc<ShardKey>, Arc<Slot>, ahash::RandomState>>,
    permits: Arc<tokio::sync::Semaphore>,
    /// Reads waiting for a request, sent by a task started on the first.
    queue: std::sync::OnceLock<tokio::sync::mpsc::UnboundedSender<Pending>>,
    /// Nanoseconds since `epoch` when the control plane last had no batched
    /// read, or `u64::MAX` if it never lacked one.
    unbatched_at: Arc<AtomicU64>,
    /// What the slots' timestamps count from.
    epoch: Instant,
    /// How long a refused request's retry is waited for, in nanoseconds.
    owed_for: u64,
}

impl Default for ConfirmedReads {
    fn default() -> Self {
        Self::new(Duration::ZERO)
    }
}

impl std::fmt::Debug for ConfirmedReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfirmedReads")
            .field("shards", &self.slots.load().len())
            .finish()
    }
}

/// One shard incarnation's answer and its reader's state.
struct Slot {
    incarnation: u64,
    /// The last answer that named a leader or said the read is not served.
    answer: ArcSwapOption<Answer>,
    /// Whether a task is reading for this slot.
    reading: AtomicBool,
    /// Nanoseconds since `epoch` of the last request.
    wanted: AtomicU64,
    /// The freshness the last request needed, in nanoseconds.
    fresh: AtomicU64,
    /// Whether the last request found no fresh answer.
    owed: AtomicBool,
    /// The longest gap seen between a request found owed and the request
    /// before it, in nanoseconds, so a retry that came late once is waited
    /// for longer next time. Kept across answers.
    late: AtomicU64,
    /// Whether a routing change asked for the read and no request has come
    /// yet. Such a slot is kept fresh only for [`WANTED_FOR`], so reads for
    /// copies nobody asks about do not crowd out the ones leaders wait on.
    early: AtomicBool,
    /// Woken by a request that found the answer stale while a task holds
    /// the slot, so a drop in the freshness needed is not waited out.
    stale: tokio::sync::Notify,
}

struct Answer {
    confirmed: Confirmed,
    /// Nanoseconds since `epoch` when it arrived.
    at: u64,
}

impl ConfirmedReads {
    /// Reads for a broker whose leaders retry a refused copy up to `retry`
    /// apart: the driver's tick, which is the control-plane sync interval.
    /// A refused shard's answer is kept fresh for two of those, so the retry
    /// finds it instead of being refused and starting another read, every
    /// time (#1791).
    pub(crate) fn new(retry: Duration) -> Self {
        Self {
            slots: ArcSwap::from_pointee(HashMap::with_hasher(felix_common::random::hash_state())),
            permits: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_READS)),
            queue: std::sync::OnceLock::new(),
            unbatched_at: Arc::new(AtomicU64::new(u64::MAX)),
            epoch: Instant::now(),
            owed_for: nanos(WANTED_FOR.max(retry.saturating_mul(2))),
        }
    }

    /// Decide from the answer for `key` at `incarnation` if one arrived
    /// within `fresh`, starting a read if none is under way. Never waits on
    /// the control plane: `None` means there is no fresh answer yet.
    pub(crate) fn decide<R>(
        self: &Arc<Self>,
        routes: &Arc<dyn ConfirmedRoutes>,
        key: &ShardKey,
        incarnation: u64,
        fresh: Duration,
        decide: impl FnOnce(&Confirmed) -> R,
    ) -> Option<R> {
        let now = self.now();
        let fresh = nanos(fresh);
        let slots = self.slots.load();
        let slot = match slots
            .get(key)
            .filter(|slot| slot.incarnation == incarnation)
        {
            Some(slot) => slot,
            None => {
                drop(slots);
                self.add(routes, key, incarnation, now, fresh, false);
                return None;
            }
        };
        let owed = slot.owed.load(Ordering::Relaxed);
        let since_refused = if owed {
            now.saturating_sub(slot.wanted.load(Ordering::Relaxed))
        } else {
            0
        };
        let spawned = self.want(routes, key, slot, now, fresh);
        let answer = slot.answer.load();
        let decided = answer
            .as_ref()
            .filter(|answer| now.saturating_sub(answer.at) < fresh)
            .map(|answer| decide(&answer.confirmed));
        // Loaded first so the usual request writes nothing here.
        if slot.early.load(Ordering::Relaxed) {
            slot.early.store(false, Ordering::Relaxed);
        }
        if owed != decided.is_none() {
            slot.owed.store(decided.is_none(), Ordering::Relaxed);
        } else if owed
            && since_refused <= nanos(MAX_OWED_FOR)
            && since_refused > slot.late.load(Ordering::Relaxed)
        {
            // Refused again: this retry came `since_refused` after the last
            // request, too late for the window it had. A gap no window can
            // cover would only add reads.
            slot.late.store(since_refused, Ordering::Relaxed);
        }
        if decided.is_none() && !spawned {
            slot.stale.notify_one();
        }
        decided
    }

    /// Start reading `key` at `incarnation` before any request for it, and
    /// keep the answer within `fresh` until the first request comes, for up
    /// to [`WANTED_FOR`].
    pub(crate) fn prefetch(
        self: &Arc<Self>,
        routes: &Arc<dyn ConfirmedRoutes>,
        key: &ShardKey,
        incarnation: u64,
        fresh: Duration,
    ) {
        let now = self.now();
        let fresh = nanos(fresh);
        let slots = self.slots.load();
        match slots
            .get(key)
            .filter(|slot| slot.incarnation == incarnation)
        {
            Some(slot) => {
                // A reader waiting for a leader's retry keeps its longer
                // window. A slot with no reader either outwaited its retry or
                // got no answer; the early window is no worse for either.
                if !slot.reading.load(Ordering::Acquire) {
                    slot.early.store(true, Ordering::Relaxed);
                }
                slot.owed.store(true, Ordering::Relaxed);
                if !self.want(routes, key, slot, now, fresh) {
                    // The reader recomputes its wait for this freshness.
                    slot.stale.notify_one();
                }
            }
            None => {
                drop(slots);
                self.add(routes, key, incarnation, now, fresh, true);
            }
        }
    }

    /// Mark `slot` asked about at `now` needing `fresh`, and start its
    /// reader if none runs. Returns whether this started it.
    fn want(
        self: &Arc<Self>,
        routes: &Arc<dyn ConfirmedRoutes>,
        key: &ShardKey,
        slot: &Arc<Slot>,
        now: u64,
        fresh: u64,
    ) -> bool {
        slot.wanted.store(now, Ordering::Relaxed);
        if slot.fresh.load(Ordering::Relaxed) != fresh {
            slot.fresh.store(fresh, Ordering::Relaxed);
        }
        // A plain load first: while a task reads, a request writes nothing
        // shared but `wanted`.
        let spawned =
            !slot.reading.load(Ordering::Acquire) && !slot.reading.swap(true, Ordering::AcqRel);
        if spawned {
            self.spawn_read(routes, key, slot);
        }
        spawned
    }

    /// The first request for a shard incarnation: add its slot and start
    /// its read. Copy-on-write: a lookup never waits on an add, and two
    /// racing adds for one shard start one read.
    fn add(
        self: &Arc<Self>,
        routes: &Arc<dyn ConfirmedRoutes>,
        key: &ShardKey,
        incarnation: u64,
        now: u64,
        fresh: u64,
        early: bool,
    ) {
        let slot = Arc::new(Slot {
            incarnation,
            answer: ArcSwapOption::empty(),
            reading: AtomicBool::new(true),
            wanted: AtomicU64::new(now),
            fresh: AtomicU64::new(fresh),
            owed: AtomicBool::new(true),
            late: AtomicU64::new(0),
            early: AtomicBool::new(early),
            stale: tokio::sync::Notify::new(),
        });
        let stale = nanos(FRESH);
        let before = self.slots.rcu(|current| {
            if current
                .get(key)
                .is_some_and(|slot| slot.incarnation == incarnation)
            {
                return Arc::clone(current);
            }
            let mut slots = HashMap::clone(current);
            if slots.len() > PRUNE_ABOVE {
                slots.retain(|_, slot| {
                    slot.reading.load(Ordering::Acquire)
                        || now.saturating_sub(slot.wanted.load(Ordering::Relaxed)) < stale
                });
            }
            slots.insert(Arc::new(key.clone()), Arc::clone(&slot));
            Arc::new(slots)
        });
        match before
            .get(key)
            .filter(|slot| slot.incarnation == incarnation)
        {
            // Another request added it meanwhile. A request racing an early
            // read was refused, so its retry gets the longer window.
            Some(added) => {
                added.wanted.store(now, Ordering::Relaxed);
                if !early {
                    added.early.store(false, Ordering::Relaxed);
                }
            }
            None => self.spawn_read(routes, key, &slot),
        }
    }

    fn spawn_read(
        self: &Arc<Self>,
        routes: &Arc<dyn ConfirmedRoutes>,
        key: &ShardKey,
        slot: &Arc<Slot>,
    ) {
        tokio::spawn(Arc::clone(self).read(Arc::clone(routes), key.clone(), Arc::clone(slot)));
    }

    fn now(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// One confirmed read of `key`, sent with whatever other reads are
    /// waiting for a request. Every caller passes the same `routes`: the
    /// first one starts the task that sends them.
    async fn confirmed(&self, routes: &Arc<dyn ConfirmedRoutes>, key: &ShardKey) -> Confirmed {
        let queue = self.queue.get_or_init(|| {
            let (queue, waiting) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(send_reads(
                waiting,
                Arc::clone(routes),
                Arc::clone(&self.permits),
                Arc::clone(&self.unbatched_at),
                self.epoch,
            ));
            queue
        });
        let (answer, answered) = tokio::sync::oneshot::channel();
        let pending = Pending {
            key: key.clone(),
            answer,
        };
        if queue.send(pending).is_err() {
            return Confirmed::Unavailable;
        }
        answered.await.unwrap_or(Confirmed::Unavailable)
    }

    /// Read, and again before the answer goes stale while the shard is still
    /// being asked about.
    async fn read(
        self: Arc<Self>,
        routes: Arc<dyn ConfirmedRoutes>,
        key: ShardKey,
        slot: Arc<Slot>,
    ) {
        let mut reading = Reading {
            slot: &slot,
            ended: false,
        };
        loop {
            let started = self.now();
            let answer = self.confirmed(&routes, &key).await;
            // Requests after this point have not been answered by this read.
            let mut since = started;
            match answer {
                // Nothing known; dropped so no older answer outlives it. Not
                // read again on its own: the next request starts a read, so
                // a shard nobody can answer costs a read per request, as the
                // inline read did.
                Confirmed::Unavailable | Confirmed::Unassigned => slot.answer.store(None),
                confirmed => {
                    let at = self.now();
                    slot.answer.store(Some(Arc::new(Answer { confirmed, at })));
                    // Refresh just before the answer goes stale, leaving twice
                    // what this read took, permit wait included, so a queue
                    // of reads widens the margin instead of letting answers
                    // lapse. Only while the shard is still asked about: a
                    // request within `owed_for`, or longer once a retry came
                    // late. Answered requests count too: a leader whose
                    // round failed on another copy, or a learner's next
                    // batch, comes back like a refused one, and under
                    // elections a 2 s retry would find every other answer
                    // stale.
                    // A request needing a shorter freshness (elections
                    // turned on) wakes the wait to recompute it.
                    loop {
                        let fresh = slot.fresh.load(Ordering::Relaxed);
                        let margin = at
                            .saturating_sub(started)
                            .saturating_mul(2)
                            .clamp(fresh / 8, fresh / 2);
                        let due = at.saturating_add(fresh - margin);
                        let now = self.now();
                        if now >= due {
                            break;
                        }
                        tokio::select! {
                            () = tokio::time::sleep(Duration::from_nanos(due - now)) => break,
                            () = slot.stale.notified() => {}
                        }
                    }
                    let fresh = slot.fresh.load(Ordering::Relaxed);
                    let asked_within = if slot.early.load(Ordering::Relaxed) {
                        nanos(WANTED_FOR).max(fresh / 2)
                    } else {
                        let late = slot.late.load(Ordering::Relaxed).saturating_mul(2);
                        self.owed_for
                            .max(late.min(nanos(MAX_OWED_FOR)))
                            .max(fresh / 2)
                    };
                    let now = self.now();
                    if now.saturating_sub(slot.wanted.load(Ordering::Relaxed)) < asked_within {
                        continue;
                    }
                    since = now;
                }
            }
            slot.reading.store(false, Ordering::Release);
            // A request after `since` saw a reader and started none; take the
            // slot back for it.
            if slot.wanted.load(Ordering::Relaxed) <= since
                || slot.reading.swap(true, Ordering::AcqRel)
            {
                reading.ended = true;
                return;
            }
        }
    }
}

/// A read waiting for a control-plane request.
struct Pending {
    key: ShardKey,
    answer: tokio::sync::oneshot::Sender<Confirmed>,
}

/// Send waiting reads, each request once a permit frees, taking every read
/// queued by then up to [`MAX_BATCH`]. A control plane without the batched
/// read gets one shard per request, as before it existed, and is asked again
/// for a batch after [`FRESH`] in case it was upgraded.
async fn send_reads(
    mut waiting: tokio::sync::mpsc::UnboundedReceiver<Pending>,
    routes: Arc<dyn ConfirmedRoutes>,
    permits: Arc<tokio::sync::Semaphore>,
    unbatched_at: Arc<AtomicU64>,
    epoch: Instant,
) {
    while let Some(first) = waiting.recv().await {
        // The permit first, so reads queued while it is awaited join this one.
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        let now = nanos(epoch.elapsed());
        let unbatched = unbatched_at.load(Ordering::Relaxed);
        let limit = if unbatched == u64::MAX || now.saturating_sub(unbatched) >= nanos(FRESH) {
            MAX_BATCH
        } else {
            1
        };
        let mut batch = vec![first];
        while batch.len() < limit {
            match waiting.try_recv() {
                Ok(pending) => batch.push(pending),
                Err(_) => break,
            }
        }
        let routes = Arc::clone(&routes);
        let unbatched_at = Arc::clone(&unbatched_at);
        tokio::spawn(async move {
            let _permit = permit;
            if batch.len() > 1 {
                let keys: Vec<ShardKey> = batch.iter().map(|pending| pending.key.clone()).collect();
                match routes.confirmed_many(&keys).await {
                    Some(answers) if answers.len() == keys.len() => {
                        for (pending, answer) in batch.into_iter().zip(answers) {
                            let _ = pending.answer.send(answer);
                        }
                        return;
                    }
                    _ => unbatched_at.store(nanos(epoch.elapsed()), Ordering::Relaxed),
                }
            }
            for pending in batch {
                let answer = routes.confirmed(&pending.key).await;
                let _ = pending.answer.send(answer);
            }
        });
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Clears a slot's reading flag if its task panics or is dropped, so the
/// next request starts another.
struct Reading<'a> {
    slot: &'a Slot,
    /// Set once the task gave the flag up itself.
    ended: bool,
}

impl Drop for Reading<'_> {
    fn drop(&mut self) {
        if !self.ended {
            self.slot.reading.store(false, Ordering::Release);
        }
    }
}

/// Starts a copy's confirmed read when this broker's routing view first
/// names it a follower, a move's destination included, so the answer is
/// there when the leader first asks. Without it that request is refused
/// while the read runs, and the leader comes back on its next tick.
///
/// Costs nothing per request: the routing feed calls it as it applies a
/// change. Reads are deduplicated per shard like a request's, and sent with
/// them, at most [`MAX_CONCURRENT_READS`] requests at once.
#[derive(Clone)]
pub struct Prefetch {
    reads: Arc<ConfirmedReads>,
    routes: Arc<dyn ConfirmedRoutes>,
    fleet: Option<Arc<crate::quorum::QuorumMarks>>,
}

impl Prefetch {
    pub(crate) fn new(
        reads: Arc<ConfirmedReads>,
        routes: Arc<dyn ConfirmedRoutes>,
        fleet: Option<Arc<crate::quorum::QuorumMarks>>,
    ) -> Self {
        Self {
            reads,
            routes,
            fleet,
        }
    }

    /// Start reads for the shards, by key and incarnation, that the routing
    /// view now names this broker a follower of and did not before. Takes
    /// at most 64; a shard past that is read on its first request.
    pub fn named(&self, shards: impl IntoIterator<Item = (ShardKey, u64)>) {
        let elections = crate::replica::joint_gate(self.fleet.as_deref()) != Some(false);
        for (key, incarnation) in shards.into_iter().take(PREFETCH_PER_CHANGE) {
            self.reads.prefetch(
                &self.routes,
                &key,
                incarnation,
                freshness(elections, incarnation),
            );
        }
    }
}

impl std::fmt::Debug for Prefetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prefetch").finish_non_exhaustive()
    }
}

/// How long an answer is used for a request at `incarnation`. Generations
/// only rise within an incarnation. Without one, a delete and re-create at
/// generation 1 would look the same; the control plane refuses that
/// re-create, but the copy does not lean on it, so such an answer is held
/// no longer than under elections.
pub(crate) fn freshness(elections: bool, incarnation: u64) -> Duration {
    if elections || incarnation == 0 {
        FRESH_UNDER_ELECTIONS
    } else {
        FRESH
    }
}

#[cfg(test)]
mod tests;

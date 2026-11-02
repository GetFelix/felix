//! A shard's assignment as the control plane confirms it.
//!
//! A copy with no promise on disk answers a leader only as the control plane
//! backs it (see `ReplicaHandler::refuse_unbacked`). The routing view is
//! seeded from whichever control-plane member answered, which can lag, so
//! such a copy also asks for a read the control plane's leader confirmed
//! (#1332).
//!
//! The read never runs on the peer lane ([`ConfirmedReads`]): the peer
//! server answers a lane's requests one at a time, so a control-plane round
//! trip there would hold up every other shard queued behind it (#1791).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, ArcSwapOption};

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
/// the retry. Longer than a leader's longest wait between fence attempts
/// (`FENCE_RETRY_MAX`, 2 s), so a retry spaced out by backoff finds a fresh
/// answer instead of being refused again.
const WANTED_FOR: Duration = Duration::from_secs(5);

/// Reads in flight at once across all shards, so a bulk create does not
/// send the control plane hundreds at the same moment.
const MAX_CONCURRENT_READS: usize = 16;

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
    slots: ArcSwap<HashMap<Arc<ShardKey>, Arc<Slot>>>,
    permits: Arc<tokio::sync::Semaphore>,
    /// What the slots' timestamps count from.
    epoch: Instant,
}

impl Default for ConfirmedReads {
    fn default() -> Self {
        Self {
            slots: ArcSwap::from_pointee(HashMap::new()),
            permits: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_READS)),
            epoch: Instant::now(),
        }
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
    /// Whether the last request found no fresh answer, so its retry is
    /// owed one.
    owed: AtomicBool,
}

struct Answer {
    confirmed: Confirmed,
    /// Nanoseconds since `epoch` when it arrived.
    at: u64,
}

impl ConfirmedReads {
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
        let fresh = u64::try_from(fresh.as_nanos()).unwrap_or(u64::MAX);
        let slots = self.slots.load();
        let slot = match slots
            .get(key)
            .filter(|slot| slot.incarnation == incarnation)
        {
            Some(slot) => slot,
            None => {
                drop(slots);
                self.add(routes, key, incarnation, now, fresh);
                return None;
            }
        };
        slot.wanted.store(now, Ordering::Relaxed);
        if slot.fresh.load(Ordering::Relaxed) != fresh {
            slot.fresh.store(fresh, Ordering::Relaxed);
        }
        // A plain load first: while a task reads, a request writes nothing
        // shared but `wanted`.
        if !slot.reading.load(Ordering::Acquire) && !slot.reading.swap(true, Ordering::AcqRel) {
            self.spawn_read(routes, key, slot);
        }
        let answer = slot.answer.load();
        let decided = answer
            .as_ref()
            .filter(|answer| now.saturating_sub(answer.at) < fresh)
            .map(|answer| decide(&answer.confirmed));
        // Loaded first so the usual request writes nothing here.
        if slot.owed.load(Ordering::Relaxed) != decided.is_none() {
            slot.owed.store(decided.is_none(), Ordering::Relaxed);
        }
        decided
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
    ) {
        let slot = Arc::new(Slot {
            incarnation,
            answer: ArcSwapOption::empty(),
            reading: AtomicBool::new(true),
            wanted: AtomicU64::new(now),
            fresh: AtomicU64::new(fresh),
            owed: AtomicBool::new(true),
        });
        let stale = u64::try_from(FRESH.as_nanos()).unwrap_or(u64::MAX);
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
            // Another request added it meanwhile.
            Some(added) => added.wanted.store(now, Ordering::Relaxed),
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
        let nanos = |d: Duration| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        loop {
            let started = self.now();
            let answer = {
                let Ok(_permit) = self.permits.acquire().await else {
                    return;
                };
                routes.confirmed(&key).await
            };
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
                    // request since the last half of the answer's life, or a
                    // refused one whose retry has not come yet. A leader
                    // backs off up to `FENCE_RETRY_MAX` between fences, so
                    // the retry is waited for up to `WANTED_FOR`.
                    let fresh = slot.fresh.load(Ordering::Relaxed);
                    let margin = at
                        .saturating_sub(started)
                        .saturating_mul(2)
                        .clamp(fresh / 8, fresh / 2);
                    tokio::time::sleep(Duration::from_nanos(fresh - margin)).await;
                    let asked_within = if slot.owed.load(Ordering::Relaxed) {
                        nanos(WANTED_FOR).max(fresh / 2)
                    } else {
                        fresh / 2
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

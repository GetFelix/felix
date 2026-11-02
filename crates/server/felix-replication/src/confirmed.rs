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

/// How long an answer is used without elections. Any read taken since this
/// process started is at least as new as every promise a wipe could have
/// cost the copy, so using an older one refuses no less than `Forget` under
/// `PromiseFromRoutes` in `docs/formal/FelixShard.tla`. The bound matters
/// for `NotServed`: a control plane upgraded to serve the read is asked
/// again within 10 s.
pub(crate) const FRESH: Duration = Duration::from_secs(10);

/// How long after the last request for a shard an answer shorter-lived than
/// this is kept fresh. Longer than a leader's longest wait between fence
/// attempts (`FENCE_RETRY_MAX`, 2 s), so a retry finds a fresh answer.
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
/// for a shard takes a lock, to add it to the map.
pub(crate) struct ConfirmedReads {
    slots: ArcSwap<HashMap<Arc<ShardKey>, Arc<Slot>>>,
    /// Serializes adding and pruning slots; never taken by a lookup.
    adding: parking_lot::Mutex<()>,
    permits: Arc<tokio::sync::Semaphore>,
    /// What the slots' timestamps count from.
    epoch: Instant,
}

impl Default for ConfirmedReads {
    fn default() -> Self {
        Self {
            slots: ArcSwap::from_pointee(HashMap::new()),
            adding: parking_lot::Mutex::new(()),
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
        slot.fresh.store(fresh, Ordering::Relaxed);
        if !slot.reading.swap(true, Ordering::AcqRel) {
            self.spawn_read(routes, key, slot);
        }
        let answer = slot.answer.load();
        answer
            .as_ref()
            .filter(|answer| now.saturating_sub(answer.at) < fresh)
            .map(|answer| decide(&answer.confirmed))
    }

    /// The first request for a shard incarnation: add its slot and start
    /// its read.
    fn add(
        self: &Arc<Self>,
        routes: &Arc<dyn ConfirmedRoutes>,
        key: &ShardKey,
        incarnation: u64,
        now: u64,
        fresh: u64,
    ) {
        let _adding = self.adding.lock();
        let current = self.slots.load_full();
        if let Some(slot) = current
            .get(key)
            .filter(|slot| slot.incarnation == incarnation)
        {
            // Another request added it meanwhile.
            slot.wanted.store(now, Ordering::Relaxed);
            return;
        }
        let mut slots = HashMap::clone(&current);
        if slots.len() > PRUNE_ABOVE {
            let stale = u64::try_from(FRESH.as_nanos()).unwrap_or(u64::MAX);
            slots.retain(|_, slot| {
                slot.reading.load(Ordering::Acquire)
                    || now.saturating_sub(slot.wanted.load(Ordering::Relaxed)) < stale
            });
        }
        let slot = Arc::new(Slot {
            incarnation,
            answer: ArcSwapOption::empty(),
            reading: AtomicBool::new(true),
            wanted: AtomicU64::new(now),
            fresh: AtomicU64::new(fresh),
        });
        slots.insert(Arc::new(key.clone()), Arc::clone(&slot));
        self.slots.store(Arc::new(slots));
        self.spawn_read(routes, key, &slot);
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
                    // Refresh halfway through the answer's life, but only for
                    // a shard still being asked about. When the answer
                    // outlives a leader's longest wait between fence
                    // attempts, a request in the last quarter of it is
                    // enough; when it does not, any within `WANTED_FOR`, so a
                    // retry spaced out by backoff still finds a fresh one.
                    let fresh = slot.fresh.load(Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_nanos(fresh / 2)).await;
                    let asked_within = if fresh < nanos(WANTED_FOR) {
                        nanos(WANTED_FOR)
                    } else {
                        fresh / 4
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

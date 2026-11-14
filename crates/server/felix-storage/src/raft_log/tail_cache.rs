//! The newest entries, as the records they were appended as.
//!
//! Replication reads an entry right after it is appended. Serving it from
//! here hands back the appender's own bytes with no read of the segment.
//!
//! Every log of a broker draws on one [`TailBudget`], so thousands of
//! groups cannot each hold a full cache. A dropped cache is always safe: a
//! read that finds nothing goes to the segments.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use crate::log::{LogRecord, Offset};
use crate::metrics_names::RAFT_TAIL_CACHE_BYTES;

/// Record bytes kept per log. A read below them goes to the segments.
/// Small under test, so the adapter's tests read from both.
const CAPACITY_BYTES: u64 = if cfg!(test) { 2 << 10 } else { 4 << 20 };

/// A byte budget for the tail caches of every Raft log that shares it.
///
/// An append adds to one atomic total and nothing else while the total is
/// under the limit, and every log keeps the full per-log capacity however
/// many logs there are. Only when the total goes over the limit does an
/// append run an eviction pass, one at a time, so one pass serves many
/// appends. The pass walks logs from the least recently appended until the
/// total is at the low-water mark (seven eighths of the limit). A log that
/// has not appended since the previous pass is emptied. One that has is
/// trimmed from its oldest records to half of what it holds, keeping at
/// least its newest record, so the entries replication is about to read
/// stay cached. Idle groups give up their bytes first, and when only busy
/// logs are left they all shrink a little.
///
/// The limit is soft: a victim whose lock is busy frees nothing and is
/// passed over until the next pass.
pub struct TailBudget {
    limit: u64,
    held: AtomicU64,
    /// Set by the appender running an eviction pass.
    evicting: AtomicBool,
    /// Full eviction passes run.
    #[cfg(test)]
    passes: AtomicU64,
    /// Orders appends across logs: the least recent is evicted first.
    clock: AtomicU64,
    /// The clock when the previous pass began. A log whose last append is
    /// not after it has been idle since.
    last_pass: AtomicU64,
    /// Taken to register a log and to evict, never on an append under the
    /// limit.
    logs: parking_lot::Mutex<Vec<Weak<Tail>>>,
}

impl TailBudget {
    /// The limit of [`Self::process`]. 64 logs can hold a full cache of
    /// 4 MiB. A broker with 2,000 groups holds 256 MiB at most, a quarter
    /// of the 1 GB memory target for the Raft groups of a broker.
    pub const DEFAULT_LIMIT_BYTES: u64 = 256 << 20;

    /// A budget of `limit` bytes.
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            held: AtomicU64::new(0),
            evicting: AtomicBool::new(false),
            #[cfg(test)]
            passes: AtomicU64::new(0),
            clock: AtomicU64::new(0),
            last_pass: AtomicU64::new(0),
            logs: parking_lot::Mutex::new(Vec::new()),
        })
    }

    /// The budget [`RaftLog::new`](super::RaftLog::new) uses, one for the
    /// process. A process that runs several brokers gives each its own.
    pub fn process() -> Arc<Self> {
        static PROCESS: OnceLock<Arc<TailBudget>> = OnceLock::new();
        Arc::clone(PROCESS.get_or_init(|| Self::new(Self::DEFAULT_LIMIT_BYTES)))
    }

    /// Record bytes held by every log on this budget now.
    pub fn held(&self) -> u64 {
        self.held.load(Ordering::Relaxed)
    }

    /// Publish [`Self::held`] as the `felix_broker_raft_tail_cache_bytes`
    /// gauge.
    pub fn report(&self) {
        metrics::gauge!(RAFT_TAIL_CACHE_BYTES).set(self.held() as f64);
    }

    /// Move the total for a log whose cache went from `before` to `after`
    /// bytes. Called with that cache locked, so each log's changes arrive in
    /// order and the total never goes below zero.
    fn moved(&self, before: u64, after: u64) {
        if after >= before {
            self.held.fetch_add(after - before, Ordering::Relaxed);
        } else {
            self.held.fetch_sub(before - after, Ordering::Relaxed);
        }
    }

    /// Free bytes from the logs appended to least recently until the total
    /// is at the low-water mark, never from `keep`. Logs idle since the
    /// previous pass are emptied first, then the busy ones trimmed. A cache
    /// in use is skipped, not waited for: no log's lock is held while
    /// another's is taken. Does nothing if another append is already running
    /// a pass.
    fn evict(&self, keep: &Tail) {
        if self.evicting.swap(true, Ordering::Acquire) {
            return;
        }
        #[cfg(test)]
        self.passes.fetch_add(1, Ordering::Relaxed);
        let since = self
            .last_pass
            .swap(self.clock.load(Ordering::Relaxed), Ordering::Relaxed);
        // Each log's time is read once: appends move it while this runs,
        // and a sort whose keys change under it may panic.
        let mut victims: Vec<(u64, Arc<Tail>)> = {
            let logs = self.logs.lock();
            logs.iter()
                .filter_map(Weak::upgrade)
                .filter(|log| !std::ptr::eq(Arc::as_ptr(log), keep) && log.held() > 0)
                .map(|log| (log.last_append.load(Ordering::Relaxed), log))
                .collect()
        };
        victims.sort_unstable_by_key(|(appended, _)| *appended);
        let low_water = self.limit - self.limit / 8;
        // Oldest first, so the idle logs are a prefix.
        for (appended, victim) in victims {
            if self.held() <= low_water {
                break;
            }
            if appended <= since {
                victim.free_if_unused(|cache| cache.clear());
            } else {
                victim.free_if_unused(|cache| cache.trim_to(cache.bytes / 2));
            }
        }
        self.evicting.store(false, Ordering::Release);
    }
}

/// One log's cache and its place in a [`TailBudget`].
pub(super) struct Tail {
    budget: Arc<TailBudget>,
    cache: parking_lot::Mutex<TailCache>,
    /// The cache's bytes, readable without its lock.
    bytes: AtomicU64,
    /// The budget's clock at this log's latest append; 0 if it has none.
    last_append: AtomicU64,
}

impl Tail {
    pub(super) fn new(budget: Arc<TailBudget>) -> Arc<Self> {
        let tail = Arc::new(Self {
            budget,
            cache: parking_lot::Mutex::default(),
            bytes: AtomicU64::new(0),
            last_append: AtomicU64::new(0),
        });
        let mut logs = tail.budget.logs.lock();
        if logs.len() == logs.capacity() {
            logs.retain(|log| log.strong_count() > 0);
        }
        logs.push(Arc::downgrade(&tail));
        drop(logs);
        tail
    }

    fn held(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Take records just written. See [`TailCache::extend`].
    pub(super) fn extend(&self, cuts: u64, records: impl IntoIterator<Item = LogRecord>) {
        {
            let mut cache = self.cache.lock();
            let before = cache.bytes;
            cache.extend(cuts, records);
            self.bytes.store(cache.bytes, Ordering::Relaxed);
            self.budget.moved(before, cache.bytes);
        }
        let now = self.budget.clock.fetch_add(1, Ordering::Relaxed) + 1;
        self.last_append.store(now, Ordering::Relaxed);
        // The lock above is released: eviction locks other logs' caches.
        if self.budget.held() > self.budget.limit {
            self.budget.evict(self);
        }
    }

    pub(super) fn clear(&self) {
        self.free(self.cache.lock(), |cache| cache.clear());
    }

    /// Shrink the cache with `shrink` unless a read or append has it.
    fn free_if_unused(&self, shrink: impl FnOnce(&mut TailCache)) {
        if let Some(cache) = self.cache.try_lock() {
            self.free(cache, shrink);
        }
    }

    fn free(
        &self,
        mut cache: parking_lot::MutexGuard<'_, TailCache>,
        shrink: impl FnOnce(&mut TailCache),
    ) {
        let before = cache.bytes;
        shrink(&mut cache);
        self.bytes.store(cache.bytes, Ordering::Relaxed);
        self.budget.moved(before, cache.bytes);
    }

    pub(super) fn range(&self, cuts: u64, start: Offset, end: Offset) -> Option<Vec<LogRecord>> {
        self.cache.lock().range(cuts, start, end)
    }
}

impl Drop for Tail {
    fn drop(&mut self) {
        let bytes = self.cache.get_mut().bytes;
        self.budget.moved(bytes, 0);
    }
}

#[derive(Default)]
pub(super) struct TailCache {
    /// `DiskLog::cuts` as read before the oldest record here was written.
    /// While the log still reports it, nothing has been cut since, so the
    /// log still holds every record here.
    cuts: u64,
    records: VecDeque<LogRecord>,
    bytes: u64,
}

impl TailCache {
    /// Take records just written, keeping at most [`CAPACITY_BYTES`].
    /// `cuts` is the log's count from before the write. Anything held from
    /// before a cut, or not directly below these, is dropped.
    fn extend(&mut self, cuts: u64, records: impl IntoIterator<Item = LogRecord>) {
        for record in records {
            let follows = self
                .records
                .back()
                .is_some_and(|last| last.offset + 1 == record.offset);
            if self.cuts != cuts || !follows {
                self.clear();
                self.cuts = cuts;
            }
            let size = size_of_record(&record);
            self.bytes += size;
            self.records.push_back(record);
        }
        self.trim_to(CAPACITY_BYTES);
    }

    /// Drop the oldest records until at most `bytes` are held, though never
    /// the newest one unless it alone exceeds the capacity.
    fn trim_to(&mut self, bytes: u64) {
        while self.bytes > bytes && self.records.len() > 1 {
            if let Some(oldest) = self.records.pop_front() {
                self.bytes -= size_of_record(&oldest);
            }
        }
        if self.bytes > CAPACITY_BYTES {
            self.clear();
        }
    }

    fn clear(&mut self) {
        self.records.clear();
        self.bytes = 0;
    }

    /// The records from `start` up to `end`, or as many of them as are
    /// held. `None` unless the one at `start` is held and the log's count
    /// of cuts is still `cuts`.
    fn range(&self, cuts: u64, start: Offset, end: Offset) -> Option<Vec<LogRecord>> {
        if self.cuts != cuts {
            return None;
        }
        let skip = start.checked_sub(self.records.front()?.offset)?;
        let skip = usize::try_from(skip).ok()?;
        if skip >= self.records.len() {
            return None;
        }
        let take = usize::try_from(end.saturating_sub(start)).unwrap_or(usize::MAX);
        Some(self.records.iter().skip(skip).take(take).cloned().collect())
    }
}

fn size_of_record(record: &LogRecord) -> u64 {
    let headers = record.headers.as_ref().map_or(0, |headers| headers.len());
    let publisher = record
        .publisher
        .as_ref()
        .map_or(0, |publisher| publisher.len());
    (std::mem::size_of::<LogRecord>() + record.payload.len() + headers + publisher) as u64
}

#[cfg(test)]
mod tests;

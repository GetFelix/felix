//! Background compaction for the cache and counter stores: the I/O budget it
//! spends from, and the tasks it runs on.
//!
//! A write that leaves a log over its threshold only starts a pass; it never
//! waits for one. The pass runs here, paced by a byte budget, and takes the
//! shard lock only for short staging steps. See `docs/durable-storage.md`,
//! "Cache and counter compaction".

use std::sync::OnceLock;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::log::SegmentDescriptor;
use crate::segment::format::{RECORD_HEADER_LEN, SEGMENT_HEADER_LEN};

/// Environment variable naming the compaction budget, in bytes per second.
pub(crate) const BYTES_PER_SEC_ENV: &str = "FELIX_STORAGE_COMPACTION_BYTES_PER_SEC";

/// Default budget. Enough to rewrite a large cache in seconds, low enough that
/// a compaction cannot take the device away from the writers it serves.
const DEFAULT_BYTES_PER_SEC: u64 = 64 * 1024 * 1024;

/// Records copied per staging step. Bounds how long the shard lock is held.
pub(crate) const COPY_BATCH: usize = 64;

/// The compaction tasks of one store, and the budget they share.
#[derive(Debug)]
pub(crate) struct Compactor {
    /// Zero means unlimited.
    bytes_per_sec: u64,
    /// When the budget next has room: the pacer's debt, as a point in time.
    next_free: Mutex<Option<Instant>>,
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    #[cfg(test)]
    held: watch::Sender<bool>,
}

impl Compactor {
    pub(crate) fn from_env() -> Self {
        Self::with_budget(configured_bytes_per_sec())
    }

    pub(crate) fn with_budget(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            next_free: Mutex::new(None),
            stop: watch::channel(false).0,
            tasks: Mutex::new(Vec::new()),
            #[cfg(test)]
            held: watch::channel(false).0,
        }
    }

    /// Run `work` on its own task. Without a runtime there is nowhere to run
    /// it, and the log simply stays uncompacted until the next trigger.
    pub(crate) fn spawn<F>(&self, work: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if *self.stop.borrow() {
            return false;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return false;
        };
        let mut tasks = self.tasks.lock();
        tasks.retain(|task| !task.is_finished());
        tasks.push(runtime.spawn(work));
        true
    }

    /// Wait until `bytes` of compaction I/O fit the budget. Returns false once
    /// the store is shutting down, and the caller abandons the pass.
    pub(crate) async fn spend(&self, bytes: u64) -> bool {
        let mut stop = self.stop.subscribe();
        #[cfg(test)]
        {
            let mut held = self.held.subscribe();
            tokio::select! {
                _ = held.wait_for(|held| !*held) => {}
                _ = stop.wait_for(|stop| *stop) => return false,
            }
        }
        if *stop.borrow() {
            return false;
        }
        if self.bytes_per_sec == 0 {
            return true;
        }
        let cost = Duration::from_secs_f64(bytes as f64 / self.bytes_per_sec as f64);
        let start = {
            let now = Instant::now();
            let mut next_free = self.next_free.lock();
            let start = next_free.map_or(now, |free| free.max(now));
            *next_free = Some(start + cost);
            start
        };
        tokio::select! {
            _ = tokio::time::sleep_until(start) => true,
            _ = stop.wait_for(|stop| *stop) => false,
        }
    }

    pub(crate) fn stopping(&self) -> bool {
        *self.stop.borrow()
    }

    /// Stop starting passes, and wait for the running ones to finish their
    /// current step. A pass cut short leaves only redundant copies behind.
    pub(crate) async fn shutdown(&self) {
        self.stop.send_replace(true);
        let tasks = std::mem::take(&mut *self.tasks.lock());
        for task in tasks {
            let _ = task.await;
        }
    }

    /// Wait for every pass started so far.
    #[cfg(test)]
    pub(crate) async fn idle(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.tasks.lock());
            if tasks.is_empty() {
                return;
            }
            for task in tasks {
                let _ = task.await;
            }
        }
    }

    /// Park every pass at its next budget check until [`Compactor::release`].
    /// Stands in for a compaction that is arbitrarily slow.
    #[cfg(test)]
    pub(crate) fn hold(&self) {
        self.held.send_replace(true);
    }

    #[cfg(test)]
    pub(crate) fn release(&self) {
        self.held.send_replace(false);
    }
}

/// Record payload bytes in a segment compaction removed, which is what the
/// stores' `log_bytes` counts. Neither store writes producer tags.
pub(crate) fn payload_bytes(segment: &SegmentDescriptor) -> u64 {
    let records = segment.last_offset + 1 - segment.base_offset;
    segment
        .size_bytes
        .saturating_sub(SEGMENT_HEADER_LEN + records * RECORD_HEADER_LEN)
}

fn configured_bytes_per_sec() -> u64 {
    static VALUE: OnceLock<u64> = OnceLock::new();
    *VALUE.get_or_init(|| match std::env::var(BYTES_PER_SEC_ENV) {
        Ok(raw) => raw.trim().parse().unwrap_or_else(|_| {
            tracing::warn!(
                value = %raw,
                "{BYTES_PER_SEC_ENV} is not a byte count; using the default",
            );
            DEFAULT_BYTES_PER_SEC
        }),
        Err(_) => DEFAULT_BYTES_PER_SEC,
    })
}

#[cfg(test)]
mod tests;

//! A cache watch's resume position, counters and end cause, readable while
//! another thread is blocked in `recv`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub(crate) struct WatchProgress {
    resume_from: AtomicU64,
    moves: AtomicU64,
    reconnects: AtomicU64,
    error: Mutex<Option<String>>,
}

impl WatchProgress {
    pub(crate) fn of(watch: &felix_client::ClusterCacheWatch) -> Self {
        let progress = Self::default();
        progress.record(watch);
        progress
    }

    /// Called after each read, while the watch is still held.
    pub(crate) fn record(&self, watch: &felix_client::ClusterCacheWatch) {
        self.resume_from
            .store(watch.resume_from(), Ordering::Release);
        self.moves.store(watch.moves(), Ordering::Release);
        self.reconnects.store(watch.reconnects(), Ordering::Release);
        if let Some(err) = watch.error() {
            *self.error.lock().unwrap_or_else(|e| e.into_inner()) = Some(format!("{err:#}"));
        }
    }

    pub(crate) fn resume_from(&self) -> u64 {
        self.resume_from.load(Ordering::Acquire)
    }

    pub(crate) fn moves(&self) -> u64 {
        self.moves.load(Ordering::Acquire)
    }
}

impl WatchProgress {
    pub(crate) fn reconnects(&self) -> u64 {
        self.reconnects.load(Ordering::Acquire)
    }

    pub(crate) fn error(&self) -> Option<String> {
        self.error.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

//! A cache watch's resume position and move count, readable while another
//! thread is blocked in `recv`.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub(crate) struct WatchProgress {
    resume_from: AtomicU64,
    moves: AtomicU64,
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
    }

    pub(crate) fn resume_from(&self) -> u64 {
        self.resume_from.load(Ordering::Acquire)
    }

    pub(crate) fn moves(&self) -> u64 {
        self.moves.load(Ordering::Acquire)
    }
}

//! A watch's or subscription's counters and end cause, readable while another
//! thread is blocked in a read.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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

/// What a single-shard subscription reports about itself, updated after each
/// read and readable while a read is in flight.
#[derive(Default)]
pub(crate) struct SubscriptionProgress {
    moves: AtomicU64,
    reconnects: AtomicU64,
    stream_deleted: AtomicBool,
    // `u64::MAX` is "not granted"; a granted capacity is a `u32`.
    queue_capacity: AtomicU64,
}

impl SubscriptionProgress {
    pub(crate) fn of(subscription: &felix_client::ClusterSubscription) -> Self {
        let progress = Self::default();
        progress.record(subscription);
        progress
    }

    pub(crate) fn record(&self, subscription: &felix_client::ClusterSubscription) {
        self.moves.store(subscription.moves(), Ordering::Release);
        self.reconnects
            .store(subscription.reconnects(), Ordering::Release);
        self.stream_deleted
            .store(subscription.stream_deleted(), Ordering::Release);
        self.queue_capacity.store(
            subscription.queue_capacity().map_or(u64::MAX, u64::from),
            Ordering::Release,
        );
    }

    pub(crate) fn moves(&self) -> u64 {
        self.moves.load(Ordering::Acquire)
    }

    pub(crate) fn reconnects(&self) -> u64 {
        self.reconnects.load(Ordering::Acquire)
    }

    pub(crate) fn stream_deleted(&self) -> bool {
        self.stream_deleted.load(Ordering::Acquire)
    }

    pub(crate) fn queue_capacity(&self) -> Option<u32> {
        u32::try_from(self.queue_capacity.load(Ordering::Acquire)).ok()
    }
}

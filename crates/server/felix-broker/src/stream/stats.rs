//! What an operator can read about one subscriber without touching its queue.
//!
//! Written from two places that already exist: the fanout counts a drop where
//! it already counts one in metrics, and the receiver notes how far it got
//! each time it takes a batch. Each is one relaxed atomic operation on this
//! subscriber's own counter, and neither takes a lock.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Nothing taken from the queue yet.
const NO_POSITION: u64 = u64::MAX;

/// One subscriber's counters, shared by its registry entry and its receiver.
#[derive(Debug)]
pub struct SubscriberStats {
    created: Instant,
    dropped: AtomicU64,
    position: AtomicU64,
    owner: OnceLock<SubscriberOwner>,
}

/// Who a subscriber delivers to, as the serving layer knows it. The broker
/// core never sees connections, so the serving layer attaches this once the
/// subscription exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriberOwner {
    /// The id the client sees on its events.
    pub subscription_id: u64,
    pub connection_id: u64,
    pub peer: String,
    /// The principal the connection authenticated as; `None` without auth.
    pub principal: Option<String>,
}

impl Default for SubscriberStats {
    fn default() -> Self {
        Self {
            created: Instant::now(),
            dropped: AtomicU64::new(0),
            position: AtomicU64::new(NO_POSITION),
            owner: OnceLock::new(),
        }
    }
}

impl SubscriberStats {
    /// Count `records` dropped because the queue was full.
    pub(crate) fn dropped(&self, records: u64) {
        self.dropped.fetch_add(records, Ordering::Relaxed);
    }

    /// Note that everything below `next` has been taken from the queue.
    pub(crate) fn taken_below(&self, next: u64) {
        self.position.store(next, Ordering::Relaxed);
    }

    /// Records dropped since the subscription started.
    pub fn dropped_records(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// One past the last offset taken from the queue for delivery. `None`
    /// until a batch with offsets has been taken.
    pub fn position(&self) -> Option<u64> {
        let position = self.position.load(Ordering::Relaxed);
        (position != NO_POSITION).then_some(position)
    }

    /// How long ago the subscriber registered.
    pub fn age(&self) -> Duration {
        self.created.elapsed()
    }

    /// Who it delivers to, once the serving layer has said.
    pub fn owner(&self) -> Option<&SubscriberOwner> {
        self.owner.get()
    }

    /// Record who it delivers to. Only the first call counts.
    pub(crate) fn set_owner(&self, owner: SubscriberOwner) {
        let _ = self.owner.set(owner);
    }
}

#[cfg(test)]
mod tests;

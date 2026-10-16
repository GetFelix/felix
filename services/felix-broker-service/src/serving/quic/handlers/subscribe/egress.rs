//! The byte bound on what one connection's subscriptions have waiting in the
//! writer lanes and the connection writer.
//!
//! The broker queue's byte bound (`FELIX_SUB_QUEUE_MAX_BYTES`) counts a batch
//! until the feeder hands its frame to the writer lanes. From when the feeder
//! takes it until its frame is written it counts here. Each subscription may hold an equal share of the connection's
//! budget, and a feeder waits only for its own subscription's frames to be
//! written. A subscription whose stream stalls fills its share and stops
//! there, its broker queue fills and sheds as usual, and the others on the
//! connection never wait for it (under the `drop_new` lane policy; `block`
//! stops the connection writer, see `writer.rs`). A shared pool would let
//! one stalled subscription take all of it.
//!
//! A frame larger than the share still goes once the subscription has
//! nothing else waiting, so each subscription can always have one batch in
//! flight.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use felix_broker::HeldBytes;
use tokio::sync::Notify;

/// One connection's budget. Zero leaves the lanes bounded by entries only.
#[derive(Debug)]
pub(crate) struct EgressBudget {
    max: usize,
    subscriptions: AtomicUsize,
}

impl EgressBudget {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            max,
            subscriptions: AtomicUsize::new(0),
        }
    }

    /// A share for one subscription, for as long as it is live.
    pub(super) fn join(self: &Arc<Self>) -> EgressShare {
        self.subscriptions.fetch_add(1, Ordering::Relaxed);
        EgressShare {
            budget: Arc::clone(self),
            held: Arc::new(ShareHeld {
                bytes: AtomicUsize::new(0),
                waiting: AtomicUsize::new(0),
                freed: Notify::new(),
                gauge: metrics::gauge!("felix_sub_egress_bytes"),
            }),
        }
    }

    fn share(&self) -> usize {
        self.max / self.subscriptions.load(Ordering::Relaxed).max(1)
    }
}

/// One subscription's part of its connection's budget.
#[derive(Debug)]
pub(super) struct EgressShare {
    budget: Arc<EgressBudget>,
    held: Arc<ShareHeld>,
}

#[derive(Debug)]
struct ShareHeld {
    bytes: AtomicUsize,
    /// Feeders parked in [`EgressShare::hold`]. A release notifies only when
    /// this is nonzero, so the common case takes no lock.
    waiting: AtomicUsize,
    freed: Notify,
    gauge: metrics::Gauge,
}

impl EgressShare {
    /// Hold `bytes` until the returned value is dropped, waiting while the
    /// subscription's own frames fill its share.
    pub(super) async fn hold(&self, bytes: usize) -> EgressBytes {
        if let Some(held) = self.try_hold(bytes) {
            return held;
        }
        let _waiting = Waiting::new(&self.held.waiting);
        loop {
            let freed = self.held.freed.notified();
            tokio::pin!(freed);
            // Counted as waiting and registered before the check, so a write
            // finishing in between either sees the count and notifies, or
            // its release is visible to the check.
            freed.as_mut().enable();
            if let Some(held) = self.try_hold(bytes) {
                return held;
            }
            freed.await;
        }
    }

    /// [`Self::hold`] without waiting. Only the subscription's feeder holds,
    /// so nothing else can take the room between the check and the add.
    pub(super) fn try_hold(&self, bytes: usize) -> Option<EgressBytes> {
        if self.budget.max == 0 || bytes == 0 {
            return Some(EgressBytes::default());
        }
        let used = self.held.bytes.load(Ordering::SeqCst);
        if used > 0 && used.saturating_add(bytes) > self.budget.share() {
            return None;
        }
        self.held.bytes.fetch_add(bytes, Ordering::AcqRel);
        self.held.gauge.increment(bytes as f64);
        Some(EgressBytes {
            bytes,
            held: Some(Arc::clone(&self.held)),
            queue: HeldBytes::default(),
        })
    }
}

/// Counts a feeder as waiting for as long as it is parked, including when
/// its wait is cancelled.
struct Waiting<'a>(&'a AtomicUsize);

impl<'a> Waiting<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Drop for EgressShare {
    fn drop(&mut self) {
        self.budget.subscriptions.fetch_sub(1, Ordering::Relaxed);
    }
}

/// What a frame in the lanes holds: its bytes against the subscription's
/// share, and what is left of its broker queue hold (the broker-wide bound,
/// when one is set). Both go back when the frame is written or dropped.
#[derive(Debug, Default)]
pub(crate) struct EgressBytes {
    bytes: usize,
    held: Option<Arc<ShareHeld>>,
    queue: HeldBytes,
}

impl EgressBytes {
    /// Take over a batch's broker queue hold, giving back its share of the
    /// subscriber's queue bound: the batch is counted here now.
    pub(super) fn carry(&mut self, mut queue: HeldBytes) {
        queue.release_subscriber();
        self.queue.absorb(queue);
    }

    /// Hold `other`'s bytes as well. Both come from the same subscription.
    pub(super) fn absorb(&mut self, mut other: EgressBytes) {
        if self.held.is_none() {
            self.held = other.held.take();
        }
        self.bytes += std::mem::take(&mut other.bytes);
        self.queue.absorb(std::mem::take(&mut other.queue));
    }
}

impl Drop for EgressBytes {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        if let Some(held) = &self.held {
            // SeqCst pairs with `hold`: either the waiter's check sees this
            // release, or this sees the waiter and wakes it.
            held.bytes.fetch_sub(self.bytes, Ordering::SeqCst);
            if held.waiting.load(Ordering::SeqCst) > 0 {
                held.freed.notify_waiters();
            }
            held.gauge.decrement(self.bytes as f64);
        }
    }
}

#[cfg(test)]
mod tests;

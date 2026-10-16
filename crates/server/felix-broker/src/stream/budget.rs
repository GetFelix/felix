//! Byte bounds on subscriber queues, cache watch queues and replay rings.
//!
//! A queue's entry count says nothing about memory when one entry can be a
//! 16 MiB batch, so every queued batch also holds its payload bytes against
//! the queue's own budget and, when one is set, a budget shared by every
//! queue of that kind on the broker. The bytes go back when the batch leaves
//! the queue, the same moment its entry does.
//!
//! The envelope is shared, so a batch queued for many subscribers is counted
//! once per subscriber: that is what each one is keeping alive.
//!
//! A receiver that queues the batch again on its way out, as the broker
//! service's writer lanes do, takes the bytes with it
//! ([`SubscriptionReceiver::recv_held`](super::SubscriptionReceiver::recv_held))
//! and gives the subscriber's share back once it has handed the batch on
//! ([`HeldBytes::take_subscriber`], [`HeldBytes::release_subscriber`]). The
//! broker-wide share stays held until the batch is written.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A pool of queued payload bytes: one subscriber's, or the broker's.
///
/// A semaphore acquire takes at most `u32::MAX` permits (fewer on 32-bit,
/// where the semaphore's own limit is about 512 MiB), so a larger budget
/// counts in units of `1 << shift` bytes, each batch rounded up to a whole
/// unit. On 64-bit a unit is one byte under 4 GiB.
#[derive(Debug, Clone)]
pub(crate) struct ByteBudget {
    permits: Arc<Semaphore>,
    /// The whole budget, in units.
    max: u32,
    shift: u32,
}

impl ByteBudget {
    /// `None` for zero, which leaves queues unbounded by bytes.
    pub(crate) fn new(max_bytes: usize) -> Option<Self> {
        let limit = (u32::MAX as usize).min(Semaphore::MAX_PERMITS);
        let mut shift = 0;
        while max_bytes >> shift > limit {
            shift += 1;
        }
        let max = u32::try_from(max_bytes >> shift).expect("within u32");
        (max > 0).then(|| Self {
            permits: Arc::new(Semaphore::new(max as usize)),
            max,
            shift,
        })
    }

    /// Bytes not held by any queued batch.
    #[cfg(test)]
    pub(crate) fn available(&self) -> usize {
        self.permits.available_permits() << self.shift
    }

    /// Whole units covering `bytes`, uncapped.
    fn units(&self, bytes: usize) -> usize {
        let partial = bytes & ((1 << self.shift) - 1) != 0;
        (bytes >> self.shift) + usize::from(partial)
    }

    /// A batch larger than the whole budget asks for all of it, so it goes
    /// through once the queue is empty instead of never.
    fn want(&self, bytes: usize) -> u32 {
        u32::try_from(self.units(bytes))
            .unwrap_or(u32::MAX)
            .min(self.max)
    }

    fn try_take(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.permits)
            .try_acquire_many_owned(self.want(bytes))
            .ok()
    }

    async fn take(&self, bytes: usize) -> OwnedSemaphorePermit {
        Arc::clone(&self.permits)
            .acquire_many_owned(self.want(bytes))
            .await
            .expect("byte budget is never closed")
    }
}

/// The byte bounds every queue of one kind is held to: subscriber queues, or
/// cache watch queues.
#[derive(Debug, Clone)]
pub(crate) struct QueueBytes {
    /// Each queue's own bound; zero for none.
    pub(crate) per_subscriber: usize,
    /// Shared by every queue of this kind on the broker.
    pub(crate) broker: Option<ByteBudget>,
    /// The gauge that reports what these queues hold.
    gauge: &'static str,
}

impl QueueBytes {
    /// `broker` of zero leaves the queues without a shared bound.
    pub(crate) fn new(per_subscriber: usize, broker: usize, gauge: &'static str) -> Self {
        Self {
            per_subscriber,
            broker: ByteBudget::new(broker),
            gauge,
        }
    }

    /// The budgets for one newly registered queue.
    pub(crate) fn for_subscriber(&self) -> SubscriberBytes {
        SubscriberBytes {
            own: ByteBudget::new(self.per_subscriber),
            broker: self.broker.clone(),
            gauge: metrics::gauge!(self.gauge),
        }
    }
}

impl Default for QueueBytes {
    /// No bounds.
    fn default() -> Self {
        Self::new(0, 0, "felix_sub_queue_bytes")
    }
}

/// One queue's budgets.
#[derive(Debug, Clone)]
pub(crate) struct SubscriberBytes {
    own: Option<ByteBudget>,
    broker: Option<ByteBudget>,
    /// Looked up once here rather than on every batch.
    gauge: metrics::Gauge,
}

impl SubscriberBytes {
    /// Bytes this subscriber's queue may still take before its own bound.
    /// `None` when it has none.
    #[cfg(test)]
    fn own_available(&self) -> Option<usize> {
        self.own.as_ref().map(ByteBudget::available)
    }

    /// Hold `bytes` if both budgets have room, without waiting. On failure,
    /// says which one was full and holds nothing.
    pub(crate) fn try_hold(&self, bytes: usize) -> Result<HeldBytes, ByteBound> {
        let own = match &self.own {
            Some(budget) => Some(budget.try_take(bytes).ok_or(ByteBound::Subscriber)?),
            None => None,
        };
        let broker = match &self.broker {
            Some(budget) => Some(budget.try_take(bytes).ok_or(ByteBound::Broker)?),
            None => None,
        };
        Ok(HeldBytes::new(bytes, own, broker, &self.gauge))
    }

    /// Hold `bytes`, waiting for room in both budgets.
    ///
    /// Own budget first: waiting on the broker's while holding this
    /// subscriber's cannot deadlock, since only this subscriber's consumer
    /// gives its own bytes back.
    pub(crate) async fn hold(&self, bytes: usize) -> HeldBytes {
        let own = match &self.own {
            Some(budget) => Some(budget.take(bytes).await),
            None => None,
        };
        let broker = match &self.broker {
            Some(budget) => Some(budget.take(bytes).await),
            None => None,
        };
        HeldBytes::new(bytes, own, broker, &self.gauge)
    }
}

/// Which byte bound turned a batch away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ByteBound {
    Subscriber,
    Broker,
}

impl ByteBound {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Subscriber => "subscriber",
            Self::Broker => "broker",
        }
    }
}

/// Bytes one or more queued batches hold against a subscriber's byte
/// bounds, given back on drop.
#[derive(Debug)]
pub struct HeldBytes {
    /// What the gauge counts: zero once the subscriber's share is released.
    bytes: usize,
    gauge: metrics::Gauge,
    own: Option<OwnedSemaphorePermit>,
    _broker: Option<OwnedSemaphorePermit>,
}

impl HeldBytes {
    fn new(
        bytes: usize,
        own: Option<OwnedSemaphorePermit>,
        broker: Option<OwnedSemaphorePermit>,
        gauge: &metrics::Gauge,
    ) -> Self {
        if bytes > 0 {
            gauge.increment(bytes as f64);
        }
        Self {
            bytes,
            gauge: if bytes > 0 {
                gauge.clone()
            } else {
                metrics::Gauge::noop()
            },
            own,
            _broker: broker,
        }
    }

    /// Give back the bytes held against the subscriber's own bound, keeping
    /// any broker-wide hold until this is dropped.
    ///
    /// For a caller that has moved the batch out of the subscriber's queue
    /// into one with a bound of its own, as the broker service's writer lanes
    /// do. The subscriber's bound then counts only what is waiting in its
    /// queue, and a batch being written never looks like the subscriber
    /// falling behind. The broker-wide bound, when set, still covers the
    /// batch until it is written.
    pub fn release_subscriber(&mut self) {
        self.own = None;
        if self.bytes > 0 {
            self.gauge.decrement(self.bytes as f64);
            self.bytes = 0;
        }
    }

    /// Split off the bytes held against the subscriber's own bound, leaving
    /// any broker-wide hold here. Dropping what is returned does what
    /// [`Self::release_subscriber`] does, at a time the caller picks.
    pub fn take_subscriber(&mut self) -> HeldBytes {
        HeldBytes {
            bytes: std::mem::take(&mut self.bytes),
            gauge: std::mem::replace(&mut self.gauge, metrics::Gauge::noop()),
            own: self.own.take(),
            _broker: None,
        }
    }

    /// Hold `other`'s bytes as well, until this is dropped.
    ///
    /// Both must come from the same subscriber, as they do when one frame is
    /// built from several of its batches. Panics if they do not.
    pub fn absorb(&mut self, mut other: HeldBytes) {
        if other.bytes > 0 {
            // Moved across without touching the gauge, which counts it once.
            self.gauge = std::mem::replace(&mut other.gauge, metrics::Gauge::noop());
        }
        self.bytes += std::mem::take(&mut other.bytes);
        merge(&mut self.own, other.own.take());
        merge(&mut self._broker, other._broker.take());
    }
}

fn merge(into: &mut Option<OwnedSemaphorePermit>, from: Option<OwnedSemaphorePermit>) {
    match (into.as_mut(), from) {
        (Some(held), Some(more)) => held.merge(more),
        (None, Some(more)) => *into = Some(more),
        (_, None) => {}
    }
}

impl Default for HeldBytes {
    /// Holds nothing.
    fn default() -> Self {
        Self {
            bytes: 0,
            gauge: metrics::Gauge::noop(),
            own: None,
            _broker: None,
        }
    }
}

impl Drop for HeldBytes {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.gauge.decrement(self.bytes as f64);
        }
    }
}

/// Bytes a store that grows at one end and shrinks at the other holds
/// against a shared budget, as one permit. The replay ring uses it.
#[derive(Debug, Default)]
pub(crate) struct PooledBytes {
    budget: Option<ByteBudget>,
    permit: Option<OwnedSemaphorePermit>,
    bytes: usize,
}

impl PooledBytes {
    /// Counts without holding anything back when `budget` is `None`.
    pub(crate) fn new(budget: Option<ByteBudget>) -> Self {
        Self {
            budget,
            permit: None,
            bytes: 0,
        }
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Hold `more` bytes as well, without waiting. `false`, holding nothing
    /// more, when the budget has no room for them.
    pub(crate) fn try_grow(&mut self, more: usize) -> bool {
        if let Some(budget) = &self.budget {
            let held = self.permit.as_ref().map_or(0, |p| p.num_permits());
            let need = budget.units(self.bytes + more).saturating_sub(held);
            if need > 0 {
                let Ok(need) = u32::try_from(need) else {
                    return false;
                };
                match Arc::clone(&budget.permits).try_acquire_many_owned(need) {
                    Ok(more) => merge(&mut self.permit, Some(more)),
                    Err(_) => return false,
                }
            }
        }
        self.bytes += more;
        true
    }

    /// Hold `more` bytes even though the budget has no room for them, taking
    /// what room it has. The shortfall is held against nothing, so the budget
    /// is exceeded by at most `more`, and the first [`Self::shrink`] pays it
    /// back before returning anything. `false`, holding nothing more, when
    /// `more` is larger than the whole budget.
    pub(crate) fn overdraw(&mut self, more: usize) -> bool {
        if let Some(budget) = &self.budget {
            if budget.units(more) > budget.max as usize {
                return false;
            }
            let held = self.permit.as_ref().map_or(0, |p| p.num_permits());
            let need = budget.units(self.bytes + more).saturating_sub(held);
            let take = need.min(budget.permits.available_permits());
            if let Ok(take) = u32::try_from(take)
                && take > 0
                && let Ok(more) = Arc::clone(&budget.permits).try_acquire_many_owned(take)
            {
                merge(&mut self.permit, Some(more));
            }
        }
        self.bytes += more;
        true
    }

    /// Give `less` bytes back.
    pub(crate) fn shrink(&mut self, less: usize) {
        self.bytes = self.bytes.saturating_sub(less);
        if let (Some(budget), Some(permit)) = (&self.budget, self.permit.as_mut()) {
            let keep = budget.units(self.bytes);
            let extra = permit.num_permits().saturating_sub(keep);
            if extra > 0 {
                drop(permit.split(extra));
            }
        }
    }
}

#[cfg(test)]
mod tests;

//! Byte bounds on subscriber queues.
//!
//! A queue's entry count says nothing about memory when one entry can be a
//! 16 MiB batch, so every queued batch also holds its payload bytes against
//! the subscriber's own budget and, when one is set, a budget shared by every
//! subscriber on the broker. The bytes go back when the batch leaves the
//! queue, the same moment its entry does.
//!
//! The envelope is shared, so a batch queued for many subscribers is counted
//! once per subscriber: that is what each one is keeping alive.
//!
//! A receiver that queues the batch again on its way out, as the broker
//! service's writer lanes do, can take the bytes with it
//! ([`SubscriptionReceiver::recv_held`](super::SubscriptionReceiver::recv_held)),
//! so the same bounds cover that queue too.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A pool of queued payload bytes: one subscriber's, or the broker's.
#[derive(Debug, Clone)]
pub(crate) struct ByteBudget {
    permits: Arc<Semaphore>,
    max: u32,
}

impl ByteBudget {
    /// `None` for zero, which leaves queues unbounded by bytes. A semaphore
    /// counts at most `u32::MAX` per acquire, so a larger budget is that.
    pub(crate) fn new(max_bytes: usize) -> Option<Self> {
        let max = u32::try_from(max_bytes).unwrap_or(u32::MAX);
        (max > 0).then(|| Self {
            permits: Arc::new(Semaphore::new(max as usize)),
            max,
        })
    }

    /// Bytes not held by any queued batch.
    #[cfg(test)]
    fn available(&self) -> usize {
        self.permits.available_permits()
    }

    /// A batch larger than the whole budget asks for all of it, so it goes
    /// through once the queue is empty instead of never.
    fn want(&self, bytes: usize) -> u32 {
        u32::try_from(bytes).unwrap_or(u32::MAX).min(self.max)
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

/// The byte bounds every subscriber of a stream is held to.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueueBytes {
    /// Each subscriber's own bound; zero for none.
    pub(crate) per_subscriber: usize,
    /// Shared by every subscriber queue on the broker.
    pub(crate) broker: Option<ByteBudget>,
}

impl QueueBytes {
    /// The budgets for one newly registered subscriber.
    pub(crate) fn for_subscriber(&self) -> SubscriberBytes {
        SubscriberBytes {
            own: ByteBudget::new(self.per_subscriber),
            broker: self.broker.clone(),
        }
    }
}

/// One subscriber's budgets.
#[derive(Debug, Clone, Default)]
pub(crate) struct SubscriberBytes {
    own: Option<ByteBudget>,
    broker: Option<ByteBudget>,
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
        Ok(HeldBytes::new(bytes, own, broker))
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
        HeldBytes::new(bytes, own, broker)
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
#[derive(Debug, Default)]
pub struct HeldBytes {
    bytes: usize,
    _own: Option<OwnedSemaphorePermit>,
    _broker: Option<OwnedSemaphorePermit>,
}

impl HeldBytes {
    fn new(
        bytes: usize,
        own: Option<OwnedSemaphorePermit>,
        broker: Option<OwnedSemaphorePermit>,
    ) -> Self {
        if bytes > 0 {
            metrics::gauge!("felix_sub_queue_bytes").increment(bytes as f64);
        }
        Self {
            bytes,
            _own: own,
            _broker: broker,
        }
    }

    /// Hold `other`'s bytes as well, until this is dropped.
    ///
    /// Both must come from the same subscriber, as they do when one frame is
    /// built from several of its batches. Panics if they do not.
    pub fn absorb(&mut self, mut other: HeldBytes) {
        self.bytes += std::mem::take(&mut other.bytes);
        merge(&mut self._own, other._own.take());
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

impl Drop for HeldBytes {
    fn drop(&mut self) {
        if self.bytes > 0 {
            metrics::gauge!("felix_sub_queue_bytes").decrement(self.bytes as f64);
        }
    }
}

#[cfg(test)]
mod tests;

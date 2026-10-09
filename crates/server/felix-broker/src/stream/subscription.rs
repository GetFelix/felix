//! Subscriber-facing handles: the receiver half plus the guard that
//! unregisters the subscriber from its stream on drop.

use std::collections::VecDeque;
use std::sync::{Arc, OnceLock, Weak};

use bytes::Bytes;
use tokio::sync::mpsc;

use super::delivery::{DeliveryEnvelope, QueuedDelivery};
use super::state::StreamState;
use super::stats::{SubscriberOwner, SubscriberStats};
use crate::handoff::ShardMoved;

/// Receiver wrapper that keeps the unsubscribe guard alive for the receiver lifetime.
#[derive(Debug)]
pub struct Subscription {
    pub(crate) receiver: SubscriptionReceiver,
    pub(crate) guard: SubscriptionGuard,
    pub(crate) pending: VecDeque<Bytes>,
    /// Live records below this offset are dropped.
    ///
    /// A publish claims its disk offsets before the record reaches the replay
    /// ring, so a cursor taken from the durable tail can name an offset the ring
    /// has not seen. Registering there yields an empty backlog and then delivers
    /// that in-flight record *live*, below the position the caller was told to
    /// resume from -- one record seen twice by anyone resuming from a
    /// checkpoint.
    ///
    /// The backlog cannot be widened to include it (it is not in the ring yet)
    /// and the cursor cannot be narrowed to exclude it (the ring can also lag
    /// permanently, when a cancelled publish consumes offsets it never
    /// delivers, and a cursor behind the ring's oldest entry is rejected as too
    /// old). Dropping it on arrival is what closes the window without breaking
    /// either.
    ///
    /// `None` for streams whose deliveries carry no offsets: an in-memory
    /// stream's cursor comes from the ring itself and so cannot overshoot.
    pub(crate) skip_below: Option<u64>,
}

impl Subscription {
    /// The next record, or `None` once the subscription has ended.
    ///
    /// **`None` means the channel closed, and nothing else.** Every caller
    /// treats it as the end of the stream, so a batch that happens to yield no
    /// records must not produce one: a batch landing entirely below
    /// the resume point is ordinary during a resume, and reporting it as an
    /// end of stream loses every record after the cursor.
    pub async fn recv(&mut self) -> Option<Bytes> {
        loop {
            if let Some(payload) = self.pending.pop_front() {
                return Some(payload);
            }
            // Terminates: each turn consumes one envelope from a finite
            // channel, and a closed one ends the loop through `?`.
            let envelope = self.receiver.recv().await?;
            self.extend_pending(&envelope);
        }
    }

    /// The next record if one is already queued.
    ///
    /// `Empty` means nothing is waiting, so — as in [`Self::recv`] — a batch
    /// that yielded no records is skipped rather than reported: the queue may
    /// still hold the record the caller is after.
    pub fn try_recv(&mut self) -> std::result::Result<Bytes, mpsc::error::TryRecvError> {
        loop {
            if let Some(payload) = self.pending.pop_front() {
                return Ok(payload);
            }
            let envelope = self.receiver.try_recv()?;
            self.extend_pending(&envelope);
        }
    }

    /// Take whatever is already queued, without waiting.
    ///
    /// Used by resume to drain what accumulated while history was being read,
    /// so the handler can spot a queue drop -- a jump in offsets -- and fill it
    /// from disk before live delivery starts.
    pub fn drain_ready(&mut self) -> Vec<DeliveryEnvelope> {
        let mut drained = Vec::new();
        while let Ok(envelope) = self.receiver.try_recv() {
            drained.push(envelope);
        }
        drained
    }

    /// Forget queue drops below `offset`: the caller filled them from the
    /// log, so they no longer end the subscription (see
    /// [`SubscriptionReceiver::end_on_lag`]).
    pub fn covered_below(&self, offset: u64) {
        self.receiver.lag.covered_below(offset);
    }

    /// Say who this subscription delivers to, for an operator listing
    /// subscriptions. Only the first call counts.
    pub fn set_owner(&self, owner: SubscriberOwner) {
        self.receiver.stats.set_owner(owner);
    }

    /// Split into the batch receiver and the guard that keeps the
    /// subscriber registered.
    ///
    /// The receiver keeps dropping records below the resume point, so a
    /// caller that delivers from it directly sees nothing below the
    /// `start_offset` it was told.
    pub fn into_parts(self) -> (SubscriptionReceiver, SubscriptionGuard) {
        let mut receiver = self.receiver;
        receiver.skip_below = self.skip_below;
        (receiver, self.guard)
    }

    /// Queue an envelope's payloads, dropping any below the resume point.
    ///
    /// Deliveries arrive in offset order, so once one lands at or above the
    /// cursor the filter has done its job and is retired -- the check costs
    /// nothing for the rest of the subscription's life.
    fn extend_pending(&mut self, envelope: &DeliveryEnvelope) {
        let payloads = envelope.payloads();
        match (self.skip_below, envelope.base_offset()) {
            (Some(skip), Some(base)) if base < skip => {
                // `skip - base` payloads of this batch precede the resume point.
                // A batch can straddle it, so this drops a prefix rather than
                // the whole envelope.
                let drop = (skip - base).min(payloads.len() as u64) as usize;
                self.pending.extend(payloads[drop..].iter().cloned());
                if (base + payloads.len() as u64) > skip {
                    self.skip_below = None;
                }
            }
            _ => {
                self.skip_below = None;
                self.pending.extend(payloads.iter().cloned());
            }
        }
    }
}

/// The receiving end of a subscription, yielding whole batches.
#[derive(Debug)]
pub struct SubscriptionReceiver {
    pub(crate) receiver: mpsc::Receiver<QueuedDelivery>,
    moved: Arc<OnceLock<ShardMoved>>,
    lag: Arc<Lag>,
    stats: Arc<SubscriberStats>,
    /// Set by [`Self::end_on_lag`].
    end_on_lag: bool,
    /// [`Subscription::skip_below`], carried over by
    /// [`Subscription::into_parts`]. `None` while the receiver is still inside
    /// a `Subscription`, which filters per record instead.
    skip_below: Option<u64>,
}

impl SubscriptionReceiver {
    pub(crate) fn new(
        receiver: mpsc::Receiver<QueuedDelivery>,
        moved: Arc<OnceLock<ShardMoved>>,
        lag: Arc<Lag>,
        stats: Arc<SubscriberStats>,
    ) -> Self {
        Self {
            receiver,
            moved,
            lag,
            stats,
            end_on_lag: false,
            skip_below: None,
        }
    }

    /// Where to resume once this subscriber's queue has dropped a batch of a
    /// durable stream: the first dropped offset not covered by
    /// [`Subscription::covered_below`]. Every batch queued before that drop
    /// is below it.
    pub fn lagged(&self) -> Option<u64> {
        self.lag.first_dropped()
    }

    /// End the subscription at its first queue drop: [`Self::recv`] yields
    /// the batches queued before it and then `None`, even while nothing more
    /// is published, and [`Self::lagged`] says where to resume. Nothing at or
    /// above that offset is yielded.
    pub fn end_on_lag(&mut self) {
        self.end_on_lag = true;
    }

    /// Where to resume, when [`Self::end_on_lag`] ended the subscription.
    pub fn lag_ended(&self) -> Option<u64> {
        self.end_on_lag.then(|| self.lagged()).flatten()
    }

    /// Why the subscription ended, when it ended because its shard moved.
    ///
    /// Set before the queue closes, so once [`Self::recv`] has returned
    /// `None` this is final.
    pub fn moved(&self) -> Option<&ShardMoved> {
        self.moved.get()
    }

    /// The next batch, or `None` once the subscription has ended.
    ///
    /// Like [`Subscription::recv`], a batch wholly below the resume point is
    /// skipped rather than reported as the end.
    pub async fn recv(&mut self) -> Option<DeliveryEnvelope> {
        loop {
            let envelope = if self.end_on_lag {
                self.recv_before_lag().await?
            } else {
                self.receiver.recv().await?.into_envelope()
            };
            if let Some(envelope) = self.admit(envelope) {
                return Some(envelope);
            }
        }
    }

    /// The next batch if one is already queued.
    pub fn try_recv(&mut self) -> std::result::Result<DeliveryEnvelope, mpsc::error::TryRecvError> {
        loop {
            let envelope = self.receiver.try_recv()?.into_envelope();
            if self.past_lag(&envelope) {
                return Err(mpsc::error::TryRecvError::Disconnected);
            }
            if let Some(envelope) = self.admit(envelope) {
                return Ok(envelope);
            }
        }
    }

    /// Like `recv`, but `None` once the queue has dropped a batch and what
    /// was queued before the drop is gone. Waking on the drop is what lets a
    /// subscription end when nothing is published after it.
    async fn recv_before_lag(&mut self) -> Option<DeliveryEnvelope> {
        loop {
            let lag = Arc::clone(&self.lag);
            let dropped = lag.notify.notified();
            tokio::pin!(dropped);
            // Registered before the check, so a drop in between still wakes.
            dropped.as_mut().enable();
            let envelope = if lag.first_dropped().is_some() {
                self.receiver.try_recv().ok()?.into_envelope()
            } else {
                tokio::select! {
                    biased;
                    queued = self.receiver.recv() => queued?.into_envelope(),
                    () = &mut dropped => continue,
                }
            };
            return (!self.past_lag(&envelope)).then_some(envelope);
        }
    }

    /// Whether `envelope` is at or past the first drop, when the
    /// subscription ends there. The queue may have had room again for later
    /// batches; delivering them would leave a hole below them.
    fn past_lag(&mut self, envelope: &DeliveryEnvelope) -> bool {
        let past = self.end_on_lag
            && self
                .lag
                .first_dropped()
                .is_some_and(|first| envelope.base_offset().is_none_or(|base| base >= first));
        if past {
            self.receiver.close();
        }
        past
    }

    /// Note how far delivery has got, for an operator. One relaxed store
    /// per batch, and only for a batch with offsets.
    fn taken(&self, envelope: &DeliveryEnvelope) {
        if let Some(base) = envelope.base_offset() {
            self.stats.taken_below(base + envelope.len() as u64);
        }
    }

    /// Drop what lies below the resume point: the whole batch, or the prefix
    /// of one that straddles it. Same rule as `Subscription::extend_pending`.
    fn admit(&mut self, envelope: DeliveryEnvelope) -> Option<DeliveryEnvelope> {
        self.taken(&envelope);
        let (Some(skip), Some(base)) = (self.skip_below, envelope.base_offset()) else {
            self.skip_below = None;
            return Some(envelope);
        };
        if base >= skip {
            self.skip_below = None;
            return Some(envelope);
        }
        let end = base + envelope.len() as u64;
        if end <= skip {
            return None;
        }
        self.skip_below = None;
        Some(envelope.skip_records((skip - base) as usize))
    }
}

impl Drop for SubscriptionReceiver {
    fn drop(&mut self) {
        self.receiver.close();
    }
}

/// RAII handle that unregisters a stream subscriber on drop.
#[derive(Debug)]
pub struct SubscriptionGuard {
    pub(crate) stream_state: Weak<StreamState>,
    pub(crate) subscriber_id: u64,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        if let Some(stream_state) = self.stream_state.upgrade() {
            stream_state.remove_subscriber(self.subscriber_id);
        }
    }
}

#[cfg(test)]
mod tests;

/// Where a subscriber's queue dropped batches, as far as resuming needs.
#[derive(Debug, Default)]
pub(crate) struct Lag {
    drops: std::sync::Mutex<Drops>,
    notify: tokio::sync::Notify,
}

#[derive(Debug, Default)]
struct Drops {
    /// Drops below this were filled in some other way.
    floor: u64,
    first: Option<u64>,
    latest: Option<u64>,
}

impl Drops {
    /// Offsets reach the queue in order, so the first drop at or above the
    /// floor is `first` when that is above it. When it is not, but a later
    /// drop is, the floor itself is a safe place to resume.
    fn resume_from(&self) -> Option<u64> {
        match (self.first, self.latest) {
            (Some(first), _) if first >= self.floor => Some(first),
            (_, Some(latest)) if latest >= self.floor => Some(self.floor),
            _ => None,
        }
    }
}

impl Lag {
    /// Record a dropped batch starting at `offset`.
    pub(crate) fn dropped(&self, offset: u64) {
        let mut drops = self.drops.lock().unwrap_or_else(|e| e.into_inner());
        let before = drops.resume_from();
        drops.first.get_or_insert(offset);
        drops.latest = Some(offset);
        if before.is_none() && drops.resume_from().is_some() {
            self.notify.notify_waiters();
        }
    }

    /// Forget drops below `offset`, which were made up for elsewhere.
    pub(crate) fn covered_below(&self, offset: u64) {
        let mut drops = self.drops.lock().unwrap_or_else(|e| e.into_inner());
        drops.floor = drops.floor.max(offset);
    }

    pub(crate) fn first_dropped(&self) -> Option<u64> {
        self.drops
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .resume_from()
    }
}

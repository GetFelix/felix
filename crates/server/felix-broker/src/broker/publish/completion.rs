//! The second half of a publish: wait for durability and the batch's turn,
//! append it to the replay ring, fan it out.
//!
//! Once a batch has its offsets its records are on disk, and they will be
//! replicated and read back whether or not the caller is still waiting. So
//! this half runs to completion even when the caller's future is dropped: a
//! cancelled caller that skipped the ring and the fanout left a hole live
//! subscribers never heard about, and a resumed one found only on disk. A
//! [`Finisher`] dropped early hands what is left to a detached task, which
//! keeps the commit turn until it is done, so later batches still wait
//! behind this one.
//!
//! A batch past the committed mark of a `Quorum` stream stops after the
//! commit: it joins the stream's `CommitHold` in commit order, and
//! [`release`] appends and fans it out once the mark passes it. See
//! `stream/committed.rs`.

use std::sync::Arc;

use bytes::Bytes;

use super::{ClaimedDurable, ClaimedPublish, PublishOutcome};
use crate::broker::shards::StreamHandle;
use crate::commit::StateOp;
use crate::error::{BrokerError, Result};
use crate::stream::{DeliveryEnvelope, HeldBatch, QueuedDelivery, SubQueuePolicy, SubscriberEntry};
use crate::telemetry::{t_histogram, t_now_if};
use crate::timings;

/// Everything left to do for one claimed batch, and how far it has got.
pub(super) struct Completion {
    handle: StreamHandle,
    payloads: Vec<Bytes>,
    commit: Option<Arc<[StateOp]>>,
    /// Holds the commit turn until the batch is fanned out.
    durable: Option<ClaimedDurable>,
    sample: bool,
    log_capacity: usize,
    /// The broker's replication wake-up.
    replication: Arc<tokio::sync::Notify>,
    /// Set once the batch is in the ring: from then on only the fanout is
    /// left, and appending again would duplicate it.
    delivery: Option<Delivery>,
    /// Where a released batch's offsets start. A claimed batch reads them
    /// from its claim instead.
    released_at: Option<u64>,
}

/// A batch in the ring, part way through its fanout.
struct Delivery {
    senders: Arc<Vec<SubscriberEntry>>,
    envelope: DeliveryEnvelope,
    first_offset: Option<u64>,
    /// The next subscriber to hand it to.
    next: usize,
    sent: usize,
    closed: Vec<u64>,
}

impl Completion {
    pub(super) fn new(
        claimed: ClaimedPublish,
        log_capacity: usize,
        replication: Arc<tokio::sync::Notify>,
    ) -> Self {
        let ClaimedPublish {
            handle,
            payloads,
            commit,
            durable,
            sample,
        } = claimed;
        Self {
            handle,
            payloads,
            commit,
            durable,
            sample,
            log_capacity,
            replication,
            delivery: None,
            released_at: None,
        }
    }

    /// A held batch the mark has passed: committed, turn long released, and
    /// only the ring append and the fanout left.
    fn released(handle: StreamHandle, batch: HeldBatch, log_capacity: usize) -> Self {
        Self {
            handle,
            payloads: batch.payloads,
            commit: batch.commit,
            durable: None,
            sample: false,
            log_capacity,
            replication: Arc::default(),
            delivery: None,
            released_at: Some(batch.first_offset),
        }
    }

    /// Drive the batch to the end, from wherever it stopped. Safe to call
    /// again after a cancelled call: a stage already done is not redone.
    async fn run(&mut self) -> Result<PublishOutcome> {
        if self.payloads.is_empty() {
            return Ok(PublishOutcome {
                subscribers: 0,
                offsets: None,
            });
        }
        if self.delivery.is_none() {
            self.commit().await?;
            if let Some(held) = self.hold()? {
                return Ok(held);
            }
            self.append()?;
        }
        self.deliver().await
    }

    /// Hold the batch back from readers if the stream's committed mark has
    /// not passed it. Runs under the commit turn, so batches join the hold in
    /// the order they take their offsets.
    ///
    /// The answer reports no subscribers: none has the batch yet.
    fn hold(&mut self) -> Result<Option<PublishOutcome>> {
        let Some(claimed) = self.durable.as_ref() else {
            return Ok(None);
        };
        let state = &self.handle.state;
        let first_offset = claimed.pending.first_offset();
        let end = first_offset + self.payloads.len() as u64;
        // A `Leader` stream with nothing held never asks for a bound, so it
        // costs one atomic load here.
        if !state.held.is_busy() && state.read_bound().covers(end) {
            return Ok(None);
        }
        // A reset discards the hold under the ring lock, so joining it is
        // checked against the turn under that lock too.
        let ring = state.log_state.lock();
        if !claimed.turn.is_current() {
            return Err(BrokerError::PublishSuperseded { first_offset });
        }
        let batch = HeldBatch {
            payloads: std::mem::take(&mut self.payloads),
            first_offset,
            commit: self.commit.take(),
        };
        let start = state.held.push(batch);
        drop(ring);
        if start {
            spawn_release(&self.handle);
        }
        Ok(Some(PublishOutcome {
            subscribers: 0,
            offsets: Some((first_offset, end - 1)),
        }))
    }

    /// Fan the batch out and settle what the fanout found.
    async fn deliver(&mut self) -> Result<PublishOutcome> {
        self.fan_out().await;
        let delivery = self.delivery.as_mut().expect("appended above");
        if !delivery.closed.is_empty() {
            delivery.closed.sort_unstable();
            delivery.closed.dedup();
            self.handle.state.remove_subscribers(&delivery.closed);
            delivery.closed.clear();
        }
        let item_count = delivery.envelope.len();
        Ok(PublishOutcome {
            subscribers: delivery.sent,
            // Inclusive, and contiguous by construction: a batch consumes one
            // run of offsets.
            offsets: delivery
                .first_offset
                .map(|first| (first, first + item_count as u64 - 1)),
        })
    }

    /// Wait until the batch is durable and every earlier batch has gone
    /// through its fanout.
    async fn commit(&mut self) -> Result<()> {
        let (Some(claimed), Some(log)) = (&self.durable, &self.handle.state.durable) else {
            return Ok(());
        };
        log.commit(&claimed.pending).await?;
        // A reset while this batch waited (the shard went to a follower, or
        // its log was rebuilt) cleared the ring and the hold. Applying the
        // batch now would put records from the old log in front of readers.
        claimed
            .turn
            .wait()
            .await
            .map_err(|_| BrokerError::PublishSuperseded {
                first_offset: claimed.pending.first_offset(),
            })?;
        // Replication waits on this. Under `Quorum` the publish is about to
        // block on a majority, so the shipping that produces it should already
        // be under way rather than waiting out a tick.
        //
        // `notify_one`, not `notify_waiters`: the latter wakes only waiters
        // already registered, so an append landing while replication is
        // mid-pass would be lost and that record would wait for the tick after
        // all. `notify_one` leaves a permit, so the next wait returns at once --
        // and it stores only one, so a burst becomes a single extra pass rather
        // than a storm.
        self.replication.notify_one();
        // `notify_waiters` here, unlike above: an offset reader registers
        // before it reads the tail, so it cannot miss this, and one that is not
        // waiting has nothing to be told.
        self.handle.state.appended.notify_waiters();
        if let Some(start) = claimed.durable_start {
            let durable_ns = start.elapsed().as_nanos() as u64;
            t_histogram!("broker_publish_durable_append_ns").record(durable_ns as f64);
        }
        Ok(())
    }

    /// Append to the in-memory ring so cursors can replay without touching
    /// disk. A durable stream pins the sequence numbers to the offsets the log
    /// assigned, so a cursor and a disk offset are the same value.
    fn append(&mut self) -> Result<()> {
        let append_start = t_now_if(self.sample);
        let first_offset = self
            .durable
            .as_ref()
            .map(|claimed| claimed.pending.first_offset())
            .or(self.released_at);
        let turn = self.durable.as_ref().map(|claimed| &claimed.turn);
        // `wait` passed, but a reset can still land before the ring lock.
        let (senders, skipped_before) = self
            .handle
            .state
            .append_batch_at(
                &self.payloads,
                first_offset,
                turn,
                self.log_capacity,
                self.commit.as_deref(),
            )
            .ok_or(BrokerError::PublishSuperseded {
                first_offset: first_offset.unwrap_or_default(),
            })?;
        if let Some(start) = append_start {
            let append_ns = start.elapsed().as_nanos() as u64;
            timings::record_append_ns(append_ns);
            t_histogram!("broker_publish_append_ns").record(append_ns as f64);
        }
        self.delivery = Some(Delivery {
            senders,
            // The offsets travel with the batch, so live delivery reports them
            // exactly as replay does.
            envelope: DeliveryEnvelope::with_offsets(&self.payloads, first_offset, skipped_before),
            first_offset,
            next: 0,
            sent: 0,
            closed: Vec::new(),
        });
        Ok(())
    }

    /// Hand the batch to every subscriber the ring append saw, starting from
    /// the first one not yet served. One envelope is shared by all of them.
    async fn fan_out(&mut self) {
        let fanout_start = t_now_if(self.sample);
        let stream_state = &self.handle.state;
        let delivery = self.delivery.as_mut().expect("appended before fanout");
        let item_count = delivery.envelope.len();
        while let Some(subscriber) = delivery.senders.get(delivery.next) {
            match stream_state.subscriber_queue_policy {
                SubQueuePolicy::Block => {
                    // The one await in the fanout. `next` only moves once the
                    // subscriber is served, so a resumed finisher retries it.
                    if let Ok(permit) = subscriber.sender.reserve().await {
                        metrics::counter!("felix_sub_shared_batch_handles_total").increment(1);
                        stream_state.increment_queue_depth(item_count);
                        permit.send(QueuedDelivery::new(
                            delivery.envelope.clone(),
                            Arc::clone(&stream_state.queued_items),
                        ));
                        delivery.sent += item_count;
                    } else {
                        delivery.closed.push(subscriber.id);
                    }
                }
                SubQueuePolicy::DropNew | SubQueuePolicy::DropOld => {
                    match subscriber.sender.try_reserve() {
                        Ok(permit) => {
                            metrics::counter!("felix_sub_shared_batch_handles_total").increment(1);
                            stream_state.increment_queue_depth(item_count);
                            permit.send(QueuedDelivery::new(
                                delivery.envelope.clone(),
                                Arc::clone(&stream_state.queued_items),
                            ));
                            delivery.sent += item_count;
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            metrics::counter!("felix_subscribe_dropped_total")
                                .increment(item_count as u64);
                            metrics::counter!("felix_sub_queue_dropped_total")
                                .increment(item_count as u64);
                            if matches!(
                                stream_state.subscriber_queue_policy,
                                SubQueuePolicy::DropOld
                            ) {
                                metrics::counter!("felix_sub_queue_drop_old_emulated_total")
                                    .increment(item_count as u64);
                            }
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            delivery.closed.push(subscriber.id);
                        }
                    }
                }
            }
            delivery.next += 1;
        }
        if let Some(start) = fanout_start {
            let fanout_ns = start.elapsed().as_nanos() as u64;
            timings::record_enqueue_ns(fanout_ns);
            timings::record_fanout_ns(fanout_ns);
            t_histogram!("broker_publish_fanout_total_ns").record(fanout_ns as f64);
        }
    }
}

/// Start releasing `handle`'s held batches on a task of their own.
///
/// Never inline in a caller's future: a release cancelled half way would
/// leave the hold marked as releasing, and nothing after it would ever go
/// out. Outside a runtime the process is going down and the batches are on
/// disk for whoever opens the log next.
pub(crate) fn spawn_release(handle: &StreamHandle) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    runtime.spawn(release(handle.clone()));
}

/// Append and fan out, in order, every held batch the bound covers, until
/// none is left that it does.
///
/// One per stream at a time (`CommitHold` sees to it), which is what keeps
/// released batches in offset order across subscribers and the ring.
async fn release(handle: StreamHandle) {
    let state = &handle.state;
    let log_capacity = state.held.log_capacity().unwrap_or(1);
    loop {
        state.held.begin_pass();
        let bound = state.read_bound();
        let Some(ready) = state.held.take_ready(bound) else {
            return;
        };
        for batch in ready {
            let mut completion = Completion::released(handle.clone(), batch, log_capacity);
            // A released batch holds no turn, so the append cannot be refused.
            if completion.append().is_ok() {
                let _ = completion.deliver().await;
            }
        }
    }
}

/// Holds a [`Completion`] while its caller drives it, and finishes it on a
/// detached task if the caller is dropped first.
pub(super) struct Finisher(Option<Completion>);

impl Finisher {
    pub(super) fn new(completion: Completion) -> Self {
        Self(Some(completion))
    }

    pub(super) async fn run(mut self) -> Result<PublishOutcome> {
        let completion = self.0.as_mut().expect("armed until run");
        let outcome = completion.run().await;
        // Finished, or failed before anything reached the ring: a failed
        // commit leaves nothing to apply, and what reached the disk is found
        // by the next reader of the log.
        self.0 = None;
        outcome
    }
}

impl Drop for Finisher {
    fn drop(&mut self) {
        let Some(mut completion) = self.0.take() else {
            return;
        };
        // No runtime means the process is going down; the records are on disk
        // for whoever opens the log next.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            let _ = completion.run().await;
        });
    }
}

#[cfg(test)]
mod tests;

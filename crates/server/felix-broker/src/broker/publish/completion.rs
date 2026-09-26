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

use std::sync::Arc;

use bytes::Bytes;

use super::{ClaimedDurable, ClaimedPublish, PublishOutcome};
use crate::broker::shards::StreamHandle;
use crate::error::Result;
use crate::stream::{DeliveryEnvelope, QueuedDelivery, SubQueuePolicy, SubscriberEntry};
use crate::telemetry::{t_histogram, t_now_if};
use crate::timings;

/// Everything left to do for one claimed batch, and how far it has got.
pub(super) struct Completion {
    handle: StreamHandle,
    payloads: Vec<Bytes>,
    /// Holds the commit turn until the batch is fanned out.
    durable: Option<ClaimedDurable>,
    /// An in-memory stream's stand-in for the commit turn, over the same
    /// span: taken before the ring append, released once the fanout is done
    /// (or the detached finisher is), so every subscriber sees ring order.
    in_memory_turn: Option<tokio::sync::OwnedMutexGuard<()>>,
    sample: bool,
    log_capacity: usize,
    /// The broker's replication wake-up.
    replication: Arc<tokio::sync::Notify>,
    /// Set once the batch is in the ring: from then on only the fanout is
    /// left, and appending again would duplicate it.
    delivery: Option<Delivery>,
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
            durable,
            sample,
        } = claimed;
        Self {
            handle,
            payloads,
            durable,
            in_memory_turn: None,
            sample,
            log_capacity,
            replication,
            delivery: None,
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
            if self.durable.is_none() && self.in_memory_turn.is_none() {
                self.in_memory_turn = Some(
                    Arc::clone(&self.handle.state.fanout_order)
                        .lock_owned()
                        .await,
                );
            }
            self.commit().await?;
            self.append();
        }
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
        claimed.turn.wait().await;
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
    fn append(&mut self) {
        let append_start = t_now_if(self.sample);
        let first_offset = self
            .durable
            .as_ref()
            .map(|claimed| claimed.pending.first_offset());
        let senders =
            self.handle
                .state
                .append_batch_at(&self.payloads, first_offset, self.log_capacity);
        if let Some(start) = append_start {
            let append_ns = start.elapsed().as_nanos() as u64;
            timings::record_append_ns(append_ns);
            t_histogram!("broker_publish_append_ns").record(append_ns as f64);
        }
        self.delivery = Some(Delivery {
            senders,
            // The offsets travel with the batch, so live delivery reports them
            // exactly as replay does.
            envelope: DeliveryEnvelope::with_base_offset(&self.payloads, first_offset),
            first_offset,
            next: 0,
            sent: 0,
            closed: Vec::new(),
        });
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

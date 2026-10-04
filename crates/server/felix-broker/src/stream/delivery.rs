//! Shared delivery batches and the queue-depth accounting that rides with them.
//!
//! `QueuedDelivery` owns one unit of queue depth: the count is incremented by
//! the publisher between reserving a permit and sending, and released in
//! `Drop`. Keeping the increment, the `Drop`, and `decrement_queue_depth`
//! together is what makes depth accounting leak-free across receiver drops and
//! cancelled `recv` calls.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use parking_lot::Mutex;

/// One published batch, shared by every subscriber it is delivered to.
#[derive(Debug, Clone)]
pub struct DeliveryEnvelope {
    inner: Arc<DeliveryBatch>,
}

impl DeliveryEnvelope {
    #[cfg(test)]
    pub(crate) fn with_base_offset(payloads: &[Bytes], base_offset: Option<u64>) -> Self {
        Self::published(payloads, base_offset, 0, None)
    }

    /// A batch published by `publisher`, whose first record follows
    /// `skipped_before` offsets that hold no event (see
    /// [`Self::skipped_before`]).
    pub(crate) fn published(
        payloads: &[Bytes],
        base_offset: Option<u64>,
        skipped_before: u64,
        publisher: Option<Bytes>,
    ) -> Self {
        Self {
            inner: Arc::new(DeliveryBatch {
                payloads: Arc::from(payloads),
                base_offset,
                skipped_before,
                publisher,
                enqueued_at: Instant::now(),
                encoded: Default::default(),
            }),
        }
    }

    /// The same records from `index` on, keeping their offsets and publisher.
    pub(crate) fn skip_records(&self, index: usize) -> Self {
        Self::published(
            &self.inner.payloads[index..],
            self.inner.base_offset.map(|base| base + index as u64),
            0,
            self.inner.publisher.clone(),
        )
    }

    /// The batch's records, in publish order.
    pub fn payloads(&self) -> &[Bytes] {
        &self.inner.payloads
    }

    /// How many records the batch holds.
    pub fn len(&self) -> usize {
        self.inner.payloads.len()
    }

    /// Whether the batch holds no records.
    pub fn is_empty(&self) -> bool {
        self.inner.payloads.is_empty()
    }

    /// Offset of the first payload, when this batch came from a durable stream.
    pub fn base_offset(&self) -> Option<u64> {
        self.inner.base_offset
    }

    /// How many offsets immediately below [`Self::base_offset`] hold no
    /// event: generation-start records, which take an offset but are never
    /// delivered. Zero for almost every batch.
    pub fn skipped_before(&self) -> u64 {
        self.inner.skipped_before
    }

    /// The principal that published the batch, when one was recorded. A
    /// batch is one publish, so one principal covers all of it.
    pub fn publisher(&self) -> Option<&Bytes> {
        self.inner.publisher.as_ref()
    }

    /// The batch encoded as one event frame, encoded on first use and shared
    /// with every subscriber after that.
    pub fn shared_event_frame(&self) -> felix_wire::Result<Bytes> {
        self.shared_event_frame_as(FrameShape::default())
    }

    /// The shared frame carrying offsets, for subscribers that negotiated them.
    ///
    /// Falls back to the plain frame when this batch has no offset to report,
    /// so an in-memory stream costs nothing extra and an offset-capable
    /// subscriber on one simply sees no offsets -- which is what the protocol
    /// says an ephemeral event carries.
    pub fn shared_event_frame_with_offsets(&self) -> felix_wire::Result<Bytes> {
        self.shared_event_frame_as(FrameShape {
            offsets: true,
            ..FrameShape::default()
        })
    }

    /// The shared frame for subscribers that negotiated offsets and skip
    /// counts. The offsets-only frame unless this batch follows a skip, so the
    /// extra encoding exists only for the rare batch that needs it.
    pub fn shared_event_frame_with_skip(&self) -> felix_wire::Result<Bytes> {
        self.shared_event_frame_as(FrameShape {
            offsets: true,
            skips: true,
            publisher: false,
        })
    }

    /// The shared frame in the shape a subscriber negotiated.
    ///
    /// A field the batch has nothing for is left off, so the frame is the
    /// plainer one other subscribers share: an in-memory batch has no
    /// offsets, most batches follow no skip, and a batch with no recorded
    /// publisher carries none. Each distinct frame is encoded once.
    pub fn shared_event_frame_as(&self, shape: FrameShape) -> felix_wire::Result<Bytes> {
        let batch = &self.inner;
        let base_offset = batch.base_offset.filter(|_| shape.offsets);
        let skipped_before = match base_offset {
            Some(_) if shape.skips => batch.skipped_before,
            _ => 0,
        };
        let publisher = batch.publisher.as_deref().filter(|_| shape.publisher);
        let slot = match (base_offset, skipped_before) {
            (None, _) => 0,
            (Some(_), 0) => 1,
            (Some(_), _) => 2,
        } + if publisher.is_some() { 3 } else { 0 };
        let mut cached = batch.encoded[slot].lock();
        if let Some(frame) = cached.as_ref() {
            return Ok(frame.clone());
        }
        let frame = if slot == 0 {
            felix_wire::binary::encode_shared_event_batch_bytes(&batch.payloads)?
        } else {
            felix_wire::binary::encode_shared_event_batch_bytes_with_meta(
                &batch.payloads,
                felix_wire::binary::EventBatchMeta {
                    base_offset,
                    skipped_before,
                    publisher,
                },
            )?
        };
        *cached = Some(frame.clone());
        Ok(frame)
    }

    /// When the batch was built, for measuring how long it waited in a queue.
    pub fn enqueued_at(&self) -> Instant {
        self.inner.enqueued_at
    }
}

/// Which optional fields a subscriber negotiated on its event frames.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FrameShape {
    /// `FLAG_EVENT_BATCH_OFFSETS`.
    pub offsets: bool,
    /// `FLAG_EVENT_BATCH_SKIPPED`; meaningful only with `offsets`.
    pub skips: bool,
    /// `FLAG_EVENT_BATCH_PUBLISHER`.
    pub publisher: bool,
}

#[derive(Debug)]
struct DeliveryBatch {
    payloads: Arc<[Bytes]>,
    /// Offset of `payloads[0]` for a durable stream; `None` for an in-memory
    /// one, which has no durable position to report.
    ///
    /// One value per batch is enough because a publish batch takes a contiguous
    /// run of offsets. That is what keeps offsets off the per-event cost model
    /// and lets them ride the shared encode-once frame: the offsets belong to
    /// the stream, not to any subscriber.
    base_offset: Option<u64>,
    /// See [`DeliveryEnvelope::skipped_before`]. Like the offsets, a property
    /// of the stream, so it rides the shared encoding too.
    skipped_before: u64,
    /// See [`DeliveryEnvelope::publisher`]. One per batch for the same reason
    /// as the offsets.
    publisher: Option<Bytes>,
    enqueued_at: Instant,
    /// The batch's encodings, one per frame shape a subscriber of it
    /// negotiated: plain, with offsets, with offsets and a skip, and each of
    /// those with the publisher. Cached apart because a stream can have
    /// subscribers of every kind and each must get the frame it agreed to;
    /// at most six encodings per batch however many subscribers there are.
    encoded: [Mutex<Option<Bytes>>; 6],
}

#[derive(Debug)]
pub(crate) struct QueuedDelivery {
    envelope: Option<DeliveryEnvelope>,
    item_count: usize,
    queued_items: Arc<AtomicUsize>,
}

impl QueuedDelivery {
    pub(crate) fn new(envelope: DeliveryEnvelope, queued_items: Arc<AtomicUsize>) -> Self {
        let item_count = envelope.len();
        Self {
            envelope: Some(envelope),
            item_count,
            queued_items,
        }
    }

    pub(crate) fn into_envelope(mut self) -> DeliveryEnvelope {
        self.envelope.take().expect("queued delivery has envelope")
    }
}

impl Drop for QueuedDelivery {
    fn drop(&mut self) {
        decrement_queue_depth(&self.queued_items, self.item_count);
    }
}

/// What a publish does when a subscriber's queue is full.
///
/// `DropNew` is the default, so a slow subscriber costs itself records rather
/// than slowing the publisher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubQueuePolicy {
    /// Wait for room. Every publisher of the shard waits with it.
    Block,
    /// Drop the batch for this subscriber.
    DropNew,
    /// Treated as `DropNew`: a bounded channel cannot evict what it already holds.
    DropOld,
}

fn decrement_queue_depth(queued_items: &AtomicUsize, count: usize) {
    if queued_items
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_sub(count)
        })
        .is_ok()
    {
        metrics::gauge!("felix_sub_queue_len").decrement(count as f64);
        metrics::counter!("felix_sub_queue_dequeued_total").increment(count as u64);
    }
}

#[cfg(test)]
mod tests;

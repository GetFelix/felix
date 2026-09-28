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
    pub(crate) fn with_base_offset(payloads: &[Bytes], base_offset: Option<u64>) -> Self {
        Self::with_offsets(payloads, base_offset, 0)
    }

    /// A batch whose first record follows `skipped_before` offsets that hold
    /// no event (see [`Self::skipped_before`]).
    pub(crate) fn with_offsets(
        payloads: &[Bytes],
        base_offset: Option<u64>,
        skipped_before: u64,
    ) -> Self {
        Self {
            inner: Arc::new(DeliveryBatch {
                payloads: Arc::from(payloads),
                base_offset,
                skipped_before,
                enqueued_at: Instant::now(),
                encoded_frame: Mutex::new(None),
                encoded_frame_with_offsets: Mutex::new(None),
                encoded_frame_with_skip: Mutex::new(None),
            }),
        }
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

    /// The batch encoded as one event frame, encoded on first use and shared
    /// with every subscriber after that.
    pub fn shared_event_frame(&self) -> felix_wire::Result<Bytes> {
        let mut cached = self.inner.encoded_frame.lock();
        if let Some(frame) = cached.as_ref() {
            return Ok(frame.clone());
        }
        let frame = felix_wire::binary::encode_shared_event_batch_bytes(&self.inner.payloads)?;
        *cached = Some(frame.clone());
        Ok(frame)
    }

    /// The shared frame carrying offsets, for subscribers that negotiated them.
    ///
    /// Falls back to the plain frame when this batch has no offset to report,
    /// so an in-memory stream costs nothing extra and an offset-capable
    /// subscriber on one simply sees no offsets -- which is what the protocol
    /// says an ephemeral event carries.
    pub fn shared_event_frame_with_offsets(&self) -> felix_wire::Result<Bytes> {
        let Some(base_offset) = self.inner.base_offset else {
            return self.shared_event_frame();
        };
        let mut cached = self.inner.encoded_frame_with_offsets.lock();
        if let Some(frame) = cached.as_ref() {
            return Ok(frame.clone());
        }
        let frame = felix_wire::binary::encode_shared_event_batch_bytes_with_offset(
            &self.inner.payloads,
            base_offset,
        )?;
        *cached = Some(frame.clone());
        Ok(frame)
    }

    /// The shared frame for subscribers that negotiated offsets and skip
    /// counts. The offsets-only frame unless this batch follows a skip, so the
    /// extra encoding exists only for the rare batch that needs it.
    pub fn shared_event_frame_with_skip(&self) -> felix_wire::Result<Bytes> {
        let (Some(base_offset), skipped) = (self.inner.base_offset, self.inner.skipped_before)
        else {
            return self.shared_event_frame();
        };
        if skipped == 0 {
            return self.shared_event_frame_with_offsets();
        }
        let mut cached = self.inner.encoded_frame_with_skip.lock();
        if let Some(frame) = cached.as_ref() {
            return Ok(frame.clone());
        }
        let frame = felix_wire::binary::encode_shared_event_batch_bytes_with_skip(
            &self.inner.payloads,
            base_offset,
            skipped,
        )?;
        *cached = Some(frame.clone());
        Ok(frame)
    }

    /// When the batch was built, for measuring how long it waited in a queue.
    pub fn enqueued_at(&self) -> Instant {
        self.inner.enqueued_at
    }
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
    enqueued_at: Instant,
    encoded_frame: Mutex<Option<Bytes>>,
    /// The same batch encoded *with* offsets, for subscribers that negotiated
    /// them. Cached separately rather than replacing the plain encoding,
    /// because a stream can have subscribers of both kinds and each must get
    /// the frame shape it agreed to. At most two encodings per batch, however
    /// many subscribers there are.
    encoded_frame_with_offsets: Mutex<Option<Bytes>>,
    /// The offsets frame with the skip count, for subscribers that negotiated
    /// it. Only ever filled when `skipped_before` is non-zero.
    encoded_frame_with_skip: Mutex<Option<Bytes>>,
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

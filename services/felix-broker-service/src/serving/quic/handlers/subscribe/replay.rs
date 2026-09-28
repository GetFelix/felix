//! Replaying stored history to a resumed subscription, and the handover from
//! history to live delivery.

use std::sync::Arc;

use anyhow::Result;
use felix_broker::Broker;

use crate::observability::tenants::TenantDelivery;

/// Bound on catch-up passes, so a stream being published to faster than it can
/// be written cannot keep a subscribe from completing. Reaching it hands over to
/// live delivery, which is correct: offsets are on the wire, so a client can see
/// any residual gap rather than being misled about it.
const MAX_CATCH_UP_PASSES: usize = 8;

/// Where replayed events are written.
///
/// A trait rather than the QUIC stream itself, so the rules below — paging
/// history, detecting a gap the subscriber queue dropped, and not re-sending
/// what history already covered — are testable without a subscriber on the
/// other end of a connection.
pub(crate) trait EventSink {
    fn write_all(&mut self, bytes: &[u8]) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Told after each batch is written: `messages` events, `bytes` of payload.
    fn delivered(&mut self, _messages: usize, _bytes: usize) {}
}

impl EventSink for quinn::SendStream {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        quinn::SendStream::write_all(self, bytes).await?;
        Ok(())
    }
}

/// An [`EventSink`] that counts what it delivers against the subscription's
/// tenant, so replayed history shows in the per-tenant delivery metrics as
/// live events do.
pub(crate) struct CountingSink<'a, S> {
    pub(crate) inner: &'a mut S,
    pub(crate) delivery: &'a TenantDelivery,
}

impl<S: EventSink + Send> EventSink for CountingSink<'_, S> {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.inner.write_all(bytes).await
    }

    fn delivered(&mut self, messages: usize, bytes: usize) {
        self.delivery.record(messages, bytes);
    }
}

/// The event frame shape a subscriber negotiated.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EventFormat {
    /// `FLAG_EVENT_BATCH_OFFSETS`.
    pub(crate) offsets: bool,
    /// `FLAG_EVENT_BATCH_SKIPPED`, only ever with `offsets`.
    pub(crate) skips: bool,
}

/// Write a resumed subscription's stored history and ring backlog.
///
/// Disk history is *paged*, never collected: `read_committed` returns at most
/// `max_bytes` per call and this advances by the last offset it saw, so a client
/// resuming from the start of a large stream costs the broker one page of memory
/// at a time rather than the whole history. Each page is written before the next
/// is read, so backpressure from a slow client propagates naturally into slower
/// reading rather than unbounded buffering.
///
/// On a `Quorum` stream a page stops at the committed mark and the next waits
/// for it (see `Broker::read_committed`): the history range is closed, but
/// part of it can be past the mark when this broker has just taken the shard.
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_replay<S: EventSink>(
    event_send: &mut S,
    broker: &Arc<Broker>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    subscription_id: u64,
    history: Option<felix_broker::HistoryRange>,
    backlog: Vec<(u64, bytes::Bytes)>,
    backlog_start: u64,
    subscription: &mut felix_broker::Subscription,
    max_events: usize,
    max_bytes: usize,
    format: EventFormat,
) -> Result<()> {
    let mut replay = Replay {
        sink: event_send,
        broker,
        tenant_id,
        namespace,
        stream,
        shard,
        subscription_id,
        max_events,
        max_bytes,
        format,
        // Where delivery begins: the history, else the backlog.
        next: match (&history, backlog.first()) {
            (Some(range), _) => range.from_offset,
            (None, Some((offset, _))) => backlog_start.min(*offset),
            (None, None) => backlog_start,
        },
        skipped: 0,
    };

    if let Some(range) = history {
        replay.history_until(range.until_offset).await?;
    }

    let mut batch = ReplayBatch::new(max_events, max_bytes);
    for (offset, payload) in backlog {
        // A hole in the ring is paged from disk like a hole in the queue
        // below. It is usually a generation-start record, which the read
        // turns into a skip for the record after it.
        if offset > replay.next {
            if let Some(ready) = batch.take() {
                replay.write(&ready).await?;
            }
            replay.history_until(offset).await?;
        }
        if offset < replay.next {
            continue;
        }
        if let Some(ready) = batch.push(offset, payload, replay.deliver(offset)) {
            replay.write(&ready).await?;
        }
    }
    if let Some(ready) = batch.take() {
        replay.write(&ready).await?;
    }

    // Catch-up. The live subscription was registered before any of this ran, so
    // publishes have been queueing on it the whole time -- into the *ordinary*
    // bounded subscriber queue, which drops under `DropNew` once it is full.
    // Relying on that queue to carry the handoff means a long replay silently
    // loses live records, so instead: drain what is queued, and wherever the
    // offsets jump, fill the hole from disk. Disk is the authority; the queue is
    // only a shortcut for the part that has not been evicted.
    //
    // Repeated because draining takes time of its own, during which more can
    // arrive. It terminates because each pass only handles what was already
    // queued, and a pass that finds nothing ends it.
    for _ in 0..MAX_CATCH_UP_PASSES {
        let ready = subscription.drain_ready();
        if ready.is_empty() {
            break;
        }
        for envelope in ready {
            if let Some(base) = envelope.base_offset() {
                if base > replay.next {
                    // The queue dropped records. Page the gap from disk.
                    replay.history_until(base).await?;
                }
                if base + envelope.len() as u64 <= replay.next {
                    // Entirely covered by history already written.
                    continue;
                }
            }
            let mut batch = ReplayBatch::new(max_events, max_bytes);
            for (index, payload) in envelope.payloads().iter().enumerate() {
                let offset = envelope
                    .base_offset()
                    .map(|base| base + index as u64)
                    .unwrap_or(replay.next);
                if offset < replay.next {
                    continue;
                }
                if let Some(chunk) = batch.push(offset, payload.clone(), replay.deliver(offset)) {
                    replay.write(&chunk).await?;
                }
            }
            if let Some(chunk) = batch.take() {
                replay.write(&chunk).await?;
            }
        }
    }
    Ok(())
}

/// One subscription's replay, and how far it has got.
struct Replay<'a, S> {
    sink: &'a mut S,
    broker: &'a Arc<Broker>,
    tenant_id: &'a str,
    namespace: &'a str,
    stream: &'a str,
    shard: u32,
    subscription_id: u64,
    max_events: usize,
    max_bytes: usize,
    format: EventFormat,
    /// Every offset below this was delivered or is known to hold no event.
    next: u64,
    /// How many offsets just below `next` hold no event, since the last one
    /// delivered. Reported on the next record if it lands exactly at `next`.
    skipped: u64,
}

impl<S: EventSink> Replay<'_, S> {
    /// Account for delivering the record at `offset`, returning how many
    /// offsets immediately before it are known to hold no event.
    fn deliver(&mut self, offset: u64) -> u64 {
        let skipped = if offset == self.next { self.skipped } else { 0 };
        self.next = offset + 1;
        self.skipped = 0;
        skipped
    }

    /// Write `[next, until)` from disk.
    ///
    /// A disk read is contiguous apart from the generation-start records
    /// `read_from` leaves out, so every offset it passes over without
    /// returning a record is one of those, and becomes a skip.
    async fn history_until(&mut self, until: u64) -> Result<()> {
        /// One page of history per read. Bounds broker memory for a resume
        /// that starts arbitrarily far back.
        const HISTORY_PAGE_BYTES: usize = 1024 * 1024;
        while self.next < until {
            let records = self
                .broker
                .read_committed(
                    self.tenant_id,
                    self.namespace,
                    self.stream,
                    self.shard,
                    self.next,
                    HISTORY_PAGE_BYTES,
                )
                .await?;
            if records.is_empty() {
                break;
            }
            let mut batch = ReplayBatch::new(self.max_events, self.max_bytes);
            for record in records {
                if record.offset >= until {
                    // Everything from `next` up to here held no event. Without
                    // moving `next` the loop would read the same page forever.
                    self.skipped += until - self.next;
                    self.next = until;
                    break;
                }
                self.skipped += record.offset - self.next;
                self.next = record.offset;
                let skipped = self.deliver(record.offset);
                if let Some(ready) = batch.push(record.offset, record.payload, skipped) {
                    self.write(&ready).await?;
                }
            }
            if let Some(ready) = batch.take() {
                self.write(&ready).await?;
            }
        }
        Ok(())
    }

    async fn write(&mut self, ready: &ReadyBatch) -> Result<()> {
        write_replay_batch(self.sink, self.subscription_id, ready, self.format).await
    }
}

/// Accumulates replay records into frames that are safe to send.
///
/// Three things force a flush, and all three are correctness rather than taste:
///
/// * **A gap in offsets.** One `base_offset` describes a batch only if its
///   records are contiguous, so a hole must start a new frame or every offset
///   after it is wrong.
/// * **The byte budget.** Chunking by record count alone lets a backlog of
///   large payloads build a frame past the configured delivery and client frame
///   limits, which fails the write after allocating the whole thing.
/// * **The record count**, matching live delivery's batching.
///
/// A record that follows skipped offsets starts a batch too, since the skip
/// count describes the batch's first record.
struct ReplayBatch {
    payloads: Vec<bytes::Bytes>,
    base_offset: u64,
    skipped_before: u64,
    next_offset: u64,
    bytes: usize,
    max_events: usize,
    max_bytes: usize,
}

impl ReplayBatch {
    fn new(max_events: usize, max_bytes: usize) -> Self {
        Self {
            payloads: Vec::new(),
            base_offset: 0,
            skipped_before: 0,
            next_offset: 0,
            bytes: 0,
            max_events: max_events.max(1),
            max_bytes: max_bytes.max(1),
        }
    }

    /// Add a record, returning a finished batch when this one had to be closed.
    fn push(
        &mut self,
        offset: u64,
        payload: bytes::Bytes,
        skipped_before: u64,
    ) -> Option<ReadyBatch> {
        let len = payload.len();
        let breaks_run =
            !self.payloads.is_empty() && (offset != self.next_offset || skipped_before > 0);
        let over_bytes = !self.payloads.is_empty() && self.bytes + len > self.max_bytes;
        let ready = if breaks_run || over_bytes {
            self.take()
        } else {
            None
        };
        if self.payloads.is_empty() {
            self.base_offset = offset;
            self.skipped_before = skipped_before;
        }
        self.payloads.push(payload);
        self.next_offset = offset + 1;
        self.bytes += len;
        if self.payloads.len() >= self.max_events {
            // Already at the count limit, so hand it over now. A batch closed
            // here and one closed above can never both be pending.
            return ready.or_else(|| self.take());
        }
        ready
    }

    fn take(&mut self) -> Option<ReadyBatch> {
        if self.payloads.is_empty() {
            return None;
        }
        self.bytes = 0;
        Some(ReadyBatch {
            base_offset: self.base_offset,
            skipped_before: std::mem::take(&mut self.skipped_before),
            payloads: std::mem::take(&mut self.payloads),
        })
    }
}

/// A closed replay batch: contiguous records from `base_offset`.
#[derive(Debug)]
pub(super) struct ReadyBatch {
    pub(super) base_offset: u64,
    /// Offsets just before `base_offset` that hold no event.
    pub(super) skipped_before: u64,
    pub(super) payloads: Vec<bytes::Bytes>,
}

impl ReadyBatch {
    #[cfg(test)]
    fn offsets(&self) -> Vec<u64> {
        (0..self.payloads.len() as u64)
            .map(|index| self.base_offset + index)
            .collect()
    }
}

/// Encode and write one replay batch in the shape the subscriber negotiated.
pub(super) async fn write_replay_batch<S: EventSink>(
    event_send: &mut S,
    subscription_id: u64,
    batch: &ReadyBatch,
    format: EventFormat,
) -> Result<()> {
    if batch.payloads.is_empty() {
        return Ok(());
    }
    let payloads = batch.payloads.as_slice();
    let frame = if format.skips {
        felix_wire::binary::encode_event_batch_bytes_with_skip(
            subscription_id,
            payloads,
            batch.base_offset,
            batch.skipped_before,
        )?
    } else if format.offsets {
        felix_wire::binary::encode_event_batch_bytes_with_offset(
            subscription_id,
            payloads,
            batch.base_offset,
        )?
    } else {
        felix_wire::binary::encode_event_batch_bytes(subscription_id, payloads)?
    };
    EventSink::write_all(event_send, &frame).await?;
    event_send.delivered(payloads.len(), payloads.iter().map(bytes::Bytes::len).sum());
    Ok(())
}

#[cfg(test)]
mod tests;

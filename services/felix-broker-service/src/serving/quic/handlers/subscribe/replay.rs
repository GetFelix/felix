//! Replaying stored history to a resumed subscription, and the handover from
//! history to live delivery.

use std::sync::Arc;

use anyhow::Result;
use felix_broker::Broker;

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
}

impl EventSink for quinn::SendStream {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        quinn::SendStream::write_all(self, bytes).await?;
        Ok(())
    }
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
    offsets_enabled: bool,
) -> Result<()> {
    /// One page of history per read. Bounds broker memory for a resume that
    /// starts arbitrarily far back.
    const HISTORY_PAGE_BYTES: usize = 1024 * 1024;

    if let Some(range) = history {
        let mut at = range.from_offset;
        while at < range.until_offset {
            let records = broker
                .read_committed(tenant_id, namespace, stream, shard, at, HISTORY_PAGE_BYTES)
                .await?;
            if records.is_empty() {
                break;
            }
            let mut batch = ReplayBatch::new(max_events, max_bytes);
            for record in records {
                if record.offset >= range.until_offset {
                    break;
                }
                at = record.offset + 1;
                if let Some(ready) = batch.push(record.offset, record.payload.clone()) {
                    write_replay_batch(event_send, subscription_id, &ready, offsets_enabled)
                        .await?;
                }
            }
            if let Some(ready) = batch.take() {
                write_replay_batch(event_send, subscription_id, &ready, offsets_enabled).await?;
            }
        }
    }

    let mut batch = ReplayBatch::new(max_events, max_bytes);
    let mut delivered_upto = backlog_start;
    for (offset, payload) in backlog {
        delivered_upto = offset + 1;
        if let Some(ready) = batch.push(offset, payload) {
            write_replay_batch(event_send, subscription_id, &ready, offsets_enabled).await?;
        }
    }
    if let Some(ready) = batch.take() {
        write_replay_batch(event_send, subscription_id, &ready, offsets_enabled).await?;
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
                if base > delivered_upto {
                    // The queue dropped records. Page the gap from disk.
                    delivered_upto = write_history_range(
                        event_send,
                        broker,
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        subscription_id,
                        delivered_upto,
                        base,
                        max_events,
                        max_bytes,
                        offsets_enabled,
                    )
                    .await?;
                }
                if base + envelope.len() as u64 <= delivered_upto {
                    // Entirely covered by history already written.
                    continue;
                }
            }
            let mut batch = ReplayBatch::new(max_events, max_bytes);
            for (index, payload) in envelope.payloads().iter().enumerate() {
                let offset = envelope
                    .base_offset()
                    .map(|base| base + index as u64)
                    .unwrap_or(delivered_upto);
                if offset < delivered_upto {
                    continue;
                }
                delivered_upto = offset + 1;
                if let Some(chunk) = batch.push(offset, payload.clone()) {
                    write_replay_batch(event_send, subscription_id, &chunk, offsets_enabled)
                        .await?;
                }
            }
            if let Some(chunk) = batch.take() {
                write_replay_batch(event_send, subscription_id, &chunk, offsets_enabled).await?;
            }
        }
    }
    Ok(())
}

/// Write `[from, until)` from disk, returning the offset reached.
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_history_range<S: EventSink>(
    event_send: &mut S,
    broker: &Arc<Broker>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    subscription_id: u64,
    from: u64,
    until: u64,
    max_events: usize,
    max_bytes: usize,
    offsets_enabled: bool,
) -> Result<u64> {
    const HISTORY_PAGE_BYTES: usize = 1024 * 1024;
    let mut at = from;
    while at < until {
        let records = broker
            .read_committed(tenant_id, namespace, stream, shard, at, HISTORY_PAGE_BYTES)
            .await?;
        if records.is_empty() {
            break;
        }
        let mut batch = ReplayBatch::new(max_events, max_bytes);
        for record in records {
            if record.offset >= until {
                break;
            }
            at = record.offset + 1;
            if let Some(ready) = batch.push(record.offset, record.payload.clone()) {
                write_replay_batch(event_send, subscription_id, &ready, offsets_enabled).await?;
            }
        }
        if let Some(ready) = batch.take() {
            write_replay_batch(event_send, subscription_id, &ready, offsets_enabled).await?;
        }
    }
    Ok(at.max(from))
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
struct ReplayBatch {
    payloads: Vec<bytes::Bytes>,
    base_offset: u64,
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
            next_offset: 0,
            bytes: 0,
            max_events: max_events.max(1),
            max_bytes: max_bytes.max(1),
        }
    }

    /// Add a record, returning a finished batch when this one had to be closed.
    fn push(&mut self, offset: u64, payload: bytes::Bytes) -> Option<Vec<(u64, bytes::Bytes)>> {
        let len = payload.len();
        let breaks_run = !self.payloads.is_empty() && offset != self.next_offset;
        let over_bytes = !self.payloads.is_empty() && self.bytes + len > self.max_bytes;
        let ready = if breaks_run || over_bytes {
            self.take()
        } else {
            None
        };
        if self.payloads.is_empty() {
            self.base_offset = offset;
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

    fn take(&mut self) -> Option<Vec<(u64, bytes::Bytes)>> {
        if self.payloads.is_empty() {
            return None;
        }
        let base = self.base_offset;
        let payloads = std::mem::take(&mut self.payloads);
        self.bytes = 0;
        Some(
            payloads
                .into_iter()
                .enumerate()
                .map(|(index, payload)| (base + index as u64, payload))
                .collect(),
        )
    }
}

/// Encode and write one replay batch, with or without offsets as negotiated.
pub(super) async fn write_replay_batch<S: EventSink>(
    event_send: &mut S,
    subscription_id: u64,
    records: &[(u64, bytes::Bytes)],
    offsets_enabled: bool,
) -> Result<()> {
    let base_offset = match records.first() {
        Some((offset, _)) => *offset,
        None => return Ok(()),
    };
    let payloads: Vec<bytes::Bytes> = records.iter().map(|(_, payload)| payload.clone()).collect();
    let payloads = payloads.as_slice();
    let frame = if offsets_enabled {
        felix_wire::binary::encode_event_batch_bytes_with_offset(
            subscription_id,
            payloads,
            base_offset,
        )?
    } else {
        felix_wire::binary::encode_event_batch_bytes(subscription_id, payloads)?
    };
    EventSink::write_all(event_send, &frame).await?;
    Ok(())
}

#[cfg(test)]
mod tests;

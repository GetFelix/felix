//! Publishing records that carry a key and headers.
//!
//! A record's key and headers travel as one block ahead of its payload
//! (`EXT_RECORD_HEADERS`). Only a broker that answered the extension takes the
//! frame, so a publish with headers is refused, not downgraded, against any
//! other: dropping the headers would lose data the caller asked to keep.

use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_wire::{AckMode, RecordHeaders};
use tokio::sync::oneshot;

use super::send::CancelledAfterEnqueue;
use super::writer::PublishRequest;
use super::{AckOutcome, Publisher};
use crate::error::RecordHeadersError;

/// Names the broker keeps for itself.
const RESERVED_PREFIX: &str = "felix.";

/// Each record's encoded key-and-headers block, `None` for a plain record.
pub(crate) type Blocks = Vec<Option<Bytes>>;

/// The payloads of `records`, and each record's encoded block. The blocks are
/// `None` when no record has headers or a key, which is a plain publish.
///
/// Fails on a header name the broker would refuse, before anything is encoded
/// for the wire.
pub(crate) fn split_records(
    records: Vec<(RecordHeaders, Vec<u8>)>,
) -> Result<(Vec<Vec<u8>>, Option<Blocks>)> {
    let mut payloads = Vec::with_capacity(records.len());
    let mut blocks = Vec::with_capacity(records.len());
    let mut any = false;
    for (headers, payload) in records {
        for (name, _) in &headers.headers {
            if name.is_empty() {
                return Err(RecordHeadersError::EmptyName.into());
            }
            if name.starts_with(RESERVED_PREFIX) {
                return Err(RecordHeadersError::ReservedName { name: name.clone() }.into());
            }
        }
        blocks.push(if headers.is_empty() {
            None
        } else {
            any = true;
            Some(headers.encode().context("encode record headers")?)
        });
        payloads.push(payload);
    }
    Ok((payloads, any.then_some(blocks)))
}

impl Publisher {
    /// Publish one payload with a key and headers.
    ///
    /// The key in `headers` belongs to the record. It does not route: the
    /// record goes to the same shard [`Self::publish`] would send it to. An
    /// empty `headers` publishes a plain record. Returns the record's log
    /// offset, when there is one; see [`Self::publish`].
    ///
    /// Fails with [`crate::RecordHeadersError`] and sends nothing when a header
    /// name is empty or starts with `felix.` (reserved for the broker), or the
    /// broker did not accept record headers. Idempotent producers and routing
    /// keys do not carry headers.
    pub async fn publish_with_headers(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        headers: RecordHeaders,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        self.publish_batch_with_headers(tenant_id, namespace, stream, vec![(headers, payload)], ack)
            .await
    }

    /// Publish a batch whose records carry keys and headers, one
    /// [`RecordHeaders`] per payload; an empty one leaves that record plain.
    ///
    /// Returns the log offset of the batch's first record, when there is one;
    /// see [`Self::publish_batch`]. Errors are those of
    /// [`Self::publish_with_headers`]. If every record is plain this is
    /// [`Self::publish_batch`].
    pub async fn publish_batch_with_headers(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        records: Vec<(RecordHeaders, Vec<u8>)>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        match split_records(records)? {
            (payloads, None) => {
                self.publish_batch(tenant_id, namespace, stream, payloads, ack)
                    .await
            }
            (payloads, Some(blocks)) => self
                .publish_blocks_reporting_owner(tenant_id, namespace, stream, payloads, blocks, ack)
                .await
                .map(|acked| acked.offset),
        }
    }

    /// One unkeyed batch with its records' blocks, reporting the shard's owner
    /// when this broker forwarded it. `blocks` holds one entry per payload.
    pub(crate) async fn publish_blocks_reporting_owner(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        blocks: Blocks,
        ack: AckMode,
    ) -> AckOutcome {
        let worker = self.route(tenant_id, namespace, stream, None, None).await?;
        if !worker.takes_record_headers {
            return Err(RecordHeadersError::Unsupported.into());
        }
        let request_id =
            (ack != AckMode::None).then(|| worker.request_counter.fetch_add(1, Ordering::Relaxed));
        let bytes = felix_wire::binary::encode_publish_batch_bytes_with_headers(
            request_id.map(|id| (id, ack)),
            None,
            None,
            tenant_id,
            namespace,
            stream,
            &payloads,
            &blocks,
        )?;
        let permit = self.inner.admission.acquire(bytes.len()).await?;
        let (response_tx, response_rx) = oneshot::channel();
        worker
            .tx
            .send(PublishRequest::BinaryBytes {
                bytes,
                item_count: payloads.len(),
                sample: false,
                declaration: worker.declaration.clone(),
                ack,
                request_id,
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue batch with headers")?;
        let cancelled = CancelledAfterEnqueue::armed();
        let answer = response_rx
            .await
            .context("batch with headers response dropped")?;
        cancelled.answered();
        answer
    }
}

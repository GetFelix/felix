//! `Client` methods for publishing: single records, batches, headers, conditional and shard-pinned publishes.

use bytes::Bytes;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::{parse_ack, u64_of};
use crate::errors::{classify, invalid};
use crate::headers::{HeaderedRecord, RecordHeadersInput};
use crate::producer::Producer;
use crate::stream_read::{BatchReport, KeyedRecord};

use super::Client;

#[napi]
impl Client {
    /// Publish one record.
    ///
    /// `key` is the routing key, and it decides the shard. Without one every
    /// record lands on shard 0, so a multi-shard stream behaves like a
    /// single-shard one. Records sharing a key share a shard and stay ordered
    /// with respect to each other; records with different keys do not.
    ///
    /// `atLeastOnce` **may duplicate the record.** By default a publish that
    /// fails after the broker may already have written it is reported, not
    /// re-sent, because nothing downstream can tell the copies apart. With
    /// `atLeastOnce` it is re-sent to another broker instead: the record is
    /// then certain to land, and may land twice. That is a delivery guarantee
    /// the caller chooses, never one this client assumes.
    ///
    /// Resolves to the record's log offset, or `null` when the broker
    /// acknowledged before writing it, the stream has no log, the broker is
    /// too old to say, or `ack` is `"none"`.
    #[napi]
    pub async fn publish(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        payload: Buffer,
        key: Option<Buffer>,
        ack: Option<String>,
        at_least_once: Option<bool>,
    ) -> Result<Option<BigInt>> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        let at_least_once = at_least_once.unwrap_or(false);
        let payload = payload.to_vec();
        let key = key.map(|k| Bytes::copy_from_slice(&k));
        let inner = self.cluster()?;
        match (key, at_least_once) {
            (Some(key), false) => {
                inner
                    .publish_keyed(&tenant_id, &namespace, &stream, payload, key, ack)
                    .await
            }
            (Some(key), true) => {
                inner
                    .publish_keyed_at_least_once(&tenant_id, &namespace, &stream, key, payload, ack)
                    .await
            }
            (None, true) => {
                inner
                    .publish_at_least_once(&tenant_id, &namespace, &stream, payload, ack)
                    .await
            }
            (None, false) => {
                inner
                    .publish(&tenant_id, &namespace, &stream, payload, ack)
                    .await
            }
        }
        .map(|offset| offset.map(BigInt::from))
        .map_err(classify)
    }

    /// Publish one record with a key and headers.
    ///
    /// The key in `headers` belongs to the record and does not route: the
    /// record goes where `publish` sends it. Subscribers read both back on
    /// `Event.headers`. Rejects with `InvalidArgumentError` and sends nothing
    /// when a header name is empty or starts with `felix.` (reserved for the
    /// broker), or the broker did not accept record headers. Resolves to the
    /// log offset as `publish` does.
    #[napi]
    pub async fn publish_with_headers(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        headers: RecordHeadersInput,
        payload: Buffer,
        ack: Option<String>,
    ) -> Result<Option<BigInt>> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        self.cluster()?
            .publish_with_headers(
                &tenant_id,
                &namespace,
                &stream,
                headers.into_client(),
                payload.to_vec(),
                ack,
            )
            .await
            .map(|offset| offset.map(BigInt::from))
            .map_err(classify)
    }

    /// Publish `records` (at least one) as one batch to shard 0, each with its
    /// own key and headers; a record with an empty `headers` stays plain.
    /// Errors and the result are those of `publishWithHeaders`, with the first
    /// record's offset.
    #[napi]
    pub async fn publish_batch_with_headers(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        records: Vec<HeaderedRecord>,
        ack: Option<String>,
    ) -> Result<Option<BigInt>> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        if records.is_empty() {
            return Err(invalid("a batch needs at least one record"));
        }
        let records = records
            .into_iter()
            .map(|r| (r.headers.into_client(), r.payload.to_vec()))
            .collect();
        self.cluster()?
            .publish_batch_with_headers(&tenant_id, &namespace, &stream, records, ack)
            .await
            .map(|offset| offset.map(BigInt::from))
            .map_err(classify)
    }

    /// Publish `payloads` (at least one) as one batch to the named shard,
    /// rather than the shard a key would hash to. For a producer that does its
    /// own partitioning. A shard the stream does not have is refused with
    /// `invalid_request` and nothing is written; a broker that does not
    /// advertise explicit shards rejects the call without sending anything.
    /// Resolves to the batch's first log offset, or `null` as `publish` does.
    #[napi]
    pub async fn publish_on_shard(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        payloads: Vec<Buffer>,
        ack: Option<String>,
    ) -> Result<Option<BigInt>> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        if payloads.is_empty() {
            return Err(invalid("a batch needs at least one payload"));
        }
        let payloads = payloads.into_iter().map(|p| p.to_vec()).collect();
        self.cluster()?
            .publish_batch_on_shard(&tenant_id, &namespace, &stream, shard, payloads, ack)
            .await
            .map(|offset| offset.map(BigInt::from))
            .map_err(classify)
    }

    /// Ask the broker for a producer id and return a `Producer` that sends
    /// under it. Its publishes land once, however many times they are sent.
    /// Close it with `close()` or `await using`.
    #[napi]
    pub async fn producer(&self) -> Result<Producer> {
        let producer = self
            .cluster()?
            .idempotent_producer()
            .await
            .map_err(classify)?;
        Ok(Producer::new(producer))
    }

    /// Publish `payloads` only if the shard's next offset is `expectedOffset`.
    /// `key` picks the shard as a plain publish does (none is shard 0). A
    /// refusal resolves with `written: false` and the shard's `tail`.
    #[napi]
    pub async fn publish_if(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        payloads: Vec<Buffer>,
        expected_offset: BigInt,
        key: Option<Buffer>,
    ) -> Result<crate::conditional::ConditionalWrite> {
        let payloads = payloads.into_iter().map(|p| p.to_vec()).collect();
        let expected = u64_of(expected_offset)?;
        let answer = self
            .cluster()?
            .publish_if(
                &tenant_id,
                &namespace,
                &stream,
                key.as_deref(),
                payloads,
                expected,
            )
            .await
            .map_err(classify)?;
        Ok(answer.into())
    }

    /// `publishIf`, to the named shard.
    #[napi]
    pub async fn publish_if_on_shard(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        payloads: Vec<Buffer>,
        expected_offset: BigInt,
    ) -> Result<crate::conditional::ConditionalWrite> {
        let payloads = payloads.into_iter().map(|p| p.to_vec()).collect();
        let expected = u64_of(expected_offset)?;
        let answer = self
            .cluster()?
            .publish_if_on_shard(&tenant_id, &namespace, &stream, shard, payloads, expected)
            .await
            .map_err(classify)?;
        Ok(answer.into())
    }

    /// Publish `payloads` as one batch, to the shard `key` hashes to or to
    /// shard 0 without one. Records sharing a key share a shard, so the batch
    /// is one frame and keeps its order. Retries and failures are those of
    /// `publish`. Resolves to the first record's log offset, or `null` as
    /// `publish` does.
    #[napi]
    pub async fn publish_batch(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        payloads: Vec<Buffer>,
        key: Option<Buffer>,
        ack: Option<String>,
    ) -> Result<Option<BigInt>> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        let payloads: Vec<Vec<u8>> = payloads.iter().map(|p| p.to_vec()).collect();
        let inner = self.cluster()?;
        match key {
            Some(key) => {
                inner
                    .publish_batch_keyed(
                        &tenant_id,
                        &namespace,
                        &stream,
                        Bytes::copy_from_slice(&key),
                        payloads,
                        ack,
                    )
                    .await
            }
            None => {
                inner
                    .publish_batch(&tenant_id, &namespace, &stream, payloads, ack)
                    .await
            }
        }
        .map(|offset| offset.map(BigInt::from))
        .map_err(classify)
    }

    /// Publish records that each carry a routing key, split by shard and sent
    /// to each shard's owner. A shard's records go as one batch, in the order
    /// given, accepted or refused as a unit; there is no order between shards.
    ///
    /// Failure is per shard and does not reject: the report says which
    /// records each piece held and why a piece failed, and a failed piece does
    /// not stop the others. Without `atLeastOnce` a piece that may have been
    /// written is not sent again. With it, such a piece is re-sent after the
    /// reconnect policy's backoff, so a record may land twice.
    #[napi]
    pub async fn publish_records(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        records: Vec<KeyedRecord>,
        ack: Option<String>,
        at_least_once: Option<bool>,
    ) -> Result<BatchReport> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        let records: Vec<(Bytes, Vec<u8>)> = records
            .iter()
            .map(|r| (Bytes::copy_from_slice(&r.key), r.payload.to_vec()))
            .collect();
        let inner = self.cluster()?;
        let report = if at_least_once.unwrap_or(false) {
            inner
                .publish_records_at_least_once(&tenant_id, &namespace, &stream, records, ack)
                .await
        } else {
            inner
                .publish_records(&tenant_id, &namespace, &stream, records, ack)
                .await
        };
        Ok(BatchReport::from_report(report))
    }
}

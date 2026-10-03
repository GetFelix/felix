//! Encoding a publish, admitting it, and handing it to a stream's writer.
//!
//! Binary frames are the data path. JSON is reached only as a fallback, for
//! a broker that never advertised the binary frame a publish needs.

use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use felix_wire::{AckMode, FrameHeader, Message};
use tokio::sync::oneshot;

use super::writer::{PublishRequest, PublishWorker};
use super::{AckOutcome, Publisher};
#[cfg(feature = "telemetry")]
use crate::telemetry::frame_counters;
use crate::telemetry::{maybe_append_publish_ts, maybe_append_publish_ts_batch};
#[cfg(feature = "telemetry")]
use crate::timings;

impl Publisher {
    /// Whether the broker advertised support for the acked binary publish frame.
    ///
    /// False against any broker that predates capability negotiation, because an
    /// unadvertised mask resolves to `ORIGINAL_V1_FLAGS`. That is the whole point:
    /// such a broker would match on `FLAG_BINARY_PUBLISH_BATCH`, know nothing of
    /// the `request_id` prefix, and read it as `tenant_len`.
    pub(super) fn supports_binary_ack(&self) -> bool {
        felix_wire::supports(
            self.inner.server_flags,
            felix_wire::FLAG_BINARY_PUBLISH_ACKED,
        )
    }

    /// Whether the broker advertised the keyed binary publish frame.
    ///
    /// False against a broker that predates it, for the same reason
    /// `supports_binary_ack` is: an unadvertised mask resolves to
    /// `ORIGINAL_V1_FLAGS`, and such a broker would read the key prefix as a
    /// `tenant_len`.
    pub(super) fn supports_binary_keyed(&self) -> bool {
        felix_wire::supports(
            self.inner.server_flags,
            felix_wire::FLAG_BINARY_PUBLISH_KEYED,
        )
    }

    /// Whether the broker takes an idempotent batch as a binary frame.
    pub(super) fn supports_binary_idempotent(&self) -> bool {
        felix_wire::supports(
            self.inner.server_flags,
            felix_wire::FLAG_BINARY_PUBLISH_ACKED | felix_wire::FLAG_BINARY_PUBLISH_IDEMPOTENT,
        )
    }

    /// Whether the broker takes this idempotent batch as a binary frame: a
    /// keyed one also needs the keyed layout.
    pub(super) fn supports_binary_idempotent_for(&self, key: Option<&bytes::Bytes>) -> bool {
        self.supports_binary_idempotent() && (key.is_none() || self.supports_binary_keyed())
    }

    pub(super) async fn publish_batch_binary_inner(
        &self,
        key: Option<&[u8]>,
        shard: Option<u32>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Vec<u8>],
    ) -> AckOutcome {
        let worker = self
            .route(tenant_id, namespace, stream, known_shard(key, shard))
            .await?;
        let payloads_with_ts;
        let payloads = if self.inner.bench_embed_ts {
            payloads_with_ts = payloads
                .iter()
                .map(|payload| maybe_append_publish_ts(payload.clone(), true))
                .collect::<Vec<_>>();
            &payloads_with_ts
        } else {
            payloads
        };
        #[cfg(feature = "telemetry")]
        let sample = crate::telemetry::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let start = crate::telemetry::t_now_if(sample);
        let (bytes, stats) = felix_wire::binary::encode_publish_batch_bytes_with_stats_keyed(
            key, tenant_id, namespace, stream, payloads,
        )?;
        #[cfg(not(feature = "telemetry"))]
        let _ = stats;
        #[cfg(feature = "telemetry")]
        if let Some(start) = start {
            let encode_ns = start.elapsed().as_nanos() as u64;
            timings::record_encode_ns(encode_ns);
            timings::record_binary_encode_ns(encode_ns);
            t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
        }
        #[cfg(feature = "telemetry")]
        if stats.reallocs > 0 {
            let counters = frame_counters();
            counters
                .binary_encode_reallocs
                .fetch_add(stats.reallocs, Ordering::Relaxed);
        }
        let permit = self.inner.admission.acquire(bytes.len()).await?;
        let (response_tx, response_rx) = oneshot::channel();
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::telemetry::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::BinaryBytes {
                bytes,
                item_count: payloads.len(),
                sample,
                ack: AckMode::None,
                request_id: None,
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue binary batch")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        let cancelled = CancelledAfterEnqueue::armed();
        let answer = response_rx.await.context("binary batch response dropped")?;
        cancelled.answered();
        answer
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn publish_batch_binary_acked_inner(
        &self,
        key: Option<&[u8]>,
        shard: Option<u32>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> AckOutcome {
        if ack == AckMode::None {
            return self
                .publish_batch_binary_inner(key, shard, tenant_id, namespace, stream, &payloads)
                .await;
        }
        let worker = self
            .route(tenant_id, namespace, stream, known_shard(key, shard))
            .await?;
        let payloads = maybe_append_publish_ts_batch(payloads, self.inner.bench_embed_ts);
        let request_id = worker.request_counter.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "telemetry")]
        let sample = crate::telemetry::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let start = crate::telemetry::t_now_if(sample);
        let bytes = felix_wire::binary::encode_acked_publish_batch_bytes_keyed(
            request_id, ack, key, tenant_id, namespace, stream, &payloads,
        )?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = start {
            let encode_ns = start.elapsed().as_nanos() as u64;
            timings::record_encode_ns(encode_ns);
            timings::record_binary_encode_ns(encode_ns);
            t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
        }
        let permit = self.inner.admission.acquire(bytes.len()).await?;
        let (response_tx, response_rx) = oneshot::channel();
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::telemetry::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::BinaryBytes {
                bytes,
                item_count: payloads.len(),
                sample,
                ack,
                request_id: Some(request_id),
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue acked binary batch")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        let cancelled = CancelledAfterEnqueue::armed();
        let answer = response_rx
            .await
            .context("acked binary batch response dropped")?;
        cancelled.answered();
        answer
    }

    pub(super) async fn publish_json_keyed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        key: Option<bytes::Bytes>,
        ack: AckMode,
    ) -> AckOutcome {
        let worker = self
            .route(
                tenant_id,
                namespace,
                stream,
                known_shard(key.as_deref(), None),
            )
            .await?;
        let payload = maybe_append_publish_ts(payload, self.inner.bench_embed_ts);
        // Enqueue publish on the single-writer publisher task.
        let (response_tx, response_rx) = oneshot::channel();
        let request_id = if ack == AckMode::None {
            None
        } else {
            Some(worker.request_counter.fetch_add(1, Ordering::Relaxed))
        };
        let message = Message::Publish {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            payload,
            key,
            request_id,
            ack: Some(ack),
        };
        let permit = self
            .inner
            .admission
            .acquire(estimate_text_publish_bytes(&message))
            .await?;
        #[cfg(feature = "telemetry")]
        let sample = crate::telemetry::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::telemetry::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::Message {
                message,
                ack,
                request_id,
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue publish")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        let cancelled = CancelledAfterEnqueue::armed();
        let answer = response_rx.await.context("publish response dropped")?;
        cancelled.answered();
        answer
    }

    pub(super) async fn publish_batch_json_keyed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        key: Option<bytes::Bytes>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        let worker = self
            .route(
                tenant_id,
                namespace,
                stream,
                known_shard(key.as_deref(), None),
            )
            .await?;
        let payloads = maybe_append_publish_ts_batch(payloads, self.inner.bench_embed_ts);
        let request_id = if ack == AckMode::None {
            None
        } else {
            Some(worker.request_counter.fetch_add(1, Ordering::Relaxed))
        };
        let message = Message::PublishBatch {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            payloads,
            key,
            request_id,
            ack: Some(ack),
        };
        self.send_message(&worker, message, ack, request_id)
            .await
            .map(|acked| acked.offset)
    }

    /// Queue a JSON publish on `worker` and wait for its answer.
    pub(super) async fn send_message(
        &self,
        worker: &PublishWorker,
        message: Message,
        ack: AckMode,
        request_id: Option<u64>,
    ) -> AckOutcome {
        // Batch publish uses the same queue/writer as single messages.
        let (response_tx, response_rx) = oneshot::channel();
        let permit = self
            .inner
            .admission
            .acquire(estimate_text_publish_bytes(&message))
            .await?;
        #[cfg(feature = "telemetry")]
        let sample = crate::telemetry::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let enqueue_start = crate::telemetry::t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::Message {
                message,
                ack,
                request_id,
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue publish batch")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        let cancelled = CancelledAfterEnqueue::armed();
        let answer = response_rx
            .await
            .context("publish batch response dropped")?;
        cancelled.answered();
        answer
    }
}

/// The shard a publish lands on, when the client can tell without asking.
///
/// An unkeyed publish is always shard 0 (`felix_wire::routing::shard_for`). A
/// keyed one is known only to a caller that has the stream's width, which
/// passes it as `shard`.
pub(super) fn known_shard(key: Option<&[u8]>, shard: Option<u32>) -> Option<u32> {
    match key {
        None => Some(0),
        Some(_) => shard,
    }
}

/// Counts a publish whose caller went away between the enqueue and the answer.
///
/// Cancelling a publish -- a `timeout`, a losing `select!` branch -- after it
/// has been handed to the worker does not cancel the publish. The record is
/// sent, and very likely lands; the only thing lost is the caller learning so.
/// That is inherent to any cancelled network call and not a defect, but it does
/// mean a timeout must not be read as "it did not happen": the outcome is
/// unknown, which is the same position a failed acknowledgement leaves you in.
///
/// Nothing can report this to the caller -- their future is gone -- so it is
/// reported to the operator instead. A publisher that cancels under load shows
/// up here, and records nobody thinks they published have somewhere to be
/// explained from.
pub(super) struct CancelledAfterEnqueue(bool);

impl CancelledAfterEnqueue {
    pub(super) fn armed() -> Self {
        Self(true)
    }

    pub(super) fn answered(mut self) {
        self.0 = false;
    }
}

impl Drop for CancelledAfterEnqueue {
    fn drop(&mut self) {
        if self.0 {
            t_counter!("felix_client_publish_cancelled_after_enqueue_total").increment(1);
        }
    }
}

fn estimate_text_publish_bytes(message: &Message) -> usize {
    let payload_bytes = match message {
        Message::Publish { payload, .. } => payload.len(),
        Message::PublishBatch { payloads, .. } => payloads.iter().map(Vec::len).sum(),
        _ => 0,
    };
    // JSON byte arrays can require up to four characters per byte including separators.
    FrameHeader::LEN
        .saturating_add(payload_bytes.saturating_mul(4))
        .saturating_add(1024)
}

//! Publishing: [`Publisher`], and the pool of single-writer streams behind it.
//!
//! A client opens several publish streams across its publish connections.
//! Each has exactly one writer task (`writer`) fed by a bounded queue, so
//! publishes on a stream are serialized without contending for it, and a
//! publish's bytes are admitted against a shared in-flight budget before
//! they are queued (`admission`), so a slow broker makes callers wait rather
//! than letting the client buffer without limit. `routing` picks the stream,
//! `send` encodes and enqueues, and `ack` reads the broker's answers.

mod ack;
mod admission;
mod idempotent;
mod routing;
mod send;
mod shard_streams;
mod widths;
mod writer;

pub use idempotent::IdempotentProducer;
pub use routing::PublishSharding;

pub(crate) use admission::PublishAdmission;
pub(crate) use shard_streams::{OpenWorker, ShardStreams};
pub(crate) use widths::{LearnWidth, StreamWidths};
pub(crate) use writer::{PublishWorker, run_publisher_writer_with_limit};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_wire::{AckMode, Message};
use tokio::sync::oneshot;

#[cfg(feature = "telemetry")]
use crate::telemetry::frame_counters;
use crate::telemetry::maybe_append_publish_ts_batch;
#[cfg(feature = "telemetry")]
use crate::timings;
use routing::{STREAM_SHARD_CACHE_CAPACITY, StreamShardCache};
use send::CancelledAfterEnqueue;
use writer::PublishRequest;

/// Publishes to one broker over the client's pool of publish streams.
///
/// Cheap to clone; clones share the pool. Built by [`crate::Client::publisher`].
#[derive(Clone)]
pub struct Publisher {
    pub(crate) inner: Arc<PublisherInner>,
}

impl Publisher {
    /// Frame-flag bits the broker advertised during the auth handshake,
    /// intersected across this publisher's streams.
    ///
    /// Resolves to `felix_wire::ORIGINAL_V1_FLAGS` against a broker that predates
    /// capability negotiation. Exposed so callers can log or assert what was
    /// actually negotiated rather than inferring it from behaviour.
    pub fn negotiated_server_flags(&self) -> u16 {
        self.inner.server_flags
    }

    /// Publish one payload using the binary data-plane encoding.
    ///
    /// Acked and unacked publishes both take the binary path: an unacked publish is
    /// a plain `FLAG_BINARY_PUBLISH_BATCH` frame, and an acked one adds
    /// `FLAG_BINARY_PUBLISH_ACKED` and waits for the broker's binary ack.
    ///
    /// Returns the record's log offset. `None` when the broker acknowledged
    /// before writing it, the stream has no log, the broker predates
    /// `FLAG_BINARY_PUBLISH_ACK_OFFSET`, or `ack` is `AckMode::None`.
    ///
    /// A broker acknowledges before writing only when it owns the shard of a
    /// `Leader` stream and runs with `ack_on_commit` off; every other ack comes
    /// after the write. A publish forwarded through another broker is one of
    /// those, so the same stream can give an offset for one record and `None`
    /// for the next, depending on which broker answered. An offset is never
    /// reported before the write, and is as durable as the broker's fsync
    /// policy makes the write.
    ///
    /// **Not cancel-safe.** Dropping this future — a `timeout`, a losing
    /// `select!` branch — after the record reaches the worker does not stop the
    /// publish. The record is sent and very likely lands; what is lost is
    /// learning so, which leaves the outcome exactly as unknown as a failed
    /// acknowledgement does. A timeout here means *do not know*, not *did not
    /// happen*, and re-sending on one may duplicate the record. Cancellations
    /// past that point are counted as
    /// `felix_client_publish_cancelled_after_enqueue_total`.
    ///
    /// JSON is reached only as a compatibility fallback, against a broker that
    /// never advertised the binary frame this call needs. There is no longer a
    /// way to ask for it: it is strictly more expensive and buys nothing the
    /// binary frames do not cover.
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        if ack == AckMode::None {
            return self
                .publish_batch_binary(tenant_id, namespace, stream, &[payload])
                .await
                .map(|()| None);
        }
        // A single acked publish is a one-item acked batch on the wire; there is no
        // separate binary encoding for single messages.
        self.publish_batch(tenant_id, namespace, stream, vec![payload], ack)
            .await
    }

    /// Publish one payload with a routing key.
    ///
    /// The key decides the shard, and therefore the broker. Records sharing a
    /// key are ordered with respect to each other; records with different keys
    /// are not, once a stream has more than one shard.
    ///
    /// Binary against a broker that advertised `FLAG_BINARY_PUBLISH_KEYED`, JSON
    /// against one that did not. A single keyed publish is a one-item keyed
    /// batch on the wire, exactly as `publish` is for the unkeyed case.
    ///
    /// Returns the record's log offset, when there is one; see [`Self::publish`].
    pub async fn publish_keyed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: bytes::Bytes,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        self.publish_batch_keyed(tenant_id, namespace, stream, key, vec![payload], ack)
            .await
    }

    /// Publish a batch.
    ///
    /// Binary, unless the broker never advertised the frame this needs — then
    /// JSON, which costs throughput and not correctness. That fallback is the
    /// only way a Felix client emits a JSON publish.
    ///
    /// Returns the log offset of the batch's first record; the rest follow it
    /// contiguously. `None` when the broker acknowledged before writing the
    /// batch, the stream has no log, the broker predates
    /// `FLAG_BINARY_PUBLISH_ACK_OFFSET`, or `ack` is `AckMode::None`.
    ///
    /// A broker acknowledges before writing only when it owns the shard of a
    /// `Leader` stream and runs with `ack_on_commit` off; every other ack comes
    /// after the write. A publish forwarded through another broker is one of
    /// those, so the same stream can give an offset for one record and `None`
    /// for the next, depending on which broker answered. An offset is never
    /// reported before the write, and is as durable as the broker's fsync
    /// policy makes the write.
    pub async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        if ack == AckMode::None {
            return self
                .publish_batch_binary(tenant_id, namespace, stream, &payloads)
                .await
                .map(|()| None);
        }
        // Fall back to the JSON encoding against a broker that has not advertised
        // the acked binary frame. Both paths are equivalent in semantics; only the
        // framing differs, so the fallback costs throughput, not correctness.
        if !self.supports_binary_ack() {
            // The private keyed form, not the deprecated public one: the
            // fallback has to keep working after that surface is removed.
            return self
                .publish_batch_json_keyed(tenant_id, namespace, stream, payloads, None, ack)
                .await;
        }
        self.publish_batch_binary_acked(tenant_id, namespace, stream, payloads, ack)
            .await
    }

    /// A batch routed by one key. Every record in it lands on the same shard,
    /// because a batch is acknowledged as a unit and splitting it across shards
    /// would make it several batches.
    ///
    /// Binary whenever the broker advertised `FLAG_BINARY_PUBLISH_KEYED`, with
    /// the JSON encoding as the fallback for brokers that predate it. The
    /// fallback costs throughput, not correctness.
    ///
    /// Returns the batch's first log offset, when there is one; see
    /// [`Self::publish_batch`].
    pub async fn publish_batch_keyed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: bytes::Bytes,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        if !self.supports_binary_keyed() {
            return self
                .publish_batch_json_keyed(tenant_id, namespace, stream, payloads, Some(key), ack)
                .await;
        }
        if ack == AckMode::None {
            return self
                .publish_batch_binary_inner(
                    Some(&key),
                    None,
                    tenant_id,
                    namespace,
                    stream,
                    &payloads,
                )
                .await
                .map(|_| None);
        }
        // An acked keyed batch needs both modifier bits, so it also needs the
        // broker to have advertised the acked frame.
        if !self.supports_binary_ack() {
            return self
                .publish_batch_json_keyed(tenant_id, namespace, stream, payloads, Some(key), ack)
                .await;
        }
        self.publish_batch_binary_acked_inner(
            Some(&key),
            None,
            tenant_id,
            namespace,
            stream,
            payloads,
            ack,
        )
        .await
        .map(|acked| acked.offset)
    }

    /// Publish a batch as one binary frame, without asking for an ack.
    pub async fn publish_batch_binary(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Vec<u8>],
    ) -> Result<()> {
        self.publish_batch_binary_inner(None, None, tenant_id, namespace, stream, payloads)
            .await
            .map(|_| ())
    }

    /// [`Publisher::publish_batch_binary`] for payloads already held as [`Bytes`].
    pub async fn publish_batch_binary_bytes(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Bytes],
    ) -> Result<()> {
        let worker = self.route(tenant_id, namespace, stream, None, None).await?;
        #[cfg(feature = "telemetry")]
        let sample = crate::telemetry::t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let start = crate::telemetry::t_now_if(sample);
        let (bytes, stats) = felix_wire::binary::encode_publish_batch_bytes_with_stats_from_bytes(
            tenant_id, namespace, stream, payloads,
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
        answer.map(|_| ())
    }

    /// Publish a batch that asks to be acknowledged, using the binary encoding.
    ///
    /// The frame is written with `FLAG_BINARY_PUBLISH_ACKED` and the broker replies
    /// with a binary ack frame.
    ///
    /// This is the unconditional form: it sends the acked binary frame whether or
    /// not the broker advertised support. Prefer `publish_batch`, which consults the
    /// mask negotiated during auth and falls back to JSON when the broker has not
    /// advertised `0x0008`.
    ///
    /// Returns the batch's first log offset, when there is one; see
    /// [`Self::publish_batch`].
    pub async fn publish_batch_binary_acked(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        self.publish_batch_binary_acked_inner(
            None, None, tenant_id, namespace, stream, payloads, ack,
        )
        .await
        .map(|acked| acked.offset)
    }

    /// One batch under a producer's sequence, appended once however many
    /// times it is sent. Always acknowledged, only once committed.
    ///
    /// The low-level send: the sequence is the caller's to keep, and a refusal
    /// comes back as a [`crate::PublishRefused`]. [`crate::IdempotentProducer`]
    /// is the form that keeps the sequence and does the re-sending.
    ///
    /// Returns the batch's first log offset, when the broker reported one. A
    /// re-send the broker already holds gets the offset it landed at the
    /// first time.
    pub async fn publish_idempotent_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        producer_id: u64,
        sequence: u64,
    ) -> Result<Option<u64>> {
        self.publish_idempotent_batch_routed(
            tenant_id,
            namespace,
            stream,
            None,
            0,
            payloads,
            producer_id,
            sequence,
        )
        .await
    }

    /// [`Self::publish_idempotent_batch`] with an optional routing key, which
    /// decides the shard and so whose sequence `sequence` is. `shard` is the
    /// one the key resolves to (0 without one), which puts the batch on that
    /// shard's stream when this publisher has shard streams.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_idempotent_batch_routed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: Option<&bytes::Bytes>,
        shard: u32,
        payloads: Vec<Vec<u8>>,
        producer_id: u64,
        sequence: u64,
    ) -> Result<Option<u64>> {
        let outcome = async {
            let worker = self
                .route(
                    tenant_id,
                    namespace,
                    stream,
                    key.map(|key| key.as_ref()),
                    Some(shard),
                )
                .await?;
            if self.supports_binary_idempotent_for(key) {
                let response_rx = self
                    .enqueue_idempotent_binary(
                        &worker,
                        tenant_id,
                        namespace,
                        stream,
                        key,
                        payloads,
                        producer_id,
                        sequence,
                    )
                    .await?;
                let cancelled = CancelledAfterEnqueue::armed();
                let answer = response_rx
                    .await
                    .context("idempotent binary batch response dropped")?;
                cancelled.answered();
                return answer.map(|acked| acked.offset);
            }
            let payloads = maybe_append_publish_ts_batch(payloads, self.inner.bench_embed_ts);
            let request_id = worker.request_counter.fetch_add(1, Ordering::Relaxed);
            let message = Message::PublishIdempotent {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
                payloads,
                key: key.cloned(),
                request_id,
                producer_id,
                sequence,
            };
            self.send_message(&worker, message, AckMode::PerBatch, Some(request_id))
                .await
                .map(|acked| acked.offset)
        }
        .await;
        self.forget_width_if_gone(
            tenant_id,
            namespace,
            stream,
            key.map(|key| key.as_ref()),
            &outcome,
        );
        outcome
    }

    /// Consecutive batches under consecutive sequences from `first_sequence`,
    /// with up to the stream's window unanswered at once.
    ///
    /// Returns the offsets of the batches acknowledged, from the first, and
    /// the first failure in sequence order if there was one. Everything from that
    /// batch on is unsettled: it may or may not have landed. All of them go on
    /// one stream, whose answers come back in the order it carried them, so
    /// the failure reported is the earliest one and not a consequence of it.
    ///
    /// Without a window, or against a broker that cannot take a binary
    /// idempotent batch, this sends one batch at a time.
    ///
    /// `shard` is the shard whose sequences these are: `key`'s, or 0 without
    /// one. A publisher with shard streams sends them on that shard's stream,
    /// so a shard's sequences only ever go out on one writer.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_idempotent_pipelined(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: Option<&bytes::Bytes>,
        shard: u32,
        batches: &[Vec<Vec<u8>>],
        producer_id: u64,
        first_sequence: u64,
    ) -> (Vec<Option<u64>>, Result<()>) {
        let worker = match self
            .route(
                tenant_id,
                namespace,
                stream,
                key.map(|key| key.as_ref()),
                Some(shard),
            )
            .await
        {
            Ok(worker) => worker,
            Err(err) => return (Vec::new(), Err(err)),
        };
        let window = if self.supports_binary_idempotent_for(key) {
            (worker.publish_window as usize).min(IDEMPOTENT_PIPELINE_MAX)
        } else {
            0
        };
        let mut acked = Vec::with_capacity(batches.len());
        if window <= 1 {
            for (index, payloads) in batches.iter().enumerate() {
                match self
                    .publish_idempotent_batch_routed(
                        tenant_id,
                        namespace,
                        stream,
                        key,
                        shard,
                        payloads.clone(),
                        producer_id,
                        first_sequence + index as u64,
                    )
                    .await
                {
                    Ok(offset) => acked.push(offset),
                    Err(err) => return (acked, Err(err)),
                }
            }
            return (acked, Ok(()));
        }
        let mut in_flight = std::collections::VecDeque::with_capacity(window);
        let mut sent = 0;
        loop {
            while sent < batches.len() && in_flight.len() < window {
                match self
                    .enqueue_idempotent_binary(
                        &worker,
                        tenant_id,
                        namespace,
                        stream,
                        key,
                        batches[sent].clone(),
                        producer_id,
                        first_sequence + sent as u64,
                    )
                    .await
                {
                    Ok(response_rx) => {
                        in_flight.push_back(response_rx);
                        sent += 1;
                    }
                    // Nothing from here on went out. What already did is
                    // settled first, in order, so an earlier failure wins.
                    Err(err) => {
                        while let Some(response_rx) = in_flight.pop_front() {
                            match response_rx.await {
                                Ok(Ok(answer)) => acked.push(answer.offset),
                                Ok(Err(first)) => return (acked, Err(first)),
                                Err(_) => {
                                    return (
                                        acked,
                                        Err(anyhow::anyhow!(
                                            "idempotent binary batch response dropped"
                                        )),
                                    );
                                }
                            }
                        }
                        return (acked, Err(err));
                    }
                }
            }
            let Some(response_rx) = in_flight.pop_front() else {
                return (acked, Ok(()));
            };
            let cancelled = CancelledAfterEnqueue::armed();
            let answer = response_rx.await;
            cancelled.answered();
            match answer {
                Ok(Ok(answer)) => acked.push(answer.offset),
                Ok(Err(err)) => return (acked, Err(err)),
                Err(_) => {
                    return (
                        acked,
                        Err(anyhow::anyhow!("idempotent binary batch response dropped")),
                    );
                }
            }
        }
    }

    /// Hand one binary idempotent batch to `worker`, returning where its
    /// answer will arrive.
    #[allow(clippy::too_many_arguments)]
    async fn enqueue_idempotent_binary(
        &self,
        worker: &PublishWorker,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: Option<&bytes::Bytes>,
        payloads: Vec<Vec<u8>>,
        producer_id: u64,
        sequence: u64,
    ) -> Result<oneshot::Receiver<AckOutcome>> {
        let payloads = maybe_append_publish_ts_batch(payloads, self.inner.bench_embed_ts);
        let request_id = worker.request_counter.fetch_add(1, Ordering::Relaxed);
        let bytes = felix_wire::binary::encode_idempotent_publish_batch_bytes(
            request_id,
            felix_wire::binary::ProducerSequence {
                producer_id,
                sequence,
            },
            key.map(bytes::Bytes::as_ref),
            tenant_id,
            namespace,
            stream,
            &payloads,
        )?;
        let permit = self.inner.admission.acquire(bytes.len()).await?;
        let (response_tx, response_rx) = oneshot::channel();
        worker
            .tx
            .send(PublishRequest::BinaryBytes {
                bytes,
                item_count: payloads.len(),
                sample: false,
                ack: AckMode::PerBatch,
                request_id: Some(request_id),
                _permit: permit,
                response: response_tx,
            })
            .await
            .context("enqueue idempotent binary batch")?;
        Ok(response_rx)
    }

    /// Close the publish streams once everything already queued has been
    /// written and, if it asked for one, acknowledged.
    ///
    /// The streams are shared by every publisher from the same client, so this
    /// ends publishing for all of them.
    pub async fn finish(&self) -> Result<()> {
        let own = self
            .inner
            .shard_streams
            .as_ref()
            .map(|streams| streams.close())
            .unwrap_or_default();
        let mut handles = Vec::new();
        for worker in self
            .inner
            .workers
            .iter()
            .chain(own.iter().map(|worker| &**worker))
        {
            let handle = {
                let mut guard = worker.handle.lock().await;
                guard.take()
            };
            if let Some(handle) = handle {
                let (response_tx, response_rx) = oneshot::channel();
                if worker
                    .tx
                    .send(PublishRequest::Finish {
                        response: response_tx,
                    })
                    .await
                    .is_ok()
                {
                    response_rx
                        .await
                        .context("publisher finish response dropped")??;
                }
                handles.push(handle);
            }
        }
        for handle in handles {
            handle.await.context("publisher writer task")??;
        }
        Ok(())
    }

    /// [`Publisher::publish`], reporting the shard's owner when this broker
    /// forwarded the batch rather than owning it.
    ///
    /// Internal because the owner is only useful to something that can act on
    /// it -- `ClusterClient`, which holds connections to more than one broker.
    /// A `Client` speaks to one and has nowhere else to send the next batch.
    pub(crate) async fn publish_reporting_owner(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> AckOutcome {
        if ack == AckMode::None {
            return self
                .publish_batch_binary_inner(None, None, tenant_id, namespace, stream, &[payload])
                .await;
        }
        self.publish_batch_binary_acked_inner(
            None,
            None,
            tenant_id,
            namespace,
            stream,
            vec![payload],
            ack,
        )
        .await
    }

    /// [`Publisher::publish_keyed`], reporting the shard's owner when this
    /// broker forwarded the batch rather than owning it. `shard` is the one
    /// the key resolves to, which puts the publish on that shard's stream.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_keyed_reporting_owner(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: bytes::Bytes,
        shard: u32,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> AckOutcome {
        if ack == AckMode::None {
            return self
                .publish_batch_binary_inner(
                    Some(&key),
                    Some(shard),
                    tenant_id,
                    namespace,
                    stream,
                    &[payload],
                )
                .await;
        }
        self.publish_batch_binary_acked_inner(
            Some(&key),
            Some(shard),
            tenant_id,
            namespace,
            stream,
            vec![payload],
            ack,
        )
        .await
    }

    /// Publish one payload using the JSON compatibility encoding.
    #[deprecated(
        since = "0.5.0",
        note = "the data path is binary; the binary publish frame carries a routing key \
                since 0.5.0. JSON is still spoken to a broker that predates the frame, but \
                the client chooses that itself — see `publish`. Removed in 0.6.0."
    )]
    pub async fn publish_json(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<()> {
        self.publish_json_keyed(tenant_id, namespace, stream, payload, None, ack)
            .await
            .map(|_| ())
    }

    /// Publish a batch using the JSON compatibility encoding.
    #[deprecated(
        since = "0.5.0",
        note = "the data path is binary; the binary publish frame carries a routing key \
                since 0.5.0. JSON is still spoken to a broker that predates the frame, but \
                the client chooses that itself — see `publish_batch`. Removed in 0.6.0."
    )]
    pub async fn publish_batch_json(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<()> {
        self.publish_batch_json_keyed(tenant_id, namespace, stream, payloads, None, ack)
            .await
            .map(|_| ())
    }
}

pub(crate) struct PublisherInner {
    pub(crate) workers: Arc<Vec<PublishWorker>>,
    pub(crate) sharding: PublishSharding,
    pub(crate) rr: AtomicUsize,
    admission: Arc<PublishAdmission>,
    stream_cache: Mutex<StreamShardCache>,
    stream_hasher: ahash::RandomState,
    /// Streams of their own for shards, shared by every publisher from the
    /// client. `None` routes everything through `workers`.
    shard_streams: Option<Arc<ShardStreams>>,
    /// What a keyed publish with no shard named looks its shard up in.
    /// `None` sends such publishes to the pool.
    widths: Option<Arc<StreamWidths>>,
    bench_embed_ts: bool,
    /// Intersection of every worker's advertised flags.
    ///
    /// The workers all talk to the same broker, so in practice these agree; the
    /// intersection is taken anyway so a single lagging stream can never cause
    /// an encoding to be used on a connection that cannot parse it.
    server_flags: u16,
}

impl PublisherInner {
    #[cfg(test)]
    pub(crate) fn new(workers: Arc<Vec<PublishWorker>>, sharding: PublishSharding) -> Self {
        Self::with_admission(
            workers,
            sharding,
            Arc::new(PublishAdmission::new(
                crate::config::DEFAULT_PUBLISH_INFLIGHT_BYTES,
            )),
            ahash::RandomState::new(),
        )
    }

    pub(crate) fn with_admission(
        workers: Arc<Vec<PublishWorker>>,
        sharding: PublishSharding,
        admission: Arc<PublishAdmission>,
        stream_hasher: ahash::RandomState,
    ) -> Self {
        let server_flags = workers
            .iter()
            .map(|worker| worker.server_flags)
            .fold(u16::MAX, |acc, flags| acc & flags);
        Self {
            workers,
            sharding,
            rr: AtomicUsize::new(0),
            admission,
            stream_cache: Mutex::new(StreamShardCache::new(STREAM_SHARD_CACHE_CAPACITY)),
            stream_hasher,
            shard_streams: None,
            widths: None,
            bench_embed_ts: false,
            server_flags,
        }
    }

    pub(crate) fn with_runtime_config(
        workers: Arc<Vec<PublishWorker>>,
        sharding: PublishSharding,
        admission: Arc<PublishAdmission>,
        stream_hasher: ahash::RandomState,
        shard_streams: Arc<ShardStreams>,
        widths: Arc<StreamWidths>,
        bench_embed_ts: bool,
    ) -> Self {
        let mut inner = Self::with_admission(workers, sharding, admission, stream_hasher);
        inner.shard_streams = Some(shard_streams);
        inner.widths = Some(widths);
        inner.bench_embed_ts = bench_embed_ts;
        inner
    }

    /// [`Self::new`] with per-shard streams opened by `open`, up to `cap`.
    #[cfg(test)]
    pub(crate) fn with_shard_streams(
        workers: Arc<Vec<PublishWorker>>,
        cap: usize,
        open: OpenWorker,
    ) -> Self {
        let mut inner = Self::new(workers, PublishSharding::HashStream);
        inner.shard_streams = Some(Arc::new(ShardStreams::new(cap, open)));
        inner
    }

    /// [`Self::with_shard_streams`] that learns stream widths with `learn`.
    #[cfg(test)]
    pub(crate) fn with_widths(
        workers: Arc<Vec<PublishWorker>>,
        cap: usize,
        open: OpenWorker,
        learn: LearnWidth,
    ) -> Self {
        let mut inner = Self::with_shard_streams(workers, cap, open);
        inner.widths = Some(Arc::new(StreamWidths::new(learn)));
        inner
    }
}

/// Most idempotent batches one producer keeps unanswered on a stream.
///
/// A shard's leader remembers the last 64 sequences of each producer, and a
/// batch re-sent after a failure has to find the whole unsettled run still
/// remembered, or it is refused as expired instead of answered as a duplicate.
pub(crate) const IDEMPOTENT_PIPELINE_MAX: usize = 64;

/// What a successful ack said beyond "done".
#[derive(Debug, Default)]
pub(crate) struct Acked {
    /// The shard's owner, when the broker that answered forwarded the batch.
    ///
    /// It travels back so `ClusterClient` can send the next batch for this
    /// shard straight there. A forward is correct but costs a decrypt, a
    /// re-encrypt and a decrypt, roughly half the throughput per core (#536).
    pub(crate) forwarded_to: Option<felix_wire::binary::PublishOwner>,
    /// The log offset of the batch's first record, when the broker reported it.
    pub(crate) offset: Option<u64>,
}

/// What a publish learns from its answer: failure, or an [`Acked`].
pub(crate) type AckOutcome = Result<Acked>;

#[cfg(test)]
mod tests;

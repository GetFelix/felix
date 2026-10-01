//! Binary publish batches on the control stream, acked and unacked.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;

use anyhow::{Context, Result, anyhow};
use felix_authz::{Action, Namespace, StreamName, TenantId, stream_resource};
use felix_broker::Broker;
use felix_wire::Frame;
use tokio::sync::{Mutex, Semaphore, mpsc, watch};
use tracing::Instrument;

use super::batch::handle_publish_batch_message;
use crate::observability::timings;
use crate::serving::auth::AuthContext;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::ack::{
    AckEncoding, AckTimeoutState, AckWaiterMessage, Outgoing, handle_ack_enqueue_result,
    send_outgoing_critical,
};
use crate::serving::quic::handlers::publish::ingress::enqueue_tenant_publish;
use crate::serving::quic::handlers::publish::route::{
    publish_target, resolve_route, resolve_shard,
};
use crate::serving::quic::handlers::publish::{PublishContext, PublishJob, StreamHandleCache};
use crate::serving::quic::telemetry::{
    count_publish, count_publish_accepted, log_decode_error, payload_len_sum, t_histogram, t_now_if,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_binary_publish_batch_control(
    broker: &Broker,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    frame: &Frame,
    auth_ctx: Option<&AuthContext>,
    sample: bool,
    // Bounds the unacked backpressure wait: without a timer, teardown is what
    // ends it.
    cancel_tx: &watch::Sender<bool>,
) -> Result<()> {
    let decode_start = t_now_if(sample);
    let batch = match felix_wire::binary::decode_publish_batch(frame)
        .context("decode binary publish batch")
    {
        Ok(batch) => batch,
        Err(err) => {
            #[cfg(feature = "telemetry")]
            {
                let counters = crate::serving::quic::telemetry::frame_counters();
                counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
            }
            log_decode_error("binary_publish_batch", &err, frame);
            return Err(err);
        }
    };
    let auth_ctx = auth_ctx.ok_or_else(|| anyhow!("auth required"))?;
    if auth_ctx.tenant_id != batch.tenant_id {
        return Err(anyhow!("tenant mismatch"));
    }
    let resource = stream_resource(
        &TenantId::new(batch.tenant_id.as_str()),
        &Namespace::new(batch.namespace.as_str()),
        &StreamName::new(batch.stream.as_str()),
    );
    if !auth_ctx.matcher.allows(Action::StreamPublish, &resource) {
        return Err(anyhow!("forbidden"));
    }
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::serving::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters
            .pub_items_in_ok
            .fetch_add(batch.payloads.len() as u64, Ordering::Relaxed);
    }
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_decode_ns(decode_ns);
        t_histogram!("felix_broker_decode_ns").record(decode_ns as f64);
    }
    // An unkeyed frame still resolves to shard 0, so this is the same routing
    // decision the JSON path makes -- the key just arrives in a cheaper frame.
    let shard = resolve_shard(
        publish_ctx,
        &batch.tenant_id,
        &batch.namespace,
        &batch.stream,
        batch.key.as_deref(),
    );
    let Ok(target) = publish_target(
        resolve_route(
            broker,
            publish_ctx.authority(),
            stream_cache,
            stream_cache_key,
            &batch.tenant_id,
            &batch.namespace,
            &batch.stream,
            shard,
        )
        .await,
        publish_ctx,
        &batch.tenant_id,
        &batch.namespace,
        &batch.stream,
        shard,
        // Fire-and-forget: the owner is told no acknowledgement is expected, the
        // same contract the client gave this broker.
        felix_wire::internal::AckMode::None,
        &auth_ctx.token,
    ) else {
        count_publish("error");
        return Ok(());
    };
    let span = tracing::info_span!(
        "publish_batch_binary",
        tenant_id = %batch.tenant_id,
        namespace = %batch.namespace,
        stream = %batch.stream,
        count = batch.payloads.len()
    );
    async move {
        let payloads = batch.payloads;
        let payload_bytes = payload_len_sum(&payloads);
        let fanout_start = t_now_if(sample);
        let r = enqueue_tenant_publish(
            publish_ctx,
            &batch.tenant_id,
            PublishJob {
                target,
                payloads,
                response: None,
                acked_on_enqueue: false,
                admission_permit: None,
                fenced: None,
            },
            publish_ctx.overflow_policy(),
            Some(cancel_tx.subscribe()),
        )
        .await;
        match r {
            Ok(true) => {
                count_publish_accepted("accepted", payload_bytes);
            }
            Ok(false) => {
                count_publish("dropped");
            }
            Err(err) => {
                count_publish("error");
                tracing::warn!(error = %err, "publish enqueue failed");
            }
        }

        if let Some(start) = fanout_start {
            let fanout_ns = start.elapsed().as_nanos() as u64;
            timings::record_fanout_ns(fanout_ns);
            t_histogram!("felix_broker_ingress_enqueue_ns").record(fanout_ns as f64);
        }
        Ok(())
    }
    .instrument(span)
    .await
}

/// Handle a binary publish batch that asked to be acknowledged.
///
/// This is the binary counterpart of the JSON acked publish path. It decodes the
/// frame and then delegates to [`handle_publish_batch_message`] with
/// [`AckEncoding::Binary`], so both encodings share one set of admission,
/// authorization, overload and commit-ack semantics — the encoding only decides
/// how the reply is framed.
///
/// Failures here reply with an error ack rather than tearing down the stream.
/// Frames are length-prefixed, so a malformed *body* does not desynchronise the
/// framing, and the client is synchronously blocked waiting for this ack: killing
/// the stream would turn a bad request into a stalled connection.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_acked_binary_publish_batch_control(
    broker: &Broker,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    frame: &Frame,
    auth_ctx: Option<&AuthContext>,
    throttled: bool,
    ack_on_commit: bool,
    sample: bool,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    ack_waiters: &Arc<Semaphore>,
    ack_waiter_tx: &mpsc::Sender<AckWaiterMessage>,
    // Frame-flag bits this client advertised. Read only to decide whether the
    // ack may name a forwarding owner: that sets a flag bit, and a client that
    // did not advertise it rejects the whole frame rather than masking the bit
    // off -- rejecting an acknowledgement for a publish that worked.
    peer_flags: u16,
    // Feature bits this client offered, for what an idempotent batch under a
    // reused sequence is answered with.
    peer_features: u32,
) -> Result<()> {
    // Read the correlation prefix before the body, so even an undecodable batch
    // can be answered with an ack the client is able to match to its request.
    let (request_id, ack) = match felix_wire::binary::peek_acked_publish_prefix(frame) {
        Ok(prefix) => prefix,
        Err(err) => {
            // Without a request_id there is nothing to correlate, so this is a
            // protocol violation rather than a per-request failure.
            log_decode_error("acked_binary_publish_prefix", &anyhow!(err), frame);
            return Err(anyhow!("malformed acked publish prefix"));
        }
    };
    let reply_error = |error: ClientError| async move {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                AckEncoding::Binary.error(request_id, error),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await
    };

    let decode_start = t_now_if(sample);
    let batch = match felix_wire::binary::decode_acked_publish_batch(frame) {
        Ok(batch) => batch,
        Err(err) => {
            #[cfg(feature = "telemetry")]
            {
                let counters = crate::serving::quic::telemetry::frame_counters();
                counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
            }
            log_decode_error("acked_binary_publish_batch", &anyhow!(err), frame);
            count_publish("error");
            reply_error(ClientError::invalid("malformed publish batch")).await?;
            return Ok(());
        }
    };
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_decode_ns(decode_ns);
        t_histogram!("felix_broker_decode_ns").record(decode_ns as f64);
    }

    let producer = batch.producer;
    let batch = batch.batch;
    let Some(auth_ctx) = auth_ctx else {
        reply_error(ClientError::unauthenticated("auth required")).await?;
        return Ok(());
    };
    if auth_ctx.tenant_id != batch.tenant_id {
        count_publish("error");
        reply_error(ClientError::forbidden("tenant mismatch")).await?;
        return Ok(());
    }
    let resource = stream_resource(
        &TenantId::new(batch.tenant_id.as_str()),
        &Namespace::new(batch.namespace.as_str()),
        &StreamName::new(batch.stream.as_str()),
    );
    if !auth_ctx.matcher.allows(Action::StreamPublish, &resource) {
        count_publish("error");
        reply_error(ClientError::forbidden("forbidden")).await?;
        return Ok(());
    }

    handle_publish_batch_message(
        peer_flags,
        broker,
        publish_ctx,
        stream_cache,
        stream_cache_key,
        throttled,
        ack_on_commit,
        // Answered like `publish_idempotent`, so a refusal stays typed. The
        // client reads JSON and binary acks off the same stream.
        if producer.is_some() {
            AckEncoding::Idempotent
        } else {
            AckEncoding::Binary
        },
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ack_waiters,
        ack_waiter_tx,
        batch.tenant_id,
        batch.namespace,
        batch.stream,
        batch.payloads,
        batch.key,
        Some(request_id),
        // Always per batch for a producer: the sequence names the batch.
        Some(if producer.is_some() {
            felix_wire::AckMode::PerBatch
        } else {
            ack
        }),
        sample,
        auth_ctx.token.clone(),
        producer.map(|producer| {
            (
                producer.producer_id,
                producer.sequence,
                super::batch::sequence_reuse(peer_features),
            )
        }),
    )
    .await
}

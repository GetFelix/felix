//! Publish handlers for uni-directional streams (fire-and-forget, no acks).

#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_authz::{Action, Namespace, StreamName, TenantId, stream_resource};
use felix_broker::Broker;
use felix_wire::Frame;

use crate::serving::auth::AuthContext;
use crate::serving::quic::handlers::publish::ingress::enqueue_tenant_publish;
use crate::serving::quic::handlers::publish::route::{
    UNKEYED_SHARD, publish_target, resolve_route, resolve_shard,
};
use crate::serving::quic::handlers::publish::{PublishContext, PublishJob, StreamHandleCache};
use crate::serving::quic::telemetry::{
    count_publish, count_publish_accepted, log_decode_error, payload_len_sum,
};

pub(crate) async fn handle_binary_publish_batch_uni(
    broker: &Broker,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    frame: &Frame,
    auth_ctx: Option<&AuthContext>,
) -> Result<bool> {
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
            log_decode_error("uni_binary_publish_batch", &err, frame);
            return Err(err);
        }
    };
    let auth_ctx = match auth_ctx {
        Some(ctx) => ctx,
        None => return Ok(false),
    };
    if auth_ctx.tenant_id != batch.tenant_id {
        return Ok(false);
    }
    let resource = stream_resource(
        &TenantId::new(batch.tenant_id.as_str()),
        &Namespace::new(batch.namespace.as_str()),
        &StreamName::new(batch.stream.as_str()),
    );
    if !auth_ctx.matcher.allows(Action::StreamPublish, &resource) {
        return Ok(false);
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
        return Ok(true);
    };
    let payloads = batch.payloads;
    let payload_bytes = payload_len_sum(&payloads);
    match enqueue_tenant_publish(
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
        None,
    )
    .await
    {
        Ok(true) => {
            count_publish_accepted("accepted", payload_bytes);
        }
        Ok(false) => {
            count_publish("dropped");
        }
        Err(_) => {
            count_publish("error");
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_message_uni(
    broker: &Broker,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Vec<u8>,
    // The publisher's token, carried on a forward for the owner to verify.
    credential: String,
) -> Result<bool> {
    super::record_json_publish("publish");
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::serving::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_items_in_ok.fetch_add(1, Ordering::Relaxed);
    }
    let Ok(target) = publish_target(
        resolve_route(
            broker,
            publish_ctx.authority(),
            stream_cache,
            stream_cache_key,
            &tenant_id,
            &namespace,
            &stream,
            UNKEYED_SHARD,
        )
        .await,
        publish_ctx,
        &tenant_id,
        &namespace,
        &stream,
        UNKEYED_SHARD,
        // Fire-and-forget: the owner is told no acknowledgement is expected, the
        // same contract the client gave this broker.
        felix_wire::internal::AckMode::None,
        &credential,
    ) else {
        count_publish("error");
        return Ok(true);
    };

    let payload_bytes = payload.len() as u64;
    let r = enqueue_tenant_publish(
        publish_ctx,
        &tenant_id,
        PublishJob {
            target,
            payloads: vec![Bytes::from(payload)],
            response: None,
            acked_on_enqueue: false,
            admission_permit: None,
            fenced: None,
        },
        publish_ctx.overflow_policy(),
        None,
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
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_batch_message_uni(
    broker: &Broker,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    publish_ctx: &PublishContext,
    tenant_id: String,
    namespace: String,
    stream: String,
    payloads: Vec<Vec<u8>>,
    // The publisher's token, carried on a forward for the owner to verify.
    credential: String,
) -> Result<bool> {
    super::record_json_publish("publish_batch");
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::serving::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters
            .pub_items_in_ok
            .fetch_add(payloads.len() as u64, Ordering::Relaxed);
    }
    let Ok(target) = publish_target(
        resolve_route(
            broker,
            publish_ctx.authority(),
            stream_cache,
            stream_cache_key,
            &tenant_id,
            &namespace,
            &stream,
            UNKEYED_SHARD,
        )
        .await,
        publish_ctx,
        &tenant_id,
        &namespace,
        &stream,
        UNKEYED_SHARD,
        // Fire-and-forget: the owner is told no acknowledgement is expected, the
        // same contract the client gave this broker.
        felix_wire::internal::AckMode::None,
        &credential,
    ) else {
        count_publish("error");
        return Ok(true);
    };
    let payloads = payloads.into_iter().map(Bytes::from).collect::<Vec<_>>();
    let payload_bytes = payload_len_sum(&payloads);

    let r = enqueue_tenant_publish(
        publish_ctx,
        &tenant_id,
        PublishJob {
            target,
            payloads,
            response: None,
            acked_on_enqueue: false,
            admission_permit: None,
            fenced: None,
        },
        publish_ctx.overflow_policy(),
        None,
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
            return Ok(false);
        }
    }
    Ok(true)
}

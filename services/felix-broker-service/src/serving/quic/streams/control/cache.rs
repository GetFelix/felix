//! Cache requests on the control stream.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use felix_authz::Action;
use felix_wire::Message;

use super::authz::authorize_cache;
use super::{Ctx, Session, Step};
use crate::observability::timings;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::errors::{AckEnqueueError, record_ack_enqueue_failure};
use crate::serving::quic::handlers::publish::{
    Outgoing, handle_ack_enqueue_result, send_outgoing_best_effort, send_outgoing_critical,
};
use crate::serving::quic::telemetry::t_now_if;

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn cache_put(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    cache: String,
    key: String,
    value: Bytes,
    request_id: Option<u64>,
    ttl_ms: Option<u64>,
) -> Result<Step> {
    let Ctx {
        broker,
        publish_ctx,
        authz_ctx,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        sample,
        read_ns,
        decode_ns,
        ..
    } = *cx;
    if !authorize_cache(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::CacheWrite,
        &namespace,
        &cache,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    if let Some(read_ns) = read_ns {
        timings::record_cache_read_ns(read_ns);
    }
    if let Some(decode_ns) = decode_ns {
        timings::record_cache_decode_ns(decode_ns);
    }
    // Cache scope validation: cache operations are rejected if the cache isn't
    // registered for the (tenant, namespace, cache) triple.
    if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::CacheMessage(
                    ClientError::not_found(format!(
                        "cache scope not found: {tenant_id}/{namespace}/{cache}"
                    ))
                    .into_message(),
                ),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        if request_id.is_none() {
            // When request_id is None, the client is using a "best effort" cache API and the
            // stream is closed after the single request/response completes.
            return Ok(Step::Close(true));
        }
        return Ok(Step::Next);
    }
    let ttl = ttl_ms.map(Duration::from_millis);
    let lookup_start = t_now_if(sample);
    // Routed, not applied locally: exactly one broker owns this
    // key's shard, and a write served here instead would be the
    // second copy nothing reconciles.
    let applied = crate::serving::cache_routing::apply_cache_op(
        broker,
        (publish_ctx.marks.as_deref(), publish_ctx.quorum_timeout),
        publish_ctx.ingress.as_deref(),
        publish_ctx.peers.as_deref(),
        session
            .auth_ctx
            .as_ref()
            .map_or("", |ctx| ctx.token.as_str()),
        tenant_id.as_str(),
        namespace.as_str(),
        cache.as_str(),
        key.as_str(),
        crate::serving::cache_routing::put_request(value, ttl),
    )
    .await;
    if let Some(start) = lookup_start {
        let lookup_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_insert_ns(lookup_ns);
    }
    if let Err(reason) = applied {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::CacheMessage(
                    ClientError::from_anyhow(&reason)
                        .prefixed("cache put not served")
                        .into_message(),
                ),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        if request_id.is_none() {
            return Ok(Step::Close(true));
        }
        return Ok(Step::Next);
    }
    if let Some(request_id) = request_id {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::CacheMessage(Message::CacheOk { request_id }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(Step::Next);
    }
    match send_outgoing_best_effort(
        out_ack_tx,
        out_ack_depth,
        "felix_broker_out_ack_depth",
        ack_throttle_tx,
        Outgoing::CacheMessage(Message::Ok),
    )
    .await
    {
        Ok(()) => {}
        Err(AckEnqueueError::Full) => {}
        Err(err) => return Err(record_ack_enqueue_failure(err)),
    }
    Ok(Step::Close(true))
}

pub(super) async fn cache_get(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    cache: String,
    key: String,
    request_id: Option<u64>,
) -> Result<Step> {
    let Ctx {
        broker,
        publish_ctx,
        authz_ctx,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        sample,
        read_ns,
        decode_ns,
        ..
    } = *cx;
    if !authorize_cache(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::CacheRead,
        &namespace,
        &cache,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    if let Some(read_ns) = read_ns {
        timings::record_cache_read_ns(read_ns);
    }
    if let Some(decode_ns) = decode_ns {
        timings::record_cache_decode_ns(decode_ns);
    }
    // Cache scope validation: cache operations are rejected if the cache isn't
    // registered for the (tenant, namespace, cache) triple.
    if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::CacheMessage(
                    ClientError::not_found(format!(
                        "cache scope not found: {tenant_id}/{namespace}/{cache}"
                    ))
                    .into_message(),
                ),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        if request_id.is_none() {
            // When request_id is None, the client is using a "best effort" cache API and the
            // stream is closed after the single request/response completes.
            return Ok(Step::Close(true));
        }
        return Ok(Step::Next);
    }
    let lookup_start = t_now_if(sample);
    // Only a client that offered the bit is sent a version, so any other
    // client's answer is byte-identical to what it always was.
    let versioned =
        felix_wire::supports_feature(session.peer_features, felix_wire::FEATURE_CACHE_CONDITIONAL);
    let read = crate::serving::cache_routing::apply_cache_request(
        broker,
        (publish_ctx.marks.as_deref(), publish_ctx.quorum_timeout),
        publish_ctx.ingress.as_deref(),
        publish_ctx.peers.as_deref(),
        session
            .auth_ctx
            .as_ref()
            .map_or("", |ctx| ctx.token.as_str()),
        &tenant_id,
        &namespace,
        &cache,
        &key,
        if versioned {
            crate::serving::forward::CacheRequest::GetVersioned
        } else {
            crate::serving::forward::CacheRequest::Get
        },
    )
    .await;
    if let Some(start) = lookup_start {
        let lookup_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_lookup_ns(lookup_ns);
    }
    let (value, version) = match read {
        Ok(answer) => (
            answer.value,
            answer.outcome.and_then(|outcome| outcome.version),
        ),
        Err(reason) => {
            // A read this broker cannot route is an error, never an
            // empty answer: reporting a miss would let a client
            // conclude the key does not exist when it does, on the
            // owner.
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::CacheMessage(
                        ClientError::from_anyhow(&reason)
                            .prefixed("cache get not served")
                            .into_message(),
                    ),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            if request_id.is_none() {
                return Ok(Step::Close(true));
            }
            return Ok(Step::Next);
        }
    };
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::CacheMessage(Message::CacheValue {
                tenant_id,
                namespace,
                cache,
                key,
                value,
                request_id,
                version,
            }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    if request_id.is_none() {
        // When request_id is None, the client is using a "best effort" cache API and the
        // stream is closed after the single request/response completes.
        return Ok(Step::Close(true));
    }
    Ok(Step::Next)
}

pub(super) async fn cache_delete(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    cache: String,
    key: String,
    request_id: Option<u64>,
) -> Result<Step> {
    let Ctx {
        broker,
        publish_ctx,
        authz_ctx,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ..
    } = *cx;
    // A delete is a write, so it is authorized as one. Letting it
    // through on `CacheRead` would make read-only credentials able
    // to destroy data.
    if !authorize_cache(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::CacheWrite,
        &namespace,
        &cache,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::CacheMessage(
                    ClientError::not_found(format!(
                        "cache scope not found: {tenant_id}/{namespace}/{cache}"
                    ))
                    .into_message(),
                ),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        if request_id.is_none() {
            return Ok(Step::Close(true));
        }
        return Ok(Step::Next);
    }

    let removed = crate::serving::cache_routing::apply_cache_op(
        broker,
        (publish_ctx.marks.as_deref(), publish_ctx.quorum_timeout),
        publish_ctx.ingress.as_deref(),
        publish_ctx.peers.as_deref(),
        session
            .auth_ctx
            .as_ref()
            .map_or("", |ctx| ctx.token.as_str()),
        &tenant_id,
        &namespace,
        &cache,
        &key,
        crate::serving::forward::CacheRequest::Delete,
    )
    .await;

    let value = match removed {
        Ok(value) => value,
        Err(reason) => {
            // Refused rather than reported as "nothing was there".
            // A client told the key is gone when the owner still
            // holds it would be worse than a plain failure.
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::CacheMessage(
                        ClientError::from_anyhow(&reason)
                            .prefixed("cache delete not served")
                            .into_message(),
                    ),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            if request_id.is_none() {
                return Ok(Step::Close(true));
            }
            return Ok(Step::Next);
        }
    };

    // Answered with the value that was removed, so a caller learns
    // whether the key was there without a second round trip.
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::CacheMessage(Message::CacheValue {
                tenant_id,
                namespace,
                cache,
                key,
                value,
                request_id,
                version: None,
            }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    if request_id.is_none() {
        return Ok(Step::Close(true));
    }
    Ok(Step::Next)
}

/// A conditional put or delete, as the control stream carries it.
pub(super) struct ConditionalRequest {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) cache: String,
    pub(super) key: String,
    pub(super) request_id: u64,
    pub(super) request: crate::serving::forward::CacheRequest,
}

/// `cache_put_if` and `cache_delete_if`: both are writes, authorized as one,
/// and both answer with `cache_condition_result`.
pub(super) async fn cache_conditional(
    cx: &Ctx<'_>,
    session: &mut Session,
    request: ConditionalRequest,
) -> Result<Step> {
    let Ctx {
        broker,
        publish_ctx,
        authz_ctx,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ..
    } = *cx;
    let ConditionalRequest {
        tenant_id,
        namespace,
        cache,
        key,
        request_id,
        request,
    } = request;
    if !authorize_cache(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::CacheWrite,
        &namespace,
        &cache,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    let answer = if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
        ClientError::not_found(format!(
            "cache scope not found: {tenant_id}/{namespace}/{cache}"
        ))
        .into_message()
    } else {
        let what = match request {
            crate::serving::forward::CacheRequest::DeleteIf { .. } => "cache delete_if",
            _ => "cache put_if",
        };
        let applied = crate::serving::cache_routing::apply_cache_request(
            broker,
            (publish_ctx.marks.as_deref(), publish_ctx.quorum_timeout),
            publish_ctx.ingress.as_deref(),
            publish_ctx.peers.as_deref(),
            session
                .auth_ctx
                .as_ref()
                .map_or("", |ctx| ctx.token.as_str()),
            &tenant_id,
            &namespace,
            &cache,
            &key,
            request,
        )
        .await;
        match applied {
            Ok(crate::serving::cache_routing::CacheAnswer {
                outcome: Some(outcome),
                ..
            }) => Message::CacheConditionResult {
                applied: outcome.applied,
                version: outcome.version,
                request_id,
            },
            Ok(_) => {
                ClientError::internal(format!("{what} answered without an outcome")).into_message()
            }
            Err(reason) => ClientError::from_anyhow(&reason)
                .prefixed(&format!("{what} not served"))
                .into_message(),
        }
    };
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::CacheMessage(answer),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    Ok(Step::Next)
}

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn cache_watch(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    cache: String,
    key: Option<String>,
    prefix: Option<String>,
    shard: Option<u32>,
    from_offset: Option<u64>,
    retained: bool,
    subscription_id: Option<u64>,
) -> Result<Step> {
    let Ctx {
        broker,
        connection,
        config,
        publish_ctx,
        authz_ctx,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ..
    } = *cx;
    // A watch is a read of the cache, and is authorized as one.
    if !authorize_cache(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::CacheRead,
        &namespace,
        &cache,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    crate::serving::quic::handlers::cache_watch::handle_cache_watch_message(
        Arc::clone(broker),
        connection.clone(),
        config.clone(),
        publish_ctx,
        crate::serving::quic::handlers::cache_watch::WatchResponder {
            out_ack_tx,
            out_ack_depth,
            ack_throttle_tx,
            ack_timeout_state,
            cancel_tx,
        },
        crate::serving::quic::handlers::cache_watch::WatchRequest {
            tenant_id,
            namespace,
            cache,
            key,
            prefix,
            shard,
            from_offset,
            retained,
            subscription_id,
        },
        session.peer_features,
    )
    .await?;
    Ok(Step::Next)
}

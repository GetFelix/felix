//! Counter requests on the control stream.

use anyhow::Result;
use felix_authz::Action;
use felix_wire::Message;

use super::authz::authorize_cache;
use super::{Ctx, Session, Step};
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::{
    Outgoing, handle_ack_enqueue_result, send_outgoing_critical,
};

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn counter_add(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    cache: String,
    key: String,
    delta: i64,
    request_id: u64,
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
    // An add is a write, authorized as one.
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
        return Ok(Step::Next);
    }
    let applied = crate::serving::cache_routing::apply_counter_op(
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
        crate::serving::forward::CacheRequest::CounterAdd { delta },
    )
    .await;
    let value = match applied {
        Ok(value) => value,
        Err(reason) => {
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::CacheMessage(
                        ClientError::from_anyhow(&reason)
                            .prefixed("counter add not served")
                            .into_message(),
                    ),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            return Ok(Step::Next);
        }
    };
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::CacheMessage(Message::CounterValue { value, request_id }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    Ok(Step::Next)
}

pub(super) async fn counter_get(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    cache: String,
    key: String,
    request_id: u64,
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
        return Ok(Step::Next);
    }
    let read = crate::serving::cache_routing::apply_counter_op(
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
        crate::serving::forward::CacheRequest::CounterGet,
    )
    .await;
    let value = match read {
        Ok(value) => value,
        Err(reason) => {
            // A read this broker cannot route is an error, never
            // "no counter": absence is an answer about the data,
            // and this is an answer about the broker.
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::CacheMessage(
                        ClientError::from_anyhow(&reason)
                            .prefixed("counter get not served")
                            .into_message(),
                    ),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            return Ok(Step::Next);
        }
    };
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::CacheMessage(Message::CounterValue { value, request_id }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    Ok(Step::Next)
}

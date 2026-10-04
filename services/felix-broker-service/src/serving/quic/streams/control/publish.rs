//! Publishes on the control stream: single, batched, and from an idempotent producer.

use anyhow::Result;
use bytes::Bytes;
use felix_authz::Action;
use felix_wire::{AckMode, Message};

use super::authz::authorize_stream;
use super::responder::send_control_error;
use super::{Ctx, Session, Step};
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::{
    AckEncoding, Outgoing, handle_ack_enqueue_result, handle_publish_batch_message,
    handle_publish_message, send_outgoing_critical, sequence_reuse,
};

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn publish(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Vec<u8>,
    key: Option<Bytes>,
    request_id: Option<u64>,
    ack: Option<AckMode>,
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
        ack_waiters,
        ack_waiter_tx,
        ack_wait_timeout,
        throttled,
        sample,
        ..
    } = *cx;
    if !authorize_stream(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::StreamPublish,
        &namespace,
        &stream,
        request_id,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    handle_publish_message(
        broker,
        publish_ctx,
        &mut session.stream_cache,
        &mut session.stream_cache_key,
        throttled,
        session.commit_ack,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ack_waiters,
        ack_waiter_tx,
        ack_wait_timeout,
        tenant_id,
        namespace,
        stream,
        payload,
        key,
        request_id,
        ack,
        sample,
        session
            .auth_ctx
            .as_ref()
            .map_or_else(Default::default, |ctx| ctx.publishing_as()),
    )
    .await?;
    Ok(Step::Next)
}

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_batch(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    payloads: Vec<Vec<u8>>,
    key: Option<Bytes>,
    request_id: Option<u64>,
    ack: Option<AckMode>,
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
        ack_waiters,
        ack_waiter_tx,
        throttled,
        sample,
        ..
    } = *cx;
    if !authorize_stream(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::StreamPublish,
        &namespace,
        &stream,
        request_id,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    handle_publish_batch_message(
        session.peer_flags,
        broker,
        publish_ctx,
        &mut session.stream_cache,
        &mut session.stream_cache_key,
        throttled,
        session.commit_ack,
        AckEncoding::Json,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ack_waiters,
        ack_waiter_tx,
        tenant_id,
        namespace,
        stream,
        payloads.into_iter().map(bytes::Bytes::from).collect(),
        key,
        request_id,
        ack,
        sample,
        session
            .auth_ctx
            .as_ref()
            .map_or_else(Default::default, |ctx| ctx.publishing_as()),
        None,
    )
    .await?;
    Ok(Step::Next)
}

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_idempotent(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    payloads: Vec<Vec<u8>>,
    key: Option<Bytes>,
    request_id: u64,
    producer_id: u64,
    sequence: u64,
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
        ack_waiters,
        ack_waiter_tx,
        throttled,
        sample,
        ..
    } = *cx;
    if !authorize_stream(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::StreamPublish,
        &namespace,
        &stream,
        Some(request_id),
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    handle_publish_batch_message(
        session.peer_flags,
        broker,
        publish_ctx,
        &mut session.stream_cache,
        &mut session.stream_cache_key,
        throttled,
        session.commit_ack,
        AckEncoding::Idempotent,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ack_waiters,
        ack_waiter_tx,
        tenant_id,
        namespace,
        stream,
        payloads.into_iter().map(bytes::Bytes::from).collect(),
        key,
        Some(request_id),
        // Always acknowledged: a producer that never learns the
        // answer cannot know what to send next.
        Some(felix_wire::AckMode::PerBatch),
        sample,
        session
            .auth_ctx
            .as_ref()
            .map_or_else(Default::default, |ctx| ctx.publishing_as()),
        Some((producer_id, sequence, sequence_reuse(session.peer_features))),
    )
    .await?;
    Ok(Step::Next)
}

pub(super) async fn producer_init(
    cx: &Ctx<'_>,
    session: &mut Session,
    request_id: u64,
) -> Result<Step> {
    let Ctx {
        broker,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ..
    } = *cx;
    // Authenticated like everything else on this stream. The id
    // itself carries no authority: a batch under it is authorised
    // against the stream it names, like any other.
    if session.auth_ctx.is_none() {
        send_control_error(
            out_ack_tx,
            out_ack_depth,
            ack_throttle_tx,
            ack_timeout_state,
            cancel_tx,
            ClientError::unauthenticated("not authenticated"),
        )
        .await?;
        return Ok(Step::Close(false));
    }
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::ProducerInitOk {
                request_id,
                producer_id: broker.new_producer_id(),
            }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    Ok(Step::Next)
}

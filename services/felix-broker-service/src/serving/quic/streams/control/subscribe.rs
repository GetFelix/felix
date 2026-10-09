//! Subscribing on the control stream.

use std::sync::Arc;

use anyhow::Result;
use felix_authz::Action;
use felix_wire::StartPosition;

use super::authz::authorize_stream_simple;
use super::responder::send_control_error;
use super::{Ctx, Session, Step};
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::{
    Outgoing, handle_ack_enqueue_result, send_outgoing_critical,
};
use crate::serving::quic::handlers::subscribe::handle_subscribe_message;

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn subscribe(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    subscription_id: Option<u64>,
    start: Option<StartPosition>,
    shard: Option<u32>,
    queue_capacity: Option<u32>,
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
    if !authorize_stream_simple(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::StreamSubscribe,
        &namespace,
        &stream,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    // Where the shard actually lives.
    //
    // Without this a subscription for a shard this broker does not
    // own is accepted and then delivers nothing, for as long as the
    // application is willing to wait -- the worst of the available
    // answers, because it is indistinguishable from a quiet stream.
    // `docs/subscribe-routing.md` records the decision: redirect to
    // the owner rather than proxy for it.
    if let Some(answer) = crate::serving::quic::handlers::redirect::redirect_for(
        publish_ctx.ingress.as_deref(),
        publish_ctx.client_endpoints.as_deref(),
        &tenant_id,
        &namespace,
        &stream,
        // Ownership is per shard, so the redirect answers for the
        // shard being subscribed to: different shards of one stream
        // can have different owners.
        shard.unwrap_or(0),
        crate::shards::ShardKind::Stream,
        session.peer_features,
    ) {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(answer),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(Step::Next);
    }
    // Subscribe establishes server-side subscription state and typically spawns a
    // uni-directional event stream back to the client for delivery.
    let done = handle_subscribe_message(
        Arc::clone(broker),
        connection.clone(),
        config.clone(),
        &publish_ctx.identity,
        &publish_ctx.lane_manager,
        publish_ctx.ingress.as_deref(),
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        tenant_id,
        namespace,
        stream,
        subscription_id,
        start,
        shard,
        queue_capacity,
        session.peer_flags,
        session.peer_features,
        session.auth_ctx.as_ref().map(|auth| auth.subject.clone()),
    )
    .await?;
    if done {
        return Ok(Step::Close(true));
    }
    Ok(Step::Next)
}

pub(super) async fn subscribe_cursor_error(cx: &Ctx<'_>, _session: &mut Session) -> Result<Step> {
    let Ctx {
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ..
    } = *cx;
    send_control_error(
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ClientError::invalid("subscribe_cursor_error is a server-to-client message"),
    )
    .await?;
    Ok(Step::Close(false))
}

//! Atomic commits, conditional publishes and state reads on the control
//! stream.

use anyhow::Result;
use bytes::Bytes;
use felix_authz::Action;
use felix_wire::{Message, StateChange};

use super::authz::authorize_stream;
use super::{Ctx, Session, Step};
use crate::serving::commit_ops::NotWritten;
use crate::serving::quic::handlers::cache_watch::WatchResponder;
use crate::serving::quic::handlers::publish::{PublishContext, resolve_shard};
use crate::shards::routing::{Dispatch, dispatch};
use crate::shards::{ShardKey, ShardKind};

/// The shard a request names: a stream and the key that picks the shard,
/// shard 0 without one.
pub(super) struct Target {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) stream: String,
    pub(super) entity_key: Option<Bytes>,
}

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn commit(
    cx: &Ctx<'_>,
    session: &mut Session,
    target: Target,
    event: Bytes,
    changes: Vec<StateChange>,
    expected_offset: Option<u64>,
    request_id: u64,
) -> Result<Step> {
    if !authorize_stream(
        session.auth_ctx.as_ref(),
        &target.tenant_id,
        Action::StreamPublish,
        &target.namespace,
        &target.stream,
        Some(request_id),
        cx.authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    let answer = match redirect(cx.publish_ctx, session.peer_features, &target) {
        Some(answer) => answer,
        None => match crate::serving::commit_ops::commit(
            cx.broker,
            cx.publish_ctx,
            &target.tenant_id,
            &target.namespace,
            &target.stream,
            target.entity_key.as_deref().unwrap_or_default(),
            event,
            changes,
            expected_offset,
            session
                .auth_ctx
                .as_ref()
                .and_then(|auth| auth.publisher.as_ref()),
        )
        .await
        {
            Ok(offset) => Message::CommitOk { request_id, offset },
            Err(err) => not_written(err, request_id, "commit not served"),
        },
    };
    responder(cx).send(answer).await?;
    Ok(Step::Next)
}

/// A `publish_if`: answered on this stream once the batch is durable, or
/// refused with the tail. Not queued behind the stream's other publishes,
/// because its answer depends on the tail and not on their order.
pub(super) async fn publish_if(
    cx: &Ctx<'_>,
    session: &mut Session,
    target: Target,
    payloads: Vec<Vec<u8>>,
    expected_offset: u64,
    request_id: u64,
) -> Result<Step> {
    if !authorize_stream(
        session.auth_ctx.as_ref(),
        &target.tenant_id,
        Action::StreamPublish,
        &target.namespace,
        &target.stream,
        Some(request_id),
        cx.authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    let answer = match redirect(cx.publish_ctx, session.peer_features, &target) {
        Some(answer) => answer,
        None => match crate::serving::commit_ops::publish_at(
            cx.broker,
            cx.publish_ctx,
            &target.tenant_id,
            &target.namespace,
            &target.stream,
            target.entity_key.as_deref(),
            payloads.into_iter().map(Bytes::from).collect(),
            expected_offset,
            session
                .auth_ctx
                .as_ref()
                .and_then(|auth| auth.publisher.as_ref()),
        )
        .await
        {
            Ok(offset) => Message::PublishOk {
                request_id,
                offset: Some(offset),
            },
            Err(err) => not_written(err, request_id, "publish not served"),
        },
    };
    responder(cx).send(answer).await?;
    Ok(Step::Next)
}

/// A conditional write's refusal names the tail; any other failure is the
/// error it always was.
fn not_written(err: NotWritten, request_id: u64, context: &str) -> Message {
    match err {
        NotWritten::OffsetMismatch { tail } => Message::PublishRefused {
            request_id,
            reason: felix_wire::PublishRefusalReason::OffsetMismatch { tail },
            message: format!("the shard's next offset is {tail}"),
        },
        NotWritten::Refused(err) => err.prefixed(context).into_message(),
    }
}

pub(super) async fn state_get(
    cx: &Ctx<'_>,
    session: &mut Session,
    target: Target,
    key: String,
    request_id: u64,
) -> Result<Step> {
    if !authorize_stream(
        session.auth_ctx.as_ref(),
        &target.tenant_id,
        Action::StreamSubscribe,
        &target.namespace,
        &target.stream,
        Some(request_id),
        cx.authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    let answer = match redirect(cx.publish_ctx, session.peer_features, &target) {
        Some(answer) => answer,
        None => match crate::serving::commit_ops::state_get(
            cx.broker,
            cx.publish_ctx,
            &target.tenant_id,
            &target.namespace,
            &target.stream,
            target.entity_key.as_deref().unwrap_or_default(),
            &key,
        )
        .await
        {
            Ok(read) => Message::StateValue {
                value: read.value,
                version: read.version,
                as_of: read.as_of,
                request_id,
            },
            Err(err) => err.prefixed("state read not served").into_message(),
        },
    };
    responder(cx).send(answer).await?;
    Ok(Step::Next)
}

fn responder<'a>(cx: &'a Ctx<'_>) -> WatchResponder<'a> {
    WatchResponder {
        out_ack_tx: cx.out_ack_tx,
        out_ack_depth: cx.out_ack_depth,
        ack_throttle_tx: cx.ack_throttle_tx,
        ack_timeout_state: cx.ack_timeout_state,
        cancel_tx: cx.cancel_tx,
    }
}

/// `not_leader` naming the shard's leader, for a client that follows it,
/// when this broker does not lead the shard.
fn redirect(publish_ctx: &PublishContext, peer_features: u32, target: &Target) -> Option<Message> {
    if !felix_wire::supports_feature(peer_features, felix_wire::FEATURE_REDIRECT) {
        return None;
    }
    let key = ShardKey {
        tenant_id: target.tenant_id.clone(),
        namespace: target.namespace.clone(),
        stream: target.stream.clone(),
        shard: resolve_shard(
            publish_ctx,
            &target.tenant_id,
            &target.namespace,
            &target.stream,
            target.entity_key.as_deref(),
        ),
        kind: ShardKind::Stream,
    };
    let dispatched = dispatch(publish_ctx.ingress.as_deref(), &key);
    if matches!(dispatched, Dispatch::Local { .. }) {
        return None;
    }
    match crate::serving::quic::handlers::redirect::redirect_from(
        dispatched,
        publish_ctx.client_endpoints.as_deref(),
        &target.stream,
        peer_features,
    ) {
        Some(answer @ Message::NotLeader { .. }) => Some(answer),
        _ => None,
    }
}

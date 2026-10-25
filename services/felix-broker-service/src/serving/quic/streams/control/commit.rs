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
use crate::serving::quic::handlers::publish::{PublishContext, resolve_publish_shard};
use crate::shards::routing::{Dispatch, dispatch};
use crate::shards::{ShardKey, ShardKind};

/// The shard a request names: a stream and the key that picks the shard,
/// shard 0 without one, or the shard named outright.
pub(super) struct Target {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) stream: String,
    pub(super) entity_key: Option<Bytes>,
    pub(super) shard: Option<u32>,
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
            target.shard,
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
            Err(err) => moved(&err, cx, session.peer_features, &target)
                .unwrap_or_else(|| not_written(err, request_id, "commit not served")),
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
            target.shard,
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
            Err(err) => moved(&err, cx, session.peer_features, &target)
                .unwrap_or_else(|| not_written(err, request_id, "publish not served")),
        },
    };
    responder(cx).send(answer).await?;
    Ok(Step::Next)
}

/// The typed redirect for a request refused `not_leader` after [`redirect`]
/// saw the shard as local, because leadership moved in between. Nothing was
/// applied: the owner check comes before any write. `None` when the shard
/// still looks local, so the coded refusal stands.
fn moved(err: &NotWritten, cx: &Ctx<'_>, peer_features: u32, target: &Target) -> Option<Message> {
    match err {
        NotWritten::Refused(err) if *err.code() == felix_wire::ErrorCode::NotLeader => {
            redirect(cx.publish_ctx, peer_features, target)
        }
        _ => None,
    }
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
            target.shard,
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
            Err(err) => moved(
                &NotWritten::Refused(err.clone()),
                cx,
                session.peer_features,
                &target,
            )
            .unwrap_or_else(|| err.prefixed("state read not served").into_message()),
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
    // A shard out of range is not redirected anywhere; the request itself
    // answers that.
    let shard = resolve_publish_shard(
        publish_ctx,
        &target.tenant_id,
        &target.namespace,
        &target.stream,
        target.entity_key.as_deref(),
        target.shard,
    )
    .ok()?;
    let key = ShardKey {
        tenant_id: target.tenant_id.clone(),
        namespace: target.namespace.clone(),
        stream: target.stream.clone(),
        shard,
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

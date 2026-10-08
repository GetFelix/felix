//! Looking up a stream shard's offset by record time on the control stream.

use anyhow::Result;
use felix_authz::Action;
use felix_wire::Message;

use super::authz::authorize_stream;
use super::{Ctx, Session, Step};
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::cache_watch::WatchResponder;
use crate::shards::routing::{Dispatch, dispatch};
use crate::shards::{ShardKey, ShardKind};

/// The shard an `offset_for_time` names.
pub(super) struct TimeTarget {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) stream: String,
    pub(super) shard: u32,
}

/// Answer `offset_for_time` with `offset_value`. Needs what a subscribe
/// needs: the answer is only a place to subscribe from.
pub(super) async fn offset_for_time(
    cx: &Ctx<'_>,
    session: &mut Session,
    target: TimeTarget,
    at_micros: u64,
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
    let answer = match lookup(cx, session.peer_features, &target, at_micros).await {
        Ok(offset) => Message::OffsetValue { offset, request_id },
        Err(answer) => answer,
    };
    WatchResponder {
        out_ack_tx: cx.out_ack_tx,
        out_ack_depth: cx.out_ack_depth,
        ack_throttle_tx: cx.ack_throttle_tx,
        ack_timeout_state: cx.ack_timeout_state,
        cancel_tx: cx.cancel_tx,
    }
    .send(answer)
    .await?;
    Ok(Step::Next)
}

/// The offset, or the message to answer with instead. Only the shard's
/// leader answers: a follower's log can hold records a failover takes back.
async fn lookup(
    cx: &Ctx<'_>,
    peer_features: u32,
    target: &TimeTarget,
    at_micros: u64,
) -> Result<Option<u64>, Message> {
    let key = ShardKey {
        tenant_id: target.tenant_id.clone(),
        namespace: target.namespace.clone(),
        stream: target.stream.clone(),
        shard: target.shard,
        kind: ShardKind::Stream,
    };
    let publish_ctx = cx.publish_ctx;
    match dispatch(publish_ctx.ingress.as_deref(), &key) {
        Dispatch::Local { .. } => {}
        dispatched => {
            let not_here = match &dispatched {
                Dispatch::Unavailable(reason) => {
                    ClientError::unavailable(reason, reason.to_string())
                }
                _ => ClientError::new(
                    felix_wire::ErrorCode::NotLeader,
                    format!("shard {} of {} is served elsewhere", key.shard, key.stream),
                ),
            };
            if felix_wire::supports_feature(peer_features, felix_wire::FEATURE_REDIRECT)
                && let Some(answer @ Message::NotLeader { .. }) =
                    crate::serving::quic::handlers::redirect::redirect_from(
                        dispatched,
                        publish_ctx.client_endpoints.as_deref(),
                        &target.stream,
                        peer_features,
                    )
            {
                return Err(answer);
            }
            return Err(not_here.into_message());
        }
    }
    cx.broker
        .offset_for_time(
            &target.tenant_id,
            &target.namespace,
            &target.stream,
            target.shard,
            at_micros,
        )
        .await
        .map_err(|err| ClientError::from_broker(&err, "offset for time not served").into_message())
}

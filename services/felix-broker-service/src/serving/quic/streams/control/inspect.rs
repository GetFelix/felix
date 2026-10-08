//! Answering `shard_inspect` on the control stream.

use anyhow::Result;
use felix_authz::Action;
use felix_wire::Message;

use super::responder::send_control_error;
use super::{Ctx, Session, Step};
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::cache_watch::WatchResponder;
use crate::shards::{ShardKey, ShardKind};

/// Answer with this broker's view of `key`. Needs `node.view:cluster:*`, and
/// no tenant check: the cluster scope covers every tenant, and the token's own
/// tenant is only what it authenticated under.
pub(super) async fn shard_inspect(
    cx: &Ctx<'_>,
    session: &mut Session,
    key: ShardKey,
    request_id: u64,
) -> Result<Step> {
    let refusal = match session.auth_ctx.as_ref() {
        None => Some((
            ClientError::unauthenticated("auth required"),
            Step::Close(false),
        )),
        // A well-formed request the token may not make: refused, and the
        // stream stays up. Closing it here could reset the stream before the
        // refusal reaches the client, which would then see only an end.
        Some(auth) if !auth.matcher.allows_cluster(Action::NodeView) => Some((
            ClientError::forbidden("shard_inspect needs node.view:cluster:*"),
            Step::Next,
        )),
        Some(_) => None,
    };
    if let Some((refusal, step)) = refusal {
        send_control_error(
            cx.out_ack_tx,
            cx.out_ack_depth,
            cx.ack_throttle_tx,
            cx.ack_timeout_state,
            cx.cancel_tx,
            refusal,
        )
        .await?;
        return Ok(step);
    }
    let view = crate::serving::inspect::inspect(cx.broker, cx.publish_ctx, &key).await;
    WatchResponder {
        out_ack_tx: cx.out_ack_tx,
        out_ack_depth: cx.out_ack_depth,
        ack_throttle_tx: cx.ack_throttle_tx,
        ack_timeout_state: cx.ack_timeout_state,
        cancel_tx: cx.cancel_tx,
    }
    .send(Message::ShardInspectInfo {
        view: Box::new(view),
        request_id,
    })
    .await?;
    Ok(Step::Next)
}

/// The shard a `shard_inspect` names.
pub(super) fn target(
    tenant_id: String,
    namespace: String,
    name: String,
    kind: felix_wire::ShardKind,
    shard: u32,
) -> ShardKey {
    ShardKey {
        tenant_id,
        namespace,
        stream: name,
        shard,
        kind: match kind {
            felix_wire::ShardKind::Stream => ShardKind::Stream,
            felix_wire::ShardKind::Cache => ShardKind::Cache,
        },
    }
}

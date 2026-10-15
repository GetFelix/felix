//! Answering the operator inspection requests on the control stream:
//! `shard_inspect` and `subscriptions_list`.

use anyhow::Result;
use felix_authz::Action;
use felix_wire::{Message, SubscriptionCursor, SubscriptionFilter};

use super::responder::send_control_error;
use super::{Ctx, Session, Step};
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::cache_watch::WatchResponder;
use crate::shards::{ShardKey, ShardKind};

/// A page of `subscriptions_list` when the request names no limit.
const DEFAULT_SUBSCRIPTIONS_PAGE: usize = 100;
/// The most one page holds, whatever the request asks for.
const MAX_SUBSCRIPTIONS_PAGE: usize = 1000;

/// Answer with this broker's view of `key`. Needs `node.view:cluster:*`, and
/// no tenant check: the cluster scope covers every tenant, and the token's own
/// tenant is only what it authenticated under.
pub(super) async fn shard_inspect(
    cx: &Ctx<'_>,
    session: &mut Session,
    key: ShardKey,
    request_id: u64,
) -> Result<Step> {
    if let Some(step) = refuse_without_cluster_view(cx, session, "shard_inspect").await? {
        return Ok(step);
    }
    let view = crate::serving::inspect::inspect(cx.broker, cx.publish_ctx, &key).await;
    answer(
        cx,
        Message::ShardInspectInfo {
            view: Box::new(view),
            request_id,
        },
    )
    .await
}

/// Answer with one page of the subscriptions this broker serves. Same
/// permission as `shard_inspect`; the answer names principals and addresses
/// across every tenant.
pub(super) async fn subscriptions_list(
    cx: &Ctx<'_>,
    session: &mut Session,
    filter: Box<SubscriptionFilter>,
    limit: Option<u32>,
    cursor: Option<SubscriptionCursor>,
    request_id: u64,
) -> Result<Step> {
    if let Some(step) = refuse_without_cluster_view(cx, session, "subscriptions_list").await? {
        return Ok(step);
    }
    let page = cx
        .broker
        .list_subscriptions(&filter, cursor.as_ref(), page_size(limit))
        .await;
    let node_id = cx
        .publish_ctx
        .ingress
        .as_deref()
        .map(|ingress| ingress.local_node_id().to_string())
        .unwrap_or_default();
    answer(
        cx,
        Message::SubscriptionsListInfo {
            node_id,
            subscriptions: page.subscriptions,
            next_cursor: page.next_cursor,
            request_id,
        },
    )
    .await
}

/// How many subscriptions one page holds for a requested `limit`.
fn page_size(limit: Option<u32>) -> usize {
    limit.map_or(DEFAULT_SUBSCRIPTIONS_PAGE, |limit| {
        (limit as usize).clamp(1, MAX_SUBSCRIPTIONS_PAGE)
    })
}

/// Refuse a session that may not inspect, returning how the loop goes on.
async fn refuse_without_cluster_view(
    cx: &Ctx<'_>,
    session: &Session,
    request: &str,
) -> Result<Option<Step>> {
    let refusal = match session.auth_ctx.as_ref() {
        None => Some((
            ClientError::unauthenticated("auth required"),
            Step::Close(false),
        )),
        // A well-formed request the token may not make: refused, and the
        // stream stays up. Closing it here could reset the stream before the
        // refusal reaches the client, which would then see only an end.
        Some(auth) if !auth.matcher.allows_cluster(Action::NodeView) => Some((
            ClientError::forbidden(format!("{request} needs node.view:cluster:*")),
            Step::Next,
        )),
        Some(_) => None,
    };
    let Some((refusal, step)) = refusal else {
        return Ok(None);
    };
    send_control_error(
        cx.out_ack_tx,
        cx.out_ack_depth,
        cx.ack_throttle_tx,
        cx.ack_timeout_state,
        cx.cancel_tx,
        refusal,
    )
    .await?;
    Ok(Some(step))
}

async fn answer(cx: &Ctx<'_>, message: Message) -> Result<Step> {
    WatchResponder {
        out_ack_tx: cx.out_ack_tx,
        out_ack_depth: cx.out_ack_depth,
        ack_throttle_tx: cx.ack_throttle_tx,
        ack_timeout_state: cx.ack_timeout_state,
        cancel_tx: cx.cancel_tx,
    }
    .send(message)
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

#[cfg(test)]
mod tests;

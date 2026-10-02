//! Where things are: the cluster's topology, how many shards a stream or cache
//! has, and which broker owns each.

use anyhow::Result;
use felix_wire::Message;

use super::responder::send_control_error;
use super::{Ctx, Session, Step};
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::{
    Outgoing, handle_ack_enqueue_result, send_outgoing_critical,
};

pub(super) async fn topology(cx: &Ctx<'_>, session: &mut Session) -> Result<Step> {
    let Ctx {
        publish_ctx,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ..
    } = *cx;
    // Authenticated like everything else on this stream: the
    // addresses are not secret, but who may ask a broker anything
    // at all is still the tenant boundary.
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
    let brokers = publish_ctx
        .client_endpoints
        .as_ref()
        .map(|endpoints| endpoints.snapshot().as_ref().clone())
        .unwrap_or_default();
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::TopologyView { brokers }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    Ok(Step::Next)
}

pub(super) async fn stream_shards(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    request_id: u64,
) -> Result<Step> {
    let Ctx {
        broker,
        publish_ctx,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ..
    } = *cx;
    // Authenticated, and scoped: a client may ask about the shape
    // of streams in its own tenant, not another's.
    let Some(ctx) = session.auth_ctx.as_ref() else {
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
    };
    if ctx.tenant_id != tenant_id {
        send_control_error(
            out_ack_tx,
            out_ack_depth,
            ack_throttle_tx,
            ack_timeout_state,
            cancel_tx,
            ClientError::forbidden("tenant mismatch"),
        )
        .await?;
        return Ok(Step::Close(false));
    }
    // Read from the routing snapshot, which is an `ArcSwap` load.
    // A broker that has never heard of the stream answers 0 rather
    // than guessing 1: "I do not know" and "exactly one shard" are
    // different answers, and a client that assumed the latter would
    // silently read a fraction of a stream.
    let placement = match publish_ctx.ingress.as_deref() {
        Some(ingress) => ingress
            .placement_for(
                crate::shards::ShardKind::Stream,
                &tenant_id,
                &namespace,
                &stream,
            )
            .unwrap_or_default(),
        // No routing snapshot to consult, so the registry is the
        // only thing that knows whether the stream exists. It is
        // served here and unplaced, which is one shard.
        None => felix_router::StreamPlacement {
            shards: u32::from(broker.stream_exists(&tenant_id, &namespace, &stream).await),
            routing: Default::default(),
        },
    };
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::StreamShardsView {
                shards: placement.shards,
                request_id,
                // Omitted for modulo, so an older client reads the same frame.
                routing: (!placement.routing.is_modulo()).then_some(placement.routing),
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

pub(super) async fn cache_shards(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    cache: String,
    request_id: u64,
) -> Result<Step> {
    let Ctx {
        broker,
        publish_ctx,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ..
    } = *cx;
    let Some(ctx) = session.auth_ctx.as_ref() else {
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
    };
    if ctx.tenant_id != tenant_id {
        send_control_error(
            out_ack_tx,
            out_ack_depth,
            ack_throttle_tx,
            ack_timeout_state,
            cancel_tx,
            ClientError::forbidden("tenant mismatch"),
        )
        .await?;
        return Ok(Step::Close(false));
    }
    // A registered cache the snapshot has not placed is served
    // here as one shard, which is how `cache_watch` resolves it
    // too. A cache nobody knows is 0, not 1.
    let placed = publish_ctx.ingress.as_deref().and_then(|ingress| {
        ingress.placed_shards_for(
            crate::shards::ShardKind::Cache,
            &tenant_id,
            &namespace,
            &cache,
        )
    });
    let shards = match placed {
        Some(shards) => shards,
        None => u32::from(broker.cache_exists(&tenant_id, &namespace, &cache).await),
    };
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::CacheShardsView { shards, request_id }),
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
pub(super) async fn shard_owners(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    name: String,
    kind: felix_wire::ShardKind,
    request_id: u64,
) -> Result<Step> {
    let Ctx {
        broker,
        publish_ctx,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ..
    } = *cx;
    let Some(ctx) = session.auth_ctx.as_ref() else {
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
    };
    if ctx.tenant_id != tenant_id {
        send_control_error(
            out_ack_tx,
            out_ack_depth,
            ack_throttle_tx,
            ack_timeout_state,
            cancel_tx,
            ClientError::forbidden("tenant mismatch"),
        )
        .await?;
        return Ok(Step::Close(false));
    }
    let shard_kind = match kind {
        felix_wire::ShardKind::Stream => crate::shards::ShardKind::Stream,
        felix_wire::ShardKind::Cache => crate::shards::ShardKind::Cache,
    };
    let owners = match publish_ctx.ingress.as_deref() {
        Some(ingress) => {
            // The same widths `stream_shards` and `cache_shards` answer with,
            // so the two questions agree about how many shards there are.
            let shards = match shard_kind {
                crate::shards::ShardKind::Stream => {
                    ingress
                        .placement_for(shard_kind, &tenant_id, &namespace, &name)
                        .unwrap_or_default()
                        .shards
                }
                crate::shards::ShardKind::Cache => {
                    match ingress.placed_shards_for(shard_kind, &tenant_id, &namespace, &name) {
                        Some(shards) => shards,
                        None => u32::from(broker.cache_exists(&tenant_id, &namespace, &name).await),
                    }
                }
            };
            let endpoints = publish_ctx.client_endpoints.as_deref();
            (0..shards)
                .map(|shard| {
                    let key = crate::shards::ShardKey {
                        tenant_id: tenant_id.clone(),
                        namespace: namespace.clone(),
                        stream: name.clone(),
                        shard,
                        kind: shard_kind,
                    };
                    owner_of(ingress, endpoints, &key)
                })
                .collect()
        }
        // A single node owns everything it knows of, and has no node id or
        // generation to report.
        None => {
            let exists = match shard_kind {
                crate::shards::ShardKind::Stream => {
                    broker.stream_exists(&tenant_id, &namespace, &name).await
                }
                crate::shards::ShardKind::Cache => {
                    broker.cache_exists(&tenant_id, &namespace, &name).await
                }
            };
            exists
                .then_some(felix_wire::ShardOwner {
                    shard: 0,
                    node_id: None,
                    addr: None,
                    generation: 0,
                    unavailable: None,
                })
                .into_iter()
                .collect()
        }
    };
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::ShardOwnersView { owners, request_id }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    Ok(Step::Next)
}

/// Who serves one shard, by the same dispatch a request for it would take.
fn owner_of(
    ingress: &crate::shards::routing::IngressRouter,
    endpoints: Option<&crate::cluster::client_endpoints::ClientEndpoints>,
    key: &crate::shards::ShardKey,
) -> felix_wire::ShardOwner {
    use crate::shards::routing::Dispatch;

    let owned = |node_id: &str, generation| felix_wire::ShardOwner {
        shard: key.shard,
        node_id: Some(node_id.to_string()),
        addr: endpoints.and_then(|endpoints| endpoints.redirect_addr(node_id)),
        generation,
        unavailable: None,
    };
    match ingress.dispatch(key) {
        Dispatch::Local { generation } => owned(ingress.local_node_id(), generation),
        Dispatch::Forward {
            node_id,
            generation,
            ..
        } => owned(&node_id, generation),
        Dispatch::Unavailable(reason) => felix_wire::ShardOwner {
            shard: key.shard,
            node_id: None,
            addr: None,
            generation: 0,
            unavailable: Some(reason.wire_name().to_string()),
        },
    }
}

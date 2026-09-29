//! Where things are: the cluster's topology, and how many shards a stream or cache has.

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

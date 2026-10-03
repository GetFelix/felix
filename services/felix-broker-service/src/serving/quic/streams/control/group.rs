//! Consumer-group requests on the control stream.

use std::time::Duration;

use anyhow::Result;
use felix_authz::Action;
use felix_wire::{GroupRecord, Message};

use super::authz::authorize_group;
use super::{Ctx, Session, Step};
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::{
    Outgoing, PublishContext, handle_ack_enqueue_result, send_outgoing_critical,
};
use crate::shards::lifecycle::fence::FenceGuard;
use crate::shards::routing::{Dispatch, dispatch_write};
use crate::shards::{ShardKey, ShardKind};

/// Longest `consumer` name a `group_poll` may carry.
const MAX_CONSUMER_NAME_BYTES: usize = 128;

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn group_poll(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
    group: String,
    max_records: u32,
    wait_ms: u64,
    request_id: u64,
    consumer: Option<String>,
    reclaim: bool,
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
    // A stream grant covers this too unless the principal is scoped to
    // particular groups on the stream; see `PermissionMatcher::allows_group`.
    if !authorize_group(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::GroupConsume,
        &namespace,
        &stream,
        &group,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    // Each claim records its holder, so an unbounded name would cost its
    // length per record handed out.
    if let Some(name) = &consumer
        && (name.is_empty() || name.len() > MAX_CONSUMER_NAME_BYTES)
    {
        let refusal = ClientError::invalid(format!(
            "group consumer name must be 1 to {MAX_CONSUMER_NAME_BYTES} bytes, got {}",
            name.len()
        ));
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(refusal.into_message()),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(Step::Next);
    }
    // Scoped to the principal, so naming another principal's member reaches
    // nothing of theirs. Unauthenticated brokers share the empty principal.
    let consumer = consumer.map(|name| {
        let principal = session
            .auth_ctx
            .as_ref()
            .map_or("", |auth| auth.subject.as_str());
        felix_broker::GroupConsumer::new(principal, &name, connection.info().id.0, reclaim)
    });
    let admitted = match group_admit(
        publish_ctx,
        session.peer_features,
        &tenant_id,
        &namespace,
        &stream,
        shard,
    )
    .await
    {
        Ok(admitted) => admitted,
        Err(answer) => {
            crate::serving::quic::handlers::cache_watch::WatchResponder {
                out_ack_tx,
                out_ack_depth,
                ack_throttle_tx,
                ack_timeout_state,
                cancel_tx,
            }
            .send(answer)
            .await?;
            return Ok(Step::Next);
        }
    };
    let polled = crate::serving::group_ops::poll(
        broker,
        publish_ctx,
        admitted,
        &tenant_id,
        &namespace,
        &stream,
        shard,
        &group,
        max_records as usize,
        // Capped, so a client cannot hold a broker stream open for
        // as long as it likes.
        Duration::from_millis(wait_ms.min(config.group_max_wait_ms)),
        consumer.as_ref(),
    )
    .await;
    let records = match polled {
        Ok(records) => for_peer(records, session.peer_features),
        Err(reason) => {
            // Refused rather than answered with an empty batch: a
            // consumer told "nothing available" would poll for ever
            // against a shard this broker does not lead.
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(reason.prefixed("group poll not served").into_message()),
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
            Outgoing::Message(Message::GroupRecords {
                records,
                request_id,
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

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn group_ack(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
    group: String,
    offset: u64,
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
    if !authorize_group(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::GroupConsume,
        &namespace,
        &stream,
        &group,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    let admitted = match group_admit(
        publish_ctx,
        session.peer_features,
        &tenant_id,
        &namespace,
        &stream,
        shard,
    )
    .await
    {
        Ok(admitted) => admitted,
        Err(answer) => {
            crate::serving::quic::handlers::cache_watch::WatchResponder {
                out_ack_tx,
                out_ack_depth,
                ack_throttle_tx,
                ack_timeout_state,
                cancel_tx,
            }
            .send(answer)
            .await?;
            return Ok(Step::Next);
        }
    };
    if let Err(reason) = crate::serving::group_ops::settle(
        broker,
        publish_ctx,
        admitted,
        &tenant_id,
        &namespace,
        &stream,
        shard,
        &group,
        offset,
        true,
    )
    .await
    {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(reason.prefixed("group ack not served").into_message()),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(Step::Next);
    }
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::CacheOk { request_id }),
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
pub(super) async fn group_nack(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
    group: String,
    offset: u64,
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
    if !authorize_group(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::GroupConsume,
        &namespace,
        &stream,
        &group,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    let admitted = match group_admit(
        publish_ctx,
        session.peer_features,
        &tenant_id,
        &namespace,
        &stream,
        shard,
    )
    .await
    {
        Ok(admitted) => admitted,
        Err(answer) => {
            crate::serving::quic::handlers::cache_watch::WatchResponder {
                out_ack_tx,
                out_ack_depth,
                ack_throttle_tx,
                ack_timeout_state,
                cancel_tx,
            }
            .send(answer)
            .await?;
            return Ok(Step::Next);
        }
    };
    if let Err(reason) = crate::serving::group_ops::settle(
        broker,
        publish_ctx,
        admitted,
        &tenant_id,
        &namespace,
        &stream,
        shard,
        &group,
        offset,
        false,
    )
    .await
    {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(reason.prefixed("group nack not served").into_message()),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(Step::Next);
    }
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::CacheOk { request_id }),
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
pub(super) async fn group_dead_letters(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
    group: String,
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
    if !authorize_group(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::GroupConsume,
        &namespace,
        &stream,
        &group,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    let admitted = match group_admit(
        publish_ctx,
        session.peer_features,
        &tenant_id,
        &namespace,
        &stream,
        shard,
    )
    .await
    {
        Ok(admitted) => admitted,
        Err(answer) => {
            crate::serving::quic::handlers::cache_watch::WatchResponder {
                out_ack_tx,
                out_ack_depth,
                ack_throttle_tx,
                ack_timeout_state,
                cancel_tx,
            }
            .send(answer)
            .await?;
            return Ok(Step::Next);
        }
    };
    // A read does not hold the fence.
    drop(admitted);
    let listed = crate::serving::group_ops::dead_letters(
        broker,
        publish_ctx,
        &tenant_id,
        &namespace,
        &stream,
        shard,
        &group,
    )
    .await;
    let offsets = match listed {
        Ok(offsets) => offsets,
        Err(reason) => {
            // Refused rather than answered with an empty list: an
            // operator told there are no dead letters would stop
            // looking.
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(reason.prefixed("dead letters not served").into_message()),
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
            Outgoing::Message(Message::GroupDeadLetterList {
                offsets,
                request_id,
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

// One parameter per field of the message it answers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn group_discard(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
    group: String,
    offset: u64,
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
    if !authorize_group(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::GroupManage,
        &namespace,
        &stream,
        &group,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    let admitted = match group_admit(
        publish_ctx,
        session.peer_features,
        &tenant_id,
        &namespace,
        &stream,
        shard,
    )
    .await
    {
        Ok(admitted) => admitted,
        Err(answer) => {
            crate::serving::quic::handlers::cache_watch::WatchResponder {
                out_ack_tx,
                out_ack_depth,
                ack_throttle_tx,
                ack_timeout_state,
                cancel_tx,
            }
            .send(answer)
            .await?;
            return Ok(Step::Next);
        }
    };
    if let Err(reason) = crate::serving::group_ops::manage_dead_letter(
        broker,
        publish_ctx,
        admitted,
        &tenant_id,
        &namespace,
        &stream,
        shard,
        &group,
        offset,
        false,
    )
    .await
    {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(reason.prefixed("group discard not served").into_message()),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(Step::Next);
    }
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::CacheOk { request_id }),
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
pub(super) async fn group_redrive(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
    group: String,
    offset: u64,
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
    if !authorize_group(
        session.auth_ctx.as_ref(),
        &tenant_id,
        Action::GroupManage,
        &namespace,
        &stream,
        &group,
        authz_ctx,
    )
    .await?
    {
        return Ok(Step::Close(false));
    }
    let admitted = match group_admit(
        publish_ctx,
        session.peer_features,
        &tenant_id,
        &namespace,
        &stream,
        shard,
    )
    .await
    {
        Ok(admitted) => admitted,
        Err(answer) => {
            crate::serving::quic::handlers::cache_watch::WatchResponder {
                out_ack_tx,
                out_ack_depth,
                ack_throttle_tx,
                ack_timeout_state,
                cancel_tx,
            }
            .send(answer)
            .await?;
            return Ok(Step::Next);
        }
    };
    if let Err(reason) = crate::serving::group_ops::manage_dead_letter(
        broker,
        publish_ctx,
        admitted,
        &tenant_id,
        &namespace,
        &stream,
        shard,
        &group,
        offset,
        true,
    )
    .await
    {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(reason.prefixed("group redrive not served").into_message()),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(Step::Next);
    }
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(Message::CacheOk { request_id }),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    Ok(Step::Next)
}

/// Hold a group operation while its shard moves, then say where it goes:
/// here, with its place in the shard's write fence, or to the owner a
/// redirect names.
///
/// `Ok(None)` leaves the answer to the group operation itself, which refuses
/// with the reason when this broker cannot serve the shard: a client that
/// cannot decode a redirect gets the error it always did.
async fn group_admit(
    publish_ctx: &PublishContext,
    peer_features: u32,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) -> Result<Option<FenceGuard>, Message> {
    let key = ShardKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    };
    let (dispatched, fenced) = dispatch_write(publish_ctx.ingress.as_deref(), &key).await;
    if matches!(dispatched, Dispatch::Local { .. }) {
        return Ok(fenced);
    }
    if !felix_wire::supports_feature(peer_features, felix_wire::FEATURE_REDIRECT) {
        return Ok(None);
    }
    match crate::serving::quic::handlers::redirect::redirect_from(
        dispatched,
        publish_ctx.client_endpoints.as_deref(),
        stream,
        peer_features,
    ) {
        Some(answer @ Message::NotLeader { .. }) => Err(answer),
        _ => Ok(None),
    }
}

/// Clear what `peer_features` did not negotiate, so the field is left out of
/// the frame for a client that cannot read it.
fn for_peer(mut records: Vec<GroupRecord>, peer_features: u32) -> Vec<GroupRecord> {
    if !felix_wire::supports_feature(peer_features, felix_wire::FEATURE_GROUP_SKIPPED) {
        for record in &mut records {
            record.skipped_before = 0;
        }
    }
    records
}

#[cfg(test)]
mod tests;

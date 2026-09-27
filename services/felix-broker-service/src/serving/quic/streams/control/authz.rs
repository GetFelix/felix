//! Checking a request's token against the resource it names, and answering
//! the refusal.

use anyhow::Result;
use felix_authz::{
    Action, CacheScope, GroupName, Namespace, StreamName, TenantId, cache_resource, stream_resource,
};

use super::responder::{Responder, send_control_error};
use crate::serving::auth::AuthContext;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::{
    Outgoing, handle_ack_enqueue_result, send_outgoing_critical,
};

pub(super) async fn authorize_stream(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    stream: &str,
    request_id: Option<u64>,
    ctx: &Responder<'_>,
) -> Result<bool> {
    let Some(auth_ctx) = auth_ctx else {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            ClientError::unauthenticated("auth required"),
        )
        .await?;
        return Ok(false);
    };
    if auth_ctx.tenant_id != tenant_id {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            ClientError::forbidden("tenant mismatch"),
        )
        .await?;
        return Ok(false);
    }
    let resource = stream_resource(
        &TenantId::new(tenant_id),
        &Namespace::new(namespace),
        &StreamName::new(stream),
    );
    if auth_ctx.matcher.allows(action, &resource) {
        return Ok(true);
    }
    let outgoing = match request_id {
        Some(request_id) => {
            Outgoing::Message(ClientError::forbidden("forbidden").into_publish_error(request_id))
        }
        None => Outgoing::Message(ClientError::forbidden("forbidden").into_message()),
    };
    handle_ack_enqueue_result(
        send_outgoing_critical(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            "felix_broker_out_ack_depth",
            ctx.ack_throttle_tx,
            outgoing,
        )
        .await,
        ctx.ack_timeout_state,
        ctx.ack_throttle_tx,
        ctx.cancel_tx,
    )
    .await?;
    Ok(false)
}

pub(super) async fn authorize_stream_simple(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    stream: &str,
    ctx: &Responder<'_>,
) -> Result<bool> {
    let Some(auth_ctx) = auth_ctx else {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            ClientError::unauthenticated("auth required"),
        )
        .await?;
        return Ok(false);
    };
    if auth_ctx.tenant_id != tenant_id {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            ClientError::forbidden("tenant mismatch"),
        )
        .await?;
        return Ok(false);
    }
    let resource = stream_resource(
        &TenantId::new(tenant_id),
        &Namespace::new(namespace),
        &StreamName::new(stream),
    );
    if auth_ctx.matcher.allows(action, &resource) {
        return Ok(true);
    }
    send_control_error(
        ctx.out_ack_tx,
        ctx.out_ack_depth,
        ctx.ack_throttle_tx,
        ctx.ack_timeout_state,
        ctx.cancel_tx,
        ClientError::forbidden("forbidden"),
    )
    .await?;
    Ok(false)
}

/// [`authorize_stream_simple`] for one consumer group of the stream, by
/// [`felix_authz::PermissionMatcher::allows_group`]'s rule: a grant on the
/// group, or on the stream unless the principal is scoped to particular
/// groups there.
pub(super) async fn authorize_group(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    stream: &str,
    group: &str,
    ctx: &Responder<'_>,
) -> Result<bool> {
    let refused = match auth_ctx {
        None => ClientError::unauthenticated("auth required"),
        Some(auth_ctx) if auth_ctx.tenant_id != tenant_id => {
            ClientError::forbidden("tenant mismatch")
        }
        Some(auth_ctx)
            if auth_ctx.matcher.allows_group(
                action,
                &TenantId::new(tenant_id),
                &Namespace::new(namespace),
                &StreamName::new(stream),
                &GroupName::new(group),
            ) =>
        {
            return Ok(true);
        }
        Some(_) => ClientError::forbidden("forbidden"),
    };
    send_control_error(
        ctx.out_ack_tx,
        ctx.out_ack_depth,
        ctx.ack_throttle_tx,
        ctx.ack_timeout_state,
        ctx.cancel_tx,
        refused,
    )
    .await?;
    Ok(false)
}

pub(super) async fn authorize_cache(
    auth_ctx: Option<&AuthContext>,
    tenant_id: &str,
    action: Action,
    namespace: &str,
    cache: &str,
    ctx: &Responder<'_>,
) -> Result<bool> {
    let Some(auth_ctx) = auth_ctx else {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            ClientError::unauthenticated("auth required"),
        )
        .await?;
        return Ok(false);
    };
    if auth_ctx.tenant_id != tenant_id {
        send_control_error(
            ctx.out_ack_tx,
            ctx.out_ack_depth,
            ctx.ack_throttle_tx,
            ctx.ack_timeout_state,
            ctx.cancel_tx,
            ClientError::forbidden("tenant mismatch"),
        )
        .await?;
        return Ok(false);
    }
    let resource = cache_resource(
        &TenantId::new(tenant_id),
        &Namespace::new(namespace),
        &CacheScope::new(cache),
    );
    if auth_ctx.matcher.allows(action, &resource) {
        return Ok(true);
    }
    send_control_error(
        ctx.out_ack_tx,
        ctx.out_ack_depth,
        ctx.ack_throttle_tx,
        ctx.ack_timeout_state,
        ctx.cancel_tx,
        ClientError::forbidden("forbidden"),
    )
    .await?;
    Ok(false)
}

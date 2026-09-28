//! Broker membership API. A broker registers on boot and then reports health
//! on an interval; the control plane records the last report and a sweep marks
//! silent nodes down.
//!
//! A heartbeat records liveness only — it never revives a node the cluster
//! already marked down, and never resurrects a superseded incarnation. The
//! clock is always the control plane's, not the caller's, so a broker cannot
//! claim a future heartbeat and outlive its timeout.
//!
//! Every write requires `node.manage` over the node being changed. A broker's
//! credential is scoped to `node:{its own id}`, so it cannot register, drain,
//! deregister, patch, or report health for another broker; an operator holding
//! `cluster:*` can manage the whole fleet. Deleting a record takes `cluster:*`
//! outright. Reads require
//! `node.view:cluster:*`, since the listing exposes the cluster's network
//! layout.
pub(crate) mod fleet;
pub(crate) mod listing;
pub(crate) mod reports;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;

use crate::api::AppState;
use crate::api::error::{ApiError, api_conflict, api_internal, api_not_found};
use crate::api::types::{NodeRegistrationRequest, NodeRegistrationResponse};
use crate::auth::bearer::{require_cluster_action, verified_claims};
use crate::auth::rbac::authorize::{
    ACTION_NODE_MANAGE, ACTION_NODE_VIEW, ParsedObject, object_within_scope, parse_permission,
};
use crate::clock::now_millis;
use crate::model::{Node, NodeLifecycle, NodePatchRequest, NodeSpec, NodeStatus};
use crate::store::StoreError;

#[utoipa::path(
    post,
    path = "/v1/nodes",
    tag = "nodes",
    request_body = NodeRegistrationRequest,
    responses(
        (status = 200, description = "Node registered", body = NodeRegistrationResponse),
        (status = 409, description = "Identity or address rejected", body = crate::api::types::ErrorResponse)
    )
)]
/// Claim a broker identity.
///
/// Idempotent per `node_id`: a broker that restarts re-registers under the same
/// identity, keeping its original registration time and taking the next
/// incarnation. That is what lets a restart be told apart from a new node.
///
/// # Errors
/// - 409 when the identity or advertised address is invalid, the address
///   already belongs to another node, or the broker lacks a fleet feature
///   every other serving broker reported.
pub(crate) async fn register_node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<NodeRegistrationRequest>,
) -> Result<Json<NodeRegistrationResponse>, ApiError> {
    // The identity being claimed comes from the body, so that is what the
    // token has to be authorised for. A broker cannot register as someone else
    // by asking to.
    require_node_manage(&state, &headers, &request.node_id).await?;
    let now = now_millis();
    let node = Node {
        node_id: request.node_id,
        spec: NodeSpec {
            advertise_addr: request.advertise_addr,
            client_addr: request.client_addr,
            kafka_addr: request.kafka_addr,
            region: request.region,
            zone: request.zone,
            labels: request.labels,
            capacity: request.capacity,
        },
        // The control plane's to set, not the caller's. `register_node`
        // preserves the original `registered_at_millis` and owns `incarnation`.
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: now,
            registered_at_millis: now,
            incarnation: 0,
            features: request.features,
        },
    };

    let (node, fleet_features) = state
        .store
        .register_node_in_fleet(node)
        .await
        .map_err(|err| match err {
            StoreError::Conflict(ref message) => api_conflict("conflict", message),
            ref other => api_internal("register node", other),
        })?;

    Ok(Json(NodeRegistrationResponse {
        node,
        heartbeat_interval_ms: state.node_liveness.heartbeat_interval_ms,
        expiry_timeout_ms: state.node_liveness.expiry_timeout_ms,
        fleet_features,
    }))
}

#[utoipa::path(
    patch,
    path = "/v1/nodes/{node_id}",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    request_body = NodePatchRequest,
    responses(
        (status = 200, description = "Node updated", body = crate::model::Node),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Invalid spec or lifecycle move", body = crate::api::types::ErrorResponse)
    )
)]
/// Change a node's region, labels or capacity, or move it between `live` and
/// `draining`.
///
/// Setting `lifecycle` to `live` on a draining node is how an operator cancels
/// a drain. Nothing observed is patchable: a `down` or `left` node is revived
/// only by the broker registering.
///
/// # Errors
/// - 404 when the node is not registered.
/// - 409 when the patched spec is invalid or the lifecycle move is not one an
///   operator may make.
pub(crate) async fn patch_node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(patch): Json<NodePatchRequest>,
) -> Result<Json<Node>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
    state
        .store
        .patch_node(&node_id, patch)
        .await
        .map(Json)
        .map_err(|err| match err {
            StoreError::NotFound(_) => api_not_found("node is not registered"),
            StoreError::Conflict(ref message) => api_conflict("conflict", message),
            ref other => api_internal("patch node", other),
        })
}

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/drain",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    responses(
        (status = 200, description = "Node is draining", body = crate::model::Node),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Node cannot drain from its current lifecycle", body = crate::api::types::ErrorResponse)
    )
)]
/// Stop new placement without stopping service.
///
/// Called by a broker at the start of its own shutdown, so nothing new is
/// assigned to it while it finishes what it has.
///
/// # Errors
/// - 404 when the node is not registered.
/// - 409 when the node is not currently serving, since there is nothing to drain.
pub(crate) async fn drain_node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
    set_lifecycle(&state, &node_id, NodeLifecycle::Draining).await
}

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/deregister",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    responses(
        (status = 200, description = "Node has left", body = crate::model::Node),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse)
    )
)]
/// Leave the cluster on purpose.
///
/// This is what distinguishes a graceful shutdown from a crash: a node that
/// deregisters is `left`, while one that simply stops is found `down` by expiry.
/// The record is kept either way, so the identity and its incarnation survive
/// for the next boot.
///
/// Heartbeats stop renewing the node's lease from here, but the lease it
/// already holds runs on. Until it has provably run out (its last heartbeat
/// plus the expiry timeout and regrant margin, the wait a silent node gets)
/// placement treats the node as draining, so nothing it leads is handed to
/// another broker. A node already silent that long has its shards failed over
/// straight away.
///
/// # Errors
/// - 404 when the node is not registered.
pub(crate) async fn deregister_node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
    set_lifecycle(&state, &node_id, NodeLifecycle::Left).await
}

#[utoipa::path(
    delete,
    path = "/v1/nodes/{node_id}",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    responses(
        (status = 204, description = "Node record removed"),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Node is still running, or a shard still names it", body = crate::api::types::ErrorResponse)
    )
)]
/// Remove a broker's record for good.
///
/// Refused while the broker is `live` or `draining`, and while any shard names
/// it as leader or replica: the assignment is the only record of where that
/// shard's data is, and a follower slot naming a node that no longer exists is
/// never replaced. Requires `node.manage` on `cluster:*`; a broker cannot
/// remove itself.
///
/// # Errors
/// - 404 when the node is not registered.
/// - 409 when the node is still serving or still named by an assignment.
pub(crate) async fn delete_node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<axum::http::StatusCode, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;

    let node = state
        .store
        .get_node(&node_id)
        .await
        .map_err(|err| match err {
            StoreError::NotFound(_) => api_not_found("node is not registered"),
            ref other => api_internal("get node", other),
        })?;
    if matches!(
        node.status.lifecycle,
        NodeLifecycle::Live | NodeLifecycle::Draining
    ) {
        return Err(api_conflict(
            "conflict",
            &format!("node {node_id} is still running; drain it and stop it first"),
        ));
    }
    // The store refuses a named node atomically; this is only the clearer
    // message for the common case.
    let named = state
        .store
        .list_shard_assignments()
        .await
        .map_err(|ref err| api_internal("list shard assignments", err))?
        .into_iter()
        .filter(|a| a.leader == node_id || a.replicas.contains(&node_id))
        .count();
    if named > 0 {
        return Err(api_conflict(
            "conflict",
            &format!("node {node_id} is still named by {named} shard assignment(s)"),
        ));
    }

    state
        .store
        .delete_node(&node_id)
        .await
        .map_err(|err| match err {
            StoreError::NotFound(_) => api_not_found("node is not registered"),
            StoreError::Conflict(ref message) => api_conflict("conflict", message),
            ref other => api_internal("delete node", other),
        })?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// Drive a lifecycle move, treating "already there" as success.
///
/// A retried drain or deregistration must not fail: a broker that shuts down
/// twice for the same reason is not an error condition.
async fn set_lifecycle(
    state: &AppState,
    node_id: &str,
    lifecycle: NodeLifecycle,
) -> Result<Json<Node>, ApiError> {
    let moved = state
        .store
        .set_node_lifecycle(node_id, lifecycle)
        .await
        .map_err(|err| match err {
            StoreError::NotFound(_) => api_not_found("node is not registered"),
            StoreError::Conflict(ref message) => api_conflict("conflict", message),
            ref other => api_internal("set node lifecycle", other),
        })?;

    match moved {
        Some(node) => Ok(Json(node)),
        None => state
            .store
            .get_node(node_id)
            .await
            .map(Json)
            .map_err(|err| match err {
                StoreError::NotFound(_) => api_not_found("node is not registered"),
                ref other => api_internal("get node", other),
            }),
    }
}

/// Require permission to change one node's membership.
///
/// The node id comes from the request -- a path segment, or the body on
/// registration -- and the token has to carry `node.manage` over a scope that
/// contains it. A broker's credential is scoped to `node:{its own id}`, so
/// presenting it for another node fails here rather than being trusted because
/// the request said so. An operator holding `cluster:*` covers every node.
///
/// This is what closes the hole where any caller that could reach the control
/// plane could register, drain, or deregister any broker, or keep a dead one
/// looking alive.
async fn require_node_manage(
    state: &AppState,
    headers: &HeaderMap,
    node_id: &str,
) -> Result<(), ApiError> {
    let (tenant_id, claims) = verified_claims(state, headers).await?;
    let target = ParsedObject::Node {
        node_id: node_id.to_string(),
    };

    let allowed = claims.perms.iter().any(|perm| {
        // Unparsable entries are skipped rather than trusted, so a malformed
        // permission can never widen access.
        matches!(
            parse_permission(perm, &tenant_id),
            Ok(parsed) if parsed.action == ACTION_NODE_MANAGE
                && object_within_scope(&parsed.object, &target)
        )
    });

    if allowed {
        Ok(())
    } else {
        Err(crate::auth::bearer::refused(
            crate::auth::bearer::Refusal::Forbidden,
            &format!("missing node.manage on node:{node_id} or cluster:*"),
        ))
    }
}

/// Reads require `node.view:cluster:*`. The tenant comes from the token's own
/// `tid` claim rather than the path, because `/v1/nodes` is not a tenant
/// resource; see [`require_cluster_action`].
pub(super) async fn require_cluster_node_view(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    require_cluster_action(state, headers, ACTION_NODE_VIEW).await
}

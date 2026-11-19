//! Declaring a broker's data directory dead, the operator step a re-form
//! needs before it can plan without that broker. See
//! `docs/per-shard-raft.md`, "Declaring a broker dead".
use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;

use super::require_cluster_node_view;
use crate::api::AppState;
use crate::api::error::{ApiError, api_conflict, api_internal, api_not_found};
use crate::auth::bearer::{require_cluster_action, verified_claims};
use crate::auth::rbac::authorize::ACTION_NODE_MANAGE;
use crate::model::DeclaredDead;
use crate::store::StoreError;

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/declare-dead",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    responses(
        (status = 200, description = "The data directory declared dead", body = DeclaredDead),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Node is live or draining, reported no storage id, or a control-plane member is below metadata version 42", body = crate::api::types::ErrorResponse)
    )
)]
/// Declare a broker's data directory gone for good.
///
/// Records the node with the storage id it is registered under. A re-form
/// then plans without that copy, and the node registers again only with a
/// new data directory. It cannot be undone. Only a node that is down or
/// left can be declared dead. Requires `node.manage` on `cluster:*`.
///
/// # Errors
/// - 404 when the node is not registered.
/// - 409 when the node is live or draining, has no storage id, or a
///   control-plane member is below metadata version 42.
pub(crate) async fn declare_dead(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<DeclaredDead>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;
    let (_, claims) = verified_claims(&state, &headers).await?;
    let declared = state
        .store
        .declare_node_dead(&node_id)
        .await
        .map_err(|err| match err {
            StoreError::NotFound(_) => api_not_found("node is not registered"),
            StoreError::Conflict(ref message) => api_conflict("conflict", message),
            ref other => api_internal("declare node dead", other),
        })?;
    tracing::warn!(
        principal = %claims.sub,
        node_id = %declared.node_id,
        storage_id = %declared.storage_id,
        "a broker's data directory was declared dead by an operator"
    );
    Ok(Json(declared))
}

#[utoipa::path(
    get,
    path = "/v1/fleet/declared-dead",
    tag = "nodes",
    responses(
        (status = 200, description = "Every data directory declared dead", body = [DeclaredDead])
    )
)]
/// List every data directory declared dead, by node and storage id.
pub(crate) async fn list_dead(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<DeclaredDead>>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    state
        .store
        .declared_dead()
        .await
        .map(Json)
        .map_err(|ref err| api_internal("list declared dead", err))
}

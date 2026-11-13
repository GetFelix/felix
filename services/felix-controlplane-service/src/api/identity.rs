//! The cluster's identity: the metadata group's id and the shard engine it
//! was formed for.
//!
//! Needs `node.view:cluster:*`, as the snapshot feeds brokers already read.
use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};

use crate::api::AppState;
use crate::api::error::{ApiError, api_error, api_internal};
use crate::api::types::ClusterIdentityResponse;
use crate::auth::bearer::require_cluster_action;
use crate::auth::rbac::authorize::ACTION_NODE_VIEW;
use crate::store::identity::GroupIdentity;

#[utoipa::path(
    get,
    path = "/v1/cluster/identity",
    tag = "cluster",
    responses(
        (status = 200, description = "The cluster's identity", body = ClusterIdentityResponse),
        (status = 503, description = "The metadata group has not recorded its identity yet")
    )
)]
pub(crate) async fn cluster_identity(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ClusterIdentityResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_VIEW).await?;
    let identity = state
        .store
        .group_identity()
        .await
        .map_err(|err| api_internal("failed to load the cluster identity", &err))?;
    identity_response(identity).map(Json)
}

/// 503 while a group has not recorded its identity, so a broker retries.
fn identity_response(identity: GroupIdentity) -> Result<ClusterIdentityResponse, ApiError> {
    match identity {
        GroupIdentity::Recorded(identity) => Ok(ClusterIdentityResponse {
            group_id: Some(identity.group_id),
            shard_engine: identity.shard_engine,
            adopts_existing: identity.adopts_existing,
        }),
        GroupIdentity::NoGroup { shard_engine } => Ok(ClusterIdentityResponse {
            group_id: None,
            shard_engine,
            adopts_existing: false,
        }),
        GroupIdentity::Pending => Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "identity_pending",
            "the metadata group has not recorded its identity yet; retry shortly",
        )),
    }
}

/// The group id for a snapshot or a change-feed page, when one is recorded.
pub(crate) async fn group_id(state: &AppState) -> Result<Option<String>, ApiError> {
    state
        .store
        .group_identity()
        .await
        .map(|identity| identity.group_id())
        .map_err(|err| api_internal("failed to load the cluster identity", &err))
}

#[cfg(test)]
mod tests;

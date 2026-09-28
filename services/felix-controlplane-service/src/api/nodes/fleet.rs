//! Fleet features: what the serving brokers support, and finalizing one so
//! the fleet uses it.
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;

use super::require_cluster_node_view;
use crate::api::AppState;
use crate::api::error::{ApiError, api_conflict, api_internal};
use crate::api::types::{FleetFeaturesResponse, FleetFinalizeResponse};
use crate::auth::bearer::require_cluster_action;
use crate::auth::rbac::authorize::ACTION_NODE_MANAGE;
use crate::store::StoreError;

#[utoipa::path(
    get,
    path = "/v1/fleet/features",
    tag = "nodes",
    responses(
        (status = 200, description = "Supported and enabled fleet features", body = FleetFeaturesResponse)
    )
)]
/// Which fleet features every live or draining broker supports, and which an
/// operator has enabled.
///
/// # Errors
/// - 500 when the store cannot be read.
pub(crate) async fn fleet_features(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<FleetFeaturesResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let nodes = state
        .store
        .list_nodes()
        .await
        .map_err(|ref err| api_internal("list nodes", err))?;
    let enabled = state
        .store
        .enabled_fleet_features()
        .await
        .map_err(|ref err| api_internal("read the enabled fleet features", err))?;
    Ok(Json(FleetFeaturesResponse {
        supported: crate::cluster::fleet::supported(&nodes),
        enabled,
        serving_nodes: serving(&nodes),
    }))
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct FinalizeQuery {
    #[serde(default)]
    dry_run: bool,
}

#[utoipa::path(
    post,
    path = "/v1/fleet/features/{feature}/finalize",
    tag = "nodes",
    params(
        ("feature" = String, Path, description = "The feature's wire name"),
        ("dry_run" = Option<bool>, Query, description = "Preview without enabling")
    ),
    responses(
        (status = 200, description = "Enabled, or with dry_run the preview", body = FleetFinalizeResponse),
        (status = 409, description = "A serving broker does not support it, or none is serving")
    )
)]
/// Enable a fleet feature. One-way: once enabled, a broker without the
/// feature is refused at registration, so rolling back means a build that
/// still has it.
///
/// Refused unless every live or draining broker reported the feature. A
/// broker that is down when it is enabled is refused when it comes back
/// without it. With `dry_run=true` nothing changes, and the answer says
/// whether a real finalize would be accepted and which brokers lack it.
///
/// # Errors
/// - 409 when a serving broker lacks the feature, or no broker is serving.
/// - 500 when the store cannot be read or written.
pub(crate) async fn finalize_fleet_feature(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(feature): Path<String>,
    Query(query): Query<FinalizeQuery>,
) -> Result<Json<FleetFinalizeResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;
    let nodes = state
        .store
        .list_nodes()
        .await
        .map_err(|ref err| api_internal("list nodes", err))?;
    let lacking = crate::cluster::fleet::lacking(&nodes, &feature);
    let serving_nodes = serving(&nodes);

    if query.dry_run {
        let enabled = state
            .store
            .enabled_fleet_features()
            .await
            .map_err(|ref err| api_internal("read the enabled fleet features", err))?
            .contains(&feature);
        let would_enable =
            enabled || crate::cluster::fleet::check_finalize(&nodes, &feature).is_ok();
        return Ok(Json(FleetFinalizeResponse {
            feature,
            dry_run: true,
            enabled,
            would_enable,
            lacking,
            serving_nodes,
        }));
    }

    let enabled = state
        .store
        .finalize_fleet_feature(&feature)
        .await
        .map_err(|err| match err {
            StoreError::Conflict(ref message) => api_conflict("conflict", message),
            ref other => api_internal("finalize a fleet feature", other),
        })?;
    tracing::info!(%feature, "fleet feature finalized by an operator");
    Ok(Json(FleetFinalizeResponse {
        enabled: enabled.contains(&feature),
        would_enable: true,
        feature,
        dry_run: false,
        lacking,
        serving_nodes,
    }))
}

fn serving(nodes: &[crate::model::Node]) -> usize {
    nodes
        .iter()
        .filter(|node| node.status.lifecycle.is_serving())
        .count()
}

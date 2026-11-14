//! A shard's switch from the hybrid to Raft: the operator's seal order, and
//! what the seal leader sends back, its report or its give-up.
//!
//! `docs/per-shard-raft.md`, "Moving a shard from the hybrid to Raft".
use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;

use super::require_node_manage;
use crate::api::AppState;
use crate::api::error::{ApiError, api_conflict, api_internal, api_not_found};
use crate::api::types::{
    SealGiveUpRequest, SealGiveUpResponse, SealReportRequest, SealReportResponse,
};
use crate::auth::bearer::require_cluster_action;
use crate::auth::rbac::authorize::ACTION_NODE_MANAGE;
use crate::model::{ShardAssignment, ShardKey, ShardKind};
use crate::store::{AssignmentWrite, StoreError};

#[utoipa::path(
    post,
    path = "/v1/placement/seal/{tenant_id}/{namespace}/{name}/{shard}",
    tag = "placement",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace"),
        ("name" = String, Path, description = "Stream name"),
        ("shard" = u32, Path, description = "Shard number")
    ),
    responses(
        (status = 200, description = "Seal ordered at the returned generation", body = ShardAssignment),
        (status = 404, description = "No such shard", body = crate::api::types::ErrorResponse),
        (status = 409, description = "The shard cannot be sealed now", body = crate::api::types::ErrorResponse)
    )
)]
/// Order the seal that starts a stream shard's switch to Raft.
///
/// The shard closes for writes until its leader reports the seal and a Raft
/// leader opens, or until the seal is given up. There is no switch back.
///
/// # Errors
/// - 404 when the shard has no assignment.
/// - 409 `raft_unavailable` until the fleet has finalized `shard_raft`,
///   `not_sealable` for a shard that is not active, is moving or already
///   switched, and `conflict` when the shard changed under the request or a
///   control-plane member is below metadata version 33.
pub(crate) async fn seal_shard(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((tenant_id, namespace, name, shard)): Path<(String, String, String, u32)>,
) -> Result<Json<ShardAssignment>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;
    let key = ShardKey {
        tenant_id,
        namespace,
        stream: name,
        shard,
        // Only a stream shard switches; a cache shard has no route here.
        kind: ShardKind::Stream,
    };
    let enabled = state
        .store
        .enabled_fleet_features()
        .await
        .map_err(|ref err| api_internal("read the enabled fleet features", err))?;
    if !enabled.contains(felix_common::fleet::SHARD_RAFT.name()) {
        return Err(api_conflict(
            "raft_unavailable",
            "a shard switches to Raft only once the fleet has finalized shard_raft",
        ));
    }
    let current = match state.store.get_shard_assignment(&key).await {
        Ok(current) => current,
        Err(StoreError::NotFound(_)) => return Err(api_not_found("shard has no assignment")),
        Err(ref err) => return Err(api_internal("read the shard assignment", err)),
    };
    if let Err(err) = current.can_seal() {
        return Err(api_conflict("not_sealable", &err.to_string()));
    }
    match state.store.seal_shard(&key, current.generation).await {
        Ok(AssignmentWrite::Written(sealed)) => {
            tracing::info!(
                name = %key.stream,
                shard = key.shard,
                leader = %sealed.leader,
                generation = sealed.generation,
                "ordered a shard's seal for its switch to Raft",
            );
            state.placement_wakes.request_pass();
            Ok(Json(sealed))
        }
        Ok(AssignmentWrite::Stale { .. } | AssignmentWrite::Fenced { .. }) => Err(api_conflict(
            "conflict",
            "the shard changed while the seal was being ordered; try again",
        )),
        Err(StoreError::Conflict(ref message)) => Err(api_conflict("conflict", message)),
        Err(StoreError::NotFound(_)) => Err(api_not_found("shard has no assignment")),
        Err(ref err) => Err(api_internal("order the seal", err)),
    }
}

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/seal-report",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    request_body = SealReportRequest,
    responses(
        (status = 200, description = "Whether this report is the one recorded for the seal", body = SealReportResponse),
        (status = 409, description = "The report cannot be the shard's, or a control-plane member cannot record it yet", body = crate::api::types::ErrorResponse)
    )
)]
/// Record a seal leader's report as the shard's switch to Raft.
///
/// One report wins a seal. `accepted` is false when the seal no longer
/// stands: it was given up, or the shard moved to a later generation. The
/// leader then has nothing more to send for it.
///
/// A broker reports only its own seal: the node must be the one the seal
/// was ordered from.
///
/// # Errors
/// - 409 `not_sealer` when the node is not the shard's seal leader,
///   `conflict` for a report whose voters cannot be the shard's, or when a
///   control-plane member is below metadata version 33.
pub(crate) async fn report_seal(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(request): Json<SealReportRequest>,
) -> Result<Json<SealReportResponse>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
    if !speaks_for_seal(&state, &node_id, &request.key).await? {
        return Err(api_conflict(
            "not_sealer",
            "only the broker a seal was ordered from reports it",
        ));
    }
    match state
        .store
        .accept_seal_report(&request.key, request.report, &node_id)
        .await
    {
        Ok(AssignmentWrite::Written(switched)) => {
            tracing::info!(
                name = %request.key.stream,
                shard = request.key.shard,
                generation = switched.generation,
                "recorded a shard's switch to Raft",
            );
            Ok(Json(SealReportResponse { accepted: true }))
        }
        Ok(AssignmentWrite::Stale { .. } | AssignmentWrite::Fenced { .. }) => {
            Ok(Json(SealReportResponse { accepted: false }))
        }
        Err(StoreError::Conflict(ref message)) => Err(api_conflict("conflict", message)),
        Err(ref err) => Err(api_internal("record the seal report", err)),
    }
}

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/seal-give-up",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    request_body = SealGiveUpRequest,
    responses(
        (status = 200, description = "Whether the seal was given up by this request", body = SealGiveUpResponse),
        (status = 409, description = "The node is not the seal's leader, or a control-plane member cannot record it yet", body = crate::api::types::ErrorResponse)
    )
)]
/// Give up a seal its leader cannot finish.
///
/// The shard is named again at the next generation without a seal order,
/// which opens the hybrid again. `given_up` is false when no seal stands at
/// that generation: a report was recorded, or the shard already moved on.
///
/// # Errors
/// - 409 `not_sealer` when the node is not the shard's seal leader, or
///   `conflict` when a control-plane member is below metadata version 33.
pub(crate) async fn give_up_seal(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(request): Json<SealGiveUpRequest>,
) -> Result<Json<SealGiveUpResponse>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
    if !speaks_for_seal(&state, &node_id, &request.key).await? {
        return Err(api_conflict(
            "not_sealer",
            "only the broker a seal was ordered from gives it up",
        ));
    }
    match state
        .store
        .give_up_seal(&request.key, request.generation, &node_id)
        .await
    {
        Ok(AssignmentWrite::Written(reopened)) => {
            tracing::warn!(
                name = %request.key.stream,
                shard = request.key.shard,
                sealed_at = request.generation,
                generation = reopened.generation,
                reason = %request.reason,
                "a shard's seal was given up; the shard opens again on the hybrid",
            );
            state.placement_wakes.request_pass();
            Ok(Json(SealGiveUpResponse { given_up: true }))
        }
        Ok(AssignmentWrite::Stale { .. } | AssignmentWrite::Fenced { .. }) => {
            Ok(Json(SealGiveUpResponse { given_up: false }))
        }
        Err(StoreError::Conflict(ref message)) => Err(api_conflict("conflict", message)),
        Err(ref err) => Err(api_internal("give up the seal", err)),
    }
}

/// Whether `node_id` is the node the shard's switch record names as its
/// sealer. A shard with no record has no seal for anyone to speak for; the
/// store then answers the request as stale.
async fn speaks_for_seal(
    state: &AppState,
    node_id: &str,
    key: &ShardKey,
) -> Result<bool, ApiError> {
    match state.store.get_shard_assignment(key).await {
        Ok(assignment) => Ok(assignment
            .switch
            .is_none_or(|switch| switch.sealer == node_id)),
        Err(StoreError::NotFound(_)) => Ok(true),
        Err(ref err) => Err(api_internal("read the shard assignment", err)),
    }
}

#[cfg(test)]
mod tests;

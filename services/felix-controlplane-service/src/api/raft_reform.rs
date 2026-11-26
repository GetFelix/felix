//! Re-form plan routes. The plans themselves, and why they live on the
//! metadata Raft leader, are in [`crate::store::reform`].
pub use crate::store::reform::{
    ReformAnswer, ReformApplyView, ReformPlanView, ReformRestore, ReformRestoreView, ReformScope,
    ReformShardView, ReformSurvey, RestoreShard, SurveyKind,
};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::AppState;
use super::error::{ApiError, api_conflict, api_internal, api_not_found, api_validation_error};
use crate::auth::bearer::require_cluster_action;
use crate::auth::rbac::authorize::ACTION_NODE_MANAGE;
use crate::store::StoreError;
use crate::store::reform::{MAX_BATCH, ReformReply, ReformRequest};

#[utoipa::path(
    post,
    path = "/v1/raft/reform/plans",
    tag = "raft",
    request_body = CreateReformPlanRequest,
    responses(
        (status = 200, description = "The plan was started", body = CreateReformPlanResponse)
    )
)]
/// Start a read-only re-form plan for the Raft shards in `scope`.
///
/// With `restore`, each shard re-forms from a backup point: its base must
/// end at the point, and its next epoch starts at least `offset_gap` past
/// 2^14 times the control plane's clock (Unix ms) at plan creation. A gap
/// above 2^40 is refused, and one below 2^32 unless `accept_short_gap` is
/// set.
///
/// Brokers are surveyed in the background; read the plan with
/// `GET /v1/raft/reform/plans/{plan_id}`. Requires `node.manage` on
/// `cluster:*`.
pub(crate) async fn create_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateReformPlanRequest>,
) -> Result<Json<CreateReformPlanResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;
    if let Some(reason) = request.restore.as_ref().and_then(ReformRestore::refused) {
        return Err(api_validation_error(&reason));
    }
    let request = ReformRequest::Create {
        scope: request.scope,
        restore: request.restore,
    };
    match ask(&state, request).await? {
        ReformReply::Created { plan_id } => Ok(Json(CreateReformPlanResponse { plan_id })),
        ReformReply::NotCreated { reason } => Err(api_validation_error(&reason)),
        _ => Err(unexpected()),
    }
}

#[utoipa::path(
    get,
    path = "/v1/raft/reform/plans/{plan_id}",
    tag = "raft",
    params(("plan_id" = String, Path, description = "Plan identifier")),
    responses(
        (status = 200, description = "The plan", body = ReformPlanView),
        (status = 404, description = "No such plan on the control-plane leader", body = crate::api::types::ErrorResponse)
    )
)]
/// Read a re-form plan. Requires `node.manage` on `cluster:*`.
pub(crate) async fn get_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(plan_id): Path<String>,
) -> Result<Json<ReformPlanView>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;
    match ask(&state, ReformRequest::View { plan_id }).await? {
        ReformReply::Plan { plan: Some(plan) } => Ok(Json(plan)),
        // Plans are the leader's for its term: after a leader change or a
        // restart, plan again.
        ReformReply::Plan { plan: None } => Err(api_not_found(
            "no such plan; the control-plane leader may have changed since, so plan again",
        )),
        _ => Err(unexpected()),
    }
}

#[utoipa::path(
    post,
    path = "/v1/raft/reform/plans/{plan_id}/apply",
    tag = "raft",
    params(("plan_id" = String, Path, description = "Plan identifier")),
    request_body = ApplyReformPlanRequest,
    responses(
        (status = 200, description = "The stop rounds started", body = ApplyReformPlanResponse),
        (status = 409, description = "No such plan on the control-plane leader, or it is still surveying", body = crate::api::types::ErrorResponse)
    )
)]
/// Apply a re-form plan: stop each shard it marked `reform`, compare the
/// post-stop plan with this one, write the next epoch, then start it. A
/// shard whose plan may lose acknowledged records is applied only when
/// named in `accept_loss`. Read progress with
/// `GET /v1/raft/reform/plans/{plan_id}`. Requires `node.manage` on
/// `cluster:*`, like making the plan.
pub(crate) async fn apply_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(plan_id): Path<String>,
    Json(request): Json<ApplyReformPlanRequest>,
) -> Result<Json<ApplyReformPlanResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;
    let request = ReformRequest::Apply {
        plan_id,
        accept_loss: request.accept_loss,
    };
    match ask(&state, request).await? {
        ReformReply::Applied { applying } => Ok(Json(ApplyReformPlanResponse { applying })),
        ReformReply::NotApplied { reason } => Err(api_conflict("conflict", &reason)),
        _ => Err(unexpected()),
    }
}

#[utoipa::path(
    get,
    path = "/v1/nodes/{node_id}/reform/surveys",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    responses(
        (status = 200, description = "Surveys this broker has not answered", body = ReformSurveysResponse)
    )
)]
/// The re-form surveys a broker is asked to answer. Requires the node's
/// own credential.
pub(crate) async fn surveys(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<ReformSurveysResponse>, ApiError> {
    super::nodes::require_node_manage(&state, &headers, &node_id).await?;
    match ask(&state, ReformRequest::Surveys { node_id }).await? {
        ReformReply::Surveys { surveys } => Ok(Json(ReformSurveysResponse { surveys })),
        _ => Err(unexpected()),
    }
}

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/reform/reports",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    request_body = ReformReportsRequest,
    responses(
        (status = 200, description = "How many answers were taken", body = ReformReportsResponse),
        (status = 400, description = "More than 1024 answers")
    )
)]
/// A broker's answers to re-form surveys. Requires the node's own
/// credential. An answer for a round that is no longer current is ignored.
pub(crate) async fn reports(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(request): Json<ReformReportsRequest>,
) -> Result<Json<ReformReportsResponse>, ApiError> {
    super::nodes::require_node_manage(&state, &headers, &node_id).await?;
    if request.reports.len() > MAX_BATCH {
        return Err(api_validation_error(&format!(
            "at most {MAX_BATCH} reports in one request"
        )));
    }
    let request = ReformRequest::Reports {
        node_id,
        answers: request.reports,
    };
    match ask(&state, request).await? {
        ReformReply::Recorded { accepted, ignored } => {
            Ok(Json(ReformReportsResponse { accepted, ignored }))
        }
        _ => Err(unexpected()),
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateReformPlanRequest {
    pub scope: ReformScope,
    /// Re-form from a backup point instead of from the copies as they are.
    #[serde(default)]
    pub restore: Option<ReformRestore>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateReformPlanResponse {
    pub plan_id: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ApplyReformPlanRequest {
    /// Shards whose plan may lose acknowledged records, applied anyway.
    #[serde(default)]
    pub accept_loss: Vec<crate::model::ShardKey>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApplyReformPlanResponse {
    /// How many shards started their stop round.
    pub applying: usize,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReformSurveysResponse {
    pub surveys: Vec<ReformSurvey>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ReformReportsRequest {
    pub reports: Vec<ReformAnswer>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReformReportsResponse {
    pub accepted: usize,
    /// Answers for a plan, round or shard that is not current, or from a
    /// broker the round does not ask.
    pub ignored: usize,
}

fn unexpected() -> ApiError {
    api_internal(
        "answer the re-form request",
        &StoreError::Unexpected(anyhow::anyhow!("a reply of another kind")),
    )
}

/// A metadata leader of a version without plans is a 409, not a fault.
fn refused(err: StoreError) -> ApiError {
    match err {
        StoreError::Conflict(ref message) => api_conflict("conflict", message),
        ref other => api_internal("answer the re-form request", other),
    }
}

async fn ask(state: &AppState, request: ReformRequest) -> Result<ReformReply, ApiError> {
    state.store.reform(request).await.map_err(refused)
}

#[cfg(test)]
mod tests;

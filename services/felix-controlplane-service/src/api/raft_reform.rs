//! Re-form plan routes. The plans themselves, and why they live on the
//! metadata Raft leader, are in [`crate::store::reform`].
pub use crate::store::reform::{
    ReformAnswer, ReformPlanView, ReformScope, ReformShardView, ReformSurvey,
};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::AppState;
use super::error::{ApiError, api_internal, api_not_found, api_validation_error};
use crate::auth::bearer::require_cluster_action;
use crate::auth::rbac::authorize::ACTION_NODE_MANAGE;
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
/// Brokers are surveyed in the background; read the plan with
/// `GET /v1/raft/reform/plans/{plan_id}`. Requires `node.manage` on
/// `cluster:*`.
pub(crate) async fn create_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateReformPlanRequest>,
) -> Result<Json<CreateReformPlanResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;
    let request = ReformRequest::Create {
        scope: request.scope,
    };
    match ask(&state, request).await? {
        ReformReply::Created { plan_id } => Ok(Json(CreateReformPlanResponse { plan_id })),
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
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateReformPlanResponse {
    pub plan_id: String,
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
        &crate::store::StoreError::Unexpected(anyhow::anyhow!("a reply of another kind")),
    )
}

async fn ask(state: &AppState, request: ReformRequest) -> Result<ReformReply, ApiError> {
    state
        .store
        .reform(request)
        .await
        .map_err(|err| api_internal("answer the re-form request", &err))
}

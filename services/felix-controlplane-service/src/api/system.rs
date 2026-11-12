//! System metadata and health/liveness/readiness endpoints. Probes must stay
//! fast and side-effect free; see the per-endpoint docs for why liveness and
//! readiness deliberately answer different questions.
use axum::Json;
use axum::extract::State;

use crate::api::AppState;
use crate::api::error::ApiError;
use crate::api::types::{HealthStatus, SystemInfo};

#[utoipa::path(
    get,
    path = "/v1/system/info",
    tag = "system",
    responses(
        (status = 200, description = "Cluster identity and capabilities", body = SystemInfo)
    )
)]
/// Control-plane identity and feature flags, from in-memory config (no I/O).
pub(crate) async fn system_info(State(state): State<AppState>) -> Json<SystemInfo> {
    Json(SystemInfo {
        region_id: state.region.region_id.clone(),
        api_version: state.api_version.clone(),
        features: state.features.clone(),
    })
}

#[utoipa::path(
    get,
    path = "/v1/system/health",
    tag = "system",
    responses(
        (status = 200, description = "Control plane health", body = HealthStatus)
    )
)]
/// Health status — an alias for readiness.
pub(crate) async fn system_health(
    State(state): State<AppState>,
) -> Result<Json<HealthStatus>, ApiError> {
    // The original probe path, kept because deployments already point at it.
    // It answers the readiness question, which is what a caller polling
    // "health" almost always wants: whether to send this instance traffic.
    system_ready(State(state)).await
}

#[utoipa::path(
    get,
    path = "/v1/system/live",
    tag = "system",
    responses((status = 200, description = "The process is running", body = HealthStatus))
)]
/// Whether this process should be left alone.
///
/// Deliberately answers without touching the database or anything else outside
/// the process. A liveness probe drives *restarts*, and restarting every
/// instance because the store is down turns one external outage into a
/// cluster-wide restart loop that cannot fix it.
///
/// It fails when the process cannot answer at all — wedged, out of file
/// descriptors, or its runtime blocked — which is the one condition a restart
/// does address.
pub(crate) async fn system_live() -> Json<HealthStatus> {
    Json(HealthStatus {
        status: "ok".to_string(),
    })
}

#[utoipa::path(
    get,
    path = "/v1/system/ready",
    tag = "system",
    responses(
        (status = 200, description = "Ready to serve metadata", body = HealthStatus),
        (status = 503, description = "Not ready", body = crate::api::types::ErrorResponse)
    )
)]
/// Whether this instance should be sent traffic.
///
/// Checks the store, bounded and cached — see [`crate::api::readiness`]. Answers 503
/// rather than 500: this is a statement about *this instance right now*, and a
/// load balancer removing it is the correct response, not an error to alert on.
pub(crate) async fn system_ready(
    State(state): State<AppState>,
) -> Result<Json<HealthStatus>, ApiError> {
    match state.readiness.check().await {
        Ok(()) => Ok(Json(HealthStatus {
            status: "ok".to_string(),
        })),
        Err(reason) => Err(crate::api::error::api_error(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            &reason.to_string(),
        )),
    }
}

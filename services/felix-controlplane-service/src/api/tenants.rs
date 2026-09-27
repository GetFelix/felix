//! Tenant API handlers.
//!
//! The catalog -- which tenants exist -- is cluster metadata, not something
//! any one tenant owns, so every endpoint here is cluster-scoped: creating,
//! listing and deleting take `tenant.manage:cluster:*`, and the feeds the
//! brokers consume take `node.view:cluster:*` like the rest of the metadata
//! they sync. Day 0, before any operator credential exists, goes through the
//! bootstrap listener instead.
use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::api::AppState;
use crate::api::error::{
    ApiError, api_conflict, api_internal, api_internal_message, api_not_found, api_validation_error,
};
use crate::api::types::{
    TenantChangesResponse, TenantCreateRequest, TenantListResponse, TenantSnapshotResponse,
};
use crate::auth::bearer::require_cluster_action;
use crate::auth::keys::generate_signing_keys;
use crate::auth::rbac::authorize::{ACTION_NODE_VIEW, ACTION_TENANT_MANAGE};
use crate::model::{Tenant, validate_identifier};
use crate::store::StoreError;

#[utoipa::path(
    get,
    path = "/v1/tenants",
    tag = "tenants",
    responses(
        (status = 200, description = "List tenants", body = TenantListResponse),
        (status = 401, description = "Missing or invalid bearer token", body = crate::api::types::ErrorResponse),
        (status = 403, description = "Missing tenant.manage:cluster:*", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn list_tenants(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<TenantListResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_TENANT_MANAGE).await?;
    let items = state
        .store
        .list_tenants()
        .await
        .map_err(|err| api_internal("failed to list tenants", &err))?;
    Ok(Json(TenantListResponse { items }))
}

#[utoipa::path(
    post,
    path = "/v1/tenants",
    tag = "tenants",
    request_body = TenantCreateRequest,
    responses(
        (status = 201, description = "Tenant created", body = Tenant),
        (status = 401, description = "Missing or invalid bearer token", body = crate::api::types::ErrorResponse),
        (status = 403, description = "Missing tenant.manage:cluster:*", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Tenant already exists", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn create_tenant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<TenantCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    require_cluster_action(&state, &headers, ACTION_TENANT_MANAGE).await?;
    validate_identifier("tenant_id", &body.tenant_id).map_err(|err| api_validation_error(&err))?;
    let tenant = Tenant {
        tenant_id: body.tenant_id,
        display_name: body.display_name,
    };
    match state.store.create_tenant(tenant.clone()).await {
        Ok(_) => {
            let keys = generate_signing_keys().map_err(|err| {
                tracing::error!(error = ?err, "failed to generate tenant keys");
                api_internal_message("failed to generate tenant keys")
            })?;
            state
                .store
                .set_tenant_signing_keys(&tenant.tenant_id, keys)
                .await
                .map_err(|err| api_internal("failed to store tenant keys", &err))?;
            Ok((StatusCode::CREATED, Json(tenant)))
        }
        Err(StoreError::Conflict(_)) => {
            Err(api_conflict("already_exists", "tenant already exists"))
        }
        Err(err) => Err(api_internal("failed to create tenant", &err)),
    }
}

#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}",
    tag = "tenants",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    responses(
        (status = 204, description = "Tenant deleted"),
        (status = 401, description = "Missing or invalid bearer token", body = crate::api::types::ErrorResponse),
        (status = 403, description = "Missing tenant.manage:cluster:*", body = crate::api::types::ErrorResponse),
        (status = 404, description = "Tenant not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn delete_tenant(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    // Operator-only, deliberately. A tenant's own admin holds
    // `tenant.manage:tenant:{id}` and can empty it, but removing the tenant --
    // its keys included -- is a catalog change, and the catalog is the
    // operator's.
    require_cluster_action(&state, &headers, ACTION_TENANT_MANAGE).await?;
    match state.store.delete_tenant(&tenant_id).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound(_)) => Err(api_not_found("tenant not found")),
        Err(err) => Err(api_internal("failed to delete tenant", &err)),
    }
}

#[utoipa::path(
    get,
    path = "/v1/tenants/snapshot",
    tag = "tenants",
    responses(
        (status = 200, description = "Full tenant snapshot", body = TenantSnapshotResponse),
        (status = 401, description = "Missing or invalid bearer token", body = crate::api::types::ErrorResponse),
        (status = 403, description = "Missing node.view:cluster:*", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn tenant_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<TenantSnapshotResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_VIEW).await?;
    let snapshot = state
        .store
        .tenant_snapshot()
        .await
        .map_err(|err| api_internal("failed to load tenant snapshot", &err))?;
    Ok(Json(TenantSnapshotResponse {
        items: snapshot.items,
        next_seq: snapshot.next_seq,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/tenants/changes",
    tag = "tenants",
    params(
        ("since" = Option<u64>, Query, description = "Last seen sequence")
    ),
    responses(
        (status = 200, description = "Tenant change list", body = TenantChangesResponse),
        (status = 401, description = "Missing or invalid bearer token", body = crate::api::types::ErrorResponse),
        (status = 403, description = "Missing node.view:cluster:*", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn tenant_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<TenantChangesResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_VIEW).await?;
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let changes = state
        .store
        .tenant_changes(since)
        .await
        .map_err(|err| api_internal("failed to load tenant changes", &err))?;
    Ok(Json(TenantChangesResponse {
        items: changes.items,
        next_seq: changes.next_seq,
    }))
}

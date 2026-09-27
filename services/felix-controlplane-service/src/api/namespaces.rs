//! Namespace API handlers.
//!
//! Implements CRUD, snapshot, and changefeed endpoints for namespaces, including
//! tenant existence checks and error mapping.
//!
//! Every tenant-scoped endpoint requires `ns.manage` over the namespace, from
//! a token minted for the tenant in the path; the feeds require
//! `node.view:cluster:*`. The credential is checked before existence, so an
//! unauthenticated caller cannot learn what exists by asking.
use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::api::AppState;
use crate::api::ensure_tenant_exists;
use crate::api::error::{
    ApiError, api_conflict, api_internal, api_not_found, api_validation_error,
};
use crate::api::pagination::{PageParams, list_visible};
use crate::api::types::{
    NamespaceChangesResponse, NamespaceCreateRequest, NamespaceListResponse,
    NamespaceSnapshotResponse,
};
use crate::auth::bearer::{require_cluster_action, require_tenant_action, tenant_scopes_for};
use crate::auth::rbac::authorize::{
    ACTION_NODE_VIEW, ACTION_NS_MANAGE, ParsedObject, Segment, object_within_scope,
};
use crate::model::{Namespace, NamespaceKey, validate_identifier};
use crate::store::StoreError;

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces",
    tag = "namespaces",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        PageParams
    ),
    responses(
        (status = 200, description = "A page of the namespaces the caller may manage, in name order", body = NamespaceListResponse),
        (status = 400, description = "Invalid limit or cursor", body = crate::api::types::ErrorResponse),
        (status = 404, description = "Tenant not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn list_namespaces(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(page): Query<PageParams>,
) -> Result<Json<NamespaceListResponse>, ApiError> {
    let scopes = tenant_scopes_for(&state, &tenant_id, &headers, ACTION_NS_MANAGE).await?;
    let request = page.request()?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    let listed = list_visible(
        request,
        |page| state.store.list_namespaces_page(&tenant_id, page),
        // Only what the caller could manage: a namespace admin sees their
        // own, not the tenant's whole layout.
        |ns: &Namespace| {
            let target = ParsedObject::Namespace {
                tenant_id: tenant_id.clone(),
                namespace: Segment::Exact(ns.namespace.clone()),
            };
            scopes
                .iter()
                .any(|scope| object_within_scope(scope, &target))
        },
        |ns: &Namespace| ns.namespace.clone(),
    )
    .await
    .map_err(|err| api_internal("failed to list namespaces", &err))?;
    Ok(Json(NamespaceListResponse {
        items: listed.items,
        next_cursor: listed.next_cursor,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/namespaces",
    tag = "namespaces",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    request_body = NamespaceCreateRequest,
    responses(
        (status = 201, description = "Namespace created", body = Namespace),
        (status = 404, description = "Tenant not found", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Namespace already exists", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn create_namespace(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<NamespaceCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    require_ns_manage(&state, &tenant_id, &headers, &body.namespace).await?;
    validate_identifier("namespace", &body.namespace).map_err(|err| api_validation_error(&err))?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    let namespace = Namespace {
        tenant_id,
        namespace: body.namespace,
        display_name: body.display_name,
    };
    match state.store.create_namespace(namespace.clone()).await {
        Ok(ns) => Ok((StatusCode::CREATED, Json(ns))),
        Err(StoreError::Conflict(_)) => {
            Err(api_conflict("already_exists", "namespace already exists"))
        }
        Err(StoreError::NotFound(_)) => Err(api_not_found("tenant not found")),
        Err(err) => Err(api_internal("failed to create namespace", &err)),
    }
}

#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}",
    tag = "namespaces",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier")
    ),
    responses(
        (status = 204, description = "Namespace deleted"),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn delete_namespace(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    require_ns_manage(&state, &tenant_id, &headers, &namespace).await?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    let key = NamespaceKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
    };
    match state.store.delete_namespace(&key).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound(_)) => Err(api_not_found("namespace not found")),
        Err(err) => Err(api_internal("failed to delete namespace", &err)),
    }
}

/// `ns.manage` over the named namespace, from a token minted for this tenant.
async fn require_ns_manage(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
    namespace: &str,
) -> Result<(), ApiError> {
    let target = ParsedObject::Namespace {
        tenant_id: tenant_id.to_string(),
        namespace: Segment::Exact(namespace.to_string()),
    };
    require_tenant_action(state, tenant_id, headers, ACTION_NS_MANAGE, &target).await
}

#[utoipa::path(
    get,
    path = "/v1/namespaces/snapshot",
    tag = "namespaces",
    responses(
        (status = 200, description = "Full namespace snapshot", body = NamespaceSnapshotResponse)
    )
)]
pub(crate) async fn namespace_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<NamespaceSnapshotResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_VIEW).await?;
    let snapshot = state
        .store
        .namespace_snapshot()
        .await
        .map_err(|err| api_internal("failed to load namespace snapshot", &err))?;
    Ok(Json(NamespaceSnapshotResponse {
        items: snapshot.items,
        next_seq: snapshot.next_seq,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/namespaces/changes",
    tag = "namespaces",
    params(
        ("since" = Option<u64>, Query, description = "Last seen sequence")
    ),
    responses(
        (status = 200, description = "Namespace change list", body = NamespaceChangesResponse)
    )
)]
pub(crate) async fn namespace_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<NamespaceChangesResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_VIEW).await?;
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let changes = state
        .store
        .namespace_changes(since)
        .await
        .map_err(|err| api_internal("failed to load namespace changes", &err))?;
    Ok(Json(NamespaceChangesResponse {
        items: changes.items,
        next_seq: changes.next_seq,
    }))
}

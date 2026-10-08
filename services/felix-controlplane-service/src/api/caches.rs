//! Cache API handlers.
//!
//! Implements cache CRUD, patching, snapshot, and changefeed endpoints with
//! tenant/namespace validation.
//!
//! Every tenant-scoped endpoint requires `cache.manage` over the cache, from
//! a token minted for the tenant in the path; the feeds require
//! `node.view:cluster:*`. The credential is checked before existence, so an
//! unauthenticated caller cannot learn what exists by asking.
use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::api::AppState;
use crate::api::ensure_tenant_namespace;
use crate::api::error::{
    ApiError, api_conflict, api_internal, api_not_found, api_validation_error,
};
use crate::api::pagination::{PageParams, list_visible};
use crate::api::types::{
    CacheChangesResponse, CacheCreateRequest, CacheListResponse, CacheSnapshotResponse,
};
use crate::auth::bearer::{require_cluster_action, require_tenant_action, tenant_scopes_for};
use crate::auth::rbac::authorize::{
    ACTION_CACHE_MANAGE, ACTION_NODE_VIEW, ParsedObject, Segment, object_within_scope,
};
use crate::model::{Cache, CacheKey, CachePatchRequest, validate_identifier};
use crate::store::StoreError;

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        PageParams
    ),
    responses(
        (status = 200, description = "A page of the caches the caller may manage, in name order", body = CacheListResponse),
        (status = 400, description = "Invalid limit or cursor", body = crate::api::types::ErrorResponse),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn list_caches(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(page): Query<PageParams>,
) -> Result<Json<CacheListResponse>, ApiError> {
    let scopes = tenant_scopes_for(&state, &tenant_id, &headers, ACTION_CACHE_MANAGE).await?;
    let request = page.request()?;
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let listed = list_visible(
        request,
        |page| state.store.list_caches_page(&tenant_id, &namespace, page),
        // Only what the caller could manage.
        |cache: &Cache| {
            let target = cache_object(&tenant_id, &namespace, &cache.cache);
            scopes
                .iter()
                .any(|scope| object_within_scope(scope, &target))
        },
        |cache: &Cache| cache.cache.clone(),
    )
    .await
    .map_err(|err| api_internal("failed to list caches", &err))?;
    Ok(Json(CacheListResponse {
        items: listed.items,
        next_cursor: listed.next_cursor,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier")
    ),
    request_body = CacheCreateRequest,
    responses(
        (status = 201, description = "Cache created", body = Cache),
        (status = 200, description = "Cache already exists with this configuration; nothing changed", body = Cache),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Cache already exists with a different configuration", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn create_cache(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CacheCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    require_cache_manage(&state, &tenant_id, &headers, &namespace, &body.cache).await?;
    validate_identifier("cache", &body.cache).map_err(|err| api_validation_error(&err))?;
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let cache = new_cache(tenant_id, namespace, body);
    match state.store.create_cache(cache.clone()).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(StoreError::Conflict(_)) => match state.store.get_cache(&cache_key(&cache)).await {
            Ok(existing) if existing == cache => Ok((StatusCode::OK, Json(existing))),
            Ok(_) | Err(StoreError::NotFound(_)) => Err(api_conflict(
                "conflict",
                "cache already exists with a different configuration",
            )),
            Err(err) => Err(api_internal("failed to fetch cache", &err)),
        },
        Err(StoreError::NotFound(_)) => Err(api_not_found("namespace not found")),
        Err(err) => Err(api_internal("failed to create cache", &err)),
    }
}

/// The cache a request creates. Counts below one are raised to one, so a
/// retry compares against what was stored rather than what was sent.
pub(crate) fn new_cache(tenant_id: String, namespace: String, body: CacheCreateRequest) -> Cache {
    Cache {
        tenant_id,
        namespace,
        cache: body.cache,
        display_name: body.display_name,
        shards: body.shards.max(1),
        replication_factor: body.replication_factor.max(1),
        consistency: body.consistency,
    }
}

pub(crate) fn cache_key(cache: &Cache) -> CacheKey {
    CacheKey {
        tenant_id: cache.tenant_id.clone(),
        namespace: cache.namespace.clone(),
        cache: cache.cache.clone(),
    }
}

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches/{cache}",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("cache" = String, Path, description = "Cache identifier")
    ),
    responses(
        (status = 200, description = "Fetch cache", body = Cache),
        (status = 404, description = "Cache or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn get_cache(
    Path((tenant_id, namespace, cache)): Path<(String, String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Cache>, ApiError> {
    require_cache_manage(&state, &tenant_id, &headers, &namespace, &cache).await?;
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = CacheKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        cache,
    };
    match state.store.get_cache(&key).await {
        Ok(cache) => Ok(Json(cache)),
        Err(StoreError::NotFound(_)) => Err(api_not_found("cache not found")),
        Err(err) => Err(api_internal("failed to fetch cache", &err)),
    }
}

#[utoipa::path(
    patch,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches/{cache}",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("cache" = String, Path, description = "Cache identifier")
    ),
    request_body = CachePatchRequest,
    responses(
        (status = 200, description = "Cache updated", body = Cache),
        (status = 404, description = "Cache or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn patch_cache(
    Path((tenant_id, namespace, cache)): Path<(String, String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CachePatchRequest>,
) -> Result<Json<Cache>, ApiError> {
    require_cache_manage(&state, &tenant_id, &headers, &namespace, &cache).await?;
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = CacheKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        cache,
    };
    match state.store.patch_cache(&key, body).await {
        Ok(updated) => Ok(Json(updated)),
        Err(StoreError::NotFound(_)) => Err(api_not_found("cache not found")),
        Err(err) => Err(api_internal("failed to update cache", &err)),
    }
}

#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches/{cache}",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("cache" = String, Path, description = "Cache identifier")
    ),
    responses(
        (status = 204, description = "Cache deleted"),
        (status = 404, description = "Cache or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn delete_cache(
    Path((tenant_id, namespace, cache)): Path<(String, String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    require_cache_manage(&state, &tenant_id, &headers, &namespace, &cache).await?;
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = CacheKey {
        tenant_id,
        namespace,
        cache,
    };
    match state.store.delete_cache(&key).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound(_)) => Err(api_not_found("cache not found")),
        Err(err) => Err(api_internal("failed to delete cache", &err)),
    }
}

pub(crate) fn cache_object(tenant_id: &str, namespace: &str, cache: &str) -> ParsedObject {
    ParsedObject::Cache {
        tenant_id: tenant_id.to_string(),
        namespace: Segment::Exact(namespace.to_string()),
        cache: Segment::Exact(cache.to_string()),
        key: None,
    }
}

/// `cache.manage` over the named cache, from a token minted for this tenant.
async fn require_cache_manage(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
    namespace: &str,
    cache: &str,
) -> Result<(), ApiError> {
    let target = cache_object(tenant_id, namespace, cache);
    require_tenant_action(state, tenant_id, headers, ACTION_CACHE_MANAGE, &target).await
}

#[utoipa::path(
    get,
    path = "/v1/caches/snapshot",
    tag = "caches",
    responses(
        (status = 200, description = "Full cache snapshot", body = CacheSnapshotResponse)
    )
)]
pub(crate) async fn cache_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<CacheSnapshotResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_VIEW).await?;
    let snapshot = state
        .store
        .cache_snapshot()
        .await
        .map_err(|err| api_internal("failed to load cache snapshot", &err))?;
    Ok(Json(CacheSnapshotResponse {
        items: snapshot.items,
        next_seq: snapshot.next_seq,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/caches/changes",
    tag = "caches",
    params(
        ("since" = Option<u64>, Query, description = "Last seen sequence")
    ),
    responses(
        (status = 200, description = "Cache change list", body = CacheChangesResponse)
    )
)]
pub(crate) async fn cache_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<CacheChangesResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_VIEW).await?;
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let changes = state
        .store
        .cache_changes(since)
        .await
        .map_err(|err| api_internal("failed to load cache changes", &err))?;
    Ok(Json(CacheChangesResponse {
        items: changes.items,
        next_seq: changes.next_seq,
    }))
}

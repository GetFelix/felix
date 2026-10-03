//! Stream API handlers.
//!
//! Implements stream CRUD, patching, snapshot, and changefeed endpoints with
//! tenant/namespace validation.
//!
//! Every tenant-scoped endpoint requires `stream.manage` over the stream, from
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
    StreamChangesResponse, StreamCreateRequest, StreamListResponse, StreamSnapshotResponse,
};
use crate::auth::bearer::{require_cluster_action, require_tenant_action, tenant_scopes_for};
use crate::auth::rbac::authorize::{
    ACTION_NODE_VIEW, ACTION_STREAM_MANAGE, ParsedObject, Segment, object_within_scope,
};
use crate::model::{Stream, StreamKey, StreamPatchRequest, StreamRouting, validate_identifier};
use crate::store::StoreError;

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams",
    tag = "streams",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        PageParams
    ),
    responses(
        (status = 200, description = "A page of the streams the caller may manage, in name order", body = StreamListResponse),
        (status = 400, description = "Invalid limit or cursor", body = crate::api::types::ErrorResponse),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn list_streams(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(page): Query<PageParams>,
) -> Result<Json<StreamListResponse>, ApiError> {
    let scopes = tenant_scopes_for(&state, &tenant_id, &headers, ACTION_STREAM_MANAGE).await?;
    let request = page.request()?;
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let listed = list_visible(
        request,
        |page| state.store.list_streams_page(&tenant_id, &namespace, page),
        // Only what the caller could manage.
        |stream: &Stream| {
            let target = stream_object(&tenant_id, &namespace, &stream.stream);
            scopes
                .iter()
                .any(|scope| object_within_scope(scope, &target))
        },
        |stream: &Stream| stream.stream.clone(),
    )
    .await
    .map_err(|err| api_internal("failed to list streams", &err))?;
    Ok(Json(StreamListResponse {
        items: listed.items,
        next_cursor: listed.next_cursor,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams",
    tag = "streams",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier")
    ),
    request_body = StreamCreateRequest,
    responses(
        (status = 201, description = "Stream created", body = Stream),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Stream already exists, or `jump_hash` routing was asked for before the fleet finalized it", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn create_stream(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<StreamCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    require_stream_manage(&state, &tenant_id, &headers, &namespace, &body.stream).await?;
    validate_identifier("stream", &body.stream).map_err(|err| api_validation_error(&err))?;
    body.retention
        .validate()
        .map_err(|err| api_validation_error(&err))?;
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let region = match body.region {
        Some(region) if region.trim().is_empty() => {
            return Err(api_validation_error("region must not be empty when set"));
        }
        region => region,
    };
    let routing = creation_routing(&state, body.routing).await?;
    let stream = Stream {
        tenant_id,
        namespace,
        stream: body.stream,
        kind: body.kind,
        shards: body.shards,
        replication_factor: body.replication_factor,
        retention: body.retention,
        consistency: body.consistency,
        delivery: body.delivery,
        durable: body.durable,
        region,
        routing,
    };
    match state.store.create_stream(stream.clone()).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(StoreError::Conflict(_)) => Err(api_conflict("conflict", "stream already exists")),
        Err(StoreError::NotFound(_)) => Err(api_not_found("namespace not found")),
        Err(err) => Err(api_internal("failed to create stream", &err)),
    }
}

/// The mapping a new stream gets: jump hash once the fleet has finalized
/// `jump_hash_routing`, modulo before.
///
/// Read before the create rather than in the same transaction. A finalize that
/// lands in between only means this stream gets modulo, which every broker
/// serves; the reverse cannot happen because finalizing is one-way and, once
/// done, refuses any broker that could not serve jump hash.
async fn creation_routing(
    state: &AppState,
    asked: Option<StreamRouting>,
) -> Result<StreamRouting, ApiError> {
    if asked == Some(StreamRouting::Modulo) {
        return Ok(StreamRouting::Modulo);
    }
    let feature = felix_common::fleet::JUMP_HASH_ROUTING.name();
    let enabled = state
        .store
        .enabled_fleet_features()
        .await
        .map_err(|ref err| api_internal("read the enabled fleet features", err))?
        .contains(feature);
    match (asked, enabled) {
        (_, true) => Ok(StreamRouting::JumpHash),
        (None, false) => Ok(StreamRouting::Modulo),
        (Some(_), false) => Err(api_conflict(
            "not_finalized",
            &format!("routing jump_hash needs the {feature} fleet feature to be finalized"),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams/{stream}",
    tag = "streams",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("stream" = String, Path, description = "Stream identifier")
    ),
    responses(
        (status = 200, description = "Fetch stream", body = Stream),
        (status = 404, description = "Stream or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn get_stream(
    Path((tenant_id, namespace, stream)): Path<(String, String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Stream>, ApiError> {
    require_stream_manage(&state, &tenant_id, &headers, &namespace, &stream).await?;
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = StreamKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        stream,
    };
    match state.store.get_stream(&key).await {
        Ok(stream) => Ok(Json(stream)),
        Err(StoreError::NotFound(_)) => Err(api_not_found("stream not found")),
        Err(err) => Err(api_internal("failed to fetch stream", &err)),
    }
}

#[utoipa::path(
    patch,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams/{stream}",
    tag = "streams",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("stream" = String, Path, description = "Stream identifier")
    ),
    request_body = StreamPatchRequest,
    responses(
        (status = 200, description = "Stream updated", body = Stream),
        (status = 404, description = "Stream or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn patch_stream(
    Path((tenant_id, namespace, stream)): Path<(String, String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<StreamPatchRequest>,
) -> Result<Json<Stream>, ApiError> {
    require_stream_manage(&state, &tenant_id, &headers, &namespace, &stream).await?;
    if let Some(retention) = &body.retention {
        retention
            .validate()
            .map_err(|err| api_validation_error(&err))?;
    }
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = StreamKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        stream,
    };
    match state.store.patch_stream(&key, body).await {
        Ok(updated) => Ok(Json(updated)),
        Err(StoreError::NotFound(_)) => Err(api_not_found("stream not found")),
        Err(err) => Err(api_internal("failed to update stream", &err)),
    }
}

#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams/{stream}",
    tag = "streams",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("stream" = String, Path, description = "Stream identifier")
    ),
    responses(
        (status = 204, description = "Stream deleted"),
        (status = 404, description = "Stream or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn delete_stream(
    Path((tenant_id, namespace, stream)): Path<(String, String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    require_stream_manage(&state, &tenant_id, &headers, &namespace, &stream).await?;
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = StreamKey {
        tenant_id,
        namespace,
        stream,
    };
    match state.store.delete_stream(&key).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound(_)) => Err(api_not_found("stream not found")),
        Err(err) => Err(api_internal("failed to delete stream", &err)),
    }
}

fn stream_object(tenant_id: &str, namespace: &str, stream: &str) -> ParsedObject {
    ParsedObject::Stream {
        tenant_id: tenant_id.to_string(),
        namespace: Segment::Exact(namespace.to_string()),
        stream: Segment::Exact(stream.to_string()),
    }
}

/// `stream.manage` over the named stream, from a token minted for this tenant.
async fn require_stream_manage(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
    namespace: &str,
    stream: &str,
) -> Result<(), ApiError> {
    let target = stream_object(tenant_id, namespace, stream);
    require_tenant_action(state, tenant_id, headers, ACTION_STREAM_MANAGE, &target).await
}

#[utoipa::path(
    get,
    path = "/v1/streams/snapshot",
    tag = "streams",
    responses(
        (status = 200, description = "Full stream snapshot", body = StreamSnapshotResponse)
    )
)]
pub(crate) async fn stream_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StreamSnapshotResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_VIEW).await?;
    let snapshot = state
        .store
        .stream_snapshot()
        .await
        .map_err(|err| api_internal("failed to load stream snapshot", &err))?;
    Ok(Json(StreamSnapshotResponse {
        items: snapshot.items,
        next_seq: snapshot.next_seq,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/streams/changes",
    tag = "streams",
    params(
        ("since" = Option<u64>, Query, description = "Last seen sequence")
    ),
    responses(
        (status = 200, description = "Stream change list", body = StreamChangesResponse)
    )
)]
pub(crate) async fn stream_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StreamChangesResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_VIEW).await?;
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let changes = state
        .store
        .stream_changes(since)
        .await
        .map_err(|err| api_internal("failed to load stream changes", &err))?;
    Ok(Json(StreamChangesResponse {
        items: changes.items,
        next_seq: changes.next_seq,
    }))
}

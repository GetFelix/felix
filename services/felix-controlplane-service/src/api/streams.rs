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
        (status = 200, description = "Stream already exists with this configuration; nothing changed", body = Stream),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Stream already exists with a different configuration, or `jump_hash` routing was asked for before the fleet finalized it", body = crate::api::types::ErrorResponse),
        (status = 503, description = "`busy`: on Postgres, other writes to the same feed held it for more than 5 s and nothing was written, so retry. `unavailable`: on Raft, no quorum committed the write in time, and it may still land, so do not blindly retry", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn create_stream(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<StreamCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    require_stream_manage(&state, &tenant_id, &headers, &namespace, &body.stream).await?;
    validate_stream_request(&body)?;
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let asked = body.routing;
    let stream = stream_to_create(&state, tenant_id, namespace, body).await?;
    match state.store.create_stream(stream.clone()).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(StoreError::Conflict(_)) => match state.store.get_stream(&stream_key(&stream)).await {
            Ok(existing) if same_stream(&existing, &stream, asked) => {
                Ok((StatusCode::OK, Json(existing)))
            }
            Ok(_) | Err(StoreError::NotFound(_)) => Err(api_conflict(
                "conflict",
                "stream already exists with a different configuration",
            )),
            Err(err) => Err(api_internal("failed to fetch stream", &err)),
        },
        Err(StoreError::NotFound(_)) => Err(api_not_found("namespace not found")),
        Err(err) => Err(api_internal("failed to create stream", &err)),
    }
}

/// The checks a create request must pass before anything is read.
pub(crate) fn validate_stream_request(body: &StreamCreateRequest) -> Result<(), ApiError> {
    validate_identifier("stream", &body.stream).map_err(|err| api_validation_error(&err))?;
    body.retention
        .validate()
        .map_err(|err| api_validation_error(&err))?;
    if body
        .region
        .as_ref()
        .is_some_and(|region| region.trim().is_empty())
    {
        return Err(api_validation_error("region must not be empty when set"));
    }
    Ok(())
}

/// The stream a validated request creates, with its routing resolved.
pub(crate) fn new_stream(
    tenant_id: String,
    namespace: String,
    body: StreamCreateRequest,
    jump_hash_finalized: bool,
    propose_incarnation: bool,
) -> Result<Stream, ApiError> {
    Ok(Stream {
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
        region: body.region,
        routing: creation_routing(body.routing, jump_hash_finalized)?,
        // A proposal: the store gives the incarnation, from its own view of
        // the finalize and the name's tombstone, in the same write. Proposed
        // whether or not this member has seen the finalize, so one that has
        // not still sends the clock rather than 0.
        incarnation: if propose_incarnation {
            crate::model::proposed_incarnation()
        } else {
            0
        },
    })
}

/// Whether creating `wanted` again would leave `existing` as it is, which
/// makes the create a no-op rather than a conflict.
///
/// Routing counts only when the caller asked for one. Left to the server it
/// depends on fleet state, so a retry after a finalize would otherwise
/// conflict with the stream the first attempt made.
pub(crate) fn same_stream(
    existing: &Stream,
    wanted: &Stream,
    routing_asked: Option<StreamRouting>,
) -> bool {
    let routing = match routing_asked {
        Some(_) => wanted.routing,
        None => existing.routing,
    };
    // A retry proposes another incarnation; the stream it would create is
    // the existing one all the same.
    *existing
        == Stream {
            routing,
            incarnation: existing.incarnation,
            ..wanted.clone()
        }
}

pub(crate) fn stream_key(stream: &Stream) -> StreamKey {
    StreamKey {
        tenant_id: stream.tenant_id.clone(),
        namespace: stream.namespace.clone(),
        stream: stream.stream.clone(),
    }
}

/// The stream a create request asks for, routed and with the incarnation it
/// proposes as of now.
pub(crate) async fn stream_to_create(
    state: &AppState,
    tenant_id: String,
    namespace: String,
    body: StreamCreateRequest,
) -> Result<Stream, ApiError> {
    let finalized = jump_hash_finalized(state).await?;
    let incarnations = proposes_incarnation(state).await?;
    new_stream(tenant_id, namespace, body, finalized, incarnations)
}

/// Whether a create proposes the clock as the stream's incarnation: when the
/// store can carry it, or once incarnations are finalized. The second is not
/// implied by the first: a restarted Raft member counts a peer that has not
/// answered yet as level 0, and proposing 0 after the finalize gives the
/// name tombstone + 1, which a restore can repeat.
pub(crate) async fn proposes_incarnation(state: &AppState) -> Result<bool, ApiError> {
    Ok(state.store.takes_incarnation_proposals().await
        || fleet_finalized(state, felix_common::fleet::STREAM_INCARNATIONS).await?)
}

/// Whether the fleet has finalized `jump_hash_routing`.
///
/// Read before the create rather than in the same transaction. A finalize that
/// lands in between only means this stream gets modulo, which every broker
/// serves; the reverse cannot happen because finalizing is one-way and, once
/// done, refuses any broker that could not serve jump hash.
pub(crate) async fn jump_hash_finalized(state: &AppState) -> Result<bool, ApiError> {
    fleet_finalized(state, felix_common::fleet::JUMP_HASH_ROUTING).await
}

async fn fleet_finalized(
    state: &AppState,
    feature: felix_common::fleet::FleetFeature,
) -> Result<bool, ApiError> {
    let feature = feature.name();
    Ok(state
        .store
        .enabled_fleet_features()
        .await
        .map_err(|ref err| api_internal("read the enabled fleet features", err))?
        .contains(feature))
}

/// The mapping a new stream gets: jump hash once the fleet has finalized
/// `jump_hash_routing`, modulo before.
fn creation_routing(
    asked: Option<StreamRouting>,
    finalized: bool,
) -> Result<StreamRouting, ApiError> {
    match (asked, finalized) {
        (Some(StreamRouting::Modulo), _) => Ok(StreamRouting::Modulo),
        (_, true) => Ok(StreamRouting::JumpHash),
        (None, false) => Ok(StreamRouting::Modulo),
        (Some(_), false) => Err(api_conflict(
            "not_finalized",
            &format!(
                "routing jump_hash needs the {} fleet feature to be finalized",
                felix_common::fleet::JUMP_HASH_ROUTING.name()
            ),
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
        (status = 404, description = "Stream or namespace not found", body = crate::api::types::ErrorResponse),
        (status = 503, description = "`busy`: on Postgres, other writes to the same feed held it for more than 5 s and nothing was written, so retry. `unavailable`: on Raft, no quorum committed the write in time, and it may still land, so do not blindly retry", body = crate::api::types::ErrorResponse)
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
        (status = 404, description = "Stream or namespace not found", body = crate::api::types::ErrorResponse),
        (status = 503, description = "`busy`: on Postgres, other writes to the same feed held it for more than 5 s and nothing was written, so retry. `unavailable`: on Raft, no quorum committed the write in time, and it may still land, so do not blindly retry", body = crate::api::types::ErrorResponse)
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

pub(crate) fn stream_object(tenant_id: &str, namespace: &str, stream: &str) -> ParsedObject {
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
    let tombstones = state
        .store
        .stream_tombstones()
        .await
        .map_err(|err| api_internal("failed to load stream tombstones", &err))?;
    Ok(Json(StreamSnapshotResponse {
        items: snapshot.items,
        next_seq: snapshot.next_seq,
        tombstones,
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

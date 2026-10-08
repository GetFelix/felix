//! Creating a namespace's streams and caches in one request.
//!
//! The batch is all or nothing: every item is authorized and validated before
//! anything is read, and the new ones are written by one store call, which is
//! one transaction in Postgres and one log entry under Raft. An item that
//! already exists with the same configuration is reported `unchanged`, so a
//! caller that died part-way can send the same batch again.
use std::collections::HashSet;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};

use crate::api::AppState;
use crate::api::caches::{cache_key, cache_object, new_cache};
use crate::api::ensure_tenant_namespace;
use crate::api::error::{
    ApiError, api_conflict, api_error, api_internal, api_not_found, api_validation_error,
};
use crate::api::streams::{
    jump_hash_finalized, new_stream, same_stream, stream_key, stream_object,
    validate_stream_request,
};
use crate::api::types::{
    BatchCache, BatchItemStatus, BatchStream, ResourceBatchRequest, ResourceBatchResponse,
};
use crate::auth::bearer::{Refusal, refused, tenant_scopes_for};
use crate::auth::rbac::authorize::{
    ACTION_CACHE_MANAGE, ACTION_STREAM_MANAGE, ParsedObject, object_within_scope,
};
use crate::model::{Cache, Stream, StreamRouting, validate_identifier};
use crate::store::StoreError;

/// Keeps one request from holding a long transaction or a huge log entry.
pub(crate) const MAX_BATCH_ITEMS: usize = 256;

/// A create can lose a race to a concurrent one for the same name, in which
/// case nothing was written and the batch is looked at again.
const ATTEMPTS: usize = 3;

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/resources",
    tag = "resources",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier")
    ),
    request_body = ResourceBatchRequest,
    responses(
        (status = 201, description = "At least one item was created; the rest already existed as asked", body = ResourceBatchResponse),
        (status = 200, description = "Every item already existed as asked; nothing changed", body = ResourceBatchResponse),
        (status = 400, description = "Empty, too large, an invalid item, or a name given twice; nothing created", body = crate::api::types::ErrorResponse),
        (status = 403, description = "Missing `stream.manage` or `cache.manage` over an item; nothing created", body = crate::api::types::ErrorResponse),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse),
        (status = 409, description = "An item exists with a different configuration, or `jump_hash` routing was asked for before the fleet finalized it; nothing created", body = crate::api::types::ErrorResponse),
        (status = 503, description = "A control-plane member has not been upgraded to a release with batch creates", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn create_resources(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ResourceBatchRequest>,
) -> Result<(StatusCode, Json<ResourceBatchResponse>), ApiError> {
    let total = body.streams.len() + body.caches.len();
    if total == 0 {
        return Err(api_validation_error(
            "a batch needs at least one stream or cache",
        ));
    }
    if total > MAX_BATCH_ITEMS {
        return Err(api_validation_error(&format!(
            "a batch holds at most {MAX_BATCH_ITEMS} items, got {total}"
        )));
    }
    authorize(&state, &tenant_id, &namespace, &headers, &body).await?;

    let mut names = HashSet::new();
    for item in &body.streams {
        validate_stream_request(item)?;
        if !names.insert(item.stream.as_str()) {
            return Err(api_validation_error(&format!(
                "stream {} is in the batch twice",
                item.stream
            )));
        }
    }
    let mut names = HashSet::new();
    for item in &body.caches {
        validate_identifier("cache", &item.cache).map_err(|err| api_validation_error(&err))?;
        if !names.insert(item.cache.as_str()) {
            return Err(api_validation_error(&format!(
                "cache {} is in the batch twice",
                item.cache
            )));
        }
    }
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;

    let finalized = if body.streams.is_empty() {
        false
    } else {
        jump_hash_finalized(&state).await?
    };
    let streams = body
        .streams
        .into_iter()
        .map(|item| {
            let asked = item.routing;
            new_stream(tenant_id.clone(), namespace.clone(), item, finalized)
                .map(|stream| (stream, asked))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let caches: Vec<Cache> = body
        .caches
        .into_iter()
        .map(|item| new_cache(tenant_id.clone(), namespace.clone(), item))
        .collect();

    for _ in 0..ATTEMPTS {
        let plan = plan(&state, &streams, &caches).await?;
        if plan.new_streams.is_empty() && plan.new_caches.is_empty() {
            return Ok((StatusCode::OK, Json(plan.response)));
        }
        if !state.store.create_resources_ready().await {
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "upgrade_in_progress",
                "batch creates need every control-plane member on a release that has them",
            ));
        }
        match state
            .store
            .create_resources(plan.new_streams, plan.new_caches)
            .await
        {
            Ok(()) => return Ok((StatusCode::CREATED, Json(plan.response))),
            // Something in the batch was created after it was looked at.
            // Nothing was written; look again.
            Err(StoreError::Conflict(_)) => continue,
            Err(StoreError::NotFound(_)) => return Err(api_not_found("namespace not found")),
            Err(err) => return Err(api_internal("failed to create resources", &err)),
        }
    }
    Err(api_conflict(
        "conflict",
        "items in the batch kept being created concurrently; nothing was created",
    ))
}

/// `stream.manage` and `cache.manage` over every item, checked before
/// anything is read so a refusal says nothing about what exists.
async fn authorize(
    state: &AppState,
    tenant_id: &str,
    namespace: &str,
    headers: &HeaderMap,
    body: &ResourceBatchRequest,
) -> Result<(), ApiError> {
    let streams: Vec<(&str, ParsedObject)> = body
        .streams
        .iter()
        .map(|item| {
            let name = item.stream.as_str();
            (name, stream_object(tenant_id, namespace, name))
        })
        .collect();
    let caches: Vec<(&str, ParsedObject)> = body
        .caches
        .iter()
        .map(|item| {
            let name = item.cache.as_str();
            (name, cache_object(tenant_id, namespace, name))
        })
        .collect();
    for (kind, action, targets) in [
        ("stream", ACTION_STREAM_MANAGE, streams),
        ("cache", ACTION_CACHE_MANAGE, caches),
    ] {
        if targets.is_empty() {
            continue;
        }
        let scopes = tenant_scopes_for(state, tenant_id, headers, action).await?;
        for (name, target) in &targets {
            if !scopes
                .iter()
                .any(|scope| object_within_scope(scope, target))
            {
                return Err(refused(
                    Refusal::Forbidden,
                    &format!("insufficient scope for {kind} {name}"),
                ));
            }
        }
    }
    Ok(())
}

/// What the batch would do given what exists now.
struct Plan {
    response: ResourceBatchResponse,
    new_streams: Vec<Stream>,
    new_caches: Vec<Cache>,
}

async fn plan(
    state: &AppState,
    streams: &[(Stream, Option<StreamRouting>)],
    caches: &[Cache],
) -> Result<Plan, ApiError> {
    let mut plan = Plan {
        response: ResourceBatchResponse {
            streams: Vec::with_capacity(streams.len()),
            caches: Vec::with_capacity(caches.len()),
        },
        new_streams: Vec::new(),
        new_caches: Vec::new(),
    };
    let mut different = Vec::new();
    for (stream, asked) in streams {
        match state.store.get_stream(&stream_key(stream)).await {
            Ok(existing) if same_stream(&existing, stream, *asked) => {
                plan.response.streams.push(BatchStream {
                    status: BatchItemStatus::Unchanged,
                    stream: existing,
                });
            }
            Ok(_) => different.push(format!("stream {}", stream.stream)),
            Err(StoreError::NotFound(_)) => {
                plan.new_streams.push(stream.clone());
                plan.response.streams.push(BatchStream {
                    status: BatchItemStatus::Created,
                    stream: stream.clone(),
                });
            }
            Err(err) => return Err(api_internal("failed to fetch stream", &err)),
        }
    }
    for cache in caches {
        match state.store.get_cache(&cache_key(cache)).await {
            Ok(existing) if existing == *cache => {
                plan.response.caches.push(BatchCache {
                    status: BatchItemStatus::Unchanged,
                    cache: existing,
                });
            }
            Ok(_) => different.push(format!("cache {}", cache.cache)),
            Err(StoreError::NotFound(_)) => {
                plan.new_caches.push(cache.clone());
                plan.response.caches.push(BatchCache {
                    status: BatchItemStatus::Created,
                    cache: cache.clone(),
                });
            }
            Err(err) => return Err(api_internal("failed to fetch cache", &err)),
        }
    }
    if !different.is_empty() {
        return Err(api_conflict(
            "conflict",
            &format!(
                "already exist with a different configuration: {}; nothing was created",
                different.join(", ")
            ),
        ));
    }
    Ok(plan)
}

#[cfg(test)]
mod tests;

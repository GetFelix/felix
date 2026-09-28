//! Which broker leads each shard, as operators and brokers read it.
use std::collections::HashMap;
use std::time::Duration;

use tokio::time::Instant;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::HeaderMap;

use crate::api::AppState;
use crate::api::error::{ApiError, api_internal};
use crate::api::nodes::require_cluster_node_view;
use crate::api::pagination::{PageParams, list_visible};
use crate::api::types::{
    RoutedShardAssignment, RoutedShardAssignmentChange, ShardAssignmentChangesResponse,
    ShardAssignmentListResponse, ShardAssignmentSnapshotResponse,
};
use crate::model::{ShardAssignment, ShardAssignmentChange, ShardKey, StreamRouting};

/// The longest a changes request waits. Under the usual 30 s idle timeout of
/// proxies and HTTP clients, so a wait ends in an answer rather than a cut
/// connection.
pub(crate) const MAX_CHANGES_WAIT_MS: u64 = 25_000;

/// How often a waiting changes request re-reads the store.
const CHANGES_RECHECK: Duration = Duration::from_millis(50);

#[utoipa::path(
    get,
    path = "/v1/shard-assignments",
    tag = "nodes",
    params(
        ("leader" = Option<String>, Query, description = "Only shards this node leads"),
        PageParams
    ),
    responses(
        (status = 200, description = "A page of shard assignments, by tenant, namespace, kind, name and shard", body = ShardAssignmentListResponse),
        (status = 400, description = "Invalid limit or cursor", body = crate::api::types::ErrorResponse)
    )
)]
/// List shard ownership.
///
/// Answers the two questions an operator has when placement looks wrong: who
/// owns this shard, and what does this broker own. Requires the same
/// `node.view:cluster:*` as the node listing, because ownership and membership
/// are the same view of the cluster.
///
/// # Errors
/// - 400 for an invalid `limit` or `cursor`.
/// - 500 when the store cannot be read.
pub(crate) async fn list_shard_assignments(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    Query(page): Query<PageParams>,
) -> Result<Json<ShardAssignmentListResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;

    let leader = query.get("leader").map(String::as_str);
    let listed = list_visible(
        page.request()?,
        |page| state.store.list_shard_assignments_page(leader, page),
        |_| true,
        |assignment: &ShardAssignment| assignment.key.clone(),
    )
    .await
    .map_err(|ref err| api_internal("failed to list shard assignments", err))?;

    Ok(Json(ShardAssignmentListResponse {
        items: with_routing(&state, listed.items).await?,
        next_cursor: listed.next_cursor,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/shard-assignments/snapshot",
    tag = "nodes",
    responses((status = 200, description = "Full assignment snapshot", body = ShardAssignmentSnapshotResponse))
)]
/// Every current assignment, plus where to start polling.
///
/// A broker applies this and then polls `changes` from `next_seq`. The two
/// together describe every committed change exactly once — the snapshot is read
/// at a consistent point and `next_seq` is the log position at that same point.
///
/// # Errors
/// - 500 when the store cannot be read.
pub(crate) async fn shard_assignment_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ShardAssignmentSnapshotResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let snapshot = state
        .store
        .shard_assignment_snapshot()
        .await
        .map_err(|ref err| api_internal("failed to snapshot shard assignments", err))?;
    Ok(Json(ShardAssignmentSnapshotResponse {
        items: with_routing(&state, snapshot.items).await?,
        next_seq: snapshot.next_seq,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/shard-assignments/changes",
    tag = "nodes",
    params(
        ("since" = Option<u64>, Query, description = "Last seen sequence"),
        ("wait_ms" = Option<u64>, Query, description = "When nothing is newer than `since`, wait up to this many milliseconds (at most 25000) and answer as soon as a change lands")
    ),
    responses((status = 200, description = "Assignment changes", body = ShardAssignmentChangesResponse))
)]
/// Assignment changes at or after `since`.
///
/// A caller that finds the first returned `seq` above its `since`, or an empty
/// page with `next_seq` above it, has fallen outside the retention window and
/// must re-snapshot. `next_seq` below `since` means the sequence was reset —
/// a control plane restarted onto a store that does not persist it.
///
/// # Errors
/// - 500 when the store cannot be read.
pub(crate) async fn shard_assignment_changes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<ShardAssignmentChangesResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let since = query
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let deadline = query
        .get("wait_ms")
        .and_then(|value| value.parse::<u64>().ok())
        .map(|ms| Instant::now() + Duration::from_millis(ms.min(MAX_CHANGES_WAIT_MS)));
    // Subscribed before the first read, so a write landing between that read
    // and the wait still wakes it.
    let mut written = state.placement_wakes.watch_assignments();
    loop {
        let changes = state
            .store
            .shard_assignment_changes(since)
            .await
            .map_err(|ref err| api_internal("failed to load shard assignment changes", err))?;
        let nothing_new = changes.items.is_empty() && changes.next_seq == since;
        let now = Instant::now();
        match deadline {
            Some(deadline) if nothing_new && now < deadline => {
                // Nothing is held while waiting: each re-check takes a store
                // connection only for its own read. The re-check is how a
                // write by another instance is seen; this instance's own
                // writes wake the wait directly.
                tokio::select! {
                    _ = written.changed() => {}
                    _ = tokio::time::sleep(CHANGES_RECHECK.min(deadline - now)) => {}
                    _ = state.placement_wakes.closing().cancelled() => {
                        return Ok(Json(ShardAssignmentChangesResponse {
                            items: changes_with_routing(&state, changes.items).await?,
                            next_seq: changes.next_seq,
                        }));
                    }
                }
            }
            _ => {
                return Ok(Json(ShardAssignmentChangesResponse {
                    items: changes_with_routing(&state, changes.items).await?,
                    next_seq: changes.next_seq,
                }));
            }
        }
    }
}

/// The non-modulo routing of every stream `keys` name.
///
/// Read from the stream, which is where a stream's routing is fixed at
/// creation; it never changes afterwards, so an answer read after the
/// assignment cannot disagree with it. Stamped here rather than stored on the
/// assignment so no placement write can lose it.
async fn routings<'a>(
    state: &AppState,
    keys: impl Iterator<Item = &'a ShardKey>,
) -> Result<HashMap<crate::model::StreamKey, StreamRouting>, ApiError> {
    let mut streams: Vec<_> = keys.filter_map(ShardKey::stream_key).collect();
    streams.sort_by(|a, b| {
        (&a.tenant_id, &a.namespace, &a.stream).cmp(&(&b.tenant_id, &b.namespace, &b.stream))
    });
    streams.dedup();
    state
        .store
        .stream_routings(&streams)
        .await
        .map_err(|ref err| api_internal("failed to read stream routing", err))
}

fn routed(
    routings: &HashMap<crate::model::StreamKey, StreamRouting>,
    assignment: ShardAssignment,
) -> RoutedShardAssignment {
    let routing = assignment
        .key
        .stream_key()
        .and_then(|stream| routings.get(&stream).copied())
        .unwrap_or_default();
    RoutedShardAssignment {
        assignment,
        routing,
    }
}

async fn with_routing(
    state: &AppState,
    items: Vec<ShardAssignment>,
) -> Result<Vec<RoutedShardAssignment>, ApiError> {
    let routings = routings(state, items.iter().map(|item| &item.key)).await?;
    Ok(items
        .into_iter()
        .map(|item| routed(&routings, item))
        .collect())
}

async fn changes_with_routing(
    state: &AppState,
    items: Vec<ShardAssignmentChange>,
) -> Result<Vec<RoutedShardAssignmentChange>, ApiError> {
    let routings = routings(state, items.iter().map(|item| &item.key)).await?;
    Ok(items
        .into_iter()
        .map(|change| RoutedShardAssignmentChange {
            seq: change.seq,
            op: change.op,
            assignment: change.assignment.map(|item| routed(&routings, item)),
            key: change.key,
        })
        .collect())
}

#[cfg(test)]
mod tests;

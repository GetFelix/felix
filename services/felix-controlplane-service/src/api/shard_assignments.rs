//! Which broker leads each shard, as operators and brokers read it.
use felix_common::random::{HashMap, HashSet};
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
    PreviousSet, RaftTargetVersion, RaftTargetView, RoutedShardAssignment,
    RoutedShardAssignmentChange, ShardAssignmentChangesResponse, ShardAssignmentListResponse,
    ShardAssignmentSnapshotResponse, SnapshotShardAssignment,
};
use crate::model::{ShardAssignment, ShardAssignmentChange, ShardKey, StreamRouting};

/// The longest a changes request waits. Under the usual 30 s idle timeout of
/// proxies and HTTP clients, so a wait ends in an answer rather than a cut
/// connection.
pub(crate) const MAX_CHANGES_WAIT_MS: u64 = 25_000;

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
    let before = incarnation_before(&state).await?;
    let listed = list_visible(
        page.request()?,
        |page| state.store.list_shard_assignments_page(leader, page),
        |_| true,
        |assignment: &ShardAssignment| assignment.key.clone(),
    )
    .await
    .map_err(|ref err| api_internal("failed to list shard assignments", err))?;

    Ok(Json(ShardAssignmentListResponse {
        items: with_routing(&state, listed.items, Some(before)).await?,
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
    let before = incarnation_before(&state).await?;
    let snapshot = state
        .store
        .shard_assignment_snapshot()
        .await
        .map_err(|ref err| api_internal("failed to snapshot shard assignments", err))?;
    // Read after the snapshot: a first assignment stays first, so one read
    // later marks no item wrongly, and one evicted in between is only left
    // unmarked.
    let first: HashSet<(ShardKey, u64, u64)> = state
        .store
        .first_shard_assignments()
        .await
        .map_err(|ref err| api_internal("failed to read first shard assignments", err))?
        .into_iter()
        .collect();
    // Read after the snapshot too: an assignment written in between makes
    // the one found here its own, never one right before an older item.
    let previous: HashMap<ShardKey, ShardAssignment> = state
        .store
        .previous_shard_assignments()
        .await
        .map_err(|ref err| api_internal("failed to read previous shard assignments", err))?
        .into_iter()
        .map(|before| (before.key.clone(), before))
        .collect();
    let items = with_routing(&state, snapshot.items, Some(before))
        .await?
        .into_iter()
        .map(|assigned| {
            let item = &assigned.assignment;
            let first_assignment =
                first.contains(&(item.key.clone(), item.incarnation, item.generation));
            let previous = previous
                .get(&item.key)
                .and_then(|before| PreviousSet::of(before, item));
            SnapshotShardAssignment {
                assigned,
                first_assignment,
                previous,
            }
        })
        .collect();
    Ok(Json(ShardAssignmentSnapshotResponse {
        items,
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
/// must re-snapshot. `next_seq` below `since` means the answer is behind the
/// caller: a Raft member that has not applied as far as one the caller asked
/// before, or a sequence that was reset by a control plane restarting onto a
/// store that does not persist it. Only the reset calls for a re-snapshot,
/// and only time tells the two apart.
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
    let mut written = state.store.watch_shard_assignment_changes().await;
    loop {
        let before = incarnation_before(&state).await?;
        let changes = state
            .store
            .shard_assignment_changes(since)
            .await
            .map_err(|ref err| api_internal("failed to load shard assignment changes", err))?;
        let nothing_new = changes.items.is_empty() && changes.next_seq == since;
        let now = Instant::now();
        match deadline {
            Some(deadline) if nothing_new && now < deadline => {
                // Nothing is held while waiting. Every write to the store,
                // a Raft apply included, wakes the wait.
                tokio::select! {
                    woken = written.changed() => {
                        if woken.is_err() {
                            // Nothing can wake it any more.
                            tokio::time::sleep_until(deadline).await;
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {}
                    _ = state.placement_wakes.closing().cancelled() => {
                        return Ok(Json(ShardAssignmentChangesResponse {
                            items: changes_with_routing(&state, changes.items, before).await?,
                            next_seq: changes.next_seq,
                        }));
                    }
                }
            }
            _ => {
                return Ok(Json(ShardAssignmentChangesResponse {
                    items: changes_with_routing(&state, changes.items, before).await?,
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

/// The placement incarnation, read before the records a response carries.
async fn incarnation_before(state: &AppState) -> Result<u64, ApiError> {
    state
        .store
        .placement_incarnation()
        .await
        .map_err(|ref err| api_internal("failed to read the placement incarnation", err))
}

/// The incarnation a response's Raft targets go out under, for records read
/// after `before`. `None` while some registered broker has not acknowledged
/// it (`docs/per-shard-raft.md`, "Where a target comes from"), or when a
/// restore landed while the records were read: they may be from either side
/// of it, and a target from before it would go out as newer than it is.
async fn target_incarnation(state: &AppState, before: u64) -> Result<Option<u64>, ApiError> {
    let acked = state
        .store
        .placement_acked_by_all()
        .await
        .map_err(|ref err| api_internal("failed to read the placement acknowledgements", err))?;
    let after = incarnation_before(state).await?;
    Ok((acked && after == before).then_some(after))
}

/// `incarnation`: see [`target_incarnation`]; `None` leaves every target out.
fn routed(
    routings: &HashMap<crate::model::StreamKey, StreamRouting>,
    mut assignment: ShardAssignment,
    incarnation: Option<u64>,
) -> RoutedShardAssignment {
    // Brokers read copy ids from the target's members, so the record's
    // own are not sent: they would go out before the gate opens.
    assignment.raft_copy_ids.clear();
    // `placed` stays: a broker reads the group epoch from it, and
    // `raft_formed` too: a broker stops re-sending a marked group's floor.
    // No broker reads the floor, the ordered targets or the reported
    // leader yet.
    assignment.raft_floor = None;
    assignment.raft_ordered = None;
    assignment.raft_leader = None;
    let target = assignment
        .raft_target
        .take()
        .zip(incarnation)
        .map(|(target, incarnation)| RaftTargetView {
            version: RaftTargetVersion {
                incarnation,
                counter: target.counter,
            },
            members: target.members,
        });
    let routing = assignment
        .key
        .stream_key()
        .and_then(|stream| routings.get(&stream).copied())
        .unwrap_or_default();
    RoutedShardAssignment {
        assignment,
        routing,
        target,
        // Said outright: a broker reads an absent target as "none" and
        // would hand leadership away by placement during a restore.
        targets_withheld: incarnation.is_none(),
    }
}

/// `before`: the placement incarnation read before `items` were, or `None`
/// to leave every Raft target out, as for a confirmed read: it comes from
/// the leader, and this member's incarnation may be behind it.
async fn with_routing(
    state: &AppState,
    items: Vec<ShardAssignment>,
    before: Option<u64>,
) -> Result<Vec<RoutedShardAssignment>, ApiError> {
    let routings = routings(state, items.iter().map(|item| &item.key)).await?;
    let incarnation = match before {
        Some(before) => target_incarnation(state, before).await?,
        None => None,
    };
    Ok(items
        .into_iter()
        .map(|item| routed(&routings, item, incarnation))
        .collect())
}

async fn changes_with_routing(
    state: &AppState,
    items: Vec<ShardAssignmentChange>,
    before: u64,
) -> Result<Vec<RoutedShardAssignmentChange>, ApiError> {
    let routings = routings(state, items.iter().map(|item| &item.key)).await?;
    let incarnation = target_incarnation(state, before).await?;
    Ok(items
        .into_iter()
        .map(|change| RoutedShardAssignmentChange {
            seq: change.seq,
            op: change.op,
            assignment: change
                .assignment
                .map(|item| routed(&routings, item, incarnation)),
            key: change.key,
        })
        .collect())
}

#[cfg(test)]
mod tests;

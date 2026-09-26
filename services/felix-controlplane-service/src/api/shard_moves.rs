//! Operator controls over shard moves: list them, start one, cancel one, see
//! what placement would do next, and pause placement's own moves. Also the
//! one control that loses data: abandoning a shard's unreachable log.
//!
//! Reads take `node.view:cluster:*`, like the assignment listing. Everything
//! that changes a move takes `node.manage:cluster:*`, the permission that
//! drains a node, which is the other way to make a shard move.
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};

use crate::api::AppState;
use crate::api::error::{ApiError, api_conflict, api_error, api_internal};
use crate::api::nodes::require_cluster_node_view;
use crate::api::types::{
    PlacementPlanResponse, PlacementStatusResponse, PlannedShard, ShardMove, ShardMoveListResponse,
    ShardMoveRequest, ShardMoveResponse, ShardMoveStep,
};
use crate::auth::bearer::require_cluster_action;
use crate::auth::rbac::authorize::ACTION_NODE_MANAGE;
use crate::cluster::placement::{
    CaughtUp, Decision, OperatorError, PlacementRead, Refused, abandon_log, cancel_move,
    run_operator, start_move,
};
use crate::model::{ShardKey, ShardKind, ShardState};

#[utoipa::path(
    get,
    path = "/v1/shard-moves",
    tag = "placement",
    responses((status = 200, description = "Moves in progress", body = ShardMoveListResponse))
)]
/// Moves and follower replacements in progress, with how far each has got.
///
/// # Errors
/// - 500 when the store cannot be read.
pub(crate) async fn list_shard_moves(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ShardMoveListResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let read = PlacementRead::load(state.store.as_ref(), &state.node_liveness)
        .await
        .map_err(|ref err| api_internal("read placement state", err))?;
    let items = read
        .existing
        .iter()
        .filter_map(|assignment| shard_move(assignment, &read.positions))
        .collect();
    Ok(Json(ShardMoveListResponse {
        paused: read.paused,
        items,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/shard-moves",
    tag = "placement",
    request_body = ShardMoveRequest,
    responses(
        (status = 200, description = "Move started", body = ShardMoveResponse),
        (status = 404, description = "No such shard or node", body = crate::api::types::ErrorResponse),
        (status = 409, description = "The move cannot start", body = crate::api::types::ErrorResponse)
    )
)]
/// Start moving a shard's leadership to `destination`.
///
/// The move then runs like any other: staged, fenced once the destination is
/// close, cut over once the leader has drained. Held to the move limits, but
/// not to a pause.
///
/// # Errors
/// - 404 when the shard has no assignment or the destination is not
///   registered.
/// - 409 when the destination is not live, already leads the shard or is at
///   capacity, the shard is already moving, its leader is down, or a move
///   limit is reached.
pub(crate) async fn start_shard_move(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ShardMoveRequest>,
) -> Result<Json<ShardMoveResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;
    let ShardMoveRequest { key, destination } = request;
    run(&state, |catalog| start_move(catalog, &key, &destination)).await
}

/// Which kind of shard a path names; a stream's unless it says otherwise.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct KindQuery {
    #[serde(default)]
    kind: ShardKind,
}

#[utoipa::path(
    delete,
    path = "/v1/shard-moves/{tenant_id}/{namespace}/{name}/{shard}",
    tag = "placement",
    params(
        ("tenant_id" = String, Path, description = "Tenant"),
        ("namespace" = String, Path, description = "Namespace"),
        ("name" = String, Path, description = "Stream or cache name"),
        ("shard" = u32, Path, description = "Shard number"),
        ("kind" = Option<ShardKind>, Query, description = "`stream` (default) or `cache`")
    ),
    responses(
        (status = 200, description = "Move cancelled", body = ShardMoveResponse),
        (status = 404, description = "No such shard", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Nothing to cancel", body = crate::api::types::ErrorResponse)
    )
)]
/// Cancel a shard's move or follower replacement, whoever started it.
///
/// Before the fence the destination is dropped. After it the leader that
/// stopped serves again at a new generation: it holds every write it
/// accepted, and its clients find it again as they would a new owner. A move
/// that has cut over is finished, and cancelling it is a 409; move the shard
/// back instead.
///
/// Placement may choose the same move again on its next pass. Pause it first
/// to keep a shard where it is.
///
/// # Errors
/// - 404 when the shard has no assignment.
/// - 409 when no move is in progress, or the leader of a fenced move is down.
pub(crate) async fn cancel_shard_move(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((tenant_id, namespace, name, shard)): Path<(String, String, String, u32)>,
    Query(query): Query<KindQuery>,
) -> Result<Json<ShardMoveResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;
    let key = ShardKey {
        tenant_id,
        namespace,
        stream: name,
        shard,
        kind: query.kind,
    };
    run(&state, |catalog| cancel_move(catalog, &key)).await
}

#[utoipa::path(
    post,
    path = "/v1/placement/abandon/{tenant_id}/{namespace}/{name}/{shard}",
    tag = "placement",
    params(
        ("tenant_id" = String, Path, description = "Tenant"),
        ("namespace" = String, Path, description = "Namespace"),
        ("name" = String, Path, description = "Stream or cache name"),
        ("shard" = u32, Path, description = "Shard number"),
        ("kind" = Option<ShardKind>, Query, description = "`stream` (default) or `cache`")
    ),
    responses(
        (status = 200, description = "Log abandoned and the shard placed afresh", body = ShardMoveResponse),
        (status = 404, description = "No such shard", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Nothing to abandon, or no node can take the shard", body = crate::api::types::ErrorResponse)
    )
)]
/// Give up a shard's log and place the shard afresh. **This loses data.**
///
/// For a durable shard that placement is holding unplaced because the only copies
/// of its log are out of reach: its leader is not serving and no replica
/// holds everything it may have acknowledged. That includes every durable
/// shard with a replication factor of 1 whose broker is down. The shard
/// otherwise waits for that broker to return. The new leader is chosen as
/// for a new shard and starts from whatever it holds, usually nothing, at a
/// new generation; records only the old leader held are gone, acknowledged
/// ones included.
///
/// # Errors
/// - 404 when the shard has no assignment.
/// - 409 `not_stranded` when the leader is serving or a replica can take
///   over without loss, and `unplaceable` when no node can take the shard.
pub(crate) async fn abandon_shard_log(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((tenant_id, namespace, name, shard)): Path<(String, String, String, u32)>,
    Query(query): Query<KindQuery>,
) -> Result<Json<ShardMoveResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_MANAGE).await?;
    let key = ShardKey {
        tenant_id,
        namespace,
        stream: name,
        shard,
        kind: query.kind,
    };
    let response = run(&state, |catalog| abandon_log(catalog, &key)).await?;
    tracing::warn!(
        kind = %key.kind,
        name = %key.stream,
        shard = key.shard,
        leader = %response.assignment.leader,
        generation = response.assignment.generation,
        "an operator abandoned a shard's log; records only its previous leader held are lost",
    );
    Ok(response)
}

#[utoipa::path(
    get,
    path = "/v1/placement/plan",
    tag = "placement",
    responses((status = 200, description = "What the next pass would do", body = PlacementPlanResponse))
)]
/// What placement would do on its next pass, without doing it.
///
/// # Errors
/// - 500 when the store cannot be read.
pub(crate) async fn placement_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<PlacementPlanResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let read = PlacementRead::load(state.store.as_ref(), &state.node_liveness)
        .await
        .map_err(|ref err| api_internal("read placement state", err))?;
    let plan = read.plan(state.move_policy.clone());
    let items = plan
        .shards
        .into_iter()
        .filter_map(|shard| {
            let (action, assignment, reason) = match shard.decision {
                Decision::Kept => return None,
                Decision::Place(leader, replicas) => (
                    "place".to_string(),
                    Some(crate::cluster::placement::assignment_for(
                        &shard.key, &leader, replicas,
                    )),
                    None,
                ),
                Decision::Move(step, assignment) => {
                    (step.label().to_string(), Some(assignment), None)
                }
                Decision::Waiting(blocked) => {
                    ("waiting".to_string(), None, Some(blocked.to_string()))
                }
                Decision::Unplaceable(why) => {
                    ("unplaceable".to_string(), None, Some(why.to_string()))
                }
            };
            Some(PlannedShard {
                key: shard.key,
                action,
                assignment,
                reason,
            })
        })
        .collect();
    Ok(Json(PlacementPlanResponse {
        paused: read.paused,
        items,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/placement/pause",
    tag = "placement",
    responses((status = 200, description = "Placement's moves are paused", body = PlacementStatusResponse))
)]
/// Stop placement starting moves of its own, on every instance.
///
/// Moves already in flight finish: a fenced leader has stopped serving, and
/// holding it there would keep its shard unavailable. Cancel one to stop it.
/// An operator may still start moves. New shards are still placed and a
/// failed leader is still replaced; those are not moves.
///
/// # Errors
/// - 500 when the store cannot be written.
pub(crate) async fn pause_placement(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<PlacementStatusResponse>, ApiError> {
    set_paused(&state, &headers, true).await
}

#[utoipa::path(
    post,
    path = "/v1/placement/resume",
    tag = "placement",
    responses((status = 200, description = "Placement's moves are running", body = PlacementStatusResponse))
)]
/// Let placement start moves again.
///
/// # Errors
/// - 500 when the store cannot be written.
pub(crate) async fn resume_placement(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<PlacementStatusResponse>, ApiError> {
    set_paused(&state, &headers, false).await
}

async fn set_paused(
    state: &AppState,
    headers: &HeaderMap,
    paused: bool,
) -> Result<Json<PlacementStatusResponse>, ApiError> {
    require_cluster_action(state, headers, ACTION_NODE_MANAGE).await?;
    state
        .store
        .set_moves_paused(paused)
        .await
        .map_err(|ref err| api_internal("set whether placement is paused", err))?;
    tracing::info!(paused, "placement's moves paused or resumed by an operator");
    if !paused {
        state.placement_wakes.request_pass();
    }
    Ok(Json(PlacementStatusResponse { paused }))
}

async fn run(
    state: &AppState,
    decide: impl Fn(
        &crate::cluster::placement::Catalog<'_>,
    ) -> Result<crate::cluster::placement::OperatorStep, Refused>,
) -> Result<Json<ShardMoveResponse>, ApiError> {
    match run_operator(
        state.store.as_ref(),
        &state.node_liveness,
        state.move_policy.clone(),
        &state.placement_wakes,
        decide,
    )
    .await
    {
        Ok((step, assignment)) => Ok(Json(ShardMoveResponse {
            step: step.label().to_string(),
            assignment,
        })),
        Err(OperatorError::Refused(refused)) => Err(match refused {
            Refused::UnknownShard | Refused::UnknownNode(_) => {
                api_error(StatusCode::NOT_FOUND, refused.code(), &refused.to_string())
            }
            other => api_conflict(other.code(), &other.to_string()),
        }),
        Err(OperatorError::Store(err)) => Err(api_internal("change a shard move", &err)),
        Err(OperatorError::Contended) => Err(api_conflict(
            "contended",
            "the shard kept changing while this was decided; try again",
        )),
    }
}

/// The move in progress on `assignment`, if there is one.
fn shard_move(
    assignment: &crate::model::ShardAssignment,
    positions: &dyn CaughtUp,
) -> Option<ShardMove> {
    let (step, destination, replacing) = if assignment.state == ShardState::Draining {
        (ShardMoveStep::Fenced, assignment.successor.clone(), None)
    } else if let Some(successor) = &assignment.successor {
        (ShardMoveStep::Staged, Some(successor.clone()), None)
    } else if let Some(joining) = &assignment.joining {
        let replacing = assignment
            .replicas
            .iter()
            .find(|replica| *replica != joining)
            .cloned();
        (ShardMoveStep::Replacing, Some(joining.clone()), replacing)
    } else {
        return None;
    };
    let key = &assignment.key;
    // Only a report at this generation describes this replica set.
    let current = positions.reported_generation(key) == Some(assignment.generation);
    let lag_records = destination
        .as_deref()
        .filter(|_| current)
        .and_then(|node| positions.lag_records(key, node));
    let caught_up = current
        && destination
            .as_deref()
            .is_some_and(|node| positions.is_caught_up(key, node));
    Some(ShardMove {
        key: key.clone(),
        leader: assignment.leader.clone(),
        destination,
        replacing,
        step,
        reason: assignment.move_reason,
        started_at_millis: assignment.move_started_at_millis,
        generation: assignment.generation,
        lag_records,
        caught_up,
        drained: step == ShardMoveStep::Fenced && positions.is_drained(key, assignment.generation),
    })
}

#[cfg(test)]
mod tests;

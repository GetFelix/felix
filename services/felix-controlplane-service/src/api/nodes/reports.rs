//! What brokers report about themselves: liveness, and their Raft groups'
//! copy ids and offset floors.
use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;

use super::require_node_manage;
use crate::api::AppState;
use crate::api::error::{
    ApiError, api_conflict, api_internal, api_not_found, api_validation_error,
};
use crate::api::types::{
    NodeHeartbeatRequest, NodeHeartbeatResponse, RaftCopyIdRequest, RaftCopyIdResponse,
    RaftFloorsRequest, RaftFloorsResponse,
};
use crate::store::StoreError;

/// Most floors one request may carry: what a broker sends in one tick, so
/// one request is one bounded proposal.
const MAX_RAFT_FLOORS: usize = 1024;

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/heartbeat",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    request_body = NodeHeartbeatRequest,
    responses(
        (status = 200, description = "Heartbeat recorded", body = NodeHeartbeatResponse),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Heartbeat is for a superseded incarnation", body = crate::api::types::ErrorResponse)
    )
)]
/// Record that a broker is alive.
///
/// Returns the node's current lifecycle and how soon the next heartbeat is
/// expected. A broker that finds itself `down` here has been expired and must
/// register again before it is eligible for placement.
///
/// # Errors
/// - 404 when the node is not registered.
/// - 409 when the reported incarnation is older than the recorded one.
pub(crate) async fn report_health(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(request): Json<NodeHeartbeatRequest>,
) -> Result<Json<NodeHeartbeatResponse>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
    // The *store's* clock, not this instance's. Expiry is judged against the
    // same one, and with several instances over one database those are
    // different processes — see `ControlPlaneStore::now_millis`. Still not the
    // caller's, which would let a broker postpone its own timeout.
    let now = state
        .store
        .now_millis()
        .await
        .map_err(|ref err| api_internal("read the store clock", err))?;

    let node = state
        .store
        .record_node_heartbeat(&node_id, request.incarnation, now)
        .await
        .map_err(|err| match err {
            StoreError::NotFound(_) => api_not_found("node is not registered"),
            StoreError::Conflict(ref message) => api_conflict("conflict", message),
            ref other => api_internal("record node heartbeat", other),
        })?;

    // After the heartbeat was accepted, so a superseded incarnation's report
    // is never kept. A failure costs only this beat's copy: the next beat
    // carries the sequence again.
    if let Some(root) = request.root {
        let heard = crate::model::RootSequence {
            storage_id: root.storage_id,
            sequence: root.sequence,
            incarnation: request.incarnation,
        };
        if let Err(err) = crate::model::validate_storage_id(&heard.storage_id) {
            tracing::warn!(node_id = %node.node_id, error = %err, "ignored a root sequence under a malformed storage id");
        } else if let Err(err) = state.store.record_root_sequence(&node.node_id, heard).await {
            tracing::warn!(
                node_id = %node.node_id,
                error = %err,
                "could not keep this broker's root sequence",
            );
        }
    }

    // As the root sequence: after the accept, and lost only for this beat.
    // Only from the node's current process: a beat from above it is accepted
    // after a restore rewound the record, and must not stand for the process
    // the next registration starts.
    if let Some(incarnation) = request.placement_incarnation
        && request.incarnation == node.status.incarnation
    {
        let heard = crate::model::PlacementAck {
            incarnation,
            node_incarnation: request.incarnation,
        };
        if let Err(err) = state.store.record_placement_ack(&node.node_id, heard).await {
            tracing::warn!(
                node_id = %node.node_id,
                error = %err,
                "could not keep this broker's placement acknowledgement",
            );
        }
    }

    let fleet_features = state
        .store
        .enabled_fleet_features()
        .await
        .map_err(|ref err| api_internal("read the enabled fleet features", err))?;

    // After the heartbeat was recorded, so the seq covers every change
    // committed before the broker's renewal. A broker that asks is not
    // serving until it has one, so failing its heartbeat here costs nothing.
    let assignment_seq = if request.want_assignment_seq {
        state
            .store
            .confirmed_shard_assignment_head()
            .await
            .map_err(|ref err| api_internal("read the shard assignment feed's head", err))?
    } else {
        None
    };

    // Left out while 0, so a group that never restored answers as before.
    // Missing it costs the broker one beat's acknowledgement.
    let placement_incarnation = match state.store.placement_incarnation().await {
        Ok(incarnation) => (incarnation > 0).then_some(incarnation),
        Err(err) => {
            tracing::warn!(error = %err, "could not read the placement incarnation");
            None
        }
    };

    // Computed from the record and the beat, so nothing is stored. A beat
    // can be above the record only when the record lost registrations, as a
    // restore does: the broker's acknowledgements are not the record's
    // process's until it registers again. The registration time must be the
    // record's, so a process of a node deleted and added again since is not
    // asked to take the new record over. Only a live node: registering sets
    // it live, which would undo a drain.
    let register_again = request.registered_at_millis == Some(node.status.registered_at_millis)
        && request.incarnation > node.status.incarnation
        && node.status.lifecycle == crate::model::NodeLifecycle::Live;

    Ok(Json(NodeHeartbeatResponse {
        node_id: node.node_id,
        lifecycle: node.status.lifecycle,
        heartbeat_interval_ms: state.node_liveness.heartbeat_interval_ms,
        expiry_timeout_ms: state.node_liveness.expiry_timeout_ms,
        fleet_features,
        assignment_seq,
        placement_incarnation,
        register_again,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/raft-copy-id",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    request_body = RaftCopyIdRequest,
    responses(
        (status = 200, description = "The id is on the shard's record", body = RaftCopyIdResponse),
        (status = 404, description = "The shard has no assignment", body = crate::api::types::ErrorResponse),
        (status = 409, description = "The id cannot be recorded now", body = crate::api::types::ErrorResponse)
    )
)]
/// Record the Raft copy id a broker holds for a shard that switched to Raft.
///
/// Once every broker in the shard's set has reported one, the control plane
/// writes the shard's target from them, and writes it again when a broker
/// reports a new id. A change of the set does not rewrite the target yet.
/// A broker reports only its own copy, from the process it registered.
/// Placement records one for a down voter the same way, from the voters
/// its group reported (`docs/per-shard-raft.md`, "Where a target comes from").
///
/// # Errors
/// - 404 when the shard has no assignment.
/// - 409 when the shard has not switched to Raft, the node is not in its
///   set, `registered_at_millis` and `incarnation` are not the node's
///   current registration, the id would
///   change the target while a registered broker has not acknowledged the
///   placement incarnation, or a control-plane member is
///   below metadata version 36.
pub(crate) async fn report_raft_copy_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(request): Json<RaftCopyIdRequest>,
) -> Result<Json<RaftCopyIdResponse>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
    match state
        .store
        .record_raft_copy_id(
            &request.key,
            &node_id,
            request.registered_at_millis,
            request.incarnation,
            request.copy_id,
            request.shard_incarnation,
            request.group_epoch,
        )
        .await
    {
        Ok(changed) => {
            if changed {
                tracing::info!(
                    node_id = %node_id,
                    name = %request.key.stream,
                    shard = request.key.shard,
                    copy_id = request.copy_id,
                    "recorded a broker's Raft copy id",
                );
                // The id may complete the set's target, or make a voter the
                // leader already reported count: either can let a waiting
                // set step go ahead.
                state.placement_wakes.forget_raft_voters(&request.key);
                state.placement_wakes.request_pass();
            }
            Ok(Json(RaftCopyIdResponse { changed }))
        }
        Err(StoreError::NotFound(_)) => Err(api_not_found("shard has no assignment")),
        Err(StoreError::Conflict(ref message)) => Err(api_conflict("conflict", message)),
        Err(ref err) => Err(api_internal("record the Raft copy id", err)),
    }
}

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/raft-floors",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    request_body = RaftFloorsRequest,
    responses(
        (status = 200, description = "Every entry was decided", body = RaftFloorsResponse),
        (status = 400, description = "More than 1024 floors in one request", body = crate::api::types::ErrorResponse),
        (status = 409, description = "A control-plane member is below metadata version 38", body = crate::api::types::ErrorResponse)
    )
)]
/// Report the committed log id of each Raft group the broker leads.
///
/// The control plane keeps the highest per shard, stream incarnation and
/// reform epoch as the shard's offset floor, and only raises it. Each entry
/// is decided on its own: one for a missing shard, a shard not on Raft,
/// another incarnation or epoch, or a higher index at a lower term is
/// skipped and not counted, and the rest still apply.
///
/// # Errors
/// - 400 for more than 1024 entries, the most a broker sends at once.
/// - 409 for the whole batch when a control-plane member is below metadata
///   version 38 or its level is unknown. Nothing in it was recorded.
pub(crate) async fn report_raft_floors(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(request): Json<RaftFloorsRequest>,
) -> Result<Json<RaftFloorsResponse>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
    if request.floors.len() > MAX_RAFT_FLOORS {
        return Err(api_validation_error(&format!(
            "at most {MAX_RAFT_FLOORS} floors in one request"
        )));
    }
    // Judged only once the store took the batch, so a refused one wakes
    // nothing and is news again when it is retried.
    let with_voters: Vec<_> = request
        .floors
        .iter()
        .filter(|floor| floor.voters.is_some())
        .cloned()
        .collect();
    match state
        .store
        .record_raft_floors(&node_id, request.floors)
        .await
    {
        Ok(raised) => {
            // A Raft set step waits on the voters the leader reports, so
            // placement runs now rather than at its next tick.
            if state
                .placement_wakes
                .raft_voters_reported(&node_id, &with_voters)
            {
                state.placement_wakes.request_pass();
            }
            Ok(Json(RaftFloorsResponse { raised }))
        }
        Err(StoreError::Conflict(ref message)) => Err(api_conflict("conflict", message)),
        Err(ref err) => Err(api_internal("record the Raft offset floors", err)),
    }
}

#[cfg(test)]
mod tests;

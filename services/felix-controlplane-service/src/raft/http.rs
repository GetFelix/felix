//! The server half of the Raft transport: three POST routes handing each
//! RPC to the local node and shipping its whole `Result` back as JSON —
//! the shape [`super::network`] expects on the other side — plus the
//! proposal route and the two a member uses to enter the group
//! ([`super::join`]).
//!
//! Served on the peer listener only, behind [`super::peer::require_peer`]:
//! `propose` alone can replace the whole store, so nothing here is reachable
//! from the public API port or without the cluster's peer credentials.
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::types::TypeConfig;

pub(super) fn router(handle: super::RaftHandle) -> Router {
    let security = std::sync::Arc::clone(&handle.security);
    Router::new()
        .route("/internal/raft/append-entries", post(append_entries))
        .route("/internal/raft/vote", post(vote))
        .route("/internal/raft/install-snapshot", post(install_snapshot))
        .route("/internal/raft/propose", post(propose))
        .route("/internal/raft/standing", get(standing))
        .route("/internal/raft/catch-up-target", get(catch_up_target))
        // Axum's default body limit (2MB) is below openraft's default
        // snapshot chunk (3MB); a snapshot install crossing that line would
        // be refused with a 413 the sender reads as a network fault. Sized
        // to the chunk plus JSON's expansion of binary data.
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(handle)
        .layer(axum::middleware::from_fn_with_state(
            security,
            super::peer::require_peer,
        ))
}

/// A proposal over HTTP: opaque command bytes in, the state machine's
/// response bytes out. Goes through the seam's `write`, so a proposal
/// landing on a follower forwards to the leader and a caller may point at
/// **any** member — the promise the migration tool leans on. The peer token
/// is what authorizes it: that token is the cluster-admin credential. 503
/// only when the bounded write budget runs out: no leader, or no quorum.
async fn propose(State(handle): State<super::RaftHandle>, body: axum::body::Bytes) -> Response {
    match handle.write(body.to_vec()).await {
        Ok(bytes) => (StatusCode::OK, bytes).into_response(),
        Err(err) => (StatusCode::SERVICE_UNAVAILABLE, format!("{err:#}")).into_response(),
    }
}

async fn append_entries(
    State(handle): State<super::RaftHandle>,
    Json(rpc): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<Result<AppendEntriesResponse<u64>, RaftError<u64>>> {
    Json(handle.raft.append_entries(rpc).await)
}

/// A member withholding its vote answers like an unreachable one. openraft
/// would grant from an empty log, which is exactly what must not happen.
async fn vote(
    State(handle): State<super::RaftHandle>,
    Json(rpc): Json<VoteRequest<u64>>,
) -> Response {
    if !handle.may_vote.load(std::sync::atomic::Ordering::SeqCst) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "vote withheld until caught up",
        )
            .into_response();
    }
    let result: Result<VoteResponse<u64>, RaftError<u64>> = handle.raft.vote(rpc).await;
    Json(result).into_response()
}

async fn standing(State(handle): State<super::RaftHandle>) -> Json<super::join::Standing> {
    Json(handle.standing())
}

/// 503 from anything but a confirmed leader.
async fn catch_up_target(State(handle): State<super::RaftHandle>) -> Response {
    match handle.catch_up_target().await {
        Some(target) => Json(target).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn install_snapshot(
    State(handle): State<super::RaftHandle>,
    Json(rpc): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>> {
    Json(handle.raft.install_snapshot(rpc).await)
}

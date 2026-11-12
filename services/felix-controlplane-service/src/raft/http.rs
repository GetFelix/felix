//! The server half of the Raft transport: three POST routes handing each
//! RPC to the local node and answering in Felix's peer format
//! ([`super::wire`]), the shape [`super::network`] and a member on a release
//! that ran openraft 0.9 both expect, plus the
//! proposal route, the leader-only route ([`super::leader`]), and the two a
//! member uses to enter the group ([`super::join`]).
//!
//! Served on the peer listener only, behind [`super::peer::require_peer`]:
//! `propose` alone can replace the whole store, so nothing here is reachable
//! from the public API port or without the cluster's peer credentials.
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest};

use super::types::{Snapshot, Vote};
use super::{chunks, wire};

pub(super) fn router(handle: super::RaftHandle) -> Router {
    let security = std::sync::Arc::clone(&handle.security);
    Router::new()
        .route("/internal/raft/append-entries", post(append_entries))
        .route("/internal/raft/vote", post(vote))
        .route("/internal/raft/install-snapshot", post(install_snapshot))
        .route("/internal/raft/propose", post(propose))
        .route(super::leader::LEADER_ROUTE, post(leader_request))
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

/// A leader-only request forwarded by a follower. 503 from a member that
/// does not lead, so the sender asks again once it knows who does.
async fn leader_request(
    State(handle): State<super::RaftHandle>,
    body: axum::body::Bytes,
) -> Response {
    match handle.serve_leader_request(&body).await {
        Some(bytes) => (StatusCode::OK, bytes).into_response(),
        None => (StatusCode::SERVICE_UNAVAILABLE, "not the raft leader").into_response(),
    }
}

/// This member's raft node has stopped. Any status but 2xx reads as an
/// unreachable member to every version of the sender, which then backs off.
fn stopped(err: impl std::fmt::Display) -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, format!("raft node: {err}")).into_response()
}

async fn append_entries(
    State(handle): State<super::RaftHandle>,
    Json(rpc): Json<wire::AppendEntriesRequest>,
) -> Response {
    let rpc = AppendEntriesRequest {
        vote: rpc.vote.into(),
        prev_log_id: wire::lib_log_id(rpc.prev_log_id),
        entries: rpc.entries.into_iter().map(Into::into).collect(),
        leader_commit: wire::lib_log_id(rpc.leader_commit),
    };
    let sent = rpc.last_log_id();
    let response = match handle.raft.append_entries(rpc).await {
        // A member that took only some of the entries says how far, so the
        // leader never counts the rest as stored here.
        Ok(Ok(accepted)) if accepted != sent => {
            wire::AppendEntriesResponse::PartialSuccess(wire::log_id(&accepted))
        }
        Ok(result) => match AppendEntriesResponse::from(result) {
            AppendEntriesResponse::Success => wire::AppendEntriesResponse::Success,
            AppendEntriesResponse::PartialSuccess(matched) => {
                wire::AppendEntriesResponse::PartialSuccess(wire::log_id(&matched))
            }
            AppendEntriesResponse::Conflict => wire::AppendEntriesResponse::Conflict,
            AppendEntriesResponse::HigherVote(vote) => {
                wire::AppendEntriesResponse::HigherVote((&vote).into())
            }
        },
        Err(err) => return stopped(err),
    };
    Json(wire::Reply::Ok(response)).into_response()
}

/// A member withholding its vote answers like an unreachable one. openraft
/// would grant from an empty log, which is exactly what must not happen.
async fn vote(
    State(handle): State<super::RaftHandle>,
    Json(rpc): Json<wire::VoteRequest>,
) -> Response {
    if !handle.may_vote.load(std::sync::atomic::Ordering::SeqCst) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "vote withheld until caught up",
        )
            .into_response();
    }
    let rpc = VoteRequest::new(rpc.vote.into(), wire::lib_log_id(rpc.last_log_id));
    match handle.raft.vote(rpc).await {
        Ok(response) => Json(wire::Reply::Ok(wire::VoteResponse {
            vote: (&response.vote).into(),
            vote_granted: response.vote_granted,
            last_log_id: wire::log_id(&response.last_log_id),
        }))
        .into_response(),
        Err(err) => stopped(err),
    }
}

async fn standing(
    State(handle): State<super::RaftHandle>,
    axum::extract::Query(asker): axum::extract::Query<super::version::Asker>,
) -> Json<super::join::Standing> {
    handle.note_asker(asker);
    Json(handle.standing())
}

/// 503 from anything but a confirmed leader.
async fn catch_up_target(State(handle): State<super::RaftHandle>) -> Response {
    match handle.catch_up_target().await {
        Some(target) => Json(target).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// One chunk of a snapshot, as a 0.9 member sends it. The chunks are joined
/// here and the library is handed the whole snapshot with the last one.
async fn install_snapshot(
    State(handle): State<super::RaftHandle>,
    Json(chunk): Json<wire::InstallSnapshotRequest>,
) -> Response {
    let sender = Vote::from(chunk.vote);
    let mine = match handle.raft.with_raft_state(|state| *state.vote_ref()).await {
        Ok(vote) => vote,
        Err(err) => return stopped(err),
    };
    let mut answer = wire::InstallSnapshotResponse {
        vote: (&mine).into(),
    };
    // A sender that is no longer the leader this member follows learns so
    // from the vote in the answer, before any of its bytes are kept.
    if sender < mine {
        return Json(wire::Reply::Ok(answer)).into_response();
    }
    // Held to the end of the install, so two transfers cannot interleave.
    let mut receiving = handle.receiving.lock().await;
    let (meta, data) = match chunks::receive(&mut receiving, chunk) {
        Ok(Some(whole)) => whole,
        Ok(None) => return Json(wire::Reply::Ok(answer)).into_response(),
        Err(mismatch) => {
            return Json(wire::Reply::<wire::InstallSnapshotResponse>::Err(mismatch))
                .into_response();
        }
    };
    let snapshot = Snapshot {
        meta: meta.into(),
        snapshot: std::io::Cursor::new(data),
    };
    match handle.raft.install_full_snapshot(sender, snapshot).await {
        Ok(response) => {
            answer.vote = (&response.vote).into();
            Json(wire::Reply::Ok(answer)).into_response()
        }
        Err(err) => stopped(err),
    }
}

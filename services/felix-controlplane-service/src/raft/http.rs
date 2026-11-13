//! The server half of the Raft transport: three POST routes handing each
//! RPC to the local node and answering in Felix's peer format
//! ([`super::wire`]), the shape [`super::network`] and a member on a release
//! that ran openraft 0.9 both expect, plus the
//! proposal route, the leader-only route ([`super::leader`]), and the three
//! a member uses to enter the group ([`super::join`]).
//!
//! The router is the member's ([`super::member`]), which may have no Raft
//! node yet. `standing` answers either way. Every other route answers 503
//! until the node is attached, so no vote or append is answered by a member
//! that has not settled how it enters the group.
//!
//! Every route is under [`super::PEER_PREFIX`], which differs between a
//! member on redb and one on the segment store, so neither answers the
//! other.
//!
//! Served on the peer listener only, behind [`super::peer::require_peer`]:
//! `propose` alone can replace the whole store, so nothing here is reachable
//! from the public API port or without the cluster's peer credentials.
use axum::extract::{FromRequestParts, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest};

use super::types::{Snapshot, Vote};
use super::{RaftHandle, RaftMember, peer_route, wire};

pub(super) fn router(member: RaftMember) -> Router {
    let security = std::sync::Arc::clone(&member.security);
    let id = member.id;
    Router::new()
        .route(&peer_route("append-entries"), post(append_entries))
        .route(&peer_route("vote"), post(vote))
        .route(&peer_route("install-snapshot"), install_snapshot_route())
        .route(&peer_route("propose"), post(propose))
        .route(
            &peer_route(super::leader::LEADER_ROUTE),
            post(leader_request),
        )
        .route(&peer_route("standing"), get(standing))
        .route(&peer_route("forming"), post(forming))
        .route(&peer_route("catch-up-target"), get(catch_up_target))
        // Axum's default body limit (2MB) is below openraft's default
        // snapshot chunk (3MB); a snapshot install crossing that line would
        // be refused with a 413 the sender reads as a network fault. Sized
        // to the chunk plus JSON's expansion of binary data.
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(member)
        // Inside the peer check, so only a peer learns which member this is.
        .layer(axum::middleware::from_fn_with_state(
            id,
            super::peer::require_target,
        ))
        .layer(axum::middleware::from_fn_with_state(
            security,
            super::peer::require_peer,
        ))
}

/// This member's Raft node. A route that takes it answers 503 while the
/// member has none, like a member that is not there.
struct Node(RaftHandle);

impl FromRequestParts<RaftMember> for Node {
    type Rejection = Response;

    async fn from_request_parts(
        _parts: &mut axum::http::request::Parts,
        member: &RaftMember,
    ) -> Result<Self, Response> {
        match member.node() {
            Some(node) => Ok(Self(node.clone())),
            None => Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "this member has no raft node yet",
            )
                .into_response()),
        }
    }
}

/// A proposal over HTTP: opaque command bytes in, the state machine's
/// response bytes out. Goes through the seam's `write`, so a proposal
/// landing on a follower forwards to the leader and a caller may point at
/// **any** member — the promise the migration tool leans on. The peer token
/// is what authorizes it: that token is the cluster-admin credential. 503
/// only when the bounded write budget runs out: no leader, or no quorum.
async fn propose(Node(handle): Node, body: axum::body::Bytes) -> Response {
    match handle.write(body.to_vec()).await {
        Ok(bytes) => (StatusCode::OK, bytes).into_response(),
        Err(err) => (StatusCode::SERVICE_UNAVAILABLE, format!("{err:#}")).into_response(),
    }
}

/// A leader-only request forwarded by a follower. 503 from a member that
/// does not lead, so the sender asks again once it knows who does.
async fn leader_request(Node(handle): Node, body: axum::body::Bytes) -> Response {
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
    Node(handle): Node,
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
async fn vote(Node(handle): Node, Json(rpc): Json<wire::VoteRequest>) -> Response {
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

/// Answered with or without a Raft node.
async fn standing(
    State(member): State<RaftMember>,
    axum::extract::Query(asker): axum::extract::Query<super::version::Asker>,
) -> Response {
    member.note_asker(asker);
    match member.standing().await {
        Ok(standing) => Json(standing).into_response(),
        Err(err) => stopped(err),
    }
}

/// A member that set out to form the group asks to be recorded. The record
/// is on disk before the 204: the member votes once every peer has given
/// one. 409 for a group other than this member's own.
async fn forming(Node(handle): Node, Json(forming): Json<super::join::Forming>) -> Response {
    use super::join::NotRecorded;

    match handle.record_forming(&forming).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(NotRecorded::Refused(why)) => (StatusCode::CONFLICT, why).into_response(),
        Err(NotRecorded::Failed(err)) => stopped(err),
    }
}

/// 503 from anything but a confirmed leader.
async fn catch_up_target(Node(handle): Node) -> Response {
    match handle.catch_up_target().await {
        Some(target) => Json(target).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

#[cfg(not(felix_segment_store))]
fn install_snapshot_route() -> axum::routing::MethodRouter<RaftMember> {
    post(install_snapshot)
}

/// A snapshot arrives whole, so this route lifts the router's body limit:
/// the state is as large as it is, and only a peer with the cluster's
/// credentials gets this far.
#[cfg(felix_segment_store)]
fn install_snapshot_route() -> axum::routing::MethodRouter<RaftMember> {
    post(install_snapshot).layer(axum::extract::DefaultBodyLimit::disable())
}

/// A whole snapshot in one request ([`wire::snapshot_body`]).
#[cfg(felix_segment_store)]
async fn install_snapshot(Node(handle): Node, body: axum::body::Bytes) -> Response {
    let Some((head, data)) = wire::parse_snapshot_body(&body) else {
        return (StatusCode::BAD_REQUEST, "not a snapshot request").into_response();
    };
    let snapshot = Snapshot {
        meta: head.meta.into(),
        snapshot: std::io::Cursor::new(data.to_vec()),
    };
    // A sender that is no longer the leader this member follows learns so
    // from the vote in the answer; the library refuses its snapshot.
    match handle
        .raft
        .install_full_snapshot(Vote::from(head.vote), snapshot)
        .await
    {
        Ok(response) => Json(wire::Reply::Ok(wire::InstallSnapshotResponse {
            vote: (&response.vote).into(),
        }))
        .into_response(),
        Err(err) => stopped(err),
    }
}

/// One chunk of a snapshot, as a 0.9 member sends it. The chunks are joined
/// here and the library is handed the whole snapshot with the last one.
#[cfg(not(felix_segment_store))]
async fn install_snapshot(
    Node(handle): Node,
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
    let (meta, data) = match super::chunks::receive(&mut receiving, chunk) {
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

#[cfg(test)]
mod tests;

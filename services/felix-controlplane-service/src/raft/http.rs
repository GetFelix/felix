//! The server half of the Raft transport: three POST routes handing each
//! RPC to the local node and answering in Felix's peer format
//! ([`super::wire`]), the shape [`super::network`] expects, plus the
//! proposal route, the leader-only route ([`super::leader`]), and the three
//! a member uses to enter the group ([`super::join`]).
//!
//! The router is the member's ([`super::member`]), which may have no Raft
//! node yet. `standing` answers either way. Every other route answers 503
//! until the node is attached, so no vote or append is answered by a member
//! that has not settled how it enters the group.
//!
//! Every route is under [`super::PEER_PREFIX`]. Releases that kept the store
//! in redb served `/internal/raft`, so a member of such a group and this
//! one never answer each other.
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

use felix_storage::raft_log::SnapshotId;

use super::types;
use super::types::{Snapshot, Vote};
use super::{RaftHandle, RaftMember, peer_route, wire};

pub(super) fn router(member: RaftMember) -> Router {
    let security = std::sync::Arc::clone(&member.security);
    let id = member.id;
    Router::new()
        .route(&peer_route("append-entries"), post(append_entries))
        .route(&peer_route("vote"), post(vote))
        // The body is read as a stream, which the router's body limit does not
        // cover; the transfer's own cap does, from the length in the head.
        .route(&peer_route("install-snapshot"), post(install_snapshot))
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

/// A snapshot streamed in one request (`felix_storage::raft_log::transfer`).
/// The head is read first, and a stale sender, a length over the cap or a
/// busy slot is answered before any data is read. The data goes straight
/// to the receive file, never into memory.
async fn install_snapshot(Node(handle): Node, body: axum::body::Body) -> Response {
    let mut incoming =
        felix_storage::raft_log::transfer::Incoming::new(BodyPieces(body.into_data_stream()));
    let head = match incoming.head(handle.snapshot_max_bytes).await {
        Ok(head) => head,
        Err(err) => return transfer_failed(&err, incoming.received()),
    };
    let data_len = head.data_len;
    let Some(head) = wire::SnapshotHead::decode(&head.head) else {
        return (StatusCode::BAD_REQUEST, "not a snapshot head").into_response();
    };
    let sender = Vote::from(head.vote);
    let mine = match handle.raft.with_raft_state(|state| *state.vote_ref()).await {
        Ok(vote) => vote,
        Err(err) => return stopped(err),
    };
    // Advisory: the library checks the vote again when it installs. This
    // only spares a stale sender's data the trip.
    if sender < mine {
        return Json(wire::Reply::Ok(wire::InstallSnapshotResponse {
            vote: (&mine).into(),
        }))
        .into_response();
    }
    let Ok(slot) = handle.install_slot.take(sender).await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "another snapshot install is in progress",
        )
            .into_response();
    };
    let Some(snapshots) = handle.member.snapshots().await else {
        return stopped("the store is closed");
    };
    let meta = types::SnapshotMeta::from(head.meta.clone());
    let last = meta.last_log_id.map(|id| SnapshotId {
        term: *id.leader_id,
        index: id.index,
    });
    let meta_bytes = match serde_json::to_vec(&head.meta) {
        Ok(bytes) => bytes,
        Err(err) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    };
    let mut staged = match snapshots
        .begin_receive(last, meta_bytes.into(), data_len)
        .await
    {
        Ok(staged) => staged,
        Err(err) => return stopped(err),
    };
    let cancelled = slot.cancelled();
    let closing = staged.closing();
    let stop = async {
        tokio::select! {
            () = cancelled => {}
            () = closing => {}
        }
    };
    if let Err(err) = incoming.data(data_len, &mut staged, stop).await {
        return transfer_failed(&err, incoming.received());
    }
    if let Err(err) = staged.finish().await {
        return stopped(err);
    }
    let snapshot = Snapshot {
        meta,
        snapshot: types::SnapshotData::Staged(staged),
    };
    let installed = handle.raft.install_full_snapshot(sender, snapshot).await;
    drop(slot);
    match installed {
        Ok(response) => Json(wire::Reply::Ok(wire::InstallSnapshotResponse {
            vote: (&response.vote).into(),
        }))
        .into_response(),
        Err(err) => stopped(err),
    }
}

/// A request body, as the snapshot receive reads it.
struct BodyPieces(axum::body::BodyDataStream);

impl felix_storage::raft_log::transfer::Pieces for BodyPieces {
    async fn next_piece(&mut self) -> Option<std::io::Result<bytes::Bytes>> {
        use futures_util::StreamExt;

        let piece = self.0.next().await?;
        Some(piece.map_err(std::io::Error::other))
    }
}

/// A transfer that was refused or broke off, logged with how far it got
/// so one that keeps failing shows up as such.
fn transfer_failed(
    err: &felix_storage::raft_log::transfer::TransferError,
    received: u64,
) -> Response {
    use felix_storage::raft_log::transfer::TransferError;

    tracing::warn!(error = %err, received, "snapshot receive failed");
    let status = match err {
        TransferError::OverCap { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        TransferError::Stalled { .. } => StatusCode::REQUEST_TIMEOUT,
        TransferError::Cancelled | TransferError::Io(_) => StatusCode::SERVICE_UNAVAILABLE,
        TransferError::UnknownForm(_)
        | TransferError::HeadTooLarge(_)
        | TransferError::Short { .. }
        | TransferError::Trailing
        | TransferError::Crc => StatusCode::BAD_REQUEST,
    };
    (status, err.to_string()).into_response()
}

#[cfg(test)]
mod tests;

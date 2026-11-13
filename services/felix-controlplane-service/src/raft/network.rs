//! Raft RPCs to peers: JSON over each member's peer listener.
//!
//! The wire format is Felix's ([`super::wire`]). A snapshot is streamed in
//! one request. A peer's refusal
//! comes back as the `Err` half of its answer and is reported as
//! `Unreachable`, like a transport failure that will not fix itself.
//!
//! openraft only backs off on `Unreachable`; after any other transport
//! error it retries a replication at once. So everything that will not fix
//! itself within a millisecond has to be reported as `Unreachable`, or a
//! down member costs the leader a busy loop of failed RPCs.
use std::future::Future;
use std::time::Duration;

use openraft::OptionalSend;
use openraft::errors::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{Backoff, RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, AppendResult, SnapshotResponse, VoteRequest,
    VoteResponse,
};

use std::collections::BTreeMap;
use std::sync::Arc;

use felix_storage::raft_log::transfer;

use super::hold::Hold;
use super::types::{Snapshot, TypeConfig, Vote};
use super::{NodeId, PeerSecurity, wire};

pub(super) struct HttpNetworkFactory {
    client: reqwest::Client,
    scheme: &'static str,
    peer_addrs: BTreeMap<NodeId, String>,
    heartbeat: Duration,
    hold: Arc<Hold>,
}

impl HttpNetworkFactory {
    pub(super) fn new(
        security: &PeerSecurity,
        peer_addrs: BTreeMap<NodeId, String>,
        heartbeat: Duration,
        hold: Arc<Hold>,
    ) -> anyhow::Result<Self> {
        // One client, shared by every peer connection: reqwest pools per
        // host underneath. The timeout bounds a peer that accepts and then
        // hangs — an unanswered RPC must become an error openraft can react
        // to, not a stuck replication task.
        let client = security.client(std::time::Duration::from_secs(10))?;
        Ok(Self {
            client,
            scheme: security.scheme(),
            peer_addrs,
            heartbeat,
            hold,
        })
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpNetwork;

    async fn new_client(
        &mut self,
        target: u64,
        node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        // The configured address wins over the one the membership recorded
        // when the group formed, so a moved peer port needs no membership
        // change.
        let addr = self.peer_addrs.get(&target).unwrap_or(&node.addr);
        HttpNetwork {
            client: self.client.clone(),
            target,
            base: format!("{}://{addr}", self.scheme),
            heartbeat: self.heartbeat,
            hold: Arc::clone(&self.hold),
            acknowledged: None,
        }
    }
}

pub(super) struct HttpNetwork {
    client: reqwest::Client,
    target: u64,
    base: String,
    heartbeat: Duration,
    hold: Arc<Hold>,
    /// The highest log index this member acknowledged to this client. A
    /// client lives at most one leader's term (openraft also rebuilds it on
    /// every membership change), and that leader's log only grows, so a
    /// conflict at or below it means the member lost its log.
    acknowledged: Option<u64>,
}

/// The longest wait between retries to an unreachable member, so a member
/// coming back is picked up again within about a second.
const MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Retry delays for an unreachable member: one heartbeat interval, doubling
/// up to [`MAX_BACKOFF`]. The first delay is no longer than the gap a
/// healthy follower already sees between heartbeats, so a one-off blip does
/// not push it toward an election.
fn backoff_delays(heartbeat: Duration) -> impl Iterator<Item = Duration> {
    let first = heartbeat.clamp(Duration::from_millis(10), MAX_BACKOFF);
    std::iter::successors(Some(first), |delay| Some((*delay * 2).min(MAX_BACKOFF)))
}

/// A peer that answered with a non-2xx status.
#[derive(Debug)]
struct PeerStatus {
    status: reqwest::StatusCode,
    body: String,
}

impl std::fmt::Display for PeerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "peer answered {}", self.status)?;
        if !self.body.is_empty() {
            write!(f, ": {}", self.body)?;
        }
        Ok(())
    }
}

impl std::error::Error for PeerStatus {}

/// A peer that answered the request with an error of its own.
#[derive(Debug)]
struct PeerRefused(serde_json::Value);

impl std::fmt::Display for PeerRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "peer refused: {}", self.0)
    }
}

impl std::error::Error for PeerRefused {}

/// The client's own timeout is sized for a heartbeat. A snapshot is bounded
/// by its progress instead ([`transfer::Progress`]), so this only has to be
/// longer than any transfer that keeps moving.
const NO_TOTAL_LIMIT: Duration = Duration::from_secs(7 * 24 * 3600);

/// Keep error messages to one readable line, whatever the peer sent.
const BODY_EXCERPT: usize = 200;

impl HttpNetwork {
    #[cfg(test)]
    pub(super) fn for_tests(addr: &str, hold: Arc<Hold>) -> Self {
        Self::for_tests_as(2, addr, hold)
    }

    #[cfg(test)]
    pub(super) fn for_tests_as(target: NodeId, addr: &str, hold: Arc<Hold>) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("client"),
            target,
            base: format!("http://{addr}"),
            heartbeat: Duration::from_millis(150),
            hold,
            acknowledged: None,
        }
    }

    /// The peer's answer, its refusal included.
    async fn send_reply<Req, Resp>(
        &self,
        path: &str,
        request: &Req,
    ) -> Result<wire::Reply<Resp>, RPCError<TypeConfig>>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let url = format!("{}{}", self.base, super::peer_route(path));
        self.answer(self.client.post(url).json(request)).await
    }

    async fn answer<Resp>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<wire::Reply<Resp>, RPCError<TypeConfig>>
    where
        Resp: serde::de::DeserializeOwned,
    {
        // Whoever is at the address answers only if it is this member.
        let request = request.header(super::TARGET_HEADER, self.target.to_string());
        let response = request.send().await.map_err(|err| {
            if err.is_connect() || err.is_timeout() {
                RPCError::Unreachable(Unreachable::new(&err))
            } else {
                RPCError::Network(NetworkError::new(&err))
            }
        })?;
        let status = response.status();
        if !status.is_success() {
            // Every non-2xx is an answer that will be the same on an
            // immediate retry: 404 (not a raft peer), 401/403 (credentials
            // do not match), 413, 5xx — including the 503 a member sends
            // while it withholds its vote, which must read like an
            // unreachable voter. Backing off is right for all of them.
            let body = response.text().await.unwrap_or_default();
            let mut body = body.trim().to_string();
            if body.len() > BODY_EXCERPT {
                let mut end = BODY_EXCERPT;
                while !body.is_char_boundary(end) {
                    end -= 1;
                }
                body.truncate(end);
                body.push_str("...");
            }
            return Err(RPCError::Unreachable(Unreachable::new(&PeerStatus {
                status,
                body,
            })));
        }
        response
            .json()
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))
    }

    async fn send<Req, Resp>(&self, path: &str, request: &Req) -> Result<Resp, RPCError<TypeConfig>>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        match self.send_reply(path, request).await? {
            wire::Reply::Ok(response) => Ok(response),
            wire::Reply::Err(err) => Err(refused(err)),
        }
    }

    /// Entries and snapshots go only to a member not known to be below the
    /// group's floor ([`super::hold`]); otherwise it reads as unreachable.
    fn hold_back(&self) -> Result<(), Unreachable<TypeConfig>> {
        self.hold
            .check(self.target)
            .map_err(|held| Unreachable::new(&held))
    }

    /// Settle a fresh mark with a probe of this member alone, off the send
    /// path.
    fn ask(&self) {
        let hold = Arc::clone(&self.hold);
        let (client, base, target) = (self.client.clone(), self.base.clone(), self.target);
        tokio::spawn(async move { hold.ask(target, &client, &base).await });
    }

    fn acknowledged(&mut self, index: Option<u64>) {
        self.acknowledged = self.acknowledged.max(index);
    }
}

fn refused(err: serde_json::Value) -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::new(&PeerRefused(err)))
}

/// A snapshot request's body: the head, the data a piece at a time, then
/// the crc, in `transfer`'s framing. Each piece counts as progress.
fn snapshot_body(
    head: &wire::SnapshotHead,
    data: super::types::SnapshotData,
    progress: transfer::Progress,
) -> impl futures_util::Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static {
    enum Next {
        Start(bytes::Bytes),
        Data,
        Done,
    }
    let start = transfer::encode_start(&head.encode(), data.data_len());
    let state = (Next::Start(start), data, transfer::DataCrc::default());
    futures_util::stream::unfold(state, move |(next, mut data, mut crc)| {
        let progress = progress.clone();
        async move {
            let piece = match next {
                Next::Done => return None,
                Next::Start(start) => Ok((start, Next::Data)),
                Next::Data => match data.next_chunk(transfer::CHUNK).await {
                    Ok(Some(chunk)) => {
                        crc.update(&chunk);
                        Ok((chunk, Next::Data))
                    }
                    Ok(None) => {
                        progress.all_sent();
                        Ok((crc.trailer(), Next::Done))
                    }
                    Err(err) => Err(err),
                },
            };
            match piece {
                Ok((piece, next)) => {
                    progress.moved(piece.len());
                    Some((Ok(piece), (next, data, crc)))
                }
                Err(err) => Some((Err(err), (Next::Done, data, crc))),
            }
        }
    })
}

fn streaming(err: RPCError<TypeConfig>) -> StreamingError<TypeConfig> {
    match err {
        RPCError::Timeout(err) => StreamingError::Timeout(err),
        RPCError::Unreachable(err) => StreamingError::Unreachable(err),
        RPCError::Network(err) => StreamingError::Network(err),
    }
}

impl RaftNetworkV2<TypeConfig> for HttpNetwork {
    type SnapshotData = super::types::SnapshotData;

    async fn append_entries(
        &mut self,
        mut rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        if let Err(held) = self.hold_back() {
            if !rpc.entries.is_empty() {
                return Err(RPCError::Unreachable(held));
            }
            // An empty append is a heartbeat, which openraft sends only while
            // nothing else is queued for this member: it is caught up and
            // nothing new has been written. Once entries are held, openraft
            // retries them and sends no heartbeat. Without the commit index
            // the member applies nothing, not even entries it was fed before
            // the hold.
            rpc.leader_commit = None;
        }
        let request = wire::AppendEntriesRequest {
            vote: (&rpc.vote).into(),
            prev_log_id: wire::log_id(&rpc.prev_log_id),
            entries: rpc.entries.iter().map(wire::Entry::from).collect(),
            leader_commit: wire::log_id(&rpc.leader_commit),
        };
        let response: wire::AppendEntriesResponse = self.send("append-entries", &request).await?;
        let prev = request.prev_log_id.map(|log_id| log_id.index);
        let last = request.entries.last().map(|entry| entry.log_id);
        match &response {
            wire::AppendEntriesResponse::Success => {
                self.acknowledged(last.map(|log_id| log_id.index).or(prev));
            }
            wire::AppendEntriesResponse::PartialSuccess(matched) => {
                self.acknowledged(matched.map(|log_id| log_id.index));
            }
            // A lagging member conflicts above what it acknowledged; only
            // one that lost its log conflicts at or below it.
            wire::AppendEntriesResponse::Conflict
                if prev.is_some() && prev <= self.acknowledged =>
            {
                self.acknowledged = None;
                if self.hold.lost_log(self.target) {
                    self.ask();
                }
            }
            // With no acknowledgement to compare with, a member that lost its
            // log looks like one that is behind. It may lose it between two
            // conflicts, so each one asks again once the last ask settled.
            // Up-to-date members answer success and are never asked.
            wire::AppendEntriesResponse::Conflict
                if self.acknowledged.is_none() && self.hold.unconfirmed(self.target) =>
            {
                self.ask();
            }
            _ => {}
        }
        let response = match response {
            wire::AppendEntriesResponse::Success => AppendEntriesResponse::Success,
            wire::AppendEntriesResponse::PartialSuccess(matched) => {
                AppendEntriesResponse::PartialSuccess(wire::lib_log_id(matched))
            }
            wire::AppendEntriesResponse::Conflict => AppendEntriesResponse::Conflict,
            wire::AppendEntriesResponse::HigherVote(vote) => {
                AppendEntriesResponse::HigherVote(vote.into())
            }
        };
        Ok(
            response
                .into_append_result(rpc.prev_log_id, wire::lib_log_id(last).or(rpc.prev_log_id)),
        )
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let request = wire::VoteRequest {
            vote: (&rpc.vote).into(),
            last_log_id: wire::log_id(&rpc.last_log_id),
        };
        let response: wire::VoteResponse = self.send("vote", &request).await?;
        Ok(VoteResponse::new(
            Vote::from(response.vote),
            wire::lib_log_id(response.last_log_id),
            response.vote_granted,
        ))
    }

    /// Sent as one streamed request: the head, then the data read from the
    /// snapshot's file a piece at a time. Bounded by progress, not by a
    /// total: it fails only when no bytes move for `transfer::IDLE`, or the
    /// receiver takes longer than `transfer::install_bound` after the last.
    /// openraft does not enforce `option`'s ttl itself, and a fixed one
    /// would cut a large snapshot that is still moving.
    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        self.hold_back()?;
        let head = wire::SnapshotHead {
            vote: (&vote).into(),
            meta: (&snapshot.meta).into(),
        };
        let last = head.meta.last_log_id.map(|log_id| log_id.index);
        let data_len = snapshot.snapshot.data_len();
        let progress = transfer::Progress::new(data_len);
        let body = snapshot_body(&head, snapshot.snapshot, progress.clone());
        let url = format!("{}{}", self.base, super::peer_route("install-snapshot"));
        let request = self
            .client
            .post(url)
            .timeout(NO_TOTAL_LIMIT)
            .body(reqwest::Body::wrap_stream(body));
        let sent = self.answer::<wire::InstallSnapshotResponse>(request);
        let response = tokio::select! {
            closed = cancel => return Err(closed.into()),
            expired = progress.expired() => {
                tracing::warn!(target = self.target, error = %expired, "snapshot send gave up");
                return Err(StreamingError::Unreachable(Unreachable::new(&expired)));
            }
            response = sent => match response.map_err(streaming)? {
                wire::Reply::Ok(response) => response,
                wire::Reply::Err(err) => return Err(streaming(refused(err))),
            },
        };
        let theirs = Vote::from(response.vote);
        // A member that has moved on to a newer leader says so, and has not
        // taken the snapshot.
        if theirs > vote {
            return Ok(SnapshotResponse::new(theirs));
        }
        self.acknowledged(last);
        Ok(SnapshotResponse::new(vote))
    }

    fn backoff(&self) -> Option<Backoff> {
        Some(Backoff::new(backoff_delays(self.heartbeat)))
    }
}

#[cfg(test)]
mod tests;

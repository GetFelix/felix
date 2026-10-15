//! Raft RPCs to peers: JSON over each member's peer listener.
//!
//! The wire format is openraft's own request/response types serialized as
//! JSON, with the server's `Result` shipped whole — a remote `RaftError` is
//! data to the caller (openraft reacts to it), while a transport failure is
//! `Unreachable` or `Network`. Collapsing the two would turn "the peer told
//! me no" into "the peer is unreachable", which drive opposite behaviours.
//!
//! openraft only backs off on `Unreachable`; after any other transport
//! error it retries a replication at once. So everything that will not fix
//! itself within a millisecond has to be reported as `Unreachable`, or a
//! down member costs the leader a busy loop of failed RPCs.
use std::time::Duration;

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{Backoff, RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use std::collections::BTreeMap;
use std::sync::Arc;

use super::hold::Hold;
use super::types::TypeConfig;
use super::{NodeId, PeerSecurity};

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

    async fn new_client(&mut self, target: u64, node: &openraft::BasicNode) -> Self::Network {
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

    async fn send<Req, Resp, E>(
        &self,
        path: &str,
        request: &Req,
    ) -> Result<Resp, RPCError<u64, openraft::BasicNode, E>>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
        E: std::error::Error + serde::de::DeserializeOwned,
    {
        let url = format!("{}/internal/raft/{path}", self.base);
        let response = self
            .client
            .post(url)
            .json(request)
            .send()
            .await
            .map_err(|err| {
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
        let result: Result<Resp, E> = response
            .json()
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        result.map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }

    /// Entries and snapshots go only to a member not known to be below the
    /// group's floor ([`super::hold`]); otherwise it reads as unreachable.
    fn hold_back(&self) -> Result<(), Unreachable> {
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

impl RaftNetwork<TypeConfig> for HttpNetwork {
    async fn append_entries(
        &mut self,
        mut rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>>
    {
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
        let response = self.send("append-entries", &rpc).await;
        let prev = rpc.prev_log_id.map(|log_id| log_id.index);
        match &response {
            Ok(AppendEntriesResponse::Success) => {
                let last = rpc.entries.last().map(|entry| entry.log_id.index);
                self.acknowledged(last.or(prev));
            }
            Ok(AppendEntriesResponse::PartialSuccess(matched)) => {
                self.acknowledged(matched.map(|log_id| log_id.index));
            }
            // A lagging member conflicts above what it acknowledged; only
            // one that lost its log conflicts at or below it.
            Ok(AppendEntriesResponse::Conflict) if prev.is_some() && prev <= self.acknowledged => {
                self.acknowledged = None;
                if self.hold.lost_log(self.target) {
                    self.ask();
                }
            }
            // With no acknowledgement to compare with, a member that lost its
            // log looks like one that is behind. It may lose it between two
            // conflicts, so each one asks again once the last ask settled.
            // Up-to-date members answer success and are never asked.
            Ok(AppendEntriesResponse::Conflict)
                if self.acknowledged.is_none() && self.hold.unconfirmed(self.target) =>
            {
                self.ask();
            }
            _ => {}
        }
        response
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, openraft::BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.hold_back().map_err(RPCError::Unreachable)?;
        let response = self.send("install-snapshot", &rpc).await;
        if rpc.done && response.is_ok() {
            self.acknowledged(rpc.meta.last_log_id.map(|log_id| log_id.index));
        }
        response
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        self.send("vote", &rpc).await
    }

    fn backoff(&self) -> Backoff {
        Backoff::new(backoff_delays(self.heartbeat))
    }
}

#[cfg(test)]
mod tests;

//! Raft RPCs to peers: JSON over each member's peer listener.
//!
//! The wire format is openraft's own request/response types serialized as
//! JSON, with the server's `Result` shipped whole — a remote `RaftError` is
//! data to the caller (openraft reacts to it), while a transport failure is
//! a `NetworkError` (openraft retries/backs off). Collapsing the two would
//! turn "the peer told me no" into "the peer is unreachable", which drive
//! opposite behaviours.
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use std::collections::BTreeMap;

use super::types::TypeConfig;
use super::{NodeId, PeerSecurity};

pub(super) struct HttpNetworkFactory {
    client: reqwest::Client,
    scheme: &'static str,
    peer_addrs: BTreeMap<NodeId, String>,
}

impl HttpNetworkFactory {
    pub(super) fn new(
        security: &PeerSecurity,
        peer_addrs: BTreeMap<NodeId, String>,
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
        }
    }
}

pub(super) struct HttpNetwork {
    client: reqwest::Client,
    target: u64,
    base: String,
}

impl HttpNetwork {
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
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        let result: Result<Resp, E> = response
            .json()
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        result.map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }
}

impl RaftNetwork<TypeConfig> for HttpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>>
    {
        self.send("append-entries", &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, openraft::BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.send("install-snapshot", &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        self.send("vote", &rpc).await
    }
}

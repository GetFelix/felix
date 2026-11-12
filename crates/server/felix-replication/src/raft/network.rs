//! Raft messages between brokers: what the runtime hands a
//! [`RaftTransport`] to send, and the router that turns openraft's calls
//! into them.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::impls::EmptyNode;
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft_multi::{GroupNetworkAdapter, GroupRouter};

use super::types::{GroupId, MemberId, ShardTypes};

/// One Raft message for one member of one group. What is in it is this
/// module's business; a transport carries it to [`Self::to`]'s broker.
pub struct RaftRequest {
    pub(crate) group: GroupId,
    pub(crate) to: MemberId,
    pub(crate) body: Request,
}

pub(crate) enum Request {
    Append(AppendEntriesRequest<ShardTypes>),
    Vote(VoteRequest<ShardTypes>),
    PreVote(VoteRequest<ShardTypes>),
    /// A shard group's snapshot has no data, only its metadata.
    Snapshot {
        vote: VoteOf<ShardTypes>,
        meta: SnapshotMetaOf<ShardTypes>,
    },
    TransferLeader(TransferLeaderRequest<ShardTypes>),
}

impl RaftRequest {
    pub fn group(&self) -> &GroupId {
        &self.group
    }

    /// The member this is for. Its broker is where the message goes.
    pub fn to(&self) -> &MemberId {
        &self.to
    }
}

impl fmt::Debug for RaftRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.body {
            Request::Append(_) => "append",
            Request::Vote(_) => "vote",
            Request::PreVote(_) => "pre-vote",
            Request::Snapshot { .. } => "snapshot",
            Request::TransferLeader(_) => "transfer-leader",
        };
        write!(f, "{kind} for {} of {}", self.to, self.group)
    }
}

/// The answer to a [`RaftRequest`].
pub struct RaftReply(pub(crate) Reply);

pub(crate) enum Reply {
    Append(AppendResult<ShardTypes>),
    Vote(VoteResponse<ShardTypes>),
    Snapshot(SnapshotResponse<ShardTypes>),
    TransferLeader(TransferLeaderResponse<ShardTypes>),
}

/// Why a message got no answer. Raft treats each the same way: the member
/// was not reached, and the sender tries again later.
#[derive(Debug, thiserror::Error)]
pub enum Refused {
    /// The transport could not deliver it, or will not: a peer that does
    /// not speak Raft is never sent a message.
    #[error("{0}")]
    Unreachable(String),
    /// The broker holds no such member: another copy, another epoch, or a
    /// group it has not opened.
    #[error("{to} of {group} is not on this broker")]
    NoSuchMember { group: GroupId, to: MemberId },
    /// The member is there and has stopped.
    #[error("{to} of {group} has stopped: {reason}")]
    Stopped {
        group: GroupId,
        to: MemberId,
        reason: String,
    },
}

/// Carries Raft messages to peer brokers.
///
/// One transport serves every group, so groups share its connections. An
/// implementation delivers `request` to the broker named by
/// `request.to().broker`, has that broker's runtime [`handle`] it, and
/// returns the answer. It must not send to a peer that has not offered the
/// Raft capability; it answers [`Refused::Unreachable`] instead.
///
/// [`handle`]: super::GroupRuntime::handle
#[async_trait]
pub trait RaftTransport: Send + Sync + 'static {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused>;
}

/// A transport whose peers are not there: every vote is granted and every
/// append accepted. For measuring what a broker's own members cost, with
/// one runtime leading every group. Not a transport to run anything on.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct AgreeingPeers;

#[async_trait]
impl RaftTransport for AgreeingPeers {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        use openraft::entry::RaftEntry;
        Ok(RaftReply(match request.body {
            Request::Append(rpc) => {
                let last = rpc.entries.last().map(|entry| entry.log_id());
                Reply::Append(Ok(last.or(rpc.prev_log_id)))
            }
            Request::Vote(rpc) | Request::PreVote(rpc) => {
                Reply::Vote(VoteResponse::new(&rpc.vote, None, true))
            }
            Request::Snapshot { vote, .. } => Reply::Snapshot(SnapshotResponse::new(vote)),
            Request::TransferLeader(_) => {
                return Err(Refused::Unreachable("no peer to lead".into()));
            }
        }))
    }
}

/// openraft's view of the transport: every group's calls, tagged with the
/// group. This is the seam heartbeat coalescing wraps.
#[derive(Clone)]
pub(crate) struct Router {
    transport: Arc<dyn RaftTransport>,
}

#[derive(Debug, thiserror::Error)]
#[error("the answer was to another kind of message")]
struct WrongReply;

impl Router {
    pub(crate) fn new(transport: Arc<dyn RaftTransport>) -> Self {
        Self { transport }
    }

    async fn call(
        &self,
        to: MemberId,
        group: GroupId,
        body: Request,
        option: &RPCOption,
    ) -> Result<Reply, Unreachable<ShardTypes>> {
        let request = RaftRequest { group, to, body };
        match tokio::time::timeout(option.hard_ttl(), self.transport.send(request)).await {
            Ok(Ok(reply)) => Ok(reply.0),
            Ok(Err(refused)) => Err(Unreachable::new(&refused)),
            Err(elapsed) => Err(Unreachable::new(&elapsed)),
        }
    }
}

fn wrong_reply() -> Unreachable<ShardTypes> {
    Unreachable::new(&WrongReply)
}

impl GroupRouter<ShardTypes, GroupId> for Router {
    type SnapshotData = Vec<u8>;

    async fn append_entries(
        &self,
        target: MemberId,
        group_id: GroupId,
        rpc: AppendEntriesRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<AppendResult<ShardTypes>, RPCError<ShardTypes>> {
        match self
            .call(target, group_id, Request::Append(rpc), &option)
            .await?
        {
            Reply::Append(result) => Ok(result),
            _ => Err(wrong_reply().into()),
        }
    }

    async fn vote(
        &self,
        target: MemberId,
        group_id: GroupId,
        rpc: VoteRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<VoteResponse<ShardTypes>, RPCError<ShardTypes>> {
        match self
            .call(target, group_id, Request::Vote(rpc), &option)
            .await?
        {
            Reply::Vote(response) => Ok(response),
            _ => Err(wrong_reply().into()),
        }
    }

    async fn pre_vote(
        &self,
        target: MemberId,
        group_id: GroupId,
        rpc: VoteRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<VoteResponse<ShardTypes>, RPCError<ShardTypes>> {
        match self
            .call(target, group_id, Request::PreVote(rpc), &option)
            .await?
        {
            Reply::Vote(response) => Ok(response),
            _ => Err(wrong_reply().into()),
        }
    }

    async fn full_snapshot(
        &self,
        target: MemberId,
        group_id: GroupId,
        vote: VoteOf<ShardTypes>,
        snapshot: SnapshotOf<ShardTypes, Vec<u8>>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<ShardTypes>, StreamingError<ShardTypes>> {
        let body = Request::Snapshot {
            vote,
            meta: snapshot.meta,
        };
        let reply = tokio::select! {
            closed = cancel => return Err(closed.into()),
            reply = self.call(target, group_id, body, &option) => reply?,
        };
        match reply {
            Reply::Snapshot(response) => Ok(response),
            _ => Err(wrong_reply().into()),
        }
    }

    async fn transfer_leader(
        &self,
        target: MemberId,
        group_id: GroupId,
        req: TransferLeaderRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<ShardTypes>, RPCError<ShardTypes>> {
        match self
            .call(target, group_id, Request::TransferLeader(req), &option)
            .await?
        {
            Reply::TransferLeader(response) => Ok(response),
            _ => Err(wrong_reply().into()),
        }
    }
}

/// One group's side of the router.
pub(crate) struct Network {
    pub(crate) router: Router,
    pub(crate) group: GroupId,
}

impl RaftNetworkFactory<ShardTypes> for Network {
    type Network = GroupNetworkAdapter<ShardTypes, GroupId, Router>;

    async fn new_client(&mut self, target: MemberId, _node: &EmptyNode) -> Self::Network {
        GroupNetworkAdapter::new(self.router.clone(), target, self.group.clone())
    }
}

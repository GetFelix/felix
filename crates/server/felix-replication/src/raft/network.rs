//! Raft messages between brokers: what the runtime hands a
//! [`RaftTransport`] to send, and the router that turns openraft's calls
//! into them.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use openraft::EntryPayload;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::impls::EmptyNode;
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendError, AppendResult, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft_multi::{GroupNetworkAdapter, GroupRouter};

use felix_storage::raft_log::LogPoint;

use super::group::Pace;
use super::lease::Lease;
use super::types::{GroupId, MemberId, ShardTypes};
use super::written::Reports;

/// One Raft message for one member of one group. What is in it is this
/// module's business; a transport carries it to [`Self::to`]'s broker.
pub struct RaftRequest {
    pub(crate) group: GroupId,
    pub(crate) to: MemberId,
    /// On a leader's append: the longest the leader may now stay silent,
    /// in milliseconds. Zero on every other message.
    pub(crate) beat: u64,
    /// On a leader's append that carries entries: the leader wants to be
    /// told once they are written, before their fsync.
    pub(crate) report: bool,
    /// How long the sender waits for the answer. The receiver drops an
    /// answer it cannot give in time, so a stuck member holds nothing.
    pub(crate) deadline: Duration,
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
    /// A follower's written position: the last entry `from` has written,
    /// fsynced or not. Evidence for that member and that log id only.
    Written {
        from: MemberId,
        at: LogPoint,
    },
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
            Request::Written { .. } => "written",
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
    /// A written position was taken. It says nothing back.
    Written,
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
    #[error("{0} is not on this broker")]
    NoSuchMember(Box<Addressee>),
    /// The member is there and has stopped.
    #[error("{0} has stopped: {1}")]
    Stopped(Box<Addressee>, String),
    /// A shard group's snapshot holds no records, so a member that took one
    /// would purge records it was never sent. No member takes one.
    #[error("{0} takes no snapshot: a shard's records are only in its log")]
    NoSnapshots(Box<Addressee>),
}

/// The member a message was for. Boxed in [`Refused`] to keep the error
/// small.
#[derive(Debug)]
pub struct Addressee {
    pub group: GroupId,
    pub to: MemberId,
}

impl fmt::Display for Addressee {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} of {}", self.to, self.group)
    }
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
            Request::Written { .. } => Reply::Written,
        }))
    }
}

/// openraft's view of the transport: one group's calls, tagged with the
/// group. This is the seam heartbeat coalescing wraps.
#[derive(Clone)]
pub(crate) struct Router {
    transport: Arc<dyn RaftTransport>,
    pace: Arc<Pace>,
    lease: Arc<Lease>,
    reports: Arc<Reports>,
}

#[derive(Debug, thiserror::Error)]
#[error("the answer was to another kind of message")]
struct WrongReply;

impl Router {
    pub(crate) fn new(
        transport: Arc<dyn RaftTransport>,
        pace: Arc<Pace>,
        lease: Arc<Lease>,
        reports: Arc<Reports>,
    ) -> Self {
        Self {
            transport,
            pace,
            lease,
            reports,
        }
    }

    async fn call(
        &self,
        to: MemberId,
        group: GroupId,
        mut body: Request,
        option: &RPCOption,
    ) -> Result<Reply, Unreachable<ShardTypes>> {
        // A transport inside one process hands the message over as it is.
        // A token that went with it would be resolved by the follower's
        // append and tell the publisher the leader had written.
        if let Request::Append(rpc) = &mut body {
            for entry in &mut rpc.entries {
                if let EntryPayload::Normal(command) = &mut entry.payload {
                    command.written = None;
                }
            }
        }
        let sent = match &body {
            Request::Append(rpc) => Some(self.pace.sending(&to, !rpc.entries.is_empty())),
            _ => None,
        };
        // The members that take this stop refusing votes, and the target
        // stands at once. The lease for the term ends before it leaves.
        if let Request::TransferLeader(transfer) = &body {
            self.lease.void_through(term_of(transfer.from_leader()));
        }
        let pre_vote = matches!(body, Request::PreVote(_));
        let request = RaftRequest {
            group,
            to: to.clone(),
            beat: sent.as_ref().map_or(0, |sent| sent.gap),
            report: matches!(&body, Request::Append(rpc) if !rpc.entries.is_empty())
                && self.reports.wanted(),
            deadline: option.hard_ttl(),
            body,
        };
        let probe = match &request.body {
            Request::Append(rpc) if !rpc.entries.is_empty() => {
                felix_storage::stage_probe::RPC_ENTRIES_N.value(rpc.entries.len() as u64);
                Some(std::time::Instant::now())
            }
            _ => None,
        };
        let answer = tokio::time::timeout(option.hard_ttl(), self.transport.send(request)).await;
        if let Some(probe) = probe {
            felix_storage::stage_probe::RPC_ROUND_TRIP.since(probe);
        }
        if let Some(sent) = &sent {
            match &answer {
                Ok(Ok(RaftReply(Reply::Append(Ok(_))))) => self.pace.answered(&to, sent),
                _ => self.pace.unanswered(&to),
            }
        }
        // Before openraft sees the answer: it adopts the vote and may grant
        // that candidate at once.
        if let Ok(Ok(RaftReply(reply))) = &answer
            && let Some(term) = named(reply, pre_vote)
        {
            self.lease.named(term);
        }
        match answer {
            Ok(Ok(reply)) => Ok(reply.0),
            Ok(Err(refused)) => Err(Unreachable::new(&refused)),
            Err(elapsed) => Err(Unreachable::new(&elapsed)),
        }
    }
}

pub(crate) fn term_of(vote: &VoteOf<ShardTypes>) -> u64 {
    use openraft::vote::RaftLeaderId;
    vote.leader_id().term()
}

/// The term of the vote an answer names, when openraft acts on that vote.
/// A pre-vote's answer changes no vote.
pub(crate) fn named(reply: &Reply, pre_vote: bool) -> Option<u64> {
    match reply {
        Reply::Append(Err(AppendError::HigherVote(vote))) => Some(term_of(vote)),
        Reply::Vote(response) if !pre_vote => Some(term_of(&response.vote)),
        Reply::Snapshot(response) => Some(term_of(&response.vote)),
        Reply::Append(_) | Reply::Vote(_) | Reply::TransferLeader(_) | Reply::Written => None,
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

    /// Every member refuses this (`Refused::NoSnapshots`). A leader keeps
    /// its whole log, so it has no reason to send one.
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

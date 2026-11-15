//! Raft messages between brokers: what the runtime hands a
//! [`RaftTransport`] to send, and the router that turns openraft's calls
//! into them.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, FuturesOrdered};
use futures::{Stream, StreamExt};
use openraft::EntryPayload;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::impls::EmptyNode;
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{Backoff, RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendError, AppendResult, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft_multi::{GroupNetworkAdapter, GroupRouter};

use felix_storage::raft_log::LogPoint;

use super::group::Pace;
use super::lease::Lease;
use super::machine::SnapshotData;
use super::reach::{Reach, Refusals};
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
    /// One piece of a snapshot. A cache group's snapshot is its state and
    /// goes in as many pieces as it takes, in order; the member installs it
    /// when the last one arrives. A stream group's has no data, and no
    /// member takes it.
    Snapshot {
        vote: VoteOf<ShardTypes>,
        meta: SnapshotMetaOf<ShardTypes>,
        /// The whole snapshot's size, the same on every piece, so the
        /// member can refuse one over its cap at the first.
        data_len: u64,
        /// Where `data` starts in it.
        offset: u64,
        data: Bytes,
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
    /// The broker holds this group's member under another copy id, the one
    /// given. The broker that forms a group learns its members' copy ids
    /// from this.
    #[error("{0} is not on this broker, which holds copy {1:016x} of the group")]
    OtherCopy(Box<Addressee>, u64),
    /// The member is there and has stopped.
    #[error("{0} has stopped: {1}")]
    Stopped(Box<Addressee>, String),
    /// A stream group's snapshot holds no records, so a member that took
    /// one would purge records it was never sent. No member takes one.
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
    reach: Arc<Reach>,
    refusals: Arc<Refusals>,
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
        reach: Arc<Reach>,
        refusals: Arc<Refusals>,
    ) -> Self {
        Self {
            transport,
            pace,
            lease,
            reports,
            reach,
            refusals,
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
        let sent_at = self.reach.now();
        let answer = tokio::time::timeout(option.hard_ttl(), self.transport.send(request)).await;
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
        match &answer {
            Ok(Err(Refused::Unreachable(_))) | Err(_) => self.reach.lost(&to.broker, sent_at),
            // The broker is up, but this group's member is not open there.
            Ok(Err(Refused::NoSuchMember(_) | Refused::OtherCopy(..) | Refused::Stopped(..))) => {
                self.reach.answered(&to.broker);
                self.refusals.refused(&to.broker);
            }
            Ok(_) => {
                self.reach.answered(&to.broker);
                if self.refusals.answered(&to.broker, self.reach.now()) {
                    self.reach.opened(&to.broker, &self.refusals.group);
                }
            }
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
    type SnapshotData = SnapshotData;

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

    /// Sends the snapshot a piece at a time, each sized like an append, and
    /// stops at the first answer under a newer vote. A member of a stream
    /// group refuses it (`Refused::NoSnapshots`): its leader keeps its
    /// whole log, so it has no reason to send one.
    async fn full_snapshot(
        &self,
        target: MemberId,
        group_id: GroupId,
        vote: VoteOf<ShardTypes>,
        snapshot: SnapshotOf<ShardTypes, SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<ShardTypes>, StreamingError<ShardTypes>> {
        let data = snapshot.snapshot.bytes;
        let piece = super::wire::append_budget(option.hard_ttl());
        let sent = async {
            let mut offset = 0;
            loop {
                let end = data.len().min(offset + piece);
                let body = Request::Snapshot {
                    vote: vote.clone(),
                    meta: snapshot.meta.clone(),
                    data_len: data.len() as u64,
                    offset: offset as u64,
                    data: data.slice(offset..end),
                };
                let reply = self
                    .call(target.clone(), group_id.clone(), body, &option)
                    .await?;
                let Reply::Snapshot(response) = reply else {
                    return Err(wrong_reply());
                };
                // A member that has seen a newer vote takes no more of it.
                if end == data.len() || response.vote != vote {
                    return Ok(response);
                }
                offset = end;
            }
        };
        tokio::select! {
            closed = cancel => Err(closed.into()),
            sent = sent => Ok(sent?),
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
    type Network = Pipelined;

    async fn new_client(&mut self, target: MemberId, _node: &EmptyNode) -> Self::Network {
        Pipelined {
            #[cfg(test)]
            opened: (self.router.reach.clone(), target.broker.clone()),
            unary: GroupNetworkAdapter::new(self.router.clone(), target, self.group.clone()),
        }
    }
}

/// Appends a leader keeps on their way to one follower at once. A follower
/// answers an append only once it is fsynced, so with one in flight an
/// entry written during that round trip waits for all of it before it
/// leaves.
pub(crate) const APPEND_WINDOW: usize = 4;

/// One group's calls to one member, with up to [`APPEND_WINDOW`] appends
/// of a replication stream in flight. See [`pipeline`].
pub(crate) struct Pipelined {
    unary: GroupNetworkAdapter<ShardTypes, GroupId, Router>,
    /// Where to count the streams this opens.
    #[cfg(test)]
    opened: (Arc<Reach>, Arc<str>),
}

type Answer = Result<AppendResult<ShardTypes>, RPCError<ShardTypes>>;

/// Sends `input` through `send` with up to `window` appends unanswered, and
/// yields their answers in the order the appends were sent. Until the
/// follower takes one append whole, only one is in flight.
///
/// The stream goes on only past an append the follower took whole. Any
/// other answer is yielded and ends it, and what was sent behind it is
/// dropped unreported, so openraft starts again from what was acknowledged.
/// The router still counts those answers in [`Reach`]: the broker did
/// answer, so it is slow rather than down, and openraft's backoff paces it.
/// A conflict behind an acknowledged append is dropped too: its
/// `prev_log_id` is an entry the follower has just acknowledged, so the
/// follower took it before the append ahead of it (a lost append, or a
/// reconnect that reordered them), and openraft would read the conflict as
/// a follower that lost acknowledged entries. A real conflict shows again
/// on the next stream's first append.
///
/// Three rules keep a stream from throwing its window away:
/// - An append without entries, which openraft sends when only the commit
///   moved, waits until nothing is in flight. It goes on the control
///   stream, not its group's replication stream, so sent behind an append
///   it would usually overtake it. The next append with entries carries a
///   commit at least as new and replaces it.
/// - `fit` cuts an append to what one message may carry, and says if it
///   did. Nothing is sent behind a cut append: it follows entries the
///   follower will not get.
/// - Sending stops at an append under another vote than the first.
///   openraft opens one stream per leader vote.
pub(crate) fn pipeline<S, F, Fut, G>(
    input: S,
    window: usize,
    mut fit: G,
    send: F,
) -> impl Stream<Item = Answer>
where
    S: Stream<Item = AppendEntriesRequest<ShardTypes>> + Unpin,
    F: FnMut(AppendEntriesRequest<ShardTypes>) -> Fut,
    Fut: Future<Output = Answer>,
    G: FnMut(&mut AppendEntriesRequest<ShardTypes>) -> bool,
{
    use openraft::entry::RaftEntry;
    struct Window<S, F, A: Future> {
        input: Option<S>,
        send: F,
        flight: FuturesOrdered<A>,
        /// An append without entries, waiting for the flight to land or
        /// for an append with entries to replace it.
        held: Option<AppendEntriesRequest<ShardTypes>>,
        vote: Option<VoteOf<ShardTypes>>,
        acked: bool,
    }
    async fn next<S: Stream + Unpin>(input: &mut Option<S>) -> Option<S::Item> {
        match input {
            Some(input) => input.next().await,
            None => std::future::pending().await,
        }
    }
    // The answer is checked against the append openraft made, so a cut
    // one reads as the partial acknowledgement it is.
    let launch = move |send: &mut F, mut rpc: AppendEntriesRequest<ShardTypes>| {
        let last: Option<LogIdOf<ShardTypes>> = rpc
            .entries
            .last()
            .map(|entry| entry.log_id())
            .or(rpc.prev_log_id);
        let cut = fit(&mut rpc);
        let answer = send(rpc);
        (async move { (last, answer.await) }, cut)
    };
    let window = window.max(1);
    let start = Window {
        input: Some(input),
        send,
        flight: FuturesOrdered::new(),
        held: None,
        vote: None,
        acked: false,
    };
    futures::stream::unfold(Some((start, launch)), move |state| async move {
        let (mut state, mut launch) = state?;
        loop {
            if state.flight.is_empty()
                && let Some(rpc) = state.held.take()
            {
                let (answer, _) = launch(&mut state.send, rpc);
                state.flight.push_back(answer);
            }
            // One at a time until the follower takes an append whole: a
            // follower that is down or behind costs one append per stream.
            let open = if state.acked { window } else { 1 };
            let room = state.input.is_some() && state.flight.len() < open;
            if !room && state.flight.is_empty() && state.held.is_none() {
                return None;
            }
            tokio::select! {
                // Answers first, so a stream that has to end sends no more.
                biased;
                Some((last, answer)) = state.flight.next(), if !state.flight.is_empty() => {
                    return match answer {
                        Ok(Ok(acked)) if acked == last => {
                            state.acked = true;
                            Some((Ok(Ok(acked)), Some((state, launch))))
                        }
                        Ok(Err(AppendError::Conflict { .. })) if state.acked => None,
                        answer => Some((answer, None)),
                    };
                }
                rpc = next(&mut state.input), if room => match rpc {
                    Some(rpc) if *state.vote.get_or_insert_with(|| rpc.vote.clone()) != rpc.vote => {
                        state.input = None;
                    }
                    Some(rpc) if rpc.entries.is_empty() && !state.flight.is_empty() => {
                        state.held = Some(rpc);
                    }
                    Some(rpc) => {
                        state.held = None;
                        let (answer, cut) = launch(&mut state.send, rpc);
                        state.flight.push_back(answer);
                        if cut {
                            state.input = None;
                        }
                    }
                    None => state.input = None,
                },
            }
        }
    })
}

impl RaftNetworkV2<ShardTypes> for Pipelined {
    type SnapshotData = SnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<AppendResult<ShardTypes>, RPCError<ShardTypes>> {
        self.unary.append_entries(rpc, option).await
    }

    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        option: RPCOption,
    ) -> BoxFuture<'s, Result<BoxStream<'s, Answer>, RPCError<ShardTypes>>>
    where
        S: Stream<Item = AppendEntriesRequest<ShardTypes>> + Send + Unpin + 'static,
    {
        #[cfg(test)]
        {
            let (reach, broker) = &self.opened;
            *reach.streams.lock().entry(broker.clone()).or_default() += 1;
        }
        let unary = self.unary.clone();
        let deadline = option.hard_ttl();
        let fit = move |rpc: &mut AppendEntriesRequest<ShardTypes>| {
            super::wire::fit_entries(rpc, deadline)
        };
        let answers = pipeline(input, APPEND_WINDOW, fit, move |rpc| {
            let mut unary = unary.clone();
            let option = option.clone();
            async move { unary.append_entries(rpc, option).await }
        });
        Box::pin(async move { Ok(Box::pin(answers) as BoxStream<'s, Answer>) })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<VoteResponse<ShardTypes>, RPCError<ShardTypes>> {
        self.unary.vote(rpc, option).await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<VoteResponse<ShardTypes>, RPCError<ShardTypes>> {
        self.unary.pre_vote(rpc, option).await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<ShardTypes>,
        snapshot: SnapshotOf<ShardTypes, SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<ShardTypes>, StreamingError<ShardTypes>> {
        self.unary
            .full_snapshot(vote, snapshot, cancel, option)
            .await
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<ShardTypes>, RPCError<ShardTypes>> {
        self.unary.transfer_leader(req, option).await
    }

    fn backoff(&self) -> Option<Backoff> {
        self.unary.backoff()
    }
}

#[cfg(test)]
mod tests;

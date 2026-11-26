//! Raft messages between brokers: what the runtime hands a
//! [`RaftTransport`] to send, and the router that turns openraft's calls
//! into them.

mod client;

pub(crate) use client::{Hold, Links};

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use openraft::entry::RaftEntry;
use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::impls::EmptyNode;
use openraft::network::{Backoff, RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendError, AppendResult, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::{Config, EntryPayload};
use openraft_multi::{GroupNetworkAdapter, GroupRouter};

use felix_storage::raft_log::LogPoint;

use super::group::{Pace, Sent};
use super::lease::Lease;
use super::machine::SnapshotData;
use super::reach::{Reach, Refusals, Waits};
use super::types::{GroupId, MemberId, ShardTypes};
use super::written::Reports;
use crate::ShardKind;
pub(crate) use client::Client;

/// One Raft message for one member of one group. What is in it is this
/// module's business; a transport carries it to [`Self::to`]'s broker.
pub struct RaftRequest {
    pub(crate) group: GroupId,
    pub(crate) to: MemberId,
    /// On a leader's append: the gap it names. Zero on every other message.
    pub(crate) beat: Beat,
    /// On a leader's append that carries entries: what the leader asks of
    /// the follower about telling it, once they are written and before their
    /// fsync. See [`Ask`] for the three values.
    pub(crate) report: Ask,
    /// How long the sender waits for the answer. The receiver drops an
    /// answer it cannot give in time, so a stuck member holds nothing.
    pub(crate) deadline: Duration,
    pub(crate) body: Request,
}

/// The gap a leader's append names: the longest the leader may now stay
/// silent, in milliseconds, and the append's send number, which orders it
/// against the leader's other appends to the same follower.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Beat {
    pub(crate) gap: u64,
    pub(crate) seq: u64,
}

/// What a leader's append asks a follower for: a written report
/// (`Request::Written`) once its entries are written, before their fsync.
/// See `docs/per-shard-raft.md`, "What each signal means".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ask {
    /// No report: not an append with entries, or no write waits on reports.
    Nothing,
    /// Report once written. The report lets a `Quorum` write go without
    /// waiting for the commit to reach the leader's state machine. A
    /// follower whose log answers at the write sends none: the leader
    /// counts its answer instead.
    Written,
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
    /// A follower asking its leader for a read index before it serves a
    /// read that could start below a head trim's floor.
    ReadIndex,
    /// A member serving a cache watch asking its leader to append a
    /// [`super::cache::CacheCommand::Watched`], so every member records.
    Watched,
}

impl RaftRequest {
    pub fn group(&self) -> &GroupId {
        &self.group
    }

    /// The member this is for. Its broker is where the message goes.
    pub fn to(&self) -> &MemberId {
        &self.to
    }

    /// Whether this is a leader's append carrying entries, for a test
    /// network that delays or drops those.
    #[cfg(feature = "testing")]
    pub fn carries_entries(&self) -> bool {
        matches!(&self.body, Request::Append(append) if !append.entries.is_empty())
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
            Request::ReadIndex => "read-index",
            Request::Watched => "watched",
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
    /// One past the index the asking follower must apply before it reads.
    /// `None` when this member cannot give one: it does not lead, or a
    /// majority did not confirm that it still does.
    ReadIndex {
        index: Option<u64>,
    },
    /// A watched ask was taken. Whether a mark follows shows in the log.
    Watched,
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
    /// Sent with other groups' heartbeats, the member had not answered by
    /// the time the others had. Its broker is up; the member may be busy.
    #[error("{0} did not answer while the other heartbeats sent with it did")]
    Slow(Box<Addressee>),
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
///
/// [`send_beats`](Self::send_beats) takes heartbeats for members on one
/// broker that were sent within a short window, and answers each in order.
/// By default it sends each on its own.
#[async_trait]
pub trait RaftTransport: Send + Sync + 'static {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused>;

    /// Heartbeats (appends with no entries) for members on one broker, all
    /// to go at once: one answer for each, in the same order. `wait` is how
    /// long the receiver waits for a member's answer once the others have
    /// answered.
    async fn send_beats(
        &self,
        beats: Vec<RaftRequest>,
        wait: Duration,
    ) -> Vec<Result<RaftReply, Refused>> {
        let _ = wait;
        send_each(self, beats).await
    }
}

/// Sends each heartbeat on its own, all at once.
pub(crate) async fn send_each<T: RaftTransport + ?Sized>(
    transport: &T,
    beats: Vec<RaftRequest>,
) -> Vec<Result<RaftReply, Refused>> {
    futures::future::join_all(beats.into_iter().map(|beat| transport.send(beat))).await
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
            Request::ReadIndex | Request::Watched => {
                return Err(Refused::Unreachable("no peer leads".into()));
            }
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
    /// openraft's settings, for its usual retry delays.
    config: Arc<Config>,
    /// The broker of the one member this copy calls, when its retries may
    /// wait longer while that broker is down.
    to: Option<Arc<str>>,
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
        config: Arc<Config>,
    ) -> Self {
        Self {
            transport,
            pace,
            lease,
            reports,
            reach,
            refusals,
            config,
            to: None,
        }
    }

    /// This router, for openraft's client to `target` in `group`. A cache
    /// shard keeps openraft's delays: they also pace its snapshot pieces,
    /// whose wait the ticker's walk cannot cut short. A stream shard's
    /// snapshot pieces would wait up to 10 s the same way; that is harmless
    /// only while stream members refuse snapshots (`Group::answer`).
    fn toward(&self, target: &MemberId, group: &GroupId) -> Self {
        Self {
            to: (group.shard.kind == ShardKind::Stream).then(|| Arc::clone(&target.broker)),
            ..self.clone()
        }
    }

    async fn call(
        &self,
        to: MemberId,
        group: GroupId,
        mut body: Request,
        option: &RPCOption,
    ) -> Result<Reply, RPCError<ShardTypes>> {
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
        let mut sent = Pending {
            pace: &self.pace,
            to: &to,
            sent: match &body {
                Request::Append(rpc) => {
                    let new = rpc
                        .entries
                        .last()
                        .is_some_and(|last| self.pace.new_entries(&last.log_id()));
                    Some(self.pace.sending(&to, new))
                }
                _ => None,
            },
        };
        let append = sent.sent.is_some();
        // The members that take this stop refusing votes, and the target
        // stands at once. The lease for the term ends before it leaves.
        if let Request::TransferLeader(transfer) = &body {
            self.lease.void_through(term_of(transfer.from_leader()));
        }
        let pre_vote = matches!(body, Request::PreVote(_));
        let _asking =
            matches!(body, Request::Vote(_) | Request::PreVote(_)).then(|| self.pace.asking());
        if let Request::Vote(vote) = &body {
            self.pace.asking_at(term_of(&vote.vote));
        }
        let deadline = super::wire::append_deadline(option.hard_ttl(), &body);
        let report = match &body {
            Request::Append(rpc) if !rpc.entries.is_empty() => self.reports.ask(),
            _ => Ask::Nothing,
        };
        let request = RaftRequest {
            group,
            to: to.clone(),
            beat: sent.sent.as_ref().map_or(Beat::default(), |sent| Beat {
                gap: sent.gap,
                seq: sent.seq,
            }),
            report,
            deadline,
            body,
        };
        let sent_at = self.reach.now();
        let answer = tokio::time::timeout(deadline, self.transport.send(request)).await;
        if let Some(sent) = sent.sent.take() {
            match &answer {
                Ok(Ok(RaftReply(Reply::Append(Ok(_))))) => self.pace.answered(&to, &sent),
                _ => self.pace.unanswered(&to),
            }
        }
        if report == Ask::Written
            && let Ok(Ok(RaftReply(Reply::Append(Ok(Some(last)))))) = &answer
        {
            self.reports.answered(&to, last);
        }
        // Before openraft sees the answer: it adopts the vote and may grant
        // that candidate at once.
        if let Ok(Ok(RaftReply(reply))) = &answer
            && let Some(term) = named(reply, pre_vote)
        {
            self.lease.named(term);
        }
        match &answer {
            Ok(Err(Refused::Unreachable(_))) | Err(_) => {
                self.reach.lost(&to.broker, sent_at);
                // Only a leader's append: a candidate's vote leaves the
                // ticker nothing to heartbeat.
                if append {
                    self.reach.lost_by(&to.broker, &self.refusals.group);
                }
            }
            // The broker is up, but this group's member is not open there.
            Ok(Err(Refused::NoSuchMember(_) | Refused::OtherCopy(..) | Refused::Stopped(..))) => {
                self.reach.answered(&to.broker);
                self.refusals.refused(&to.broker);
            }
            // The broker answered for the other groups in the batch.
            Ok(Err(Refused::Slow(_))) => self.reach.answered(&to.broker),
            Ok(_) => {
                self.reach.answered(&to.broker);
                if self.refusals.answered(&to.broker, self.reach.now()) {
                    self.reach.opened(&to.broker, &self.refusals.group);
                }
            }
        }
        match answer {
            Ok(Ok(reply)) => Ok(reply.0),
            Ok(Err(refused)) => Err(refusal_error(&refused)),
            Err(elapsed) => Err(Unreachable::new(&elapsed).into()),
        }
    }
}

/// An append's [`Sent`] until its answer is in. A send dropped before then
/// counts as unanswered: the peer may not have taken it.
struct Pending<'a> {
    pace: &'a Pace,
    to: &'a MemberId,
    sent: Option<Sent>,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if self.sent.take().is_some() {
            self.pace.unanswered(self.to);
        }
    }
}

pub(crate) fn term_of(vote: &VoteOf<ShardTypes>) -> u64 {
    use openraft::vote::RaftLeaderId;
    vote.leader_id().term()
}

/// The term of the vote an answer names, when openraft acts on that vote.
/// A pre-vote's refusal can carry a higher vote, which openraft adopts
/// uncommitted, but only on a member that is standing and holds no lease.
pub(crate) fn named(reply: &Reply, pre_vote: bool) -> Option<u64> {
    match reply {
        Reply::Append(Err(AppendError::HigherVote(vote))) => Some(term_of(vote)),
        Reply::Vote(response) if !pre_vote => Some(term_of(&response.vote)),
        Reply::Snapshot(response) => Some(term_of(&response.vote)),
        Reply::Append(_)
        | Reply::Vote(_)
        | Reply::TransferLeader(_)
        | Reply::Written
        | Reply::ReadIndex { .. }
        | Reply::Watched => None,
    }
}

/// A pre-vote answer as the candidate takes it: a refusal names its vote
/// uncommitted, so only a really higher vote ends the round (#2889). See
/// `docs/per-shard-raft.md`, "Idle groups".
pub(crate) fn pre_vote_answer(mut answer: VoteResponse<ShardTypes>) -> VoteResponse<ShardTypes> {
    use openraft::vote::RaftVote;
    if !answer.vote_granted && answer.vote.is_committed() {
        answer.vote = VoteOf::<ShardTypes>::from_leader_id(answer.vote.leader_id().clone(), false);
    }
    answer
}

fn wrong_reply() -> RPCError<ShardTypes> {
    Unreachable::new(&WrongReply).into()
}

/// What openraft is told of a refusal. A slow group's broker is up, and a
/// busy group's replication must not back off for it (`Unreachable` ranks
/// past openraft's backoff threshold, `Network` does not).
pub(crate) fn refusal_error(refused: &Refused) -> RPCError<ShardTypes> {
    match refused {
        Refused::Slow(_) => NetworkError::new(refused).into(),
        _ => Unreachable::new(refused).into(),
    }
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
            _ => Err(wrong_reply()),
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
            _ => Err(wrong_reply()),
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
            Reply::Vote(response) => Ok(pre_vote_answer(response)),
            _ => Err(wrong_reply()),
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
            _ => Err(wrong_reply()),
        }
    }

    /// openraft's own retry delays, which grow further while a stream
    /// shard member's broker is down.
    fn backoff(&self) -> Option<Backoff> {
        let broker = Arc::clone(self.to.as_ref()?);
        let usual = self.config.build_backoff();
        Some(Backoff::new(Waits::new(
            usual,
            Arc::clone(&self.reach),
            broker,
        )))
    }
}

/// One group's side of the router.
pub(crate) struct Network {
    pub(crate) router: Router,
    pub(crate) group: GroupId,
    /// [`super::RuntimeConfig::commit_hold`], on the runtime's grid.
    pub(crate) commit_hold: Hold,
    /// What each follower's two clients share.
    pub(crate) links: Arc<Links>,
}

impl Network {
    fn client(&self, target: &MemberId, hold: Option<Hold>) -> Client {
        let router = self.router.toward(target, &self.group);
        Client {
            adapter: GroupNetworkAdapter::new(router, target.clone(), self.group.clone()),
            hold,
            link: self.links.to(target),
        }
    }
}

impl RaftNetworkFactory<ShardTypes> for Network {
    type Network = Client;

    async fn new_client(&mut self, target: MemberId, _node: &EmptyNode) -> Self::Network {
        self.client(&target, Some(self.commit_hold))
    }

    /// openraft's heartbeats are never held, and each ends the hold of a
    /// commit-only append to the same follower, which then rides along.
    async fn new_heartbeat_client(&mut self, target: MemberId, _node: &EmptyNode) -> Self::Network {
        self.client(&target, None)
    }
}

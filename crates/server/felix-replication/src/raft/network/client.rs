//! openraft's client to one member of one group.
//!
//! It sends a leader's appends one at a time, as openraft's default does,
//! with one difference. Right after a commit, openraft sends an append
//! with no entries whose only news is the new commit index. Such an append
//! waits up to the runtime's `commit_hold` for the append after it, which
//! carries the same or a later commit and so replaces it. Under load that
//! saves one message per follower per commit; with nothing more to send,
//! the commit goes out once the hold ends. Heartbeats do not come through
//! here. See docs/per-shard-raft.md, "A commit goes on the next append".

use std::time::Duration;

use futures::{Stream, StreamExt};
use openraft::base::{BoxFuture, BoxStream};
use openraft::entry::RaftEntry;
use openraft::error::{RPCError, ReplicationClosed, StreamingError};
use openraft::network::{Backoff, NetAppend, RPCOption, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft_multi::GroupNetworkAdapter;

use super::Router;
use crate::raft::machine::SnapshotData;
use crate::raft::types::{GroupId, ShardTypes};

type Append = AppendEntriesRequest<ShardTypes>;
type Answer = Result<AppendResult<ShardTypes>, RPCError<ShardTypes>>;

pub(crate) struct Client {
    pub(super) adapter: GroupNetworkAdapter<ShardTypes, GroupId, Router>,
    /// The longest an append with no entries waits for the next one.
    pub(super) hold: Duration,
}

impl RaftNetworkV2<ShardTypes> for Client {
    type SnapshotData = SnapshotData;

    async fn append_entries(&mut self, rpc: Append, option: RPCOption) -> Answer {
        RaftNetworkV2::append_entries(&mut self.adapter, rpc, option).await
    }

    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        option: RPCOption,
    ) -> BoxFuture<'s, Result<BoxStream<'s, Answer>, RPCError<ShardTypes>>>
    where
        S: Stream<Item = Append> + Send + Unpin + 'static,
    {
        let hold = self.hold;
        stream_append(self, input, option, hold)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<VoteResponse<ShardTypes>, RPCError<ShardTypes>> {
        self.adapter.vote(rpc, option).await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<VoteResponse<ShardTypes>, RPCError<ShardTypes>> {
        self.adapter.pre_vote(rpc, option).await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<ShardTypes>,
        snapshot: SnapshotOf<ShardTypes, SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<ShardTypes>, StreamingError<ShardTypes>> {
        self.adapter
            .full_snapshot(vote, snapshot, cancel, option)
            .await
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<ShardTypes>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<ShardTypes>, RPCError<ShardTypes>> {
        self.adapter.transfer_leader(req, option).await
    }

    fn backoff(&self) -> Option<Backoff> {
        self.adapter.backoff()
    }
}

/// openraft's requests, with the entry-less ones that a later request
/// replaces taken out.
struct Appends<S> {
    input: S,
    /// A request taken from `input` while holding, that did not replace
    /// the held one.
    next: Option<Append>,
    /// `input` has ended and must not be polled again.
    ended: bool,
    hold: Duration,
}

impl<S: Stream<Item = Append> + Unpin> Appends<S> {
    async fn next(&mut self) -> Option<Append> {
        let mut held = match self.next.take() {
            Some(append) => append,
            None if self.ended => return None,
            None => match self.input.next().await {
                Some(append) => append,
                None => {
                    self.ended = true;
                    return None;
                }
            },
        };
        if !held.entries.is_empty() {
            return Some(held);
        }
        // Dropping `input.next()` at the deadline loses nothing: the stream
        // keeps the request it was building and hands it over next time.
        let until = tokio::time::Instant::now() + self.hold;
        while !self.ended {
            match tokio::time::timeout_at(until, self.input.next()).await {
                Err(_) => break,
                Ok(None) => self.ended = true,
                Ok(Some(next)) if replaces(&next, &held) => {
                    let entries = !next.entries.is_empty();
                    held = next;
                    if entries {
                        break;
                    }
                }
                Ok(Some(next)) => {
                    self.next = Some(next);
                    break;
                }
            }
        }
        Some(held)
    }
}

/// openraft's sequential `stream_append`, with an entry-less append held
/// for up to `hold` (see the module docs).
///
/// openraft needs no answer per request: an answer for the replacing
/// append acknowledges every request up to its matching log id, the held
/// one included, and is the answer the follower would give to the two in
/// a row. A partial answer or an error ends the stream, as in openraft's.
fn stream_append<'s, N, S>(
    network: &'s mut N,
    input: S,
    option: RPCOption,
    hold: Duration,
) -> BoxFuture<'s, Result<BoxStream<'s, Answer>, RPCError<ShardTypes>>>
where
    N: NetAppend<ShardTypes> + ?Sized,
    S: Stream<Item = Append> + Send + Unpin + 'static,
{
    let appends = Appends {
        input,
        next: None,
        ended: false,
        hold,
    };
    let answers = futures::stream::unfold(Some((network, appends)), move |state| {
        let option = option.clone();
        async move {
            let (network, mut appends) = state?;
            let append = appends.next().await?;
            let prev = append.prev_log_id;
            let last = append.entries.last().map(|entry| entry.log_id()).or(prev);
            let answer = network.append_entries(append, option).await;
            if let Ok(Ok(acked)) = &answer {
                debug_assert!(
                    prev <= *acked && *acked <= last,
                    "acknowledged outside the sent range"
                );
            }
            let whole = matches!(&answer, Ok(Ok(acked)) if *acked == last);
            Some((answer, whole.then_some((network, appends))))
        }
    });
    let answers: BoxStream<'s, Answer> = Box::pin(answers);
    Box::pin(async move { Ok(answers) })
}

/// Whether the follower's answer to `next` is also its answer to `held`,
/// an entry-less append sent just before. Under the same vote and at the
/// same previous log id, the follower checks the same entry, and it
/// commits at least as far, since `next` names a commit no lower.
fn replaces(next: &Append, held: &Append) -> bool {
    next.vote == held.vote
        && next.prev_log_id == held.prev_log_id
        && next.leader_commit >= held.leader_commit
}

#[cfg(test)]
mod tests;

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
//!
//! When heartbeats go together (`beat_window` above zero), the hold is
//! not a wait of its own: such an append goes at once into the heartbeat
//! window, and is taken back from it if the next append comes first.

use std::sync::Arc;
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
use openraft::type_config::alias::{LogIdOf, SnapshotOf, VoteOf};
use openraft_multi::GroupNetworkAdapter;

use super::Router;
use crate::raft::beats::{self, Held};
use crate::raft::machine::SnapshotData;
use crate::raft::types::{GroupId, ShardTypes};

type Append = AppendEntriesRequest<ShardTypes>;
type LogId = LogIdOf<ShardTypes>;
type Answer = Result<AppendResult<ShardTypes>, RPCError<ShardTypes>>;

pub(crate) struct Client {
    pub(super) adapter: GroupNetworkAdapter<ShardTypes, GroupId, Router>,
    /// The longest an append with no entries waits for the next one.
    pub(super) hold: Duration,
    /// Heartbeats go together, so such an append waits in their window
    /// instead.
    pub(super) beats: bool,
    /// [`super::super::RuntimeConfig::beat_window`].
    pub(super) window: Duration,
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
        let wait = match (self.hold.is_zero(), self.beats) {
            (false, true) => Wait::InWindow {
                before: self.hold.saturating_sub(self.window),
            },
            _ => Wait::Hold(self.hold),
        };
        stream_append(self, input, option, wait)
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
    wait: Wait,
}

/// Where an entry-less append waits for the next one.
#[derive(Clone, Copy)]
enum Wait {
    /// Here, for up to this long.
    Hold(Duration),
    /// In the heartbeat window it is sent into, from which it is taken
    /// back if the next append comes first ([`super::super::beats::Held`]),
    /// after a hold of `before`: what the hold has beyond the window, so
    /// the two together wait as long as the hold alone did.
    InWindow { before: Duration },
}

/// How a send into the heartbeat window ended.
enum Sent {
    Answered(Answer),
    /// Taken back for this append, which replaces it.
    Replaced(Append),
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
        let hold = match self.wait {
            _ if !held.entries.is_empty() => return Some(held),
            Wait::InWindow { before } if before.is_zero() => return Some(held),
            Wait::InWindow { before } => before,
            Wait::Hold(hold) => hold,
        };
        // Dropping `input.next()` at the deadline loses nothing: the stream
        // keeps the request it was building and hands it over next time.
        let until = tokio::time::Instant::now() + hold;
        while !self.ended {
            #[cfg(test)]
            crate::raft::beats::counts::timer();
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

    /// Sends `held`, an entry-less append, into the heartbeat window, and
    /// takes it back if the next append comes while it waits there and
    /// replaces it. Taken back, it was never sent: the window leaves it
    /// out. Once it has gone, the next append waits for its answer, as
    /// every append waits for the one before.
    async fn send_held<N>(&mut self, network: &mut N, held: Append, option: &RPCOption) -> Sent
    where
        N: NetAppend<ShardTypes> + ?Sized,
    {
        let key = Replaceable {
            vote: held.vote.clone(),
            prev_log_id: held.prev_log_id,
            leader_commit: held.leader_commit,
        };
        let ticket = Held::new();
        let call = beats::holding(
            Arc::clone(&ticket),
            network.append_entries(held, option.clone()),
        );
        tokio::pin!(call);
        while !self.ended {
            tokio::select! {
                biased;
                answer = &mut call => return Sent::Answered(answer),
                next = self.input.next() => match next {
                    None => self.ended = true,
                    Some(next)
                        if !next.entries.is_empty()
                            && key.replaced_by(&next)
                            && ticket.withdraw() =>
                    {
                        return Sent::Replaced(next);
                    }
                    Some(next) => {
                        self.next = Some(next);
                        break;
                    }
                },
            }
        }
        Sent::Answered(call.await)
    }
}

/// What [`replaces`] reads of the held append.
struct Replaceable {
    vote: VoteOf<ShardTypes>,
    prev_log_id: Option<LogId>,
    leader_commit: Option<LogId>,
}

impl Replaceable {
    fn replaced_by(&self, next: &Append) -> bool {
        next.vote == self.vote
            && next.prev_log_id == self.prev_log_id
            && next.leader_commit >= self.leader_commit
    }
}

/// openraft's sequential `stream_append`, with an entry-less append held
/// for the next one as `wait` says (see the module docs).
///
/// openraft needs no answer per request: an answer for the replacing
/// append acknowledges every request up to its matching log id, the held
/// one included, and is the answer the follower would give to the two in
/// a row. A partial answer or an error ends the stream, as in openraft's.
fn stream_append<'s, N, S>(
    network: &'s mut N,
    input: S,
    option: RPCOption,
    wait: Wait,
) -> BoxFuture<'s, Result<BoxStream<'s, Answer>, RPCError<ShardTypes>>>
where
    N: NetAppend<ShardTypes> + ?Sized,
    S: Stream<Item = Append> + Send + Unpin + 'static,
{
    let appends = Appends {
        input,
        next: None,
        ended: false,
        wait,
    };
    let answers = futures::stream::unfold(Some((network, appends)), move |state| {
        let option = option.clone();
        async move {
            let (network, mut appends) = state?;
            let append = appends.next().await?;
            let (prev, last, answer) = match appends.wait {
                Wait::InWindow { .. } if append.entries.is_empty() => {
                    let prev = append.prev_log_id;
                    match appends.send_held(network, append, &option).await {
                        Sent::Answered(answer) => (prev, prev, answer),
                        Sent::Replaced(next) => send(network, next, option).await,
                    }
                }
                _ => send(network, append, option).await,
            };
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

/// Sends `append`, with the range it covers.
async fn send<N>(
    network: &mut N,
    append: Append,
    option: RPCOption,
) -> (Option<LogId>, Option<LogId>, Answer)
where
    N: NetAppend<ShardTypes> + ?Sized,
{
    let prev = append.prev_log_id;
    let last = append.entries.last().map(|entry| entry.log_id()).or(prev);
    (prev, last, network.append_entries(append, option).await)
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

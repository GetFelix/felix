//! openraft's client to one member of one group.
//!
//! It sends a leader's appends one at a time, as openraft's default does,
//! with one difference. Right after a commit, openraft sends an append
//! with no entries whose only news is the new commit index. Such an append
//! is held for the next append, which carries the same or a later commit
//! and so replaces it. Holds end on a grid shared by every group on the
//! broker, so the held appends of many groups to one follower go out at
//! the same instant and ride one heartbeat batch (`raft/beats.rs`). A
//! heartbeat to the follower, or a read on it waiting for the commit, ends
//! the hold at once. See docs/per-shard-raft.md, "A commit goes on the next
//! append".

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use felix_common::random::HashMap;
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
use crate::raft::types::{GroupId, MemberId, ShardTypes};

type Append = AppendEntriesRequest<ShardTypes>;
type Answer = Result<AppendResult<ShardTypes>, RPCError<ShardTypes>>;

pub(crate) struct Client {
    pub(super) adapter: GroupNetworkAdapter<ShardTypes, GroupId, Router>,
    /// How an append with no entries waits for the next one. `None` on the
    /// heartbeat client, whose every append is a heartbeat and goes at once.
    pub(super) hold: Option<Hold>,
    pub(super) link: Arc<Link>,
}

/// How long a commit-only append waits: until the first point of a grid of
/// `every`, counted from `base`, that is at least `every` away. Every group
/// of a runtime shares the grid, so their holds end together.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Hold {
    pub(crate) every: Duration,
    pub(crate) base: tokio::time::Instant,
}

impl Hold {
    /// When a hold that starts at `now` ends: in `[now + every, now + 2 * every)`.
    fn until(&self, now: tokio::time::Instant) -> tokio::time::Instant {
        let every = self.every.as_nanos().max(1);
        let earliest = (now + self.every).saturating_duration_since(self.base);
        let points = earliest.as_nanos().div_ceil(every);
        let past = u64::try_from(points * every).unwrap_or(u64::MAX);
        self.base + Duration::from_nanos(past)
    }
}

/// What a leader's two clients to one follower, for appends and for
/// heartbeats, share.
#[derive(Debug, Default)]
pub(crate) struct Link {
    /// The highest commit index plus one an append to the follower carried.
    sent: AtomicU64,
    /// The highest commit index plus one a reader on the follower waits
    /// for. Above `sent`, a held append that carries it goes at once.
    wanted: AtomicU64,
    /// Heartbeats sent to the follower. A held append goes with the next.
    beats: AtomicU64,
    wake: tokio::sync::Notify,
}

impl Link {
    fn sent(&self, commit: u64) {
        self.sent.fetch_max(commit, Ordering::AcqRel);
    }

    fn beat(&self) {
        self.beats.fetch_add(1, Ordering::AcqRel);
        self.wake.notify_waiters();
    }

    /// Whether a held append that carries commit `end` must go now.
    fn wanted_by(&self, end: u64) -> bool {
        let wanted = self.wanted.load(Ordering::Acquire);
        wanted > self.sent.load(Ordering::Acquire) && end >= wanted
    }
}

/// The links of one group's leader to its followers, by follower.
#[derive(Debug, Default)]
pub(crate) struct Links(parking_lot::Mutex<HashMap<MemberId, Arc<Link>>>);

impl Links {
    pub(crate) fn to(&self, member: &MemberId) -> Arc<Link> {
        Arc::clone(self.0.lock().entry(member.clone()).or_default())
    }

    /// A reader on a follower waits for the commit index to reach `end`
    /// (one past the last index it needs): a held append that carries it
    /// goes to every follower now, instead of at the end of its hold.
    pub(crate) fn want(&self, end: u64) {
        for link in self.0.lock().values() {
            if link.wanted.fetch_max(end, Ordering::AcqRel) < end {
                link.wake.notify_waiters();
            }
        }
    }
}

fn commit_end(append: &Append) -> u64 {
    append.leader_commit.map_or(0, |at| at.index + 1)
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
        let (hold, link) = (self.hold, Arc::clone(&self.link));
        stream_append(&mut self.adapter, input, option, hold, link)
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
    hold: Option<Hold>,
    link: Arc<Link>,
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
        let Some(hold) = self.hold.filter(|hold| !hold.every.is_zero()) else {
            return Some(held);
        };
        if !held.entries.is_empty() {
            return Some(held);
        }
        #[cfg(test)]
        crate::raft::beats::counts::timer();
        let until = tokio::time::sleep_until(hold.until(tokio::time::Instant::now()));
        tokio::pin!(until);
        let beats = self.link.beats.load(Ordering::Acquire);
        while !self.ended {
            let woken = self.link.wake.notified();
            tokio::pin!(woken);
            // Registered before the checks, so a wake between them is kept.
            woken.as_mut().enable();
            if self.link.beats.load(Ordering::Acquire) != beats
                || self.link.wanted_by(commit_end(&held))
            {
                break;
            }
            // Dropping `input.next()` when another branch wins loses
            // nothing: the stream keeps the request it was building and
            // hands it over next time.
            tokio::select! {
                biased;
                next = self.input.next() => match next {
                    None => self.ended = true,
                    Some(next) if replaces(&next, &held) => {
                        let entries = !next.entries.is_empty();
                        held = next;
                        if entries {
                            break;
                        }
                    }
                    Some(next) => {
                        self.next = Some(next);
                        break;
                    }
                },
                () = &mut woken => {}
                () = &mut until => break,
            }
        }
        Some(held)
    }
}

/// openraft's sequential `stream_append`, with an entry-less append held
/// as `hold` says (see the module docs). With no hold, every append is a
/// heartbeat, and the link hears of each.
///
/// openraft needs no answer per request: an answer for the replacing
/// append acknowledges every request up to its matching log id, the held
/// one included, and is the answer the follower would give to the two in
/// a row. A partial answer or an error ends the stream, as in openraft's.
fn stream_append<'s, N, S>(
    network: &'s mut N,
    input: S,
    option: RPCOption,
    hold: Option<Hold>,
    link: Arc<Link>,
) -> BoxFuture<'s, Result<BoxStream<'s, Answer>, RPCError<ShardTypes>>>
where
    N: NetAppend<ShardTypes> + ?Sized,
    S: Stream<Item = Append> + Send + Unpin + 'static,
{
    let beats = hold.is_none();
    let appends = Appends {
        input,
        next: None,
        ended: false,
        hold,
        link,
    };
    let answers = futures::stream::unfold(Some((network, appends)), move |state| {
        let option = option.clone();
        async move {
            let (network, mut appends) = state?;
            let append = appends.next().await?;
            let prev = append.prev_log_id;
            let last = append.entries.last().map(|entry| entry.log_id()).or(prev);
            appends.link.sent(commit_end(&append));
            if beats {
                // Before the send, so a held append joins its batch.
                appends.link.beat();
            }
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

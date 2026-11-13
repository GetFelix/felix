//! Writing a publish through openraft: the queue a shard's batches wait in,
//! the task that hands them to the group, and what a publisher waits on
//! before it is told where its batch landed.
//!
//! The queue is the shard's commit order. A batch is queued when `propose`
//! is called, not when its future is first polled, so a publisher that goes
//! away leaves its batch in place and the one behind keeps its offsets. See
//! `docs/per-shard-raft.md`, "Writing through openraft".

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use felix_storage::log::FsyncMode;
use felix_storage::raft_log::{LogPoint, LogPositions, WrittenAt, WrittenToken};
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{Future, StreamExt};
use openraft::error::{ClientWriteError, Fatal, ForwardReason};
use openraft::impls::ProgressResponder;
use openraft::raft::WriteResult;
use openraft::{Raft, ServerState};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::group::Pace;
use super::machine::Machine;
use super::types::{Command, ShardTypes};
use crate::engine::{CommitError, Written};

/// One record of a batch to write.
#[derive(Debug, Clone)]
pub struct Record {
    pub timestamp_micros: u64,
    pub payload: Bytes,
}

/// What a publisher's acknowledgement waits for on the leader's own log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckAt {
    /// The records are written. A power cut can still take them.
    Written,
    /// The records are fsynced here, or committed, whichever is first.
    Flushed,
}

impl AckAt {
    /// What a stream with fsync policy `mode` waits for: the flush when it
    /// fsyncs on commit, the write otherwise. The Raft log under it fsyncs
    /// on commit either way; this only places the acknowledgement.
    pub fn for_fsync(mode: &FsyncMode) -> Self {
        match mode {
            FsyncMode::OnCommit => Self::Flushed,
            FsyncMode::Periodic { .. } | FsyncMode::None => Self::Written,
        }
    }
}

/// A publish batch for [`super::RaftEngine::propose`]. It must hold a record.
#[derive(Debug, Clone)]
pub struct Batch {
    pub records: Vec<Record>,
    pub ack: AckAt,
    /// The term the caller was admitted at. The batch is written only
    /// while this member leads that term. `None` takes any term.
    pub term: Option<u64>,
}

/// A drain takes no batch that would bring it past this many payload bytes
/// or this many records. openraft appends as much of a drain as it finds
/// waiting in one go, and the group handles nothing else meanwhile. A batch
/// past either on its own is drained alone.
pub(super) const DRAIN_BYTES: usize = 4 * 1024 * 1024;
pub(super) const DRAIN_RECORDS: usize = 4096;

const NOT_LEADING: &str = "this broker does not lead the shard";
const OTHER_TERM: &str =
    "this broker does not lead the term the batch was admitted at, so nothing was written";
const IN_PART: &str = "only some of the batch's records were written";
const ELECTING: &str = "this broker is standing for election, so the batch may still be written";
const LEASE_LAPSED: &str = "no majority has answered this leader lately, so nothing was written";
const DISCARDED: &str = "a newer leader discarded the entry";
const STOPPED: &str = "the shard's group stopped";
const UNKNOWN: &str = "the entry was not reported written";
const EMPTY: &str = "the batch held no record";

pub(crate) fn lost(detail: &'static str) -> CommitError {
    CommitError::LeadershipLost {
        what: "write",
        detail,
    }
}

pub(crate) fn stopped() -> CommitError {
    lost(STOPPED)
}

/// Where a batch's first and last record are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Span {
    pub(crate) first: LogPoint,
    pub(crate) last: LogPoint,
}

impl Span {
    /// Where a batch of `records` landed, if this is one run of that many
    /// entries of one term. Anything else is not the batch as proposed.
    fn written(self, records: u64) -> Result<Written, CommitError> {
        let Self { first, last } = self;
        if first.term != last.term || last.index.checked_sub(first.index) != Some(records - 1) {
            tracing::error!(?first, ?last, records, "a proposed batch is not one run");
            return Err(lost(UNKNOWN));
        }
        Ok(Written {
            end: last.index + 1,
            generation: last.term,
        })
    }
}

/// A batch on its way to the group.
pub(crate) struct Proposal {
    commands: Vec<Command>,
    /// [`Batch::term`].
    term: Option<u64>,
    bytes: usize,
    /// Where openraft applied the batch, or why it did not.
    answer: oneshot::Sender<Result<Span, CommitError>>,
}

/// The publisher's side of a queued batch. `W` is a token's future.
pub(crate) struct Waiting<W = WrittenAt> {
    pub(super) ack: AckAt,
    pub(super) records: u64,
    /// `None` for a batch of one record, where `last` is also the first.
    pub(super) first: Option<W>,
    pub(super) last: W,
    pub(super) answer: oneshot::Receiver<Result<Span, CommitError>>,
}

/// Split `batch` into what is queued and what its publisher waits on.
///
/// The tokens are made here, once per call, and a batch is never proposed
/// twice: a second attempt could land beside the first. The first and the
/// last record each carry one, because one record's index says nothing
/// certain about another's.
pub(crate) fn split(batch: Batch) -> Result<(Proposal, Waiting), CommitError> {
    let Batch { records, ack, term } = batch;
    let count = records.len();
    if count == 0 {
        return Err(lost(EMPTY));
    }
    let mut bytes = 0;
    let mut commands: Vec<Command> = records
        .into_iter()
        .map(|record| {
            bytes += record.payload.len();
            Command {
                timestamp_micros: record.timestamp_micros,
                payload: record.payload,
                written: None,
            }
        })
        .collect();
    let token = |command: &mut Command| {
        let (token, at) = WrittenToken::channel();
        command.written = Some(token);
        at
    };
    let first = (count > 1).then(|| token(&mut commands[0]));
    let last = token(&mut commands[count - 1]);
    let (answer, answered) = oneshot::channel();
    Ok((
        Proposal {
            commands,
            term,
            bytes,
            answer,
        },
        Waiting {
            ack,
            records: count as u64,
            first,
            last,
            answer: answered,
        },
    ))
}

/// The batches of one call, in queue order.
pub(super) struct Drain {
    pub(super) commands: Vec<Command>,
    /// The term every batch of the call was admitted at.
    pub(super) term: Option<u64>,
    /// Each batch's record count and where its answer goes.
    pub(super) answers: Vec<(usize, oneshot::Sender<Result<Span, CommitError>>)>,
}

/// Take `first` and what is queued behind it, up to [`DRAIN_BYTES`] and
/// [`DRAIN_RECORDS`]. A batch that would pass either, or that was admitted
/// at another term than `first`, is left in `held`, to start the next
/// drain, so it keeps its place.
pub(super) fn drain(
    first: Proposal,
    queue: &mut mpsc::UnboundedReceiver<Proposal>,
    held: &mut Option<Proposal>,
) -> Drain {
    let term = first.term;
    let mut bytes = first.bytes;
    let mut commands = first.commands;
    let mut answers = vec![(commands.len(), first.answer)];
    while let Ok(next) = queue.try_recv() {
        if next.term != term
            || bytes + next.bytes > DRAIN_BYTES
            || commands.len() + next.commands.len() > DRAIN_RECORDS
        {
            *held = Some(next);
            break;
        }
        bytes += next.bytes;
        answers.push((next.commands.len(), next.answer));
        commands.extend(next.commands);
    }
    Drain {
        commands,
        term,
        answers,
    }
}

/// Hand a shard's queued batches to its group, in the order they were
/// queued, until `stop` or until the queue's sender is gone.
///
/// Everything queued when the task looks goes down in one call, up to
/// the drain bounds, so batches that arrive while an append runs share
/// the next one. The task does not wait for a
/// call's entries to commit before it makes the next: openraft takes calls
/// in the order they are made.
pub(crate) async fn run(
    raft: Raft<ShardTypes, Machine>,
    pace: Arc<Pace>,
    mut queue: mpsc::UnboundedReceiver<Proposal>,
    stop: CancellationToken,
    calls: Arc<AtomicU64>,
    lease: Lease,
) {
    let mut answering = FuturesUnordered::new();
    let mut held = None;
    loop {
        let first = match held.take() {
            Some(first) => first,
            None => tokio::select! {
                biased;
                () = stop.cancelled() => return,
                Some(()) = answering.next(), if !answering.is_empty() => continue,
                next = queue.recv() => match next {
                    Some(proposal) => proposal,
                    None => return,
                },
            },
        };
        let Drain {
            commands,
            term,
            answers,
        } = drain(first, &mut queue, &mut held);
        if leads(&raft) {
            pace.wrote();
        }
        if alongside(lease.renew(&raft), &mut answering, &stop)
            .await
            .is_none()
        {
            return;
        }
        calls.fetch_add(1, Ordering::Relaxed);
        let sent = alongside(write(&raft, commands, term), &mut answering, &stop).await;
        let Some(sent) = sent else {
            return;
        };
        match sent {
            Ok(results) => answering.push(answer(results, answers, term.is_some())),
            Err(fatal) => {
                tracing::warn!(error = %fatal, "a stopped Raft group took no write");
                for (_, to) in answers {
                    let _ = to.send(Err(stopped()));
                }
            }
        }
    }
}

/// Hand `commands` to openraft, in order. With a term, each is appended
/// only if this member still leads that term, which openraft checks as it
/// appends: what was admitted at one term never lands in another.
pub(super) async fn write(
    raft: &Raft<ShardTypes, Machine>,
    commands: Vec<Command>,
    term: Option<u64>,
) -> Result<BoxStream<'static, Result<WriteResult<ShardTypes>, Fatal<ShardTypes>>>, Fatal<ShardTypes>>
{
    let Some(term) = term else {
        return raft.client_write_many(commands).await;
    };
    let mut answers = Vec::with_capacity(commands.len());
    for command in commands {
        let (responder, answered) = ProgressResponder::complete_only();
        raft.write(command)
            .with_leader(term)
            .responder(responder)
            .await?;
        answers.push(answered);
    }
    let results = futures::stream::iter(answers).then(|answered| async move {
        match answered.await {
            Ok(result) => Ok(result.map(Into::into)),
            Err(_) => Err(Fatal::Stopped),
        }
    });
    Ok(results.boxed())
}

/// Run `work` until it ends or `stop`, and meanwhile hand on the answers of
/// calls already made. A publisher whose call openraft has answered is not
/// held for the next call's lease renewal, which can take a whole lease.
async fn alongside<T>(
    work: impl Future<Output = T>,
    answering: &mut FuturesUnordered<impl Future<Output = ()>>,
    stop: &CancellationToken,
) -> Option<T> {
    let mut work = std::pin::pin!(work);
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return None,
            done = &mut work => return Some(done),
            Some(()) = answering.next(), if !answering.is_empty() => {}
        }
    }
}

/// openraft's write lease, as the publish task has to mind it.
///
/// openraft refuses a write when no majority has answered the leader for
/// its lease, which is the longest election timeout. It does not wait for
/// commit. An idle group heartbeats about that far apart, so its first
/// write after a pause could be refused. The task therefore asks for a
/// heartbeat first and waits for the answer, for up to one lease. The
/// batches behind wait with it in the queue, so nothing is proposed twice
/// and nothing changes place. A write that is refused all the same was not
/// appended. It is not sent again: later calls may be on their way, and it
/// would land behind them.
#[derive(Clone, Copy)]
pub(crate) struct Lease {
    /// openraft's lease: `election_max`.
    pub(crate) length: Duration,
    /// How often the heartbeat is asked for again while no majority has
    /// answered. One can be lost.
    pub(crate) again: Duration,
}

impl Lease {
    /// Whether a majority's last answer is old enough that a write sent
    /// now could find the lease lapsed. Half the lease leaves room for the
    /// write to reach the group. A busy leader is answered every heartbeat
    /// and never gets here, and a leader that is the only voter needs no
    /// answer.
    fn stale(&self, raft: &Raft<ShardTypes, Machine>) -> bool {
        use openraft::Instant as _;
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = raft.metrics();
        let metrics = metrics.borrow_watched();
        if metrics.state != ServerState::Leader {
            return false;
        }
        let voters = metrics.membership_config.membership().voter_ids();
        if voters.take(2).count() < 2 {
            return false;
        }
        metrics
            .last_quorum_acked
            .is_none_or(|acked| acked.elapsed() >= self.length / 2)
    }

    /// Returns once a majority has answered lately enough, or after one
    /// lease of asking. The write is sent either way: with no majority in
    /// reach openraft refuses it, and nothing is appended.
    async fn renew(&self, raft: &Raft<ShardTypes, Machine>) {
        use openraft::async_runtime::watch::WatchReceiver;
        let mut metrics = raft.metrics();
        let answered = async {
            while self.stale(raft) && raft.trigger().heartbeat().await.is_ok() {
                let changed =
                    async { while self.stale(raft) && metrics.changed().await.is_ok() {} };
                let _ = tokio::time::timeout(self.again, changed).await;
            }
        };
        let _ = tokio::time::timeout(self.length, answered).await;
    }
}

fn leads(raft: &Raft<ShardTypes, Machine>) -> bool {
    use openraft::async_runtime::watch::WatchReceiver;
    raft.metrics().borrow_watched().state == ServerState::Leader
}

/// Tell each batch of one call how openraft answered its records. The
/// answers come in the order the records went in. `bound` says the call
/// named a term.
pub(super) async fn answer(
    results: BoxStream<'static, Result<WriteResult<ShardTypes>, Fatal<ShardTypes>>>,
    answers: Vec<(usize, oneshot::Sender<Result<Span, CommitError>>)>,
    bound: bool,
) {
    // A group that stops ends the stream early, and openraft's stream must
    // not be polled past its end. The records left are answered as stopped.
    let mut results = results.fuse();
    for (records, to) in answers {
        let mut span: Result<Option<Span>, CommitError> = Ok(None);
        let mut written = false;
        for _ in 0..records {
            let applied = match results.next().await {
                Some(Ok(Ok(applied))) => Ok(LogPoint {
                    term: **applied.log_id.committed_leader_id(),
                    index: applied.log_id.index,
                }),
                // Refused before anything was appended: openraft answers
                // an appended entry only when it is applied, or with
                // `LogEntryDiscarded` when it is cut or purged, never on
                // a step-down.
                Some(Ok(Err(ClientWriteError::ForwardToLeader(forward)))) => {
                    Err(lost(match forward.reason {
                        ForwardReason::LeaseExpired => LEASE_LAPSED,
                        _ if bound => OTHER_TERM,
                        _ => NOT_LEADING,
                    }))
                }
                Some(Ok(Err(_))) => Err(lost(DISCARDED)),
                Some(Err(_)) | None => Err(stopped()),
            };
            written |= applied.is_ok();
            span = match (span, applied) {
                (Err(err), _) | (Ok(_), Err(err)) => Err(err),
                (Ok(None), Ok(at)) => Ok(Some(Span {
                    first: at,
                    last: at,
                })),
                (Ok(Some(span)), Ok(at)) => Ok(Some(Span {
                    first: span.first,
                    last: at,
                })),
            };
        }
        // Records that name a term are refused one by one, so some of a
        // batch can be in the log when the rest is not. No refusal's
        // "nothing was written" holds for it.
        let span = match span {
            Err(_) if written => Err(lost(IN_PART)),
            span => span,
        };
        // The publisher may have stopped waiting.
        let _ = to.send(span.and_then(|span| span.ok_or_else(|| lost(EMPTY))));
    }
}

/// Why this member no longer leads as a waiting publisher needs it to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lost {
    /// Another member leads, or this one leads another term.
    Deposed,
    /// This member is standing for election. If it wins, it still appends
    /// what it was sent before.
    Electing,
    /// The group's core has stopped, as it does on a storage error.
    Stopped,
}

/// Whether this member still leads, as a waiting publisher asks it.
pub(crate) trait Leading: Send {
    /// Returns once this member is not the leader, or, with a term, not the
    /// leader of that term, and says why.
    fn lost(&mut self, term: Option<u64>) -> impl Future<Output = Lost> + Send;
}

/// What a publisher is told when its member stops leading, while a batch
/// openraft has not answered may still be appended (`unwritten`) or after
/// it was written.
fn unled(why: Lost, unwritten: bool) -> CommitError {
    match why {
        Lost::Stopped => stopped(),
        Lost::Electing if unwritten => lost(ELECTING),
        Lost::Deposed | Lost::Electing => lost(NOT_LEADING),
    }
}

/// Wait for what `waiting.ack` asks for and say where the batch landed.
///
/// A token has no bound of its own, so every wait here also ends on
/// openraft's answer, on this member no longer leading, and on `stop`. An
/// error means the outcome is unknown, never that the batch is absent: it
/// may be in the log and may commit.
pub(crate) async fn acknowledged<W: Future<Output = Option<LogPoint>> + Send>(
    waiting: Waiting<W>,
    mut positions: watch::Receiver<LogPositions>,
    mut leading: impl Leading,
    stop: CancellationToken,
) -> Result<Written, CommitError> {
    let Waiting {
        ack,
        records,
        first,
        last,
        mut answer,
    } = waiting;
    // Applied means committed: on a majority's disks, which is more than
    // either acknowledgement asks of this one.
    let committed = |answered: Result<Result<Span, CommitError>, oneshot::error::RecvError>| {
        answered
            .unwrap_or_else(|_| Err(stopped()))?
            .written(records)
    };
    let written = async {
        let last = last.await?;
        let first = match first {
            Some(first) => first.await?,
            None => last,
        };
        Some(Span { first, last })
    };
    let span = tokio::select! {
        biased;
        answered = &mut answer => return committed(answered),
        span = written => span,
        why = leading.lost(None) => return Err(unled(why, true)),
        () = stop.cancelled() => return Err(stopped()),
    };
    let Some(span) = span else {
        // No append reported the batch: refused, dropped, or its append
        // failed. openraft says which, and its task answers every batch.
        return tokio::select! {
            biased;
            answered = &mut answer => committed(answered),
            why = leading.lost(None) => Err(unled(why, false)),
            () = stop.cancelled() => Err(stopped()),
        };
    };
    let written = span.written(records)?;
    if ack == AckAt::Written {
        return Ok(written);
    }
    // A later term at the flushed position means another leader wrote
    // there, and whether this batch is still under it is not known here.
    let last = span.last;
    let flushed = async {
        let reached = |at: &LogPositions| {
            at.flushed.is_some_and(|flushed| {
                flushed.term > last.term
                    || (flushed.term == last.term && flushed.index >= last.index)
            })
        };
        let at = positions.wait_for(reached).await.ok()?;
        at.flushed.map(|flushed| flushed.term)
    };
    tokio::select! {
        biased;
        answered = &mut answer => committed(answered),
        term = flushed => match term {
            Some(term) if term == last.term => Ok(written),
            Some(_) => Err(lost(NOT_LEADING)),
            None => Err(stopped()),
        },
        why = leading.lost(Some(last.term)) => Err(unled(why, false)),
        () = stop.cancelled() => Err(stopped()),
    }
}

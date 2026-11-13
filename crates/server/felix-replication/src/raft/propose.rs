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

use bytes::Bytes;
use felix_storage::log::FsyncMode;
use felix_storage::raft_log::{LogPoint, LogPositions, WrittenAt, WrittenToken};
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{Future, StreamExt};
use openraft::error::{ClientWriteError, Fatal};
use openraft::raft::WriteResult;
use openraft::type_config::alias::WatchReceiverOf;
use openraft::{Raft, RaftMetrics, ServerState};
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
}

/// A drain stops taking batches once it holds this many payload bytes. One
/// drain is one append, and the group handles nothing else while it runs.
const DRAIN_BYTES: usize = 4 * 1024 * 1024;

const NOT_LEADING: &str = "this broker does not lead the shard";
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
    let Batch { records, ack } = batch;
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

/// Hand a shard's queued batches to its group, in the order they were
/// queued, until `stop` or until the queue's sender is gone.
///
/// Everything queued when the task looks goes down as one
/// `client_write_many`, so batches that arrive while an append runs share
/// the next one. The task does not wait for a call's entries to commit
/// before it makes the next: openraft takes calls in the order they are
/// made.
pub(crate) async fn run(
    raft: Raft<ShardTypes, Machine>,
    pace: Arc<Pace>,
    mut queue: mpsc::UnboundedReceiver<Proposal>,
    stop: CancellationToken,
    calls: Arc<AtomicU64>,
) {
    let mut answering = FuturesUnordered::new();
    loop {
        let first = tokio::select! {
            biased;
            () = stop.cancelled() => return,
            Some(()) = answering.next(), if !answering.is_empty() => continue,
            next = queue.recv() => match next {
                Some(proposal) => proposal,
                None => return,
            },
        };
        let mut bytes = first.bytes;
        let mut commands = first.commands;
        let mut answers = vec![(commands.len(), first.answer)];
        while bytes < DRAIN_BYTES
            && let Ok(next) = queue.try_recv()
        {
            bytes += next.bytes;
            answers.push((next.commands.len(), next.answer));
            commands.extend(next.commands);
        }
        if leads(&raft) {
            pace.wrote();
        }
        calls.fetch_add(1, Ordering::Relaxed);
        let sent = tokio::select! {
            biased;
            () = stop.cancelled() => return,
            sent = raft.client_write_many(commands) => sent,
        };
        match sent {
            Ok(results) => answering.push(answer(results, answers)),
            Err(fatal) => {
                tracing::warn!(error = %fatal, "a stopped Raft group took no write");
                for (_, to) in answers {
                    let _ = to.send(Err(stopped()));
                }
            }
        }
    }
}

fn leads(raft: &Raft<ShardTypes, Machine>) -> bool {
    use openraft::async_runtime::watch::WatchReceiver;
    raft.metrics().borrow_watched().state == ServerState::Leader
}

/// Tell each batch of one call how openraft answered its records. The
/// answers come in the order the records went in.
async fn answer(
    mut results: BoxStream<'static, Result<WriteResult<ShardTypes>, Fatal<ShardTypes>>>,
    answers: Vec<(usize, oneshot::Sender<Result<Span, CommitError>>)>,
) {
    for (records, to) in answers {
        let mut span: Result<Option<Span>, CommitError> = Ok(None);
        for _ in 0..records {
            let applied = match results.next().await {
                Some(Ok(Ok(applied))) => Ok(LogPoint {
                    term: **applied.log_id.committed_leader_id(),
                    index: applied.log_id.index,
                }),
                // Refused before anything was appended.
                Some(Ok(Err(ClientWriteError::ForwardToLeader(_)))) => Err(lost(NOT_LEADING)),
                Some(Ok(Err(_))) => Err(lost(DISCARDED)),
                Some(Err(_)) | None => Err(stopped()),
            };
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
        // The publisher may have stopped waiting.
        let _ = to.send(span.and_then(|span| span.ok_or_else(|| lost(EMPTY))));
    }
}

/// Whether this member still leads, as a waiting publisher asks it.
pub(crate) trait Leading: Send {
    /// Returns once this member is not the leader, or, with a term, not the
    /// leader of that term.
    fn lost(&mut self, term: Option<u64>) -> impl Future<Output = ()> + Send;
}

/// [`Leading`] read from the group's metrics. openraft publishes them a
/// moment after the change, so this can answer late. It never answers
/// "lost" for a leader that is leading: a leader's metrics say so before it
/// takes its first write.
pub(crate) struct Metrics(pub(crate) WatchReceiverOf<ShardTypes, RaftMetrics<ShardTypes>>);

impl Leading for Metrics {
    async fn lost(&mut self, term: Option<u64>) {
        use openraft::async_runtime::watch::WatchReceiver;
        loop {
            {
                let metrics = self.0.borrow_watched();
                if metrics.running_state.is_err()
                    || metrics.state != ServerState::Leader
                    || term.is_some_and(|term| metrics.current_term != term)
                {
                    return;
                }
            }
            if self.0.changed().await.is_err() {
                return;
            }
        }
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
        () = leading.lost(None) => return Err(lost(NOT_LEADING)),
        () = stop.cancelled() => return Err(stopped()),
    };
    // No append reported the batch: refused, dropped, or its append failed.
    let span = span.ok_or_else(|| lost(UNKNOWN))?;
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
        () = leading.lost(Some(last.term)) => Err(lost(NOT_LEADING)),
        () = stop.cancelled() => Err(stopped()),
    }
}

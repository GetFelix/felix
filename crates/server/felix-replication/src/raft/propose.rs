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

use super::cache::{self, CacheCommand};
use super::group::Pace;
use super::lead::Leadership;
use super::machine::{Cache, Machine};
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

/// A drain takes no batch that would bring it past this many payload bytes
/// or this many records. One drain is one append, and the group handles
/// nothing else while it runs. A batch past either on its own is drained
/// alone.
pub(super) const DRAIN_BYTES: usize = 4 * 1024 * 1024;
pub(super) const DRAIN_RECORDS: usize = 4096;

const NOT_LEADING: &str = "this broker does not lead the shard";
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

/// A cache shard's clock, in microseconds. The wall clock outside tests.
pub(crate) type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// A cache write that has no stamp yet. It gets one when the publish task
/// hands it to the group, not when it is queued: see [`Stamper`].
#[derive(Debug, Clone)]
pub(crate) struct Unstamped {
    pub(crate) command: CacheCommand,
    /// How long after its stamp a put expires, in microseconds. Zero is
    /// never.
    pub(crate) ttl: u64,
}

impl Unstamped {
    fn encode(mut self, at: u64) -> Bytes {
        if let CacheCommand::Put { expires_at, .. } = &mut self.command
            && self.ttl > 0
        {
            *expires_at = at.saturating_add(self.ttl);
        }
        self.command.encode()
    }
}

/// Stamps a cache shard's entries as the publish task writes them.
///
/// A stamp is the leader's clock held up to a floor, so stamps never go
/// back along the log. The floor is the log's time as this member has
/// applied it, or the last stamp issued here if that is later. A term opens
/// only once one of its entries is applied, so by then every entry the
/// leader inherited is applied too, its uncommitted tail included, and
/// what it wrote since was stamped here.
///
/// The write then names the term the floor was read in, and openraft
/// appends it only if this member still leads that term. A member deposed
/// and elected again while a write waited would otherwise append a stamp
/// from below the tail of the leader in between.
pub(crate) struct Stamper {
    #[cfg(test)]
    pub(crate) gate: Arc<Gate>,
    pub(crate) cache: Cache,
    pub(crate) leadership: Arc<Leadership>,
    pub(crate) clock: Clock,
    /// The last stamp issued. It never goes back, whatever term it was for.
    pub(crate) last: u64,
}

/// Where a test holds a stamped write back before openraft is handed it.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct Gate {
    pub(crate) shut: tokio::sync::Mutex<()>,
    /// Drains stamped so far.
    pub(crate) stamped: AtomicU64,
}

impl Stamper {
    /// Stamp `commands` for the term this member has open and return that
    /// term. `None` if it has none open: nothing may be written.
    pub(super) fn stamp(
        &mut self,
        commands: &mut [Command],
        unstamped: Vec<Unstamped>,
    ) -> Option<u64> {
        let lead = self.leadership.lead();
        let term = lead.term.filter(|_| lead.open)?;
        let mut at = self.last.max(self.cache.read().time());
        for (command, unstamped) in commands.iter_mut().zip(unstamped) {
            at = cache::stamp((self.clock)(), at);
            command.timestamp_micros = at;
            command.payload = unstamped.encode(at);
        }
        self.last = at;
        Some(term)
    }
}

/// A batch on its way to the group.
pub(crate) struct Proposal {
    commands: Vec<Command>,
    /// A cache shard's batch: what each command is once it is stamped.
    /// Empty for a stream's.
    unstamped: Vec<Unstamped>,
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
    let mut bytes = 0;
    let commands = records
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
    queued(commands, Vec::new(), bytes, ack)
}

/// The same for a cache shard's writes. Each is one entry, and the batch
/// is acknowledged once written: its caller waits for the commit.
pub(crate) fn split_cache(writes: Vec<Unstamped>) -> Result<(Proposal, Waiting), CommitError> {
    let commands = writes
        .iter()
        .map(|_| Command {
            timestamp_micros: 0,
            payload: Bytes::new(),
            written: None,
        })
        .collect();
    queued(commands, writes, 0, AckAt::Written)
}

fn queued(
    mut commands: Vec<Command>,
    unstamped: Vec<Unstamped>,
    bytes: usize,
    ack: AckAt,
) -> Result<(Proposal, Waiting), CommitError> {
    let count = commands.len();
    if count == 0 {
        return Err(lost(EMPTY));
    }
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
            unstamped,
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
    pub(super) unstamped: Vec<Unstamped>,
    /// Each batch's record count and where its answer goes.
    pub(super) answers: Vec<(usize, oneshot::Sender<Result<Span, CommitError>>)>,
}

/// Take `first` and what is queued behind it, up to [`DRAIN_BYTES`] and
/// [`DRAIN_RECORDS`]. A batch that would pass either is left in `held`, to
/// start the next drain, so it keeps its place.
pub(super) fn drain(
    first: Proposal,
    queue: &mut mpsc::UnboundedReceiver<Proposal>,
    held: &mut Option<Proposal>,
) -> Drain {
    let mut bytes = first.bytes;
    let mut commands = first.commands;
    let mut unstamped = first.unstamped;
    let mut answers = vec![(commands.len(), first.answer)];
    while let Ok(next) = queue.try_recv() {
        if bytes + next.bytes > DRAIN_BYTES || commands.len() + next.commands.len() > DRAIN_RECORDS
        {
            *held = Some(next);
            break;
        }
        bytes += next.bytes;
        answers.push((next.commands.len(), next.answer));
        commands.extend(next.commands);
        unstamped.extend(next.unstamped);
    }
    Drain {
        commands,
        unstamped,
        answers,
    }
}

/// Hand a shard's queued batches to its group, in the order they were
/// queued, until `stop` or until the queue's sender is gone.
///
/// Everything queued when the task looks goes down as one
/// `client_write_many`, up to the drain bounds, so batches that arrive
/// while an append runs share the next one. The task does not wait for a
/// call's entries to commit before it makes the next: openraft takes calls
/// in the order they are made.
pub(crate) async fn run(
    raft: Raft<ShardTypes, Machine>,
    pace: Arc<Pace>,
    mut queue: mpsc::UnboundedReceiver<Proposal>,
    stop: CancellationToken,
    calls: Arc<AtomicU64>,
    lease: Lease,
    mut stamper: Option<Stamper>,
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
            mut commands,
            unstamped,
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
        // Stamped after the renewal, which can take a whole lease.
        let term = match &mut stamper {
            None => None,
            Some(stamper) => match stamper.stamp(&mut commands, unstamped) {
                Some(term) => Some(term),
                None => {
                    for (_, to) in answers {
                        let _ = to.send(Err(lost(NOT_LEADING)));
                    }
                    continue;
                }
            },
        };
        #[cfg(test)]
        if let Some(stamper) = &stamper {
            stamper.gate.stamped.fetch_add(1, Ordering::Relaxed);
            drop(stamper.gate.shut.lock().await);
        }
        calls.fetch_add(1, Ordering::Relaxed);
        let sent = alongside(write(&raft, commands, term), &mut answering, &stop).await;
        let Some(sent) = sent else {
            return;
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

/// Hand `commands` to openraft, in order. With a term, each is appended
/// only if this member still leads that term, which openraft checks as it
/// appends: what was stamped for one term never lands in another.
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
/// answers come in the order the records went in.
pub(super) async fn answer(
    results: BoxStream<'static, Result<WriteResult<ShardTypes>, Fatal<ShardTypes>>>,
    answers: Vec<(usize, oneshot::Sender<Result<Span, CommitError>>)>,
) {
    // A group that stops ends the stream early, and openraft's stream must
    // not be polled past its end. The records left are answered as stopped.
    let mut results = results.fuse();
    for (records, to) in answers {
        let mut span: Result<Option<Span>, CommitError> = Ok(None);
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
                        _ => NOT_LEADING,
                    }))
                }
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

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
use felix_storage::waiters::Waiters;
use futures::Future;
use openraft::error::{ClientWriteError, Fatal, ForwardReason};
use openraft::raft::{ClientWriteResult, WriteOption};
use openraft::{Raft, ServerState};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::cache::{self, CacheCommand};
use super::group::Pace;
use super::lead::Leadership;
use super::machine::{Cache, Machine};
use super::types::{Command, CondPlace, ProducerMark, ShardTypes};
use super::wire;
use crate::engine::{CommitError, Written};

/// One record of a batch to write.
#[derive(Debug, Clone)]
pub struct Record {
    pub timestamp_micros: u64,
    pub payload: Bytes,
    /// The record's encoded key and headers block.
    pub headers: Option<Bytes>,
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
    /// fsyncs on commit, the write otherwise. This only places the
    /// acknowledgement: when the Raft log under it fsyncs is the log's own
    /// mode.
    pub fn for_fsync(mode: &FsyncMode) -> Self {
        match mode {
            FsyncMode::OnCommit => Self::Flushed,
            FsyncMode::Periodic { .. } | FsyncMode::None => Self::Written,
        }
    }
}

/// What wakes a publisher waiting for its batch to be written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Heard {
    /// The log's write of the batch, as soon as it is on this leader's log.
    Written,
    /// openraft's answer at the apply, for a publisher that waits for the
    /// commit anyway: one wake instead of two. If this member stops
    /// leading or stops before the answer, the publisher is told what the
    /// write says, as with [`Self::Written`].
    Applied,
}

/// When [`super::RaftEngine::publish`] answers the publisher. Subscribers
/// get the batch at its commit either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnswerAt {
    /// When the batch is committed. A `Quorum` stream.
    Commit,
    /// Once the batch is on this leader's log as far as its [`AckAt`]
    /// asks, if the leader lease holds then
    /// ([`super::RaftEngine::admit_local_ack`]). If it does not, as
    /// [`Self::Majority`] answers under the same [`AckAt`], so never later
    /// than a `Quorum` publish. A `Leader` stream: the batch can still be
    /// lost if this broker stops leading before it commits.
    Lease,
    /// Once a majority of the shard's voters has written the batch
    /// ([`super::RaftEngine::await_quorum_ack`]), or at the commit if that
    /// is first. A `Quorum` stream that is not fsynced on commit: the batch
    /// survives a change of leader and process crashes, and is lost only
    /// if a majority of copies each lose their unsynced tail, by a power
    /// cut or a failed flush. With
    /// [`AckAt::Flushed`] it is the commit.
    Majority,
}

/// A publish batch for [`super::RaftEngine::propose`]. It must hold a record.
#[derive(Debug, Clone)]
pub struct Batch {
    pub records: Vec<Record>,
    pub ack: AckAt,
    /// The term the caller was admitted at. The batch is written only
    /// while this member leads that term. `None` takes any term.
    pub term: Option<u64>,
    /// Set when the batch is an idempotent producer's.
    pub producer: Option<Sequenced>,
    /// Set on a conditional publish: the offset its one record must be
    /// written at. Written anywhere else, the leader's log stores a blank
    /// entry there, followers copy it, and the record is never delivered.
    /// Never set with `producer`.
    pub expected: Option<u64>,
    /// Set on a head trim: the floor below which reads stop once its one
    /// record, the trim's headers, is committed. With `expected` for a
    /// client's trim; without it for retention's, whose floor is at or
    /// below the tail it was proposed at.
    pub trim: Option<u64>,
    /// Set on an atomic commit: its one record is the encoded commit,
    /// stored with the commit mark. Never with `producer` or `trim`.
    pub commit: bool,
    /// The principal publishing the batch, stored with each record.
    pub publisher: Option<Bytes>,
}

/// An idempotent producer's batch: whose it is and its sequence. The
/// records are stored with both, so every member's log can answer a
/// re-send of the batch. The caller checks the sequence against the
/// leader's log before proposing, and proposes the producer's next batch
/// only once this one is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sequenced {
    pub producer_id: u64,
    pub sequence: u64,
    /// The place in the producer's batch of the first record handed over.
    /// Zero for a whole batch. Above zero the records are the rest of a
    /// batch the log already holds up to there.
    pub position: u32,
    /// Records in the producer's whole batch.
    pub len: u32,
    /// Set for a producer that numbers records, not batches (Kafka): each
    /// record is a one-record batch of its own, at `sequence` plus its
    /// place here, so `position` is 0 and `len` 1. The records are still
    /// one proposal, which is what lets such a producer's records share a
    /// drain instead of taking a round trip each.
    pub per_record: bool,
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
const WRONG_KIND: &str = "the shard's group does not take this kind of write";
const NOT_THE_REST: &str =
    "the records are not the rest of the producer's batch, so nothing was written";
const CONDITIONAL_SHAPE: &str = "a conditional publish or a trim cannot carry a producer, and a trim or a commit is one record, so nothing was written";

pub(crate) fn lost(detail: &'static str) -> CommitError {
    CommitError::LeadershipLost {
        what: "write",
        detail,
    }
}

pub(crate) fn stopped() -> CommitError {
    lost(STOPPED)
}

/// A stream's batch sent to a cache shard's group, or a cache write to a
/// stream's. Nothing was written.
pub(crate) fn not_a_stream() -> CommitError {
    lost(WRONG_KIND)
}

/// Why a publisher has no acknowledgement. Like any error from the
/// proposer, the outcome is unknown: records of the batch may commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Unacknowledged {
    pub(crate) error: CommitError,
    /// One past the last offset a record of the batch is known to be
    /// written at, if one is. Readers resuming below it go to the log.
    pub(crate) written_end: Option<u64>,
}

impl Unacknowledged {
    /// Whether openraft refused the write before appending any of it while
    /// this member may still lead: its lease had lapsed. Every other error
    /// either leaves the member not leading the write's term or leaves the
    /// append unknown.
    pub(crate) fn refused_on_lease(&self) -> bool {
        self.written_end.is_none()
            && matches!(&self.error, CommitError::LeadershipLost { detail, .. } if *detail == LEASE_LAPSED)
    }

    /// The error a publisher is told. A refusal openraft made before it
    /// appended any of the batch, with no record of it known written,
    /// becomes [`CommitError::NotAppended`]: a lapsed lease, or a term the
    /// batch was not admitted at. Anything else may have been appended.
    pub(crate) fn told(self) -> CommitError {
        match self.error {
            CommitError::LeadershipLost { what, detail }
                if self.written_end.is_none()
                    && (detail == LEASE_LAPSED || detail == OTHER_TERM) =>
            {
                CommitError::NotAppended { what, detail }
            }
            error => error,
        }
    }

    /// The batch is known to be written up to `end`, whatever else was said.
    fn reaching(mut self, end: u64) -> Self {
        self.written_end = Some(self.written_end.map_or(end, |known| known.max(end)));
        self
    }
}

impl From<CommitError> for Unacknowledged {
    fn from(error: CommitError) -> Self {
        Self {
            error,
            written_end: None,
        }
    }
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

    /// One past the furthest record reported written, whether or not the
    /// span is one run.
    fn end(self) -> u64 {
        self.first.index.max(self.last.index) + 1
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
    /// At least what the encoded entry holds of the client's bytes.
    fn bytes(&self) -> usize {
        match &self.command {
            CacheCommand::Put { key, value, .. } => key.len() + value.len(),
            CacheCommand::Delete { key, .. }
            | CacheCommand::Expire { key, .. }
            | CacheCommand::Add { key, .. } => key.len(),
            CacheCommand::Retire { .. } => 0,
        }
    }

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
    /// Answer every group write as not known to be written.
    pub(crate) unknown_groups: std::sync::atomic::AtomicBool,
    /// Times the shard's publish task has been polled: one per wake.
    pub(crate) polls: AtomicU64,
}

/// The publish task, counting its polls into [`Gate::polls`].
#[cfg(test)]
pub(crate) struct Polled<F> {
    pub(crate) run: std::pin::Pin<Box<F>>,
    pub(crate) gate: Arc<Gate>,
}

#[cfg(test)]
impl<F: Future> Future for Polled<F> {
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        self.gate.polls.fetch_add(1, Ordering::Relaxed);
        self.run.as_mut().poll(cx)
    }
}

/// A write this member cannot say whether it appended.
#[cfg(test)]
pub(crate) fn unknown() -> CommitError {
    lost(UNKNOWN)
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
    /// [`Batch::term`].
    term: Option<u64>,
    bytes: usize,
    /// Where openraft applied the batch, or why it did not.
    answer: oneshot::Sender<Result<Span, Unacknowledged>>,
}

/// The publisher's side of a queued batch. `W` is a token's future.
pub(crate) struct Waiting<W = WrittenAt> {
    pub(super) ack: AckAt,
    pub(super) heard: Heard,
    pub(super) records: u64,
    /// `None` for a batch of one record, where `last` is also the first.
    pub(super) first: Option<W>,
    pub(super) last: W,
    pub(super) answer: oneshot::Receiver<Result<Span, Unacknowledged>>,
}

/// Split `batch` into what is queued and what its publisher waits on.
///
/// The tokens are made here, once per call, and a batch is never proposed
/// twice: a second attempt could land beside the first. The first and the
/// last record each carry one, because one record's index says nothing
/// certain about another's.
pub(crate) fn split(batch: Batch) -> Result<(Proposal, Waiting), CommitError> {
    let Batch {
        records,
        ack,
        term,
        producer,
        expected,
        trim,
        commit,
        publisher,
    } = batch;
    let count = records.len();
    if count == 0 {
        return Err(lost(EMPTY));
    }
    // A trim is one entry, so its index alone says whether it landed where
    // expected. A trim with no expected offset is retention's: its floor is
    // at or below the tail it was proposed at, so below any index it can
    // take. A conditional batch of several records is one run of entries,
    // each naming its place, so all land at their offsets or none do.
    if (expected.is_some() || trim.is_some()) && producer.is_some() || trim.is_some() && count != 1
    {
        return Err(lost(CONDITIONAL_SHAPE));
    }
    let cond_len = match expected {
        Some(_) if count > 1 => Some(u32::try_from(count).map_err(|_| lost(CONDITIONAL_SHAPE))?),
        _ => None,
    };
    // A commit is one record, so it lands whole or not at all.
    if commit && (count != 1 || producer.is_some() || trim.is_some()) {
        return Err(lost(CONDITIONAL_SHAPE));
    }
    let per_record = producer.is_some_and(|producer| producer.per_record);
    let first = match producer {
        None => None,
        // Each record is a whole batch, so a cut anywhere leaves whole
        // batches behind.
        Some(Sequenced {
            per_record: true,
            position,
            len,
            ..
        }) if position != 0 || len != 1 => return Err(lost(NOT_THE_REST)),
        // Several records are several entries, which a leader change can
        // cut. Each names its place, so a log that holds only some of them
        // knows it, and the re-send writes just the rest.
        Some(Sequenced {
            position,
            len,
            per_record: false,
            ..
        }) if u64::from(position) + count as u64 != u64::from(len) => {
            return Err(lost(NOT_THE_REST));
        }
        Some(Sequenced {
            producer_id,
            sequence,
            position,
            len,
            ..
        }) => Some(ProducerMark {
            producer_id,
            sequence,
            position,
            len,
        }),
    };
    let mut bytes = 0;
    // Each record stores the publisher; the last takes it, so a batch of
    // one copies nothing.
    let mut publisher = publisher;
    let commands = records
        .into_iter()
        .zip(0u32..)
        .map(|(record, index)| {
            let publisher = match index as usize + 1 == count {
                true => publisher.take(),
                false => publisher.clone(),
            };
            bytes += record.payload.len() + record.headers.as_ref().map_or(0, Bytes::len);
            Command {
                timestamp_micros: record.timestamp_micros,
                payload: record.payload,
                producer: first.map(|first| match per_record {
                    true => ProducerMark {
                        sequence: first.sequence + u64::from(index),
                        ..first
                    },
                    false => ProducerMark {
                        position: first.position + index,
                        ..first
                    },
                }),
                expected,
                trim,
                commit,
                group: false,
                publisher,
                headers: record.headers.map(Box::new),
                written: None,
                cond: cond_len.map(|len| CondPlace {
                    position: index,
                    len,
                }),
            }
        })
        .collect();
    queued(commands, Vec::new(), term, bytes, ack)
}

/// The same for one consumer-group write: one entry, its payload the
/// encoded [`felix_broker::GroupEntry`], stored with the log's group mark.
/// Acknowledged once written; its caller waits for the apply.
pub(crate) fn split_group(
    entry: Bytes,
    timestamp_micros: u64,
    term: u64,
) -> Result<(Proposal, Waiting), CommitError> {
    let bytes = entry.len();
    let command = Command {
        timestamp_micros,
        payload: entry,
        producer: None,
        expected: None,
        trim: None,
        commit: false,
        group: true,
        publisher: None,
        headers: None,
        written: None,
        cond: None,
    };
    queued(vec![command], Vec::new(), Some(term), bytes, AckAt::Written)
}

/// The same for a cache shard's writes. Each is one entry, and the batch
/// is acknowledged once written: its caller waits for the commit.
pub(crate) fn split_cache(writes: Vec<Unstamped>) -> Result<(Proposal, Waiting), CommitError> {
    let commands = writes
        .iter()
        .map(|_| Command {
            timestamp_micros: 0,
            payload: Bytes::new(),
            producer: None,
            expected: None,
            trim: None,
            commit: false,
            group: false,
            publisher: None,
            headers: None,
            written: None,
            cond: None,
        })
        .collect();
    queued(commands, writes, None, 0, AckAt::Written)
}

fn queued(
    mut commands: Vec<Command>,
    unstamped: Vec<Unstamped>,
    term: Option<u64>,
    bytes: usize,
    ack: AckAt,
) -> Result<(Proposal, Waiting), CommitError> {
    let count = commands.len();
    if count == 0 {
        return Err(lost(EMPTY));
    }
    // Every write reaches the log through here. A record no append can
    // carry would be sent and refused forever, holding back every entry
    // after it, so it is refused before it takes an index.
    let largest = commands
        .iter()
        .map(wire::command_bytes)
        .chain(unstamped.iter().map(Unstamped::bytes))
        .max()
        .unwrap_or(0);
    if largest > wire::MAX_RECORD_BYTES {
        return Err(CommitError::TooLarge {
            bytes: largest,
            max: wire::MAX_RECORD_BYTES,
        });
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
            term,
            bytes,
            answer,
        },
        Waiting {
            ack,
            heard: Heard::Written,
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
    /// The term every batch of the call was admitted at.
    pub(super) term: Option<u64>,
    /// Each batch's record count and where its answer goes.
    pub(super) answers: Vec<(usize, oneshot::Sender<Result<Span, Unacknowledged>>)>,
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
    let mut unstamped = first.unstamped;
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
        unstamped.extend(next.unstamped);
    }
    Drain {
        commands,
        unstamped,
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
/// in the order they are made. It does not hand on answers either: each
/// batch is answered by its [`Respond`]ers, on the task that applies it, so
/// a drain wakes this task once.
pub(crate) async fn run(
    raft: Raft<ShardTypes, Machine>,
    pace: Arc<Pace>,
    mut queue: mpsc::UnboundedReceiver<Proposal>,
    stop: CancellationToken,
    calls: Arc<AtomicU64>,
    lease: Lease,
    mut stamper: Option<Stamper>,
) {
    let mut held = None;
    loop {
        let first = match held.take() {
            Some(first) => first,
            None => tokio::select! {
                biased;
                () = stop.cancelled() => return,
                next = queue.recv() => match next {
                    Some(proposal) => proposal,
                    None => return,
                },
            },
        };
        let Drain {
            mut commands,
            unstamped,
            term,
            answers,
        } = drain(first, &mut queue, &mut held);
        felix_storage::hops::mark(felix_storage::hops::Stage::Dequeued);
        // Every entry of a cache shard is a stamped command, and a stream's
        // are raw records. A mix would stamp the wrong entries and append
        // ones no member can apply. A cache write names no term: it is
        // written in the one its stamp is for.
        let stamped = if stamper.is_some() { commands.len() } else { 0 };
        if unstamped.len() != stamped || (stamper.is_some() && term.is_some()) {
            tracing::error!(
                entries = commands.len(),
                cache_writes = unstamped.len(),
                "a drain mixed stream and cache writes"
            );
            for (_, to) in answers {
                let _ = to.send(Err(not_a_stream().into()));
            }
            continue;
        }
        if leads(&raft) {
            pace.wrote();
        }
        if until_stopped(lease.renew(&raft), &stop).await.is_none() {
            return;
        }
        // Stamped after the renewal, which can take a whole lease.
        let term = match &mut stamper {
            None => term,
            Some(stamper) => match stamper.stamp(&mut commands, unstamped) {
                Some(term) => Some(term),
                None => {
                    for (_, to) in answers {
                        let _ = to.send(Err(lost(NOT_LEADING).into()));
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
        let responders = respond(answers, term.is_some());
        // A write cut short by `stop` drops its responders, which answer
        // their batches as stopped.
        let Some(sent) = until_stopped(write(&raft, commands, responders, term), &stop).await
        else {
            return;
        };
        felix_storage::hops::mark(felix_storage::hops::Stage::Proposed);
        if let Err(fatal) = sent {
            // openraft dropped the call, and with it the responders, which
            // answer every batch as stopped.
            tracing::warn!(error = %fatal, "a stopped Raft group took no write");
        }
    }
}

/// Hand `commands` to openraft, in order, as one write, each with the
/// responder at its place. With a term, the
/// write is appended only if this member still leads that term, which
/// openraft checks as it appends: what was stamped or admitted at one term
/// never lands in another. openraft admits or refuses the write whole, for
/// the term and for its lease alike, and appends it at consecutive indexes.
pub(super) async fn write(
    raft: &Raft<ShardTypes, Machine>,
    commands: Vec<Command>,
    responders: Vec<Respond<ClientWriteResult<ShardTypes>>>,
    term: Option<u64>,
) -> Result<(), Fatal<ShardTypes>> {
    debug_assert_eq!(commands.len(), responders.len());
    let option = match term {
        Some(term) => WriteOption::expect_leader(term),
        None => WriteOption::default(),
    };
    let writes = commands.into_iter().zip(responders.into_iter().map(Some));
    raft.write_many(writes).with_option(option).await
}

/// Run `work` until it ends, or return `None` on `stop`.
async fn until_stopped<T>(work: impl Future<Output = T>, stop: &CancellationToken) -> Option<T> {
    tokio::select! {
        biased;
        () = stop.cancelled() => None,
        done = work => Some(done),
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

/// One responder per record of a call, in order, each batch's tied to
/// its answer. `bound` says the call names a term.
pub(super) fn respond(
    answers: Vec<(usize, oneshot::Sender<Result<Span, Unacknowledged>>)>,
    bound: bool,
) -> Vec<Respond<ClientWriteResult<ShardTypes>>> {
    let mut responders = Vec::with_capacity(answers.iter().map(|(records, _)| records).sum());
    for (records, to) in answers {
        // `queued` refuses an empty batch; one here would get no responder
        // and be told it stopped.
        debug_assert!(records > 0);
        let tally = Arc::new(Mutex::new(Tally {
            entries: (0..records).map(|_| None).collect(),
            left: records,
            bound,
            to: Some(to),
        }));
        responders.extend((0..records).map(|at| Respond {
            tally: Arc::clone(&tally),
            at,
            settle: settle_write,
            done: false,
        }));
    }
    responders
}

/// openraft's answer for one record of a call. It goes straight to the
/// record's batch, on whatever task openraft answers on, which is the
/// task that applies the entry. A responder dropped unanswered, as a
/// stopped group drops them, answers its record as stopped.
pub(crate) struct Respond<T> {
    tally: Arc<Mutex<Tally>>,
    /// The record's place in its batch.
    at: usize,
    settle: fn(&Mutex<Tally>, usize, Option<T>),
    done: bool,
}

impl<T> openraft::raft::responder::Responder<ShardTypes, T> for Respond<T>
where
    T: Send + 'static,
{
    fn on_complete(mut self, result: T) {
        self.done = true;
        (self.settle)(&self.tally, self.at, Some(result));
    }
}

impl<T> Drop for Respond<T> {
    fn drop(&mut self) {
        if !self.done {
            self.done = true;
            (self.settle)(&self.tally, self.at, None);
        }
    }
}

/// What a batch's records have been answered so far.
struct Tally {
    entries: Vec<Option<Result<LogPoint, CommitError>>>,
    left: usize,
    bound: bool,
    to: Option<oneshot::Sender<Result<Span, Unacknowledged>>>,
}

/// Note one record's answer, and once every record of the batch has one,
/// tell the batch.
fn settle_write(tally: &Mutex<Tally>, at: usize, result: Option<ClientWriteResult<ShardTypes>>) {
    let mut tally = tally.lock();
    let applied = match result {
        Some(Ok(applied)) => {
            felix_storage::hops::mark(felix_storage::hops::Stage::Applied);
            Ok(LogPoint {
                term: **applied.log_id.committed_leader_id(),
                index: applied.log_id.index,
            })
        }
        // Refused before anything was appended: openraft answers an
        // appended entry only when it is applied, or with
        // `LogEntryDiscarded` when it is cut or purged, never on a
        // step-down.
        Some(Err(ClientWriteError::ForwardToLeader(forward))) => Err(lost(match forward.reason {
            ForwardReason::LeaseExpired => LEASE_LAPSED,
            _ if tally.bound => OTHER_TERM,
            _ => NOT_LEADING,
        })),
        Some(Err(_)) => Err(lost(DISCARDED)),
        None => Err(stopped()),
    };
    let Some(slot @ None) = tally.entries.get_mut(at) else {
        return;
    };
    *slot = Some(applied);
    tally.left -= 1;
    if tally.left > 0 {
        return;
    }
    let Some(to) = tally.to.take() else {
        return;
    };
    let told = told(tally.entries.drain(..).flatten());
    // The publisher may have stopped waiting.
    let _ = to.send(told);
}

/// A batch's answer from its records', in the order they went in.
fn told(
    entries: impl Iterator<Item = Result<LogPoint, CommitError>>,
) -> Result<Span, Unacknowledged> {
    let mut span: Result<Option<Span>, CommitError> = Ok(None);
    // One past the last record of the batch openraft applied.
    let mut written_end = None;
    for applied in entries {
        if let Ok(at) = &applied {
            written_end = Some(at.index + 1);
        }
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
    // openraft admits a call whole, but a leader that stops before the
    // call commits can leave the next leader only its start: the rest
    // is discarded. Some of a batch is then in the log when the rest
    // is not. No refusal's "nothing was written" holds for it, and the
    // records that were applied are committed.
    match (span, written_end) {
        (Ok(span), _) => span.ok_or_else(|| lost(EMPTY).into()),
        (Err(_), Some(end)) => Err(Unacknowledged {
            error: lost(IN_PART),
            written_end: Some(end),
        }),
        (Err(err), None) => Err(err.into()),
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
/// may be in the log and may commit. Once the batch is known written, the
/// error says how far.
pub(crate) async fn acknowledged<W: Future<Output = Option<LogPoint>> + Send>(
    waiting: Waiting<W>,
    positions: watch::Receiver<LogPositions>,
    flush_waiters: Waiters<(u64, u64)>,
    mut leading: impl Leading,
    stop: CancellationToken,
) -> Result<Written, Unacknowledged> {
    let Waiting {
        ack,
        heard,
        records,
        first,
        last,
        mut answer,
    } = waiting;
    // Applied means committed: on a majority's disks, which is more than
    // either acknowledgement asks of this one.
    let committed = |answered: Result<Result<Span, Unacknowledged>, oneshot::error::RecvError>|
     -> Result<Written, Unacknowledged> {
        let span = answered.unwrap_or_else(|_| Err(stopped().into()))?;
        span.written(records)
            .map_err(|err| Unacknowledged::from(err).reaching(span.end()))
    };
    if heard == Heard::Applied {
        tokio::select! {
            biased;
            answered = &mut answer => return committed(answered),
            // Answered below, from the write if it was seen.
            _ = leading.lost(None) => {}
            () = stop.cancelled() => {}
        }
    }
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
        span = written => {
            felix_storage::hops::mark(felix_storage::hops::Stage::WrittenSeen);
            span
        }
        why = leading.lost(None) => return Err(unled(why, true).into()),
        () = stop.cancelled() => return Err(stopped().into()),
    };
    let Some(span) = span else {
        // No append reported the batch: refused, dropped, or its append
        // failed. openraft says which, through the batch's responders.
        return tokio::select! {
            biased;
            answered = &mut answer => committed(answered),
            why = leading.lost(None) => Err(unled(why, false).into()),
            () = stop.cancelled() => Err(stopped().into()),
        };
    };
    let written = match span.written(records) {
        Ok(written) => written,
        // Tokens exist only for the first and last record. A call is
        // appended as one run, so this is not expected, but openraft's
        // answer says how far the batch was written either way.
        Err(_) => {
            let told = tokio::select! {
                biased;
                answered = &mut answer => committed(answered),
                why = leading.lost(None) => Err(unled(why, false).into()),
                () = stop.cancelled() => Err(stopped().into()),
            };
            return told.map_err(|err| err.reaching(span.end()));
        }
    };
    if ack == AckAt::Written {
        return Ok(written);
    }
    // A later term at the flushed position means another leader wrote
    // there, and whether this batch is still under it is not known here.
    let last = span.last;
    let flushed = async {
        let reached = |at: &LogPositions| {
            at.flushed.filter(|flushed| {
                flushed.term > last.term
                    || (flushed.term == last.term && flushed.index >= last.index)
            })
        };
        let key = (last.term, last.index);
        let at = flush_waiters
            .wait_for(&positions, key, reached)
            .await
            .ok()?;
        Some(at.term)
    };
    let flushed = tokio::select! {
        biased;
        answered = &mut answer => committed(answered),
        term = flushed => match term {
            Some(term) if term == last.term => Ok(written),
            Some(_) => Err(lost(NOT_LEADING).into()),
            None => Err(stopped().into()),
        },
        why = leading.lost(Some(last.term)) => Err(unled(why, false).into()),
        () = stop.cancelled() => Err(stopped().into()),
    };
    flushed.map_err(|err: Unacknowledged| err.reaching(written.end))
}

//! A publish whose offsets the shard's engine gives it: a shard on Raft,
//! where openraft makes each record the next log index.
//!
//! The batch is never read back. The engine writes and commits it, and the
//! records the publisher handed in are what joins the commit hold and is
//! fanned out, in one shared envelope, once the engine says they are
//! committed. A record a later leader cut was never committed, so no
//! subscriber has it. See `docs/per-shard-raft.md`, "Delivering committed
//! records".

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::CommitTurn;
use felix_storage::disk_log::ProducerSequence;
use felix_storage::log::PayloadDigest;
use tokio::sync::oneshot;

use super::completion::spawn_release;
use super::{PublishOutcome, SequenceReuse};
use crate::broker::Broker;
use crate::broker::shards::StreamHandle;
use crate::commit::StateOp;
use crate::error::{BrokerError, Result};
use crate::stream::HeldBatch;

type Committing<E> =
    Pin<Box<dyn Future<Output = std::result::Result<ProposalCommitted, ProposalFailed<E>>> + Send>>;
type Finishing<E> = Pin<Box<dyn Future<Output = std::result::Result<PublishOutcome, E>> + Send>>;

impl Broker {
    /// Hand `payloads` to the shard's engine and take the batch's place in
    /// the shard's delivery order.
    ///
    /// `propose` is called once, with the time to stamp every record, while
    /// the place is held. It must queue the batch with the engine before it
    /// returns, because the order of these calls is taken as the order of
    /// the batches' offsets. Its future resolves once the engine has
    /// committed the batch under the leader that wrote it, or to the
    /// engine's error ([`ProposalFailed`]), which delivers nothing. It must
    /// not resolve to an
    /// error while the batch may still commit (a timeout): the batches
    /// behind it would go out while it never did. Bound the publisher's
    /// wait with [`ProposedPublish::answer_within`] instead.
    ///
    /// [`BrokerError::ProposalsStalled`], with `propose` not called, while
    /// the oldest batch the engine has not settled has waited longer than
    /// `stalled_after`.
    ///
    /// The batch is delivered from `payloads` as they are, after every batch
    /// handed over before it. Finish it with [`ProposedPublish::complete`];
    /// one dropped on the way still delivers, on a task of its own, so the
    /// batches behind it are not held up.
    pub fn propose_publish<F, E>(
        &self,
        handle: &StreamHandle,
        payloads: Vec<Bytes>,
        stalled_after: Duration,
        propose: impl FnOnce(u64) -> F,
    ) -> Result<ProposedPublish<E>>
    where
        F: Future<Output = std::result::Result<ProposalCommitted, ProposalFailed<E>>>
            + Send
            + 'static,
        E: Send + 'static,
    {
        self.propose_publish_with(
            handle,
            payloads,
            Attribution::default(),
            stalled_after,
            propose,
        )
    }

    /// [`Self::propose_publish`] for records with a publisher or headers,
    /// as [`Self::admit_attribution`] let them through. They are delivered
    /// with the batch, so `propose` must store the same on every record.
    pub fn propose_publish_with<F, E>(
        &self,
        handle: &StreamHandle,
        payloads: Vec<Bytes>,
        attribution: Attribution,
        stalled_after: Duration,
        propose: impl FnOnce(u64) -> F,
    ) -> Result<ProposedPublish<E>>
    where
        F: Future<Output = std::result::Result<ProposalCommitted, ProposalFailed<E>>>
            + Send
            + 'static,
        E: Send + 'static,
    {
        self.propose(
            handle,
            payloads,
            attribution,
            None,
            None,
            None,
            stalled_after,
            propose,
        )
    }

    /// What a batch on an engine log may store of its publisher and
    /// headers, as the hybrid's [`Self::claim_publish`] decides it: the
    /// publisher only when [`Self::records_publishers`] says so, and
    /// headers refused with [`BrokerError::RecordHeadersNotEnabled`] rather
    /// than dropped. Both also wait for [`Self::allow_raft_record_headers_when`]:
    /// a member that predates them would refuse the entry.
    pub fn admit_attribution(
        &self,
        payloads: &[Bytes],
        attribution: Attribution,
    ) -> Result<Attribution> {
        let Attribution { publisher, headers } = attribution;
        let headers = super::usable_headers(payloads, headers.as_deref())?.map(<[_]>::to_vec);
        let raft = self.raft_record_headers_allowed();
        if headers.is_some() {
            if !self.records_headers() {
                return Err(BrokerError::RecordHeadersNotEnabled {
                    feature: felix_common::fleet::RECORD_HEADERS.name(),
                });
            }
            if !raft {
                return Err(BrokerError::RecordHeadersNotEnabled {
                    feature: felix_common::fleet::RAFT_RECORD_HEADERS.name(),
                });
            }
        }
        Ok(Attribution {
            publisher: publisher.filter(|_| raft && self.records_publishers()),
            headers,
        })
    }

    /// [`Self::propose_publish`] for a conditional publish: one record that
    /// must be written at offset `expected`.
    ///
    /// The engine writes the record wherever its turn falls, and `propose`
    /// must have it store a blank entry in place of the record when that is
    /// not `expected`, decided where the leader appends it, at the entry's
    /// final index, and copied by followers. Such a batch is
    /// never delivered, and [`ProposedPublish::complete`] answers it with
    /// `mismatch` given the offset after it, the shard's next offset once
    /// the blank is written. It is answered only once committed, so the
    /// outcome is the group's. See `docs/per-shard-raft.md`, "Conditional
    /// publishes".
    ///
    /// No record is [`BrokerError::EmptyConditionalPublish`] and more than
    /// one is [`BrokerError::NotOnEngineStream`], with `propose` not called.
    #[allow(clippy::too_many_arguments)]
    pub fn propose_publish_at<F, E>(
        &self,
        handle: &StreamHandle,
        payload: Vec<Bytes>,
        attribution: Attribution,
        expected: u64,
        stalled_after: Duration,
        mismatch: impl FnOnce(u64) -> E + Send + 'static,
        propose: impl FnOnce(u64) -> F,
    ) -> Result<ProposedPublish<E>>
    where
        F: Future<Output = std::result::Result<ProposalCommitted, ProposalFailed<E>>>
            + Send
            + 'static,
        E: Send + 'static,
    {
        if payload.is_empty() {
            return Err(BrokerError::EmptyConditionalPublish);
        }
        if payload.len() != 1 {
            return Err(BrokerError::NotOnEngineStream {
                what: SEVERAL_CONDITIONAL,
            });
        }
        let expected = Expected {
            offset: expected,
            mismatch: Box::new(mismatch),
        };
        self.propose(
            handle,
            payload,
            attribution,
            Some(expected),
            None,
            None,
            stalled_after,
            propose,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::broker) fn propose<F, E>(
        &self,
        handle: &StreamHandle,
        payloads: Vec<Bytes>,
        attribution: Attribution,
        expected: Option<Expected<E>>,
        trim: Option<HeldTrim>,
        commit: Option<Arc<[StateOp]>>,
        stalled_after: Duration,
        propose: impl FnOnce(u64) -> F,
    ) -> Result<ProposedPublish<E>>
    where
        F: Future<Output = std::result::Result<ProposalCommitted, ProposalFailed<E>>>
            + Send
            + 'static,
        E: Send + 'static,
    {
        let state = &handle.state;
        if !state.active.load(std::sync::atomic::Ordering::Acquire) {
            return Err(BrokerError::StreamHandleInactive(handle.id()));
        }
        if payloads.is_empty() {
            return Ok(ProposedPublish {
                finishing: None,
                count: 0,
                answer_early: false,
                answer_at: None,
                answer_within: None,
                batch_first: None,
            });
        }
        let timestamp_micros = crate::append_time_now();
        let handed = state
            .proposals
            .hand_over(stalled_after, || {
                Box::pin(propose(timestamp_micros)) as Committing<E>
            })
            .map_err(|waited| BrokerError::ProposalsStalled { waited })?;
        Ok(ProposedPublish {
            count: payloads.len() as u64,
            // Whether the record landed where expected is known from where
            // it was written, but the answer waits for the commit so it is
            // the group's outcome.
            answer_early: handed.answer_early && expected.is_none(),
            answer_at: None,
            finishing: Some(Box::pin(finish(
                handle.clone(),
                payloads,
                attribution,
                timestamp_micros,
                expected,
                trim,
                commit,
                handed.queued,
                handed.turn,
            ))),
            answer_within: None,
            batch_first: None,
        })
    }
}

impl Broker {
    /// `producer_id`'s turn on the stream, for [`Self::propose_idempotent`].
    ///
    /// The future takes its place behind the producer's earlier batches
    /// when it is first polled, so poll it once in the stream's publish
    /// order. The rest of the wait needs no order kept, and holds up no
    /// other producer's batch.
    pub fn producer_turn(
        &self,
        handle: &StreamHandle,
        producer_id: u64,
    ) -> impl Future<Output = ProducerTurn> + Send + 'static {
        let turn = handle.state.producers.serialise(producer_id);
        async move {
            ProducerTurn {
                producer_id,
                _held: turn.lock_owned().await,
            }
        }
    }

    /// Whether a stream on an engine takes a new idempotent batch of
    /// `records` records: [`BrokerError::NotOnEngineStream`] for more than
    /// one. [`Self::propose_idempotent`] asks after its look at the log, so
    /// a re-send of a batch the log holds whole or in part is still
    /// answered. A caller must not refuse a batch by this before then.
    pub fn engine_takes_idempotent(records: usize) -> Result<()> {
        if records > 1 {
            return Err(BrokerError::NotOnEngineStream {
                what: SEVERAL_RECORDS,
            });
        }
        Ok(())
    }

    /// [`Self::propose_publish`] for an idempotent producer's batch: the
    /// sequence is checked against the stream's log first, as
    /// [`Self::claim_batch_idempotent`] checks it, and only the producer's
    /// next batch is handed to the engine.
    ///
    /// The log learns a batch when the engine writes it, not when it is
    /// handed over. So the producer's batches take turns: `turn` is the
    /// producer's ([`Self::producer_turn`]), and `propose` is given it and
    /// must drop it once the engine has written the batch to this broker's
    /// log or given up on it, whether or not the future it returns is
    /// polled by then.
    ///
    /// `propose` is also given the records to write ([`OwedRecords`]) and
    /// must store each with the producer's id, the sequence, its place in
    /// the batch and the batch's length.
    ///
    /// A batch the log already holds is [`IdempotentProposal::Held`], with
    /// nothing handed over. Its record may not be committed yet; the
    /// caller answers once it is.
    ///
    /// A new batch is one record. One of several is refused with
    /// [`BrokerError::NotOnEngineStream`] before anything is written: its
    /// records would be an entry each, and a leader change or a lapsed
    /// lease can cut those anywhere.
    ///
    /// A batch the log holds part of (a leader stopped partway) is never
    /// written again whole. A re-send whose start differs from the records
    /// held is refused with [`BrokerError::SequenceReused`] whatever `reuse`
    /// is, only the records the log does not hold are handed
    /// to `propose`, and the answer's first offset is the start's. When
    /// the log cannot tell what it holds of the batch, the re-send is
    /// refused with [`BrokerError::SequenceExpired`]. See
    /// `docs/per-shard-raft.md`, "Idempotent publishes".
    #[allow(clippy::too_many_arguments)]
    pub fn propose_idempotent<F, E>(
        &self,
        handle: &StreamHandle,
        turn: ProducerTurn,
        sequence: u64,
        mut payloads: Vec<Bytes>,
        mut attribution: Attribution,
        reuse: SequenceReuse,
        stalled_after: Duration,
        propose: impl FnOnce(u64, ProducerTurn, OwedRecords) -> F,
    ) -> Result<IdempotentProposal<E>>
    where
        F: Future<Output = std::result::Result<ProposalCommitted, ProposalFailed<E>>>
            + Send
            + 'static,
        E: Send + 'static,
    {
        let Some(log) = &handle.state.durable else {
            return Err(BrokerError::Storage(
                "an idempotent batch was proposed on a stream with no log".to_string(),
            ));
        };
        let producer_id = turn.producer_id;
        let len = payloads.len();
        let mut position = 0;
        let mut batch_first = None;
        // Asked first: the log's place for the producer does not count a
        // batch it holds only part of, and would call this one new.
        if let Some(unfinished) = log.producer_unfinished(producer_id, sequence) {
            let held = unfinished.held as usize;
            let Some(first) = unfinished.first else {
                return Err(BrokerError::SequenceExpired { sequence });
            };
            if unfinished.len as usize != len || held >= len {
                return Err(BrokerError::SequenceExpired { sequence });
            }
            // Whatever `reuse` says: writing the rest of other payloads
            // after this start would make one batch of two.
            match unfinished.digest {
                Some(start) if start == PayloadDigest::of(&payloads[..held]) => {}
                Some(_) => return Err(BrokerError::SequenceReused { sequence }),
                None => return Err(BrokerError::SequenceExpired { sequence }),
            }
            payloads.drain(..held);
            if let Some(headers) = &mut attribution.headers {
                headers.drain(..held.min(headers.len()));
            }
            // A rest with no block is stored, and so read back, without any.
            if attribution
                .headers
                .as_ref()
                .is_some_and(|headers| headers.iter().all(Option::is_none))
            {
                attribution.headers = None;
            }
            position = unfinished.held;
            batch_first = Some(first);
        } else {
            match log.producer_sequence(producer_id, sequence) {
                ProducerSequence::Held {
                    first,
                    last,
                    digest,
                } => {
                    reuse.check(sequence, digest, &payloads)?;
                    return Ok(IdempotentProposal::Held { first, last });
                }
                ProducerSequence::Unknown if sequence != 0 => {
                    return Err(BrokerError::UnknownProducer { producer_id });
                }
                ProducerSequence::Gap { expected } => {
                    return Err(BrokerError::SequenceGap { expected });
                }
                // An open batch is unfinished, and the producer's turn
                // keeps one from opening between the two looks.
                ProducerSequence::Expired | ProducerSequence::Partial { .. } => {
                    return Err(BrokerError::SequenceExpired { sequence });
                }
                ProducerSequence::Unknown | ProducerSequence::Next => {}
            }
            // After the look: a multi-record batch the log holds whole is
            // still answered as held.
            Self::engine_takes_idempotent(len)?;
        }
        let owed = OwedRecords {
            payloads: payloads.clone(),
            headers: attribution.headers.clone(),
            position,
            len: len as u32,
        };
        let mut proposed = self.propose_publish_with(
            handle,
            payloads,
            attribution,
            stalled_after,
            |timestamp_micros| propose(timestamp_micros, turn, owed),
        )?;
        proposed.batch_first = batch_first;
        Ok(IdempotentProposal::Proposed(proposed))
    }
}

/// The records [`Broker::propose_idempotent`] hands to the engine: a whole
/// batch, or the rest of one the log holds the start of.
pub struct OwedRecords {
    pub payloads: Vec<Bytes>,
    /// Each record's headers block, when any has one.
    pub headers: Option<Vec<Option<Bytes>>>,
    /// The first record's place in the producer's batch. Zero for a whole
    /// batch.
    pub position: u32,
    /// Records in the producer's whole batch.
    pub len: u32,
}

/// A batch's publisher and each record's key and headers block, for a
/// shard whose engine stores them in its own entries.
#[derive(Debug, Clone, Default)]
pub struct Attribution {
    pub publisher: Option<Bytes>,
    /// One per record when set.
    pub headers: Option<Vec<Option<Bytes>>>,
}

/// What [`Broker::propose_idempotent`] refuses, as
/// [`BrokerError::NotOnEngineStream`] names it.
const SEVERAL_RECORDS: &str = "idempotent batches of more than one record";

/// What [`Broker::propose_publish_at`] refuses.
const SEVERAL_CONDITIONAL: &str = "conditional publishes of more than one record";

/// The offset a conditional publish must land at, and its answer when it
/// does not.
pub(in crate::broker) struct Expected<E> {
    pub(in crate::broker) offset: u64,
    pub(in crate::broker) mismatch: Box<dyn FnOnce(u64) -> E + Send>,
}

/// A producer's turn on a stream. While it is held, no other batch of the
/// producer is checked against the log. See [`Broker::propose_idempotent`].
pub struct ProducerTurn {
    producer_id: u64,
    _held: tokio::sync::OwnedMutexGuard<()>,
}

impl ProducerTurn {
    pub fn producer_id(&self) -> u64 {
        self.producer_id
    }
}

/// What [`Broker::propose_idempotent`] did with a batch.
#[must_use = "a proposed publish reports its outcome only through `complete`"]
pub enum IdempotentProposal<E: Send + 'static> {
    /// The log already holds the batch at these offsets, inclusive. Nothing
    /// was handed to the engine.
    Held { first: u64, last: u64 },
    /// The batch was new and is on its way.
    Proposed(ProposedPublish<E>),
}

/// Where an engine committed a proposed batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProposalCommitted {
    /// One past the batch's last offset.
    pub end: u64,
    /// How many offsets just below the batch hold no record (a leader's
    /// blank entry, a membership change). Subscribers are told they were
    /// skipped, so the jump is not read as a drop.
    pub skipped_before: u64,
}

/// An engine gave up on a batch it was handed.
#[derive(Debug)]
pub struct ProposalFailed<E> {
    pub error: E,
    /// One past the batch's last offset, if the engine wrote it. The batch
    /// may still commit, so readers resuming here start above it and read
    /// it from the log.
    pub written_end: Option<u64>,
}

impl<E> ProposalFailed<E> {
    /// The engine never wrote the batch.
    pub fn unwritten(error: E) -> Self {
        Self {
            error,
            written_end: None,
        }
    }

    /// The engine wrote the batch up to `end` and cannot say it committed.
    pub fn written(error: E, end: u64) -> Self {
        Self {
            error,
            written_end: Some(end),
        }
    }
}

/// A batch handed to the shard's engine, on its way to its subscribers.
///
/// Dropped before [`Self::complete`] finishes, the rest runs on a task of
/// its own.
#[must_use = "a proposed publish reports its outcome only through `complete`"]
pub struct ProposedPublish<E: Send + 'static> {
    finishing: Option<Finishing<E>>,
    /// How many records the batch holds.
    count: u64,
    /// The stream had little enough waiting for a commit when this batch
    /// was handed over for [`Self::answer_at`] to apply.
    answer_early: bool,
    answer_at: Option<oneshot::Receiver<u64>>,
    answer_within: Option<AnswerWithin<E>>,
    /// Where the batch starts when these records are only the rest of it:
    /// the first offset of the start an earlier leader wrote.
    batch_first: Option<u64>,
}

type AnswerWithin<E> = (Duration, Box<dyn FnOnce() -> E + Send>);

impl<E: Send + 'static> ProposedPublish<E> {
    /// Answer [`Self::complete`] as soon as `written` is sent the batch's
    /// end (one past its last offset), without waiting for the commit: an
    /// acknowledgement on the leader's own log. Delivery does not move. The
    /// batch still goes out only once the engine commits it, in its place,
    /// and not at all if the engine gives up on it, though its publisher
    /// was answered. A sender dropped unsent leaves the answer at the
    /// commit.
    ///
    /// The answer also stays at the commit when the stream already had
    /// [`crate::ANSWERED_AHEAD`] batches waiting for theirs at the hand-over, or
    /// its oldest had waited a quarter of `stalled_after`. A publisher that
    /// outruns the engine's commits is then slowed to their pace, instead
    /// of piling batches up until `propose_publish` refuses the next.
    pub fn answer_at(mut self, written: oneshot::Receiver<u64>) -> Self {
        if self.answer_early {
            self.answer_at = Some(written);
        }
        self
    }

    /// Answer [`Self::complete`] with `timed_out` if the batch is not
    /// committed within `timeout`. The batch keeps its place and is still
    /// delivered if it commits.
    pub fn answer_within(
        mut self,
        timeout: Duration,
        timed_out: impl FnOnce() -> E + Send + 'static,
    ) -> Self {
        self.answer_within = Some((timeout, Box::new(timed_out)));
        self
    }

    /// Wait until the batch is committed and queued for delivery behind
    /// every batch handed over before it, or until [`Self::answer_at`]
    /// says it may be answered. Reports its offsets and no subscribers:
    /// the fanout runs after this returns. The offsets of a batch finished
    /// after a leader stopped partway run from the start's first record to
    /// the last one written now, and are not one run: other records lie
    /// between its pieces.
    pub async fn complete(mut self) -> std::result::Result<PublishOutcome, E> {
        let Some(finishing) = self.finishing.as_mut() else {
            return Ok(PublishOutcome {
                subscribers: 0,
                offsets: None,
            });
        };
        let count = self.count;
        let written = self.answer_at.take();
        // The batch's offsets, if the engine says it may be answered where
        // it was written.
        let written = async move {
            let end = written?.await.ok()?;
            Some((end.checked_sub(count)?, end - 1))
        };
        let answered = async {
            tokio::select! {
                biased;
                // First: this is what sends `written`, and a commit that is
                // already known wins.
                outcome = finishing => Ok(outcome),
                Some(offsets) = written => Err(offsets),
            }
        };
        let answered = match self.answer_within.take() {
            None => answered.await,
            Some((timeout, timed_out)) => {
                match tokio::time::timeout(timeout, answered).await {
                    Ok(answered) => answered,
                    // `Drop` finishes the batch on a task.
                    Err(_) => return Err(timed_out()),
                }
            }
        };
        let batch_first = self.batch_first;
        let outcome = match answered {
            Ok(outcome) => {
                self.finishing = None;
                outcome
            }
            // Not committed yet. `Drop` runs the rest on a task.
            Err(offsets) => Ok(PublishOutcome {
                subscribers: 0,
                offsets: Some(offsets),
            }),
        };
        outcome.map(|mut outcome| {
            if let (Some(first), Some((_, last))) = (batch_first, outcome.offsets) {
                outcome.offsets = Some((first, last));
            }
            outcome
        })
    }
}

impl<E: Send + 'static> Drop for ProposedPublish<E> {
    fn drop(&mut self) {
        let Some(finishing) = self.finishing.take() else {
            return;
        };
        // Outside a runtime the process is going down, and nothing is left
        // to deliver to.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            let _ = finishing.await;
        });
    }
}

/// Wait for the commit, then for the turn of the batch handed over before
/// this one, and join the hold. The turn is released however this ends.
///
/// A batch the engine gave up on still takes its turn, as a gap: its
/// records may have committed, so the replay ring restarts above it and a
/// reader resuming below it reads the log, never a ring that skips it.
/// A trim's floor, and who is told once it reaches the ring.
pub(in crate::broker) struct HeldTrim {
    pub(in crate::broker) before: u64,
    pub(in crate::broker) applied: oneshot::Sender<()>,
}

#[allow(clippy::too_many_arguments)]
async fn finish<E>(
    handle: StreamHandle,
    payloads: Vec<Bytes>,
    attribution: Attribution,
    timestamp_micros: u64,
    expected: Option<Expected<E>>,
    trim: Option<HeldTrim>,
    commit: Option<Arc<[StateOp]>>,
    committed: Committing<E>,
    turn: CommitTurn<'static>,
) -> std::result::Result<PublishOutcome, E> {
    let committed = committed.await;
    let count = payloads.len() as u64;
    let first_offset = match &committed {
        Ok(at) => at.end.checked_sub(count),
        Err(_) => None,
    };
    // Off its offset, the leader's log stored a blank, which followers copy.
    // Its outcome is known, so it is held as a blank, not a gap: nothing is
    // sent and the replay ring stays.
    let missed = match (&committed, &expected) {
        (Ok(at), Some(expected)) if first_offset != Some(expected.offset) => Some(at.end),
        _ => None,
    };
    if let (Ok(at), None) = (&committed, first_offset) {
        tracing::error!(
            end = at.end,
            count,
            "an engine's committed end is below its batch"
        );
    }
    // Never reset, so a wait cannot be refused.
    let _ = turn.wait().await;
    let state = &handle.state;
    // A commit is answered once its state updates are in the view, so the
    // writer's next state read has them. Dropped unsent if a reset took the
    // view, which a read then rebuilds from the log.
    let (applied, in_view) = match commit {
        Some(_) => {
            let (applied, in_view) = oneshot::channel();
            (Some(applied), Some(in_view))
        }
        None => (None, None),
    };
    let batch = match (&committed, first_offset) {
        (Ok(_), Some(first_offset)) if missed.is_some() => HeldBatch::blank(first_offset),
        (Ok(at), Some(first_offset)) if let Some(trim) = trim => HeldBatch {
            payloads,
            first_offset,
            trim: Some(trim.before),
            ring_applied: Some(trim.applied),
            skipped_before: at.skipped_before,
            gap: false,
            ..HeldBatch::gap(None)
        },
        (Ok(at), Some(first_offset)) => HeldBatch {
            payloads,
            publisher: attribution.publisher,
            record_headers: attribution.headers.map(Into::into),
            timestamp_micros: Some(timestamp_micros),
            first_offset,
            commit,
            trim: None,
            ring_applied: applied,
            skipped_before: at.skipped_before,
            gap: false,
            gap_end: None,
            blank: false,
        },
        (Err(failed), _) => HeldBatch::gap(failed.written_end),
        (Ok(at), None) => HeldBatch::gap(Some(at.end)),
    };
    let start = state.held.push(batch);
    state.proposals.settled();
    drop(turn);
    if start {
        spawn_release(&handle);
    }
    committed.map_err(|failed| failed.error)?;
    if let (Some(tail), Some(expected)) = (missed, expected) {
        return Err((expected.mismatch)(tail));
    }
    if let Some(in_view) = in_view {
        let _ = in_view.await;
    }
    Ok(PublishOutcome {
        subscribers: 0,
        offsets: first_offset.map(|first| (first, first + count - 1)),
    })
}

#[cfg(test)]
mod tests;

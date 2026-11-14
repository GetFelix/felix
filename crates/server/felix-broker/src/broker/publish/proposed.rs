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
use std::time::Duration;

use bytes::Bytes;
use felix_storage::CommitTurn;
use felix_storage::disk_log::ProducerSequence;
use felix_storage::log::PayloadDigest;

use super::completion::spawn_release;
use super::{PublishOutcome, SequenceReuse};
use crate::broker::Broker;
use crate::broker::shards::StreamHandle;
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
        let state = &handle.state;
        if !state.active.load(std::sync::atomic::Ordering::Acquire) {
            return Err(BrokerError::StreamHandleInactive(handle.id()));
        }
        if payloads.is_empty() {
            return Ok(ProposedPublish {
                finishing: None,
                answer_within: None,
                batch_first: None,
            });
        }
        let timestamp_micros = crate::append_time_now();
        let (committed, turn) = state
            .proposals
            .hand_over(stalled_after, || {
                Box::pin(propose(timestamp_micros)) as Committing<E>
            })
            .map_err(|waited| BrokerError::ProposalsStalled { waited })?;
        Ok(ProposedPublish {
            finishing: Some(Box::pin(finish(
                handle.clone(),
                payloads,
                timestamp_micros,
                committed,
                turn,
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

    /// Whether a stream on an engine takes an idempotent batch of `records`
    /// records: [`BrokerError::NotOnEngineStream`] for more than one. For a
    /// caller to refuse the batch before it waits for the producer's turn;
    /// [`Self::propose_idempotent`] refuses a new one too.
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
            position,
            len: len as u32,
        };
        let mut proposed =
            self.propose_publish(handle, payloads, stalled_after, |timestamp_micros| {
                propose(timestamp_micros, turn, owed)
            })?;
        proposed.batch_first = batch_first;
        Ok(IdempotentProposal::Proposed(proposed))
    }
}

/// The records [`Broker::propose_idempotent`] hands to the engine: a whole
/// batch, or the rest of one the log holds the start of.
pub struct OwedRecords {
    pub payloads: Vec<Bytes>,
    /// The first record's place in the producer's batch. Zero for a whole
    /// batch.
    pub position: u32,
    /// Records in the producer's whole batch.
    pub len: u32,
}

/// What [`Broker::propose_idempotent`] refuses, as
/// [`BrokerError::NotOnEngineStream`] names it.
const SEVERAL_RECORDS: &str = "idempotent batches of more than one record";

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
    answer_within: Option<AnswerWithin<E>>,
    /// Where the batch starts when these records are only the rest of it:
    /// the first offset of the start an earlier leader wrote.
    batch_first: Option<u64>,
}

type AnswerWithin<E> = (Duration, Box<dyn FnOnce() -> E + Send>);

impl<E: Send + 'static> ProposedPublish<E> {
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
    /// every batch handed over before it. Reports its offsets and no
    /// subscribers: the fanout runs after this returns. The offsets of a
    /// batch finished after a leader stopped partway run from the start's
    /// first record to the last one written now, and are not one run:
    /// other records lie between its pieces.
    pub async fn complete(mut self) -> std::result::Result<PublishOutcome, E> {
        let Some(finishing) = self.finishing.as_mut() else {
            return Ok(PublishOutcome {
                subscribers: 0,
                offsets: None,
            });
        };
        let outcome = match self.answer_within.take() {
            None => finishing.await,
            Some((timeout, timed_out)) => {
                match tokio::time::timeout(timeout, finishing).await {
                    Ok(outcome) => outcome,
                    // `Drop` finishes the batch on a task.
                    Err(_) => return Err(timed_out()),
                }
            }
        };
        self.finishing = None;
        let batch_first = self.batch_first;
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
async fn finish<E>(
    handle: StreamHandle,
    payloads: Vec<Bytes>,
    timestamp_micros: u64,
    committed: Committing<E>,
    turn: CommitTurn<'static>,
) -> std::result::Result<PublishOutcome, E> {
    let committed = committed.await;
    let count = payloads.len() as u64;
    let first_offset = match &committed {
        Ok(at) => at.end.checked_sub(count),
        Err(_) => None,
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
    let batch = match (&committed, first_offset) {
        (Ok(at), Some(first_offset)) => HeldBatch {
            payloads,
            publisher: None,
            record_headers: None,
            timestamp_micros: Some(timestamp_micros),
            first_offset,
            commit: None,
            trim: None,
            trim_applied: None,
            skipped_before: at.skipped_before,
            gap: false,
            gap_end: None,
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
    Ok(PublishOutcome {
        subscribers: 0,
        offsets: first_offset.map(|first| (first, first + count - 1)),
    })
}

#[cfg(test)]
mod tests;

//! Idempotent publishes from a producer that numbers records, not batches.
//!
//! A Kafka producer gives each record of a partition the next sequence, so a
//! batch's base sequence advances by the batch's record count. Felix's log
//! expects the next batch to carry the last sequence plus one. Rather than
//! teach the log a second rule, each record is stored as a producer batch of
//! its own: record `i` of a batch carries sequence `base + i`, and the log's
//! "last plus one" is exactly Kafka's rule.
//!
//! Everything else is the log's, as for any idempotent publish (see
//! `stream/producers.rs`): the marks replicate with the records, so a promoted
//! leader or a move's destination answers a re-sent batch as a duplicate
//! instead of appending it again. The cost is a producer mark on every record
//! rather than on the first of each batch.
//!
//! A re-send is not checked against the payload digest the log keeps. Kafka
//! answers a duplicate sequence by its numbers alone, and has no error that
//! means "same sequence, different records"; its producers start a new epoch
//! rather than reuse a sequence whose outcome they do not know.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::disk_log::ProducerSequence;
use felix_storage::log::{ProducerBatch, RecordMark};

use super::{
    Append, Attribution, IdempotentOutcome, IdempotentProposal, ProducerTurn, ProposalCommitted,
    ProposalFailed, PublishOutcome,
};
use crate::broker::Broker;
use crate::broker::shards::StreamHandle;
use crate::error::{BrokerError, Result};

/// Record sequences are 31 bits and wrap to zero, as Kafka's producers number
/// them. The log counts in 64 bits, so a sequence is lifted to the count
/// nearest the one its producer owes.
pub const RECORD_SEQUENCE_WRAP: u64 = 1 << 31;

impl Broker {
    /// Publish a batch whose records are numbered one by one from
    /// `first_sequence`, appending each record at most once.
    ///
    /// The batch is appended when `first_sequence` is the next record the
    /// producer owes. When every record is already in the log, nothing is
    /// written and the answer is a duplicate carrying where the batch landed.
    /// When the log holds only the start of it (its leader stopped partway),
    /// the rest is written. A sequence ahead of what the producer owes is
    /// [`BrokerError::SequenceGap`], a producer the log does not know starting
    /// anywhere but zero is [`BrokerError::UnknownProducer`], and a duplicate
    /// too old to say where it landed, or at or below a forgotten producer's
    /// tombstone, is [`BrokerError::SequenceExpired`].
    ///
    /// `headers` is each record's key and headers block, as for
    /// [`Broker::claim_publish`]. A broker that does not accept headers
    /// refuses a batch that needs to write any, but a batch already fully in
    /// the log is answered as a duplicate without checking the gate.
    ///
    /// Needs a durable stream, whose log is what remembers the sequences. Any
    /// other is refused with [`BrokerError::StreamNotDurable`], without names,
    /// since the handle does not carry them.
    pub async fn publish_records_idempotent(
        &self,
        handle: &StreamHandle,
        producer_id: u64,
        first_sequence: u64,
        payloads: &[Bytes],
        headers: Option<&[Option<Bytes>]>,
    ) -> Result<IdempotentOutcome> {
        let claim = self
            .claim_records_idempotent(handle, producer_id, first_sequence, payloads, headers)
            .await?;
        self.complete_records_idempotent(handle, claim).await
    }

    /// The first half of [`Self::publish_records_idempotent`]: check the
    /// sequences and claim the records' offsets, without waiting for them
    /// to be durable. Refused as that call is.
    ///
    /// The log counts the records as the producer's from here on, so a
    /// later batch from the same producer is checked against them and may
    /// be claimed before these are durable. Complete the claim with
    /// [`Self::complete_records_idempotent`].
    pub async fn claim_records_idempotent(
        &self,
        handle: &StreamHandle,
        producer_id: u64,
        first_sequence: u64,
        payloads: &[Bytes],
        headers: Option<&[Option<Bytes>]>,
    ) -> Result<RecordsClaim> {
        let Some(log) = &handle.state.durable else {
            return Err(BrokerError::StreamNotDurable {
                tenant_id: String::new(),
                namespace: String::new(),
                stream: String::new(),
            });
        };
        if payloads.is_empty() {
            return Ok(RecordsClaim(Claim::Empty));
        }
        let headers = super::usable_headers(payloads, headers)?;
        // Serialises this producer's batches, so two re-sends of one batch
        // cannot both find it unwritten.
        let turn = handle.state.producers.serialise(producer_id);
        let _turn = turn.lock().await;

        let owed = log.producer_next_sequence(producer_id);
        let first = lift(first_sequence, log.producer_lift_basis(producer_id));
        let count = payloads.len() as u64;
        let last = first + count - 1;
        let written = match owed {
            // A forgotten producer's tombstone: this may be a re-send of
            // records the log no longer remembers, so it is not written again.
            None if log.producer_sequence(producer_id, first) == ProducerSequence::Retired => {
                return Err(BrokerError::SequenceExpired { sequence: first });
            }
            None if first == 0 => 0,
            None => return Err(BrokerError::UnknownProducer { producer_id }),
            Some(owed) if owed < first => return Err(BrokerError::SequenceGap { expected: owed }),
            Some(owed) => owed - first,
        };

        if written >= count {
            let (Some(start), Some(end)) = (
                held(log.producer_sequence(producer_id, first)),
                held(log.producer_sequence(producer_id, last)),
            ) else {
                return Err(BrokerError::SequenceExpired { sequence: first });
            };
            return Ok(RecordsClaim(Claim::Held((start.0, end.1))));
        }

        // Where the part already written landed, looked up before appending
        // the rest can push it out of the window.
        let start = (written > 0)
            .then(|| held(log.producer_sequence(producer_id, first)))
            .flatten();
        let marks: Vec<RecordMark> = (first + written..=last)
            .map(|sequence| {
                RecordMark::Opens(ProducerBatch {
                    producer_id,
                    sequence,
                    len: 1,
                })
            })
            .collect();
        let claimed = self
            .claim(
                handle,
                &payloads[written as usize..],
                Append::Marked(&marks),
                None,
                headers.map(|headers| &headers[written as usize..]),
            )
            .await?
            .expect("a marked append always claims");
        Ok(RecordsClaim(Claim::Written {
            start,
            claimed: Box::new(claimed),
        }))
    }

    /// Wait for a [`Self::claim_records_idempotent`] claim to be durable, and
    /// say where its records landed.
    pub async fn complete_records_idempotent(
        &self,
        handle: &StreamHandle,
        claim: RecordsClaim,
    ) -> Result<IdempotentOutcome> {
        let (start, claimed) = match claim.0 {
            Claim::Empty => {
                return Ok(IdempotentOutcome {
                    outcome: PublishOutcome {
                        subscribers: 0,
                        offsets: None,
                    },
                    duplicate: false,
                });
            }
            Claim::Held(offsets) => {
                // As for any duplicate: vouch for it only once it is as
                // durable here as a fresh append would be.
                if let Some(log) = &handle.state.durable {
                    log.wait_durable(offsets.1 + 1).await?;
                }
                return Ok(IdempotentOutcome {
                    outcome: PublishOutcome {
                        subscribers: 0,
                        offsets: Some(offsets),
                    },
                    duplicate: true,
                });
            }
            Claim::Written { start, claimed } => (start, *claimed),
        };
        let outcome = self.complete_publish(claimed).await?;
        // A batch finished after its start was written reports the start's
        // offset when it is still known, else the offset of what this call
        // wrote. The two parts need not be adjacent: others may have
        // appended in between.
        let offsets = match (start, outcome.offsets) {
            (Some((first, _)), Some((_, end))) => Some((first, end)),
            (_, offsets) => offsets,
        };
        Ok(IdempotentOutcome {
            outcome: PublishOutcome {
                subscribers: outcome.subscribers,
                offsets,
            },
            duplicate: false,
        })
    }

    /// [`Self::propose_idempotent`] for a producer that numbers records:
    /// `payloads` are records `first_sequence..`, each a one-record batch
    /// of its own as [`Self::publish_records_idempotent`] stores them, and
    /// the new ones go to the engine in one `propose` call under one turn.
    ///
    /// The log is asked about each record in turn. Those it holds are not
    /// written again; the first it does not hold is checked as
    /// [`Self::propose_idempotent`] checks a one-record batch, with the same
    /// refusals, and every record after it is new. That holds because the
    /// turn keeps the producer's other batches out until `propose` has
    /// written these or given up on them, and each record's sequence is the
    /// one before plus one.
    ///
    /// Every record held is [`IdempotentProposal::Held`]. Otherwise the
    /// answer runs from the first held record's offset, if any, to the last
    /// written. A re-send is answered by its numbers alone, as Kafka does.
    /// `propose` must store record `i` of [`OwedSequences`] with sequence
    /// `sequence + i`, position 0 and length 1, and drop the turn once the
    /// engine has written them or given up, whether or not its future is
    /// polled by then.
    #[allow(clippy::too_many_arguments)]
    pub fn propose_records_idempotent<F, E>(
        &self,
        handle: &StreamHandle,
        turn: ProducerTurn,
        first_sequence: u64,
        mut payloads: Vec<Bytes>,
        mut attribution: Attribution,
        stalled_after: Duration,
        propose: impl FnOnce(u64, ProducerTurn, OwedSequences) -> F,
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
        // As the hybrid answers it: no offsets, nothing proposed, and the
        // producer's state untouched.
        if payloads.is_empty() {
            return Ok(IdempotentProposal::Empty);
        }
        let producer_id = turn.producer_id();
        let mut start: Option<(u64, u64)> = None;
        let mut held = 0;
        while held < payloads.len() {
            let sequence = first_sequence + held as u64;
            // A one-record batch is never unfinished, so this is another
            // writer's batch of several under the same id.
            if log.producer_unfinished(producer_id, sequence).is_some() {
                return Err(BrokerError::SequenceExpired { sequence });
            }
            match log.producer_sequence(producer_id, sequence) {
                ProducerSequence::Held { first, last, .. } => {
                    start = Some((start.map_or(first, |(first, _)| first), last));
                    held += 1;
                    continue;
                }
                ProducerSequence::Unknown if sequence != 0 => {
                    return Err(BrokerError::UnknownProducer { producer_id });
                }
                ProducerSequence::Gap { expected } => {
                    return Err(BrokerError::SequenceGap { expected });
                }
                ProducerSequence::Expired
                | ProducerSequence::Partial { .. }
                | ProducerSequence::Retired => {
                    return Err(BrokerError::SequenceExpired { sequence });
                }
                ProducerSequence::Unknown | ProducerSequence::Next => break,
            }
        }
        if held == payloads.len() {
            let (first, last) = start.expect("at least one record");
            return Ok(IdempotentProposal::Held { first, last });
        }
        payloads.drain(..held);
        if let Some(headers) = &mut attribution.headers {
            headers.drain(..held.min(headers.len()));
        }
        let owed = OwedSequences {
            payloads: payloads.clone(),
            headers: attribution.headers.clone(),
            sequence: first_sequence + held as u64,
        };
        let mut proposed = self.propose_publish_with(
            handle,
            payloads,
            attribution,
            stalled_after,
            |timestamp_micros| propose(timestamp_micros, turn, owed),
        )?;
        proposed.batch_first = start.map(|(first, _)| first);
        Ok(IdempotentProposal::Proposed(proposed))
    }

    /// The log's 64-bit count for a record `producer_id` numbered
    /// `sequence` (31 bits, wrapping), as [`Self::publish_records_idempotent`]
    /// reads it. For a caller that writes such a producer's records through
    /// an engine, one record per batch.
    pub fn record_sequence(&self, handle: &StreamHandle, producer_id: u64, sequence: u64) -> u64 {
        let owed = handle
            .state
            .durable
            .as_ref()
            .and_then(|log| log.producer_lift_basis(producer_id));
        lift(sequence, owed)
    }
}

/// The records [`Broker::propose_records_idempotent`] hands to the engine.
pub struct OwedSequences {
    pub payloads: Vec<Bytes>,
    /// Each record's headers block, when any has one.
    pub headers: Option<Vec<Option<Bytes>>>,
    /// The first record's sequence. Each next record's is one more.
    pub sequence: u64,
}

/// Records claimed by [`Broker::claim_records_idempotent`], not yet durable.
#[must_use = "complete the claim, or its records are never answered for"]
pub struct RecordsClaim(Claim);

enum Claim {
    Empty,
    /// Every record was already in the log, at these offsets.
    Held((u64, u64)),
    /// The rest written; `start` is where the part already in the log
    /// landed, when there was one and it is still known.
    Written {
        start: Option<(u64, u64)>,
        claimed: Box<super::ClaimedPublish>,
    },
}

/// The 64-bit count `sequence` stands for: of the values that agree with it
/// modulo [`RECORD_SEQUENCE_WRAP`], the one nearest `owed`.
pub(crate) fn lift(sequence: u64, owed: Option<u64>) -> u64 {
    let sequence = sequence % RECORD_SEQUENCE_WRAP;
    let Some(owed) = owed else {
        return sequence;
    };
    let base = owed - owed % RECORD_SEQUENCE_WRAP;
    [
        base.checked_sub(RECORD_SEQUENCE_WRAP),
        Some(base),
        base.checked_add(RECORD_SEQUENCE_WRAP),
    ]
    .into_iter()
    .flatten()
    .map(|base| base + sequence)
    .min_by_key(|candidate| candidate.abs_diff(owed))
    .expect("the base itself is always a candidate")
}

fn held(sequence: ProducerSequence) -> Option<(u64, u64)> {
    match sequence {
        ProducerSequence::Held { first, last, .. } => Some((first, last)),
        _ => None,
    }
}

#[cfg(test)]
mod tests;

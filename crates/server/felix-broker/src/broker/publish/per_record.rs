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

use bytes::Bytes;
use felix_storage::disk_log::ProducerSequence;
use felix_storage::log::{ProducerBatch, RecordMark};

use super::{Append, IdempotentOutcome, PublishOutcome};
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
    /// too old to say where it landed is [`BrokerError::SequenceExpired`].
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
    ) -> Result<IdempotentOutcome> {
        let Some(log) = &handle.state.durable else {
            return Err(BrokerError::StreamNotDurable {
                tenant_id: String::new(),
                namespace: String::new(),
                stream: String::new(),
            });
        };
        if payloads.is_empty() {
            return Ok(IdempotentOutcome {
                outcome: PublishOutcome {
                    subscribers: 0,
                    offsets: None,
                },
                duplicate: false,
            });
        }
        // Serialises this producer's batches, so two re-sends of one batch
        // cannot both find it unwritten.
        let turn = handle.state.producers.serialise(producer_id);
        let _turn = turn.lock().await;

        let owed = log.producer_next_sequence(producer_id);
        let first = lift(first_sequence, owed);
        let count = payloads.len() as u64;
        let last = first + count - 1;
        let written = match owed {
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
            // As for any duplicate: vouch for it only once it is as durable
            // here as a fresh append would be.
            log.wait_durable(end.1 + 1).await?;
            return Ok(IdempotentOutcome {
                outcome: PublishOutcome {
                    subscribers: 0,
                    offsets: Some((start.0, end.1)),
                },
                duplicate: true,
            });
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
            )
            .await?
            .expect("a marked append always claims");
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

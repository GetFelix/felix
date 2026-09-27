//! Producer sequences: what lets a re-sent batch land once.
//!
//! A producer numbers its batches on a shard from zero. The shard's leader
//! keeps, per producer, the next sequence it expects and the outcome of the
//! last few it appended. A batch carrying the expected sequence is appended
//! and remembered; one carrying a sequence already remembered is answered
//! from memory and not appended, unless its payloads differ from the batch
//! remembered (see `SequenceReuse`); anything else is refused with a reason. That
//! is the whole mechanism, and it is what turns "the acknowledgement never
//! arrived" from a duplicate-or-loss coin toss into a safe re-send.
//!
//! On a durable stream the sequences are the log's: each batch is stored with
//! its producer and sequence, and the log keeps every producer's place (see
//! `felix-storage`'s `disk_log/producers.rs`). They replicate with the
//! records, so a promoted leader, a move's destination and a restarted broker
//! answer a re-send as the leader that took it would have. This table then
//! only serialises each producer's batches.
//!
//! An in-memory stream has no log to keep them in, so this table keeps them,
//! with a digest of each batch's payloads, and they last as long as the
//! leader does.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use felix_storage::log::PayloadDigest;
use parking_lot::Mutex;

use crate::broker::PublishOutcome;
use crate::error::BrokerError;

/// Batches remembered per producer. A re-send arrives shortly after the
/// original, so the window covers a producer's in-flight pipeline rather
/// than its history.
pub(crate) const WINDOW: usize = 64;

/// Producers remembered per shard before the least recently used is let go.
/// Letting one go turns its next batch into `UnknownProducer`, which the
/// producer recovers from; keeping every producer that ever connected would
/// grow without bound.
pub(crate) const MAX_PRODUCERS: usize = 4096;

/// The producers a shard's leader remembers.
#[derive(Debug, Default)]
pub(crate) struct ProducerTable {
    inner: Mutex<Table>,
}

impl ProducerTable {
    /// The producer's turn, creating the producer on its first batch.
    ///
    /// A producer this table has never seen may only start at sequence zero:
    /// anything later says batches came before, and there is nothing here to
    /// check them against.
    pub(crate) fn turn(
        &self,
        producer_id: u64,
        sequence: u64,
    ) -> Result<Arc<tokio::sync::Mutex<()>>, BrokerError> {
        if sequence != 0 && !self.inner.lock().producers.contains_key(&producer_id) {
            return Err(BrokerError::UnknownProducer { producer_id });
        }
        Ok(self.serialise(producer_id))
    }

    /// The lock that serialises `producer_id`'s batches, with no say over its
    /// sequence. For a durable stream, whose log keeps the sequences.
    pub(crate) fn serialise(&self, producer_id: u64) -> Arc<tokio::sync::Mutex<()>> {
        let mut table = self.inner.lock();
        table.clock += 1;
        let clock = table.clock;
        if let Some(producer) = table.producers.get_mut(&producer_id) {
            producer.last_used = clock;
            return Arc::clone(&producer.turn);
        }
        if table.producers.len() >= MAX_PRODUCERS
            && let Some(coldest) = table
                .producers
                .iter()
                .min_by_key(|(_, producer)| producer.last_used)
                .map(|(id, _)| *id)
        {
            table.producers.remove(&coldest);
        }
        let turn = Arc::new(tokio::sync::Mutex::new(()));
        table.producers.insert(
            producer_id,
            Producer {
                next_sequence: 0,
                recent: VecDeque::new(),
                last_used: clock,
                turn: Arc::clone(&turn),
            },
        );
        turn
    }

    /// Classify a batch. Called holding the producer's turn, so the answer
    /// cannot change between deciding and acting on it.
    pub(crate) fn classify(
        &self,
        producer_id: u64,
        sequence: u64,
    ) -> Result<Sequenced, BrokerError> {
        let table = self.inner.lock();
        let Some(producer) = table.producers.get(&producer_id) else {
            // Evicted between taking the turn and using it. The producer was
            // cold enough to lose its place; starting it again from here
            // would accept a batch nothing can check.
            return Err(BrokerError::UnknownProducer { producer_id });
        };
        if sequence == producer.next_sequence {
            return Ok(Sequenced::Append);
        }
        if sequence > producer.next_sequence {
            return Err(BrokerError::SequenceGap {
                expected: producer.next_sequence,
            });
        }
        producer
            .recent
            .iter()
            .rev()
            .find(|(remembered, ..)| *remembered == sequence)
            .map(|(_, outcome, digest)| Sequenced::Duplicate(*outcome, *digest))
            .ok_or(BrokerError::SequenceExpired { sequence })
    }

    /// Record that `sequence`, whose payloads have `digest`, was appended with
    /// `outcome`, and expect the next.
    pub(crate) fn remember(
        &self,
        producer_id: u64,
        sequence: u64,
        outcome: PublishOutcome,
        digest: PayloadDigest,
    ) {
        let mut table = self.inner.lock();
        if let Some(producer) = table.producers.get_mut(&producer_id) {
            producer.next_sequence = sequence + 1;
            if producer.recent.len() == WINDOW {
                producer.recent.pop_front();
            }
            producer.recent.push_back((sequence, outcome, digest));
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().producers.len()
    }
}

/// What to do with a batch, decided under the producer's turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sequenced {
    /// The next batch this producer owes: append it.
    Append,
    /// A batch already appended: what happened the first time, and the
    /// digest of the payloads it was appended with.
    Duplicate(PublishOutcome, PayloadDigest),
}

#[derive(Debug, Default)]
struct Table {
    producers: HashMap<u64, Producer>,
    /// Advances on every use; the smallest value is the coldest producer.
    clock: u64,
}

#[derive(Debug)]
struct Producer {
    next_sequence: u64,
    recent: VecDeque<(u64, PublishOutcome, PayloadDigest)>,
    last_used: u64,
    /// Serialises this producer's batches. Two re-sends of one sequence in
    /// flight at once must not both find it unappended.
    turn: Arc<tokio::sync::Mutex<()>>,
}

#[cfg(test)]
mod tests;

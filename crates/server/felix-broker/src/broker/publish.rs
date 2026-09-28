//! The publish path: claim offsets and a place in the commit order, make the
//! batch durable, append it to the replay ring, then fan it out.
//!
//! The order is the design. Offsets are consumed before the durability wait so
//! a batch holds its place from the moment it has one, and fanout comes only
//! after the batch is durable.

mod completion;
mod per_record;

pub(crate) use completion::spawn_release;

pub use per_record::RECORD_SEQUENCE_WRAP;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use felix_storage::disk_log::ProducerSequence;
use felix_storage::log::{PayloadDigest, RecordMark};

use super::Broker;
use super::shards::StreamHandle;
use crate::error::{BrokerError, Result};
use crate::stream::Sequenced;
use crate::telemetry::{t_histogram, t_now_if, t_should_sample};
use crate::timings;

impl Broker {
    /// Publish one payload to a single-shard stream.
    ///
    /// Shard 0 by construction: a caller with a routing key resolves the shard
    /// first and uses [`Broker::publish_batch`].
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Bytes,
    ) -> Result<usize> {
        let payloads = [payload];
        self.publish_batch(tenant_id, namespace, stream, 0, &payloads)
            .await
    }

    /// Publish a batch to one shard, resolving the stream by name.
    pub async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        payloads: &[Bytes],
    ) -> Result<usize> {
        let sample = t_should_sample();
        let lookup_start = t_now_if(sample);
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        if let Some(start) = lookup_start {
            let lookup_ns = start.elapsed().as_nanos() as u64;
            timings::record_lookup_ns(lookup_ns);
            t_histogram!("broker_publish_lookup_ns").record(lookup_ns as f64);
        }
        self.publish_batch_to_handle(&handle, payloads).await
    }

    /// Publish a batch through a handle resolved earlier.
    pub async fn publish_batch_to_handle(
        &self,
        handle: &StreamHandle,
        payloads: &[Bytes],
    ) -> Result<usize> {
        Ok(self
            .publish_batch_with_outcome(handle, payloads)
            .await?
            .subscribers)
    }

    /// Persist, append and fan out one batch, and report the log offsets it
    /// was assigned.
    ///
    /// Same path as [`Self::publish_batch_to_handle`]; the offsets are what a
    /// forwarding broker relays to the requester, which cannot see this log.
    ///
    /// The two phases back to back. A caller that needs the claim ordered
    /// against other publishes while the flushes overlap should call
    /// [`Broker::claim_publish`] and [`Broker::complete_publish`] itself.
    pub async fn publish_batch_with_outcome(
        &self,
        handle: &StreamHandle,
        payloads: &[Bytes],
    ) -> Result<PublishOutcome> {
        let claimed = self.claim_publish(handle, payloads).await?;
        self.complete_publish(claimed).await
    }

    /// Claim this batch's offsets and its place in the commit order.
    ///
    /// Split out of [`Broker::publish_batch_with_outcome`] so a caller that
    /// must preserve arrival order can do *this* part serially and let the
    /// durability wait overlap. That wait is a device flush under
    /// `FsyncMode::OnCommit` -- hundreds of microseconds against the handful
    /// this costs -- and group commit only has something to coalesce when
    /// several of them are in flight at once (#535).
    ///
    /// Offsets are consumed here, so the order calls return in *is* the order
    /// records land on disk. Complete every claim: dropping one releases its
    /// commit range, but the offsets it consumed stay consumed.
    pub async fn claim_publish(
        &self,
        handle: &StreamHandle,
        payloads: &[Bytes],
    ) -> Result<ClaimedPublish> {
        Ok(self
            .claim(handle, payloads, Append::Plain)
            .await?
            .expect("a plain append always claims"))
    }

    /// [`Broker::claim_publish`], writing the records as `append` says. `None`
    /// only for [`Append::Continuing`] whose batch is no longer open.
    async fn claim(
        &self,
        handle: &StreamHandle,
        payloads: &[Bytes],
        append: Append<'_>,
    ) -> Result<Option<ClaimedPublish>> {
        if !handle.state.active.load(Ordering::Acquire) {
            return Err(BrokerError::StreamHandleInactive(handle.id()));
        }

        let sample = t_should_sample();
        let mut claimed = ClaimedPublish {
            handle: handle.clone(),
            payloads: payloads.to_vec(),
            durable: None,
            sample,
        };
        if payloads.is_empty() {
            return Ok(Some(claimed));
        }

        if let Some(durable) = &handle.state.durable {
            let durable_start = t_now_if(sample);
            // Offsets are consumed here. The commit order has to be claimed
            // against them immediately, before the durability wait, because
            // from this point the records exist on disk and everything
            // behind them queues on this range. Claiming it only after a
            // *successful* wait stranded the stream: a failed or cancelled
            // publish abandoned its range, and every later publish waited
            // on a turn that could never arrive.
            let pending = match append {
                Append::Plain => durable.begin_append(payloads).await?,
                Append::Marked(marks) => durable.begin_append_marked(payloads, marks).await?,
                Append::Continuing {
                    producer_id,
                    sequence,
                } => match durable
                    .continue_batch(producer_id, sequence, payloads)
                    .await?
                {
                    Some(pending) => pending,
                    None => return Ok(None),
                },
            };
            let turn = handle
                .state
                .commit_sequencer
                .reserve_owned(pending.first_offset(), pending.last_offset() + 1);
            claimed.durable = Some(ClaimedDurable {
                pending,
                turn,
                durable_start,
            });
        }
        Ok(Some(claimed))
    }

    /// Make a [`ClaimedPublish`] durable, then append and fan it out.
    ///
    /// Safe to run concurrently with other completions on the same stream:
    /// the commit turn claimed in [`Broker::claim_publish`] is what keeps disk
    /// order, cursor order and delivery order in agreement, so overlapping the
    /// flushes does not disturb what anybody observes.
    ///
    /// Cancelling the returned future does not cancel the batch: once its
    /// offsets are claimed its records exist, so the ring append and fanout
    /// finish on a detached task. See `publish/completion.rs`.
    pub async fn complete_publish(&self, claimed: ClaimedPublish) -> Result<PublishOutcome> {
        let send_start = t_now_if(claimed.sample);
        let outcome = completion::Finisher::new(completion::Completion::new(
            claimed,
            self.log_capacity,
            Arc::clone(&self.appended),
        ))
        .run()
        .await;
        if let Some(start) = send_start {
            let send_ns = start.elapsed().as_nanos() as u64;
            timings::record_send_ns(send_ns);
            t_histogram!("broker_publish_send_ns").record(send_ns as f64);
        }
        outcome
    }

    /// A producer id no other producer of this broker holds.
    ///
    /// Random rather than counted, so ids from two brokers, or from one
    /// broker across a restart, do not collide with each other's sequences.
    pub fn new_producer_id(&self) -> u64 {
        use std::hash::{BuildHasher, Hasher};
        let mut hasher = self.producer_ids.build_hasher();
        hasher.write_u64(
            self.producer_id_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        );
        hasher.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_nanos())
                .unwrap_or(0),
        );
        // Zero is reserved for "no producer" in places that carry the id
        // beside an optional; never hand it out.
        hasher.finish().max(1)
    }

    /// Publish a batch that is appended once however many times it arrives.
    ///
    /// `sequence` is this producer's count of batches on this shard, from
    /// zero. The next expected is appended; one already appended is answered
    /// with where it landed and nothing is written; a gap, a producer this
    /// shard does not know, or a sequence older than it remembers is refused
    /// with the matching [`BrokerError`], and nothing is written then either.
    ///
    /// A batch under a sequence already held is checked against a digest of
    /// the held batch's payloads; `reuse` says what one that differs gets.
    /// A batch held without a digest (known only from a snapshot written
    /// before digests were kept) is taken to match.
    ///
    /// On a durable stream the log is what knows, so the answer is the same
    /// on any replica that holds the batch. See `stream/producers.rs`.
    pub async fn publish_batch_idempotent(
        &self,
        handle: &StreamHandle,
        producer_id: u64,
        sequence: u64,
        payloads: &[Bytes],
        reuse: SequenceReuse,
    ) -> Result<IdempotentOutcome> {
        let claimed = self
            .claim_batch_idempotent(handle, producer_id, sequence, payloads, reuse)
            .await?;
        self.complete_idempotent(claimed).await
    }

    /// The ordered half of [`Self::publish_batch_idempotent`]: the sequence
    /// check and, when the batch is new, the append that takes its offsets.
    ///
    /// Once this returns, the log answers the batch's sequence as held, so the
    /// producer's next batch can be checked and claimed without waiting for
    /// this one's flush. [`Self::complete_idempotent`] does the rest, and the
    /// commit sequencer keeps its answer behind every earlier claim's.
    pub async fn claim_batch_idempotent(
        &self,
        handle: &StreamHandle,
        producer_id: u64,
        sequence: u64,
        payloads: &[Bytes],
        reuse: SequenceReuse,
    ) -> Result<IdempotentClaim> {
        let Some(log) = &handle.state.durable else {
            return self
                .publish_idempotent_in_memory(handle, producer_id, sequence, payloads, reuse)
                .await
                .map(IdempotentClaim::Done);
        };
        // The turn serialises this producer's batches, so two re-sends of one
        // sequence cannot both find it unwritten. Held across the append for
        // that reason; the log sees the batch the moment it is written.
        let turn = handle.state.producers.serialise(producer_id);
        let _turn = turn.lock().await;
        loop {
            match log.producer_sequence(producer_id, sequence) {
                ProducerSequence::Held {
                    first,
                    last,
                    digest,
                } => {
                    reuse.check(sequence, digest, payloads)?;
                    return Ok(IdempotentClaim::Held {
                        handle: handle.clone(),
                        first,
                        last,
                    });
                }
                ProducerSequence::Unknown if sequence != 0 => {
                    return Err(BrokerError::UnknownProducer { producer_id });
                }
                ProducerSequence::Unknown | ProducerSequence::Next => {
                    let marks: Vec<RecordMark> =
                        RecordMark::for_batch(producer_id, sequence, payloads.len()).collect();
                    let claimed = self
                        .claim(handle, payloads, Append::Marked(&marks))
                        .await?
                        .expect("a marked append always claims");
                    return Ok(IdempotentClaim::Appended {
                        claimed,
                        offsets: None,
                    });
                }
                // The log holds the start of this batch and nothing after it:
                // its leader stopped partway. Writing the rest finishes it
                // without writing the start twice.
                ProducerSequence::Partial {
                    first,
                    held,
                    len,
                    digest,
                } => {
                    if payloads.len() != len as usize {
                        return Err(BrokerError::SequenceExpired { sequence });
                    }
                    // Finishing a different batch's start with this one's rest
                    // would write a batch nobody sent.
                    reuse.check(sequence, digest, &payloads[..held as usize])?;
                    let append = Append::Continuing {
                        producer_id,
                        sequence,
                    };
                    let Some(claimed) = self
                        .claim(handle, &payloads[held as usize..], append)
                        .await?
                    else {
                        // Something landed after it since it was classified,
                        // so it can no longer be finished; ask again.
                        continue;
                    };
                    return Ok(IdempotentClaim::Appended {
                        claimed,
                        offsets: Some((first, first + u64::from(len) - 1)),
                    });
                }
                ProducerSequence::Gap { expected } => {
                    return Err(BrokerError::SequenceGap { expected });
                }
                ProducerSequence::Expired => {
                    return Err(BrokerError::SequenceExpired { sequence });
                }
            }
        }
    }

    /// Finish what [`Self::claim_batch_idempotent`] started: wait for the
    /// batch to be as durable as a fresh append would be, and fan it out if
    /// it is new.
    pub async fn complete_idempotent(&self, claimed: IdempotentClaim) -> Result<IdempotentOutcome> {
        match claimed {
            IdempotentClaim::Done(outcome) => Ok(outcome),
            IdempotentClaim::Held {
                handle,
                first,
                last,
            } => {
                // Its writer may have been cancelled before waiting, or be a
                // leader that is gone: vouch for it only once it is as
                // durable here as a fresh append would be.
                if let Some(log) = &handle.state.durable {
                    log.wait_durable(last + 1).await?;
                }
                Ok(IdempotentOutcome {
                    outcome: PublishOutcome {
                        subscribers: 0,
                        offsets: Some((first, last)),
                    },
                    duplicate: true,
                })
            }
            IdempotentClaim::Appended { claimed, offsets } => {
                let outcome = self.complete_publish(claimed).await?;
                Ok(IdempotentOutcome {
                    outcome: PublishOutcome {
                        subscribers: outcome.subscribers,
                        offsets: offsets.or(outcome.offsets),
                    },
                    duplicate: false,
                })
            }
        }
    }

    /// The same contract for a stream with no log: the sequences are kept in
    /// memory, and last as long as this leader does.
    async fn publish_idempotent_in_memory(
        &self,
        handle: &StreamHandle,
        producer_id: u64,
        sequence: u64,
        payloads: &[Bytes],
        reuse: SequenceReuse,
    ) -> Result<IdempotentOutcome> {
        let producers = &handle.state.producers;
        let turn = producers.turn(producer_id, sequence)?;
        let _turn = turn.lock().await;
        match producers.classify(producer_id, sequence)? {
            Sequenced::Duplicate(outcome, digest) => {
                reuse.check(sequence, Some(digest), payloads)?;
                Ok(IdempotentOutcome {
                    outcome,
                    duplicate: true,
                })
            }
            Sequenced::Append => {
                let digest = PayloadDigest::of(payloads);
                let outcome = self.publish_batch_with_outcome(handle, payloads).await?;
                producers.remember(producer_id, sequence, outcome, digest);
                Ok(IdempotentOutcome {
                    outcome,
                    duplicate: false,
                })
            }
        }
    }
}

/// How [`Broker::claim`] writes a batch to a durable log.
enum Append<'a> {
    Plain,
    /// With a producer mark per record.
    Marked(&'a [RecordMark]),
    /// The rest of a producer batch the log holds the start of.
    Continuing {
        producer_id: u64,
        sequence: u64,
    },
}

/// What a publish did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishOutcome {
    /// Subscribers the batch was enqueued to.
    pub subscribers: usize,
    /// First and last log offset, inclusive. `None` for an ephemeral stream,
    /// which has no log and therefore no offsets to report.
    pub offsets: Option<(u64, u64)>,
}

/// A publish that has consumed its offsets and taken its place in the commit
/// order, but is not yet durable.
///
/// The point of the split is that the first half must be ordered and the second
/// half must not be: claiming is a few microseconds of offset arithmetic, while
/// completing waits on a device flush. Holding a claim does not hold the log --
/// other publishes claim and complete freely around it -- so several
/// completions overlap and group commit has something to coalesce (#535).
///
/// Complete it. Dropping a claim without completing it releases its commit
/// range so later publishes are not stranded, but the offsets it consumed are
/// gone either way; once [`Broker::complete_publish`] has started, the batch
/// finishes even if its caller goes away.
/// An idempotent batch whose sequence has been checked and, if it was new,
/// whose offsets are taken. See [`Broker::claim_batch_idempotent`].
pub enum IdempotentClaim {
    /// Answered already: a stream with no log decides in one step.
    Done(IdempotentOutcome),
    /// The log already holds it at these offsets.
    Held {
        handle: StreamHandle,
        first: u64,
        last: u64,
    },
    /// Appended here; `offsets` overrides the claim's own when the batch
    /// finished one a stopped leader had started.
    Appended {
        claimed: ClaimedPublish,
        offsets: Option<(u64, u64)>,
    },
}

pub struct ClaimedPublish {
    handle: StreamHandle,
    payloads: Vec<Bytes>,
    durable: Option<ClaimedDurable>,
    sample: bool,
}

impl ClaimedPublish {
    /// The first offset this batch consumed, on a durable stream.
    pub fn first_offset(&self) -> Option<u64> {
        self.durable.as_ref().map(|d| d.pending.first_offset())
    }
}

struct ClaimedDurable {
    pending: felix_storage::disk_log::PendingAppend,
    turn: felix_storage::CommitTurn<'static>,
    durable_start: Option<std::time::Instant>,
}

/// What an idempotent publish did: the batch's outcome, and whether that
/// outcome is from this call or from the batch's first arrival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdempotentOutcome {
    pub outcome: PublishOutcome,
    /// The batch had already been appended; nothing was written this time.
    pub duplicate: bool,
}

/// What an idempotent batch gets when its sequence already holds a batch with
/// different payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SequenceReuse {
    /// Refused with [`BrokerError::SequenceReused`], and nothing written.
    Refuse,
    /// Answered as a duplicate of the batch held, and nothing written: its
    /// records are reported written when they are not. Only for a client that
    /// cannot read the refusal, which got this answer before digests existed.
    AnswerDuplicate,
}

impl SequenceReuse {
    /// Refuse `payloads` if asked to and `held`, the digest of the batch
    /// already under `sequence`, says they differ from it.
    fn check(self, sequence: u64, held: Option<PayloadDigest>, payloads: &[Bytes]) -> Result<()> {
        if self == Self::Refuse
            && let Some(held) = held
            && held != PayloadDigest::of(payloads)
        {
            return Err(BrokerError::SequenceReused { sequence });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

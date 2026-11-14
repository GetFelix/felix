//! Getting a Raft shard's records to its subscribers: committed records
//! only, from the publisher's own batch on the leader and from the log on
//! a follower. See `docs/per-shard-raft.md`, "Delivering committed records".
//!
//! Nothing is decoded from the log for fanout. The state machine applies an
//! entry by moving the commit table, and the broker's commit hold releases
//! the batch the publisher handed in once the engine says it committed.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use felix_broker::{
    Broker, BrokerError, IdempotentOutcome, IdempotentProposal, ProducerTurn, ProposalCommitted,
    ProposalFailed, ProposedPublish, PublishOutcome, ReadBound, SequenceReuse, StreamHandle,
};
use futures::future::BoxFuture;
use tokio::task::JoinHandle;

use super::engine::RaftEngine;
use super::propose::{AckAt, Batch, Record, Sequenced, Unacknowledged};
use crate::engine::{CommitError, Written};
use crate::{ShardKey, ShardKind};

const OTHER_TERM: &str = "the batch was written at a later term than it was admitted at";

impl RaftEngine {
    /// Publish `payloads` to `shard` through `broker`'s stream handle
    /// `stream`. The batch is proposed in the stream's delivery order and
    /// reaches its subscribers once it is committed under the term that
    /// wrote it, after every batch proposed before it.
    ///
    /// [`ProposedPublish::complete`] returns at that point, with the
    /// engine's error, or with [`CommitError::TimedOut`] after `timeout`.
    /// A timeout leaves the batch in its place: it is still delivered if it
    /// commits, and the batches behind it wait for it. Only a lost
    /// leadership or a stopped group gives the place up. Such a batch may
    /// still have committed; live subscribers then see its offsets as a
    /// drop, and a reader resuming below it is served from the log.
    ///
    /// [`BrokerError::ProposalsStalled`], with nothing proposed, while the
    /// oldest batch still waiting for its commit has waited past `timeout`.
    ///
    /// `term` is the term the caller was admitted at, and the batch is
    /// written only while this broker leads it. A broker deposed and
    /// elected again after the caller looked writes nothing: the stream
    /// may not be at the new term's committed tail yet. `None` takes any
    /// term.
    #[allow(clippy::too_many_arguments)]
    pub fn publish(
        &self,
        broker: &Broker,
        stream: &StreamHandle,
        shard: &ShardKey,
        term: Option<u64>,
        payloads: Vec<Bytes>,
        ack: AckAt,
        timeout: Duration,
    ) -> Result<ProposedPublish<CommitError>, BrokerError> {
        let records = payloads.clone();
        // A leader cut off from its majority is not deposed, and every batch
        // it is handed waits, payload in hand, until the partition heals.
        // Refusing once the oldest has waited out its timeout bounds them.
        let proposed = broker.propose_publish(stream, payloads, timeout, |timestamp_micros| {
            let count = records.len() as u64;
            let records = records
                .into_iter()
                .map(|payload| Record {
                    timestamp_micros,
                    payload,
                })
                .collect();
            let batch = Batch {
                records,
                ack,
                term,
                producer: None,
            };
            let written = self.propose_reaching(shard, batch);
            settled(self.clone(), shard.clone(), term, count, timeout, written)
        })?;
        Ok(
            proposed.answer_within(timeout, move || CommitError::TimedOut {
                what: "batch",
                timeout,
            }),
        )
    }

    /// [`Self::publish`] for an idempotent producer's batch, which is
    /// written once however many times it arrives, on whichever member
    /// leads when it does.
    ///
    /// The records are stored with the producer's id and sequence, so each
    /// member's log knows every producer's place, and a leader elected
    /// later answers a re-send from its own log. The sequence is checked
    /// against this broker's log as the hybrid checks it
    /// ([`Broker::claim_batch_idempotent`]), with the same refusals. A
    /// batch the log holds is answered with where it landed, once that is
    /// committed, and nothing is written.
    ///
    /// The producer's batches go in one at a time. `turn` is the
    /// producer's ([`Broker::producer_turn`]), and it is held until the
    /// batch is on this broker's log, which is when the log can answer for
    /// it. `term` is required. A batch that could still be written after
    /// the broker stopped leading the term would be missed by the check of
    /// its own re-send.
    ///
    /// A batch is one record. One of several is refused with nothing
    /// written ([`Broker::engine_takes_idempotent`]).
    #[allow(clippy::too_many_arguments)]
    pub fn publish_idempotent(
        &self,
        broker: &Broker,
        stream: &StreamHandle,
        shard: &ShardKey,
        term: u64,
        turn: ProducerTurn,
        sequence: u64,
        payloads: Vec<Bytes>,
        reuse: SequenceReuse,
        timeout: Duration,
    ) -> Result<IdempotentPublish, BrokerError> {
        let records = payloads.clone();
        let producer = Sequenced {
            producer_id: turn.producer_id(),
            sequence,
        };
        let proposal = broker.propose_idempotent(
            stream,
            turn,
            sequence,
            payloads,
            reuse,
            timeout,
            |timestamp_micros, turn| {
                let count = records.len() as u64;
                let records = records
                    .into_iter()
                    .map(|payload| Record {
                        timestamp_micros,
                        payload,
                    })
                    .collect();
                // The answer waits for the commit whatever this says.
                // Written is when the log knows the batch, so the
                // producer's next one is not held for the fsync.
                let batch = Batch {
                    records,
                    ack: AckAt::Written,
                    term: Some(term),
                    producer: Some(producer),
                };
                let written = self.propose_reaching(shard, batch);
                // On a task of its own, so the turn is given up when
                // the batch is written whether or not its publisher
                // has started waiting.
                let written = tokio::spawn(async move {
                    let written = written.await;
                    drop(turn);
                    written
                });
                let written = async move {
                    let stopped = |_| Err(super::propose::stopped().into());
                    written.await.unwrap_or_else(stopped)
                };
                settled(
                    self.clone(),
                    shard.clone(),
                    Some(term),
                    count,
                    timeout,
                    written,
                )
            },
        )?;
        let timed_out = move || CommitError::TimedOut {
            what: "batch",
            timeout,
        };
        Ok(IdempotentPublish(match proposal {
            IdempotentProposal::Proposed(proposed) => {
                Answer::Proposed(proposed.answer_within(timeout, timed_out))
            }
            IdempotentProposal::Held { first, last } => {
                // Below the bound it is committed, whoever wrote it.
                // Above, this term wrote it: a term is served only once
                // every earlier entry is applied.
                let committed = match self.read_bound(shard) {
                    Ok(bound) if bound > last => None,
                    _ => {
                        let written = Written {
                            end: last + 1,
                            generation: term,
                        };
                        Some(self.await_commit(shard, written, timeout))
                    }
                };
                Answer::Held {
                    offsets: (first, last),
                    committed: Box::pin(async move {
                        match committed {
                            Some(committed) => committed.await,
                            None => Ok(()),
                        }
                    }),
                }
            }
        }))
    }

    /// Kick `stream`'s readers each time `shard`'s commit index moves here:
    /// held batches the bound now covers are released, and readers waiting
    /// at the bound for the log (a follower's subscribers, history reads)
    /// look again. Ends when the group stops or the stream closes. `None`
    /// when this broker holds no member of the shard.
    pub fn follow_commits(&self, shard: &ShardKey, stream: StreamHandle) -> Option<JoinHandle<()>> {
        let mut commits = self.commits(shard)?;
        Some(tokio::spawn(async move {
            loop {
                stream.release_committed();
                stream.appended().notify_waiters();
                if !stream.is_active() || commits.changed().await.is_err() {
                    return;
                }
            }
        }))
    }
}

/// An idempotent batch on its way to an answer. See
/// [`RaftEngine::publish_idempotent`].
#[must_use = "an idempotent publish reports its outcome only through `complete`"]
pub struct IdempotentPublish(Answer);

enum Answer {
    Proposed(ProposedPublish<CommitError>),
    Held {
        offsets: (u64, u64),
        committed: BoxFuture<'static, Result<(), CommitError>>,
    },
}

impl IdempotentPublish {
    /// Wait until the batch is committed: the one just proposed, or the
    /// one the log held. A duplicate reports the offsets its first arrival
    /// was given. Reports no subscribers: the fanout runs after this.
    pub async fn complete(self) -> Result<IdempotentOutcome, CommitError> {
        match self.0 {
            Answer::Proposed(proposed) => Ok(IdempotentOutcome {
                outcome: proposed.complete().await?,
                duplicate: false,
            }),
            Answer::Held { offsets, committed } => {
                committed.await?;
                Ok(IdempotentOutcome {
                    outcome: PublishOutcome {
                        subscribers: 0,
                        offsets: Some(offsets),
                    },
                    duplicate: true,
                })
            }
        }
    }
}

/// What the broker is told of a proposed batch of `count` records:
/// committed and where, or why not and how far it is known written.
pub(super) async fn settled(
    engine: RaftEngine,
    shard: ShardKey,
    term: Option<u64>,
    count: u64,
    timeout: Duration,
    written: impl Future<Output = Result<Written, Unacknowledged>>,
) -> Result<ProposalCommitted, ProposalFailed<CommitError>> {
    // A batch written in part or in whole may commit. Readers resuming
    // below its end go to the log, or they would join live above records
    // that are in it.
    let written = written.await.map_err(|unacked| match unacked.written_end {
        Some(end) => ProposalFailed::written(unacked.error, end),
        None => ProposalFailed::unwritten(unacked.error),
    })?;
    // openraft refuses the write at another term. Should one land all the
    // same, it is not delivered.
    if term.is_some_and(|term| term != written.generation) {
        let moved = super::propose::lost(OTHER_TERM);
        return Err(ProposalFailed::written(moved, written.end));
    }
    // The publisher's own wait is bounded by `answer_within`. Delivery is
    // not: a batch skipped here and committed later would reach no live
    // subscriber, while the one behind it did.
    loop {
        match engine.await_commit(&shard, written, timeout).await {
            Err(CommitError::TimedOut { .. }) => continue,
            outcome => {
                outcome.map_err(|err| ProposalFailed::written(err, written.end))?;
                break;
            }
        }
    }
    // Everything below the batch is applied, so its run of entries holding
    // no record is known.
    let first = written.end.saturating_sub(count);
    let skipped_before = engine
        .commits(&shard)
        .map_or(0, |commits| commits.borrow().skipped_before(first));
    Ok(ProposalCommitted {
        end: written.end,
        skipped_before,
    })
}

/// [`RaftEngine::read_bound`] as the broker core asks for it, for streams
/// whose handles are [`StreamHandle::bound_every_level`].
#[derive(Clone)]
pub struct RaftReads {
    engine: RaftEngine,
}

impl RaftReads {
    pub fn new(engine: RaftEngine) -> Self {
        Self { engine }
    }
}

impl std::fmt::Debug for RaftReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftReads").finish_non_exhaustive()
    }
}

impl felix_broker::ReadBounds for RaftReads {
    fn stream_bound(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> ReadBound {
        let key = ShardKey {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            kind: ShardKind::Stream,
        };
        match self.engine.read_bound(&key) {
            Ok(bound) => ReadBound::Committed(bound),
            Err(_) => ReadBound::Refused,
        }
    }
}

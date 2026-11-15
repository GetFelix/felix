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
    Attribution, Broker, BrokerError, CommitRecord, IdempotentOutcome, IdempotentProposal,
    LogDelivery, ProducerTurn, ProposalCommitted, ProposalFailed, ProposedPublish, ProposedTrim,
    PublishOutcome, ReadBound, SequenceReuse, StateOp, StreamHandle,
};
use futures::future::BoxFuture;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::engine::RaftEngine;
use super::propose::{AckAt, AnswerAt, Batch, Record, Sequenced, Unacknowledged};
use crate::engine::{CommitError, Written};
use crate::{ShardKey, ShardKind};

/// How soon a follower's delivery from the log that had to wait looks again.
const LOG_RECHECK: Duration = Duration::from_millis(100);

/// The longest a failing delivery from the log waits before trying again.
const LOG_RETRY_MAX: Duration = Duration::from_secs(5);

/// Failed deliveries from the log in a row after which the stream's
/// subscribers are ended, so their clients resubscribe where the log reads.
const LOG_FAILURES_TO_END: u32 = 5;

/// `payloads` as records stamped `timestamp_micros`, each with its headers
/// block when `headers` has one per record.
fn with_headers(
    payloads: Vec<Bytes>,
    headers: Option<Vec<Option<Bytes>>>,
    timestamp_micros: u64,
) -> Vec<Record> {
    let mut headers = headers.unwrap_or_default().into_iter();
    payloads
        .into_iter()
        .map(|payload| Record {
            timestamp_micros,
            payload,
            headers: headers.next().flatten(),
        })
        .collect()
}

/// A stream batch on its way to its commit, as the broker waits on it.
type Settling = BoxFuture<'static, Result<ProposalCommitted, ProposalFailed<CommitError>>>;

const OTHER_TERM: &str = "the batch was written at a later term than it was admitted at";

impl RaftEngine {
    /// Publish `payloads` to `shard` through `broker`'s stream handle
    /// `stream`. The batch is proposed in the stream's delivery order and
    /// reaches its subscribers once it is committed under the term that
    /// wrote it, after every batch proposed before it.
    ///
    /// [`ProposedPublish::complete`] returns at that point, with the
    /// engine's error, or with [`CommitError::TimedOut`] after `timeout`.
    /// With [`AnswerAt::Lease`] it returns earlier, once the batch is on
    /// this leader's log, if the leader lease holds then and the stream had
    /// fewer than [`felix_broker::ANSWERED_AHEAD`] batches waiting for a
    /// commit, none of them for a quarter of `timeout`. Delivery does not
    /// move: such a batch is acknowledged before its commit is known, and
    /// is lost if this broker stops leading before it commits.
    /// [`AnswerAt::Majority`] returns early the same way, under the same
    /// limits, once a majority of the voters has written the batch.
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
        answer: AnswerAt,
        timeout: Duration,
    ) -> Result<ProposedPublish<CommitError>, BrokerError> {
        self.publish_with(
            broker,
            stream,
            shard,
            term,
            payloads,
            Attribution::default(),
            ack,
            answer,
            timeout,
        )
    }

    /// [`Self::publish`] with the batch's publisher and its records'
    /// headers, stored in each entry as the hybrid stores them in each
    /// record, and so read back by every member, after a restart and by
    /// history reads. What is stored is what
    /// [`Broker::admit_attribution`] lets through: headers it refuses
    /// refuse the publish with nothing proposed.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_with(
        &self,
        broker: &Broker,
        stream: &StreamHandle,
        shard: &ShardKey,
        term: Option<u64>,
        payloads: Vec<Bytes>,
        attribution: Attribution,
        ack: AckAt,
        answer: AnswerAt,
        timeout: Duration,
    ) -> Result<ProposedPublish<CommitError>, BrokerError> {
        let attribution = broker.admit_attribution(&payloads, attribution)?;
        let records = payloads.clone();
        let (on_lease, answered) = oneshot::channel();
        let (on_lease, on_majority) = match answer {
            AnswerAt::Commit => (None, None),
            AnswerAt::Lease => (Some(on_lease), None),
            AnswerAt::Majority => (None, Some((ack, on_lease))),
        };
        // A leader cut off from its majority is not deposed, and every batch
        // it is handed waits, payload in hand, until the partition heals.
        // Refusing once the oldest has waited out its timeout bounds them.
        let proposed = broker.propose_publish_with(
            stream,
            payloads,
            attribution.clone(),
            timeout,
            self.proposer(
                shard,
                term,
                records,
                attribution,
                None,
                None,
                false,
                ack,
                on_lease,
                on_majority,
                timeout,
            ),
        )?;
        // The early answers share `answer_at`'s pacing: a publisher answered
        // before the commit comes back for more, and is slowed to the
        // commit's pace once too many batches, or too old a one, wait for it.
        let proposed = match answer {
            AnswerAt::Lease | AnswerAt::Majority => proposed.answer_at(answered),
            AnswerAt::Commit => proposed,
        };
        Ok(
            proposed.answer_within(timeout, move || CommitError::TimedOut {
                what: "batch",
                timeout,
            }),
        )
    }

    /// [`Self::publish`] for a conditional publish: one record, written
    /// only if it lands at offset `expected`. Anything but one record is
    /// refused with nothing proposed.
    ///
    /// The condition is decided where the leader's log appends the entry,
    /// at its final index and from that index alone: off `expected`, it
    /// stores a blank entry there instead of the record, and followers copy
    /// what it stored. So an entry openraft or another writer puts ahead of
    /// the record cannot move it off its offset unnoticed, and a deposed
    /// leader or a re-proposal cannot store it elsewhere. A
    /// refused publish still takes an offset, which readers skip, and is
    /// answered with [`CommitError::OffsetMismatch`] once committed,
    /// carrying the offset after it. The answer always waits for the
    /// commit. The record carries `attribution` as [`Self::publish_with`]
    /// stores it.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_at(
        &self,
        broker: &Broker,
        stream: &StreamHandle,
        shard: &ShardKey,
        term: Option<u64>,
        payloads: Vec<Bytes>,
        attribution: Attribution,
        expected: u64,
        ack: AckAt,
        timeout: Duration,
    ) -> Result<ProposedPublish<CommitError>, BrokerError> {
        let attribution = broker.admit_attribution(&payloads, attribution)?;
        let records = payloads.clone();
        let proposed = broker.propose_publish_at(
            stream,
            payloads,
            attribution.clone(),
            expected,
            timeout,
            |tail| CommitError::OffsetMismatch { tail },
            self.proposer(
                shard,
                term,
                records,
                attribution,
                Some(expected),
                None,
                false,
                ack,
                None,
                None,
                timeout,
            ),
        )?;
        Ok(
            proposed.answer_within(timeout, move || CommitError::TimedOut {
                what: "batch",
                timeout,
            }),
        )
    }

    /// Commit `event` and `ops` as one entry: an atomic commit, with the
    /// hybrid's guarantee that every reader sees all of it or none of it.
    ///
    /// The entry's record is the commit record, stored with the commit mark
    /// on every member, so a member's state view is rebuilt from its own
    /// log after a restart or a catch-up, and a follower's delivery applies
    /// the state updates with the event. With `expected`, it is decided as
    /// [`Self::publish_at`] decides: off its offset, the leader's log stores
    /// a blank in its place and the answer is
    /// [`CommitError::OffsetMismatch`]. The answer waits for the commit.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_at(
        &self,
        broker: &Broker,
        stream: &StreamHandle,
        shard: &ShardKey,
        term: Option<u64>,
        event: Bytes,
        ops: Vec<StateOp>,
        publisher: Option<Bytes>,
        expected: Option<u64>,
        ack: AckAt,
        timeout: Duration,
    ) -> Result<ProposedPublish<CommitError>, BrokerError> {
        let attribution = broker.admit_attribution(
            std::slice::from_ref(&event),
            Attribution {
                publisher,
                headers: None,
            },
        )?;
        let record = CommitRecord { event, ops };
        let stored = record.encode();
        let proposed = broker.propose_commit(
            stream,
            record.event,
            record.ops.into(),
            attribution.clone(),
            expected,
            timeout,
            |tail| CommitError::OffsetMismatch { tail },
            self.proposer(
                shard,
                term,
                vec![stored],
                attribution,
                expected,
                None,
                true,
                ack,
                None,
                None,
                timeout,
            ),
        )?;
        Ok(
            proposed.answer_within(timeout, move || CommitError::TimedOut {
                what: "commit",
                timeout,
            }),
        )
    }

    /// Hide the shard's records below `before`, only if its next offset is
    /// `expected`: a head trim, with the hybrid's checks and answers
    /// ([`Broker::propose_trim_at`]).
    ///
    /// The trim is one entry, its record the headers, conditional on its
    /// final index as [`Self::publish_at`] is: off `expected` the leader's
    /// log stores a blank there, and the answer is
    /// [`CommitError::OffsetMismatch`] with the offset after it. Committed,
    /// the entry sets the floor on every member's log, which deletes
    /// nothing below it until openraft purges the log. Answered once the
    /// floor is in this broker's replay ring.
    #[allow(clippy::too_many_arguments)]
    pub async fn trim_at(
        &self,
        broker: &Broker,
        stream: &StreamHandle,
        shard: &ShardKey,
        term: Option<u64>,
        before: u64,
        expected: u64,
        headers: Bytes,
        ack: AckAt,
        timeout: Duration,
    ) -> Result<ProposedTrim<CommitError>, BrokerError> {
        let proposed = broker
            .propose_trim_at(
                stream,
                before,
                expected,
                headers.clone(),
                timeout,
                |tail| CommitError::OffsetMismatch { tail },
                self.proposer(
                    shard,
                    term,
                    vec![headers],
                    Attribution::default(),
                    Some(expected),
                    Some(before),
                    false,
                    ack,
                    None,
                    None,
                    timeout,
                ),
            )
            .await?;
        Ok(
            proposed.answer_within(timeout, move || CommitError::TimedOut {
                what: "trim",
                timeout,
            }),
        )
    }

    /// What [`Broker::propose_publish`] is handed to queue a stream batch.
    #[allow(clippy::too_many_arguments)]
    fn proposer(
        &self,
        shard: &ShardKey,
        term: Option<u64>,
        records: Vec<Bytes>,
        attribution: Attribution,
        expected: Option<u64>,
        trim: Option<u64>,
        commit: bool,
        ack: AckAt,
        on_lease: Option<oneshot::Sender<u64>>,
        on_majority: Option<(AckAt, oneshot::Sender<u64>)>,
        timeout: Duration,
    ) -> impl FnOnce(u64) -> Settling + use<> {
        let engine = self.clone();
        let shard = shard.clone();
        move |timestamp_micros| {
            let count = records.len() as u64;
            let Attribution { publisher, headers } = attribution;
            let records = with_headers(records, headers, timestamp_micros);
            let batch = Batch {
                records,
                ack,
                term,
                producer: None,
                expected,
                trim,
                commit,
                publisher,
            };
            let written = engine.propose_reaching(&shard, batch);
            let batch = Proposed {
                shard,
                term,
                count,
                timeout,
                on_lease,
                on_majority,
            };
            Box::pin(settled(engine, batch, written))
        }
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
    /// A batch is one record. A new one of several is refused with nothing
    /// written ([`Broker::engine_takes_idempotent`]). One the log already
    /// holds the start of, which only a leader that stopped partway
    /// leaves, is finished: the records the log does not hold are written
    /// as pieces of it, and the answer runs from the start's first offset.
    /// It is answered at its commit, on a `Leader` stream too: never on
    /// the lease.
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
        attribution: Attribution,
        reuse: SequenceReuse,
        timeout: Duration,
    ) -> Result<IdempotentPublish, BrokerError> {
        let producer_id = turn.producer_id();
        let attribution = broker.admit_attribution(&payloads, attribution)?;
        let publisher = attribution.publisher.clone();
        let proposal = broker.propose_idempotent(
            stream,
            turn,
            sequence,
            payloads,
            attribution,
            reuse,
            timeout,
            |timestamp_micros, turn, owed| {
                let producer = Sequenced {
                    producer_id,
                    sequence,
                    position: owed.position,
                    len: owed.len,
                };
                let count = owed.payloads.len() as u64;
                let records = with_headers(owed.payloads, owed.headers, timestamp_micros);
                // The answer waits for the commit whatever this says.
                // Written is when the log knows the batch, so the
                // producer's next one is not held for the fsync.
                let batch = Batch {
                    records,
                    ack: AckAt::Written,
                    term: Some(term),
                    producer: Some(producer),
                    expected: None,
                    trim: None,
                    commit: false,
                    publisher,
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
                let batch = Proposed {
                    shard: shard.clone(),
                    term: Some(term),
                    count,
                    timeout,
                    on_lease: None,
                    on_majority: None,
                };
                settled(self.clone(), batch, written)
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

    /// Serve `stream`'s readers each time `shard`'s commit index moves here:
    /// held batches the bound now covers are released, a broker that does
    /// not lead the shard delivers the newly committed records to its
    /// subscribers from the log, and readers waiting at the bound (history
    /// reads) look again. Ends when the group stops or the stream closes.
    /// `None` when this broker holds no member of the shard.
    pub fn follow_commits(&self, shard: &ShardKey, stream: StreamHandle) -> Option<JoinHandle<()>> {
        let mut commits = self.commits(shard)?;
        Some(tokio::spawn(async move {
            let mut failures = 0u32;
            loop {
                stream.release_committed();
                stream.appended().notify_waiters();
                let recheck = match stream.deliver_from_log().await {
                    Ok(LogDelivery::CaughtUp) => {
                        failures = 0;
                        None
                    }
                    Ok(LogDelivery::Waiting) => Some(LOG_RECHECK),
                    Err(err) => {
                        failures = failures.saturating_add(1);
                        tracing::warn!(error = %err, failures, "could not deliver a shard's committed records from its log");
                        // Subscribers left here would wait, told nothing.
                        if failures == LOG_FAILURES_TO_END {
                            stream.end_subscribers();
                        }
                        if !stream.is_active() {
                            return;
                        }
                        // Not woken early by commits: they keep moving.
                        let backoff = LOG_RECHECK.saturating_mul(1 << failures.min(6));
                        tokio::time::sleep(backoff.min(LOG_RETRY_MAX)).await;
                        continue;
                    }
                };
                if !stream.is_active() {
                    return;
                }
                // A pass that had to wait is not told when it may go on: a
                // deposed term's batches settle without the commit index
                // moving.
                let moved = match recheck {
                    None => commits.changed().await,
                    Some(after) => tokio::time::timeout(after, commits.changed())
                        .await
                        .unwrap_or(Ok(())),
                };
                if moved.is_err() {
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

/// A batch handed to the group, as [`settled`] follows it.
pub(super) struct Proposed {
    pub(super) shard: ShardKey,
    pub(super) term: Option<u64>,
    /// How many records it holds.
    pub(super) count: u64,
    pub(super) timeout: Duration,
    /// Told the batch's end if it may be acknowledged on the lease.
    pub(super) on_lease: Option<oneshot::Sender<u64>>,
    /// Told the batch's end once a majority of the voters has written it,
    /// as far as the [`AckAt`] asks.
    pub(super) on_majority: Option<(AckAt, oneshot::Sender<u64>)>,
}

/// What the broker is told of a proposed batch: committed and where, or
/// why not and how far it is known written.
pub(super) async fn settled(
    engine: RaftEngine,
    batch: Proposed,
    written: impl Future<Output = Result<Written, Unacknowledged>>,
) -> Result<ProposalCommitted, ProposalFailed<CommitError>> {
    let Proposed {
        shard,
        term,
        count,
        timeout,
        on_lease,
        on_majority,
    } = batch;
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
    // The lease is asked only now, with the batch on this log under the
    // term it names. A refusal leaves the publisher waiting for the commit
    // below, like a `Quorum` one.
    if let Some(on_lease) = on_lease
        && engine.admit_local_ack(&shard, written.generation).is_ok()
    {
        let _ = on_lease.send(written.end);
    }
    // Beside the commit, never in front of it: delivery still waits for
    // the commit. A batch of this term on a majority is in every later
    // leader's log, since a voter refuses a candidate whose log is behind
    // its own (openraft `handle_vote_req`, engine_impl.rs:448 at the pinned
    // rev). See `docs/per-shard-raft.md`, "`Quorum` on a `Periodic` stream".
    let majority = async {
        if let Some((ack, on_majority)) = on_majority
            && engine
                .await_quorum_ack(&shard, written, ack, timeout)
                .await
                .is_ok()
        {
            let _ = on_majority.send(written.end);
        }
        std::future::pending::<()>().await;
    };
    // The publisher's own wait is bounded by `answer_within`. Delivery is
    // not: a batch skipped here and committed later would reach no live
    // subscriber, while the one behind it did.
    let committed = async {
        loop {
            match engine.await_commit(&shard, written, timeout).await {
                Err(CommitError::TimedOut { .. }) => continue,
                outcome => break outcome,
            }
        }
    };
    let committed = tokio::select! {
        biased;
        committed = committed => committed,
        () = majority => unreachable!("the majority wait never ends"),
    };
    committed.map_err(|err| ProposalFailed::written(err, written.end))?;
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

    fn records_end(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        end: u64,
    ) -> u64 {
        let key = ShardKey {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            kind: ShardKind::Stream,
        };
        self.engine.records_end(&key, end)
    }
}

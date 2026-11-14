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
    Broker, BrokerError, ProposalCommitted, ProposalFailed, ProposedPublish, ReadBound,
    StreamHandle,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::engine::RaftEngine;
use super::propose::{AckAt, AnswerAt, Batch, Record, Unacknowledged};
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
    /// With [`AnswerAt::Lease`] it returns earlier, once the batch is on
    /// this leader's log, if the leader lease holds then. Delivery does not
    /// move: such a batch is acknowledged before its commit is known, and
    /// is lost if this broker stops leading before it commits.
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
        let records = payloads.clone();
        let (on_lease, answered) = oneshot::channel();
        let on_lease = (answer != AnswerAt::Commit).then_some((answer, on_lease));
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
            let batch = Batch { records, ack, term };
            let written = self.propose_reaching(shard, batch);
            let batch = Proposed {
                shard: shard.clone(),
                term,
                count,
                timeout,
                on_lease,
            };
            settled(self.clone(), batch, written)
        })?;
        let proposed = match answer {
            AnswerAt::Lease | AnswerAt::MajorityWritten => proposed.answer_at(answered),
            AnswerAt::Commit => proposed,
        };
        Ok(
            proposed.answer_within(timeout, move || CommitError::TimedOut {
                what: "batch",
                timeout,
            }),
        )
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

/// A batch handed to the group, as [`settled`] follows it.
pub(super) struct Proposed {
    pub(super) shard: ShardKey,
    pub(super) term: Option<u64>,
    /// How many records it holds.
    pub(super) count: u64,
    pub(super) timeout: Duration,
    /// Told the batch's end if it may be acknowledged before its commit,
    /// and which rule decides that.
    pub(super) on_lease: Option<(AnswerAt, oneshot::Sender<u64>)>,
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
    } = batch;
    // A batch written in part or in whole may commit. Readers resuming
    // below its end go to the log, or they would join live above records
    // that are in it.
    let probe_start = std::time::Instant::now();
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
    let mut on_majority = None;
    match on_lease {
        Some((AnswerAt::Lease, on_lease)) => {
            if engine.admit_local_ack(&shard, written.generation).is_ok() {
                let _ = on_lease.send(written.end);
            }
        }
        Some((_, early)) => on_majority = Some(early),
        None => {}
    }
    // Ends no later than the commit wait below: it takes the commit too.
    let majority = {
        let waited = on_majority.map(|early| {
            let wait = engine.await_quorum_ack(&shard, written, AckAt::Written, timeout);
            (early, wait)
        });
        async move {
            if let Some((early, wait)) = waited
                && wait.await.is_ok()
            {
                let _ = early.send(written.end);
            }
        }
    };
    tokio::spawn(majority);
    // The publisher's own wait is bounded by `answer_within`. Delivery is
    // not: a batch skipped here and committed later would reach no live
    // subscriber, while the one behind it did.
    let probe_written = std::time::Instant::now();
    loop {
        match engine.await_commit(&shard, written, timeout).await {
            Err(CommitError::TimedOut { .. }) => continue,
            outcome => {
                outcome.map_err(|err| ProposalFailed::written(err, written.end))?;
                break;
            }
        }
    }
    felix_storage::stage_probe::WRITTEN_TO_APPLIED.since(probe_written);
    felix_storage::stage_probe::PROPOSE_TO_ANSWER.since(probe_start);
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

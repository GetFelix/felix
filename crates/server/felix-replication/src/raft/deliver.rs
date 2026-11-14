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
    Broker, BrokerError, LogDelivery, ProposalCommitted, ProposalFailed, ProposedPublish,
    ReadBound, StreamHandle,
};
use tokio::task::JoinHandle;

use super::engine::RaftEngine;
use super::propose::{AckAt, Batch, Record, Unacknowledged};
use crate::engine::{CommitError, Written};
use crate::{ShardKey, ShardKind};

/// How soon a follower's delivery from the log that had to wait looks again.
const LOG_RECHECK: Duration = Duration::from_millis(100);

/// The longest a failing delivery from the log waits before trying again.
const LOG_RETRY_MAX: Duration = Duration::from_secs(5);

/// Failed deliveries from the log in a row after which the stream's
/// subscribers are ended, so their clients resubscribe where the log reads.
const LOG_FAILURES_TO_END: u32 = 5;

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
            let batch = Batch { records, ack, term };
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

//! Getting a Raft shard's records to its subscribers: committed records
//! only, from the publisher's own batch on the leader and from the log on
//! a follower. See `docs/per-shard-raft.md`, "Delivering committed records".
//!
//! Nothing is decoded from the log for fanout. The state machine applies an
//! entry by moving the commit table, and the broker's commit hold releases
//! the batch the publisher handed in once the engine says it committed.

use std::time::Duration;

use bytes::Bytes;
use felix_broker::{
    Broker, BrokerError, ProposalCommitted, ProposalFailed, ProposedPublish, ReadBound,
    StreamHandle,
};
use tokio::task::JoinHandle;

use super::engine::RaftEngine;
use super::propose::{AckAt, Batch, Record};
use crate::engine::CommitError;
use crate::{ShardKey, ShardKind};

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
    pub fn publish(
        &self,
        broker: &Broker,
        stream: &StreamHandle,
        shard: &ShardKey,
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
            let written = self.propose(shard, Batch { records, ack });
            let engine = self.clone();
            let shard = shard.clone();
            async move {
                let written = written.await.map_err(ProposalFailed::unwritten)?;
                // The publisher's own wait is bounded by `answer_within`.
                // Delivery is not: a batch skipped here and committed later
                // would reach no live subscriber, while the one behind it
                // did.
                loop {
                    match engine.await_commit(&shard, written, timeout).await {
                        Err(CommitError::TimedOut { .. }) => continue,
                        outcome => {
                            break outcome
                                .map_err(|err| ProposalFailed::written(err, written.end))?;
                        }
                    }
                }
                // Everything below the batch is applied, so its run of
                // entries holding no record is known.
                let first = written.end.saturating_sub(count);
                let skipped_before = engine
                    .commits(&shard)
                    .map_or(0, |commits| commits.borrow().skipped_before(first));
                Ok(ProposalCommitted {
                    end: written.end,
                    skipped_before,
                })
            }
        })?;
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

//! Getting a Raft shard's records to its subscribers: committed records
//! only, from the publisher's own batch on the leader and from the log on
//! a follower. See `docs/per-shard-raft.md`, "Delivering committed records".
//!
//! Nothing is decoded from the log for fanout. The state machine applies an
//! entry by moving the commit table, and the broker's commit hold releases
//! the batch the publisher handed in once the engine says it committed.

use std::time::Duration;

use bytes::Bytes;
use felix_broker::{Broker, BrokerError, ProposedPublish, ReadBound, StreamHandle};
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
    /// [`ProposedPublish::complete`] returns at that point, or with the
    /// engine's error, which delivers nothing: a deposed leader's batch may
    /// still commit, and readers then find it in the log.
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
        broker.propose_publish(stream, payloads, |timestamp_micros| {
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
                let written = written.await?;
                engine.await_commit(&shard, written, timeout).await?;
                Ok(written.end)
            }
        })
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

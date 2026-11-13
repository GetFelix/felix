//! Which replication engine a shard's writes and reads go to.

use std::sync::Arc;
use std::time::Duration;

use felix_replication::engine::{CommitError, Engine, Hybrid, Placement, Written};
use felix_replication::quorum::{Access, CacheWrite, QuorumMarks, Unreadable};
use felix_wire::internal::{InternalMessage, PeerCapabilities};

use super::ShardKey;
use super::routing::IngressRouter;

/// The engines this broker runs, read against how it serves each shard.
/// What the stream write and read paths hold in place of the marks and the
/// router.
///
/// A shard has one engine: a Raft group's when this broker holds a member
/// of it ([`StreamEngine::raft`]), the hybrid's otherwise. Every [`Engine`]
/// call goes to the hybrid, which is the only engine that serves writes
/// today, and the hybrid's own methods are reached by deref.
pub struct StreamEngine {
    hybrid: Hybrid<IngressRouter>,
    #[cfg(feature = "raft")]
    raft: std::sync::OnceLock<Arc<super::raft::RaftShards>>,
}

impl StreamEngine {
    /// `None` for either outside a cluster, where nothing is replicated.
    pub fn new(marks: Option<Arc<QuorumMarks>>, serving: Option<Arc<IngressRouter>>) -> Self {
        Self {
            hybrid: Hybrid::new(marks, serving),
            #[cfg(feature = "raft")]
            raft: std::sync::OnceLock::new(),
        }
    }

    /// Send the shards `raft` holds to their groups. Only the first call
    /// counts.
    #[cfg(feature = "raft")]
    pub(crate) fn set_raft(&self, raft: Arc<super::raft::RaftShards>) {
        let _ = self.raft.set(raft);
    }

    /// The Raft engine, when a Raft group runs `shard` on this broker.
    #[cfg(feature = "raft")]
    pub fn raft(&self, shard: &ShardKey) -> Option<&felix_replication::raft::RaftEngine> {
        self.raft.get()?.engine_for(shard)
    }

    /// The term this broker leads `shard`'s Raft group in and may take its
    /// writes at.
    #[cfg(feature = "raft")]
    pub fn raft_term(&self, shard: &ShardKey) -> Option<u64> {
        self.raft.get()?.leading(shard)
    }
}

impl Default for StreamEngine {
    /// For tests only. See [`Hybrid::default`].
    fn default() -> Self {
        Self::new(None, None)
    }
}

impl std::ops::Deref for StreamEngine {
    type Target = Hybrid<IngressRouter>;

    fn deref(&self) -> &Self::Target {
        &self.hybrid
    }
}

impl Engine for StreamEngine {
    fn appended(&self, shard: &ShardKey, written: Written) {
        self.hybrid.appended(shard, written);
    }

    async fn await_commit(
        &self,
        handle: &felix_broker::StreamHandle,
        shard: Option<&ShardKey>,
        outcome: &felix_broker::PublishOutcome,
        generation: u64,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        self.hybrid
            .await_commit(handle, shard, outcome, generation, timeout)
            .await
    }

    fn admit_local_ack(&self, shard: &ShardKey, generation: u64) -> Result<(), CommitError> {
        self.hybrid.admit_local_ack(shard, generation)
    }

    fn read_bound(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        shard: &ShardKey,
    ) -> Result<Option<u64>, Unreadable> {
        self.hybrid.read_bound(consistency, shard)
    }

    fn committed_bound(&self, shard: &ShardKey) -> felix_broker::ReadBound {
        self.hybrid.committed_bound(shard)
    }

    async fn confirm_read(&self, shard: &ShardKey) -> anyhow::Result<()> {
        self.hybrid.confirm_read(shard).await
    }

    async fn cache_write_at(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        generation: u64,
    ) -> Option<CacheWrite> {
        self.hybrid.cache_write_at(broker, shard, generation).await
    }

    async fn await_cache_quorum(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        timeout: Duration,
        access: Access,
        written: Option<CacheWrite>,
    ) -> anyhow::Result<()> {
        self.hybrid
            .await_cache_quorum(broker, shard, timeout, access, written)
            .await
    }

    async fn await_counter_quorum(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        timeout: Duration,
        end: Option<u64>,
        generation: u64,
        what: &'static str,
    ) -> anyhow::Result<()> {
        self.hybrid
            .await_counter_quorum(broker, shard, timeout, end, generation, what)
            .await
    }

    async fn read_skips_lease(&self, broker: &felix_broker::Broker, shard: &ShardKey) -> bool {
        self.hybrid.read_skips_lease(broker, shard).await
    }

    async fn await_readable(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        key: &ShardKey,
        end: u64,
        timeout: Duration,
    ) -> Result<(), CommitError> {
        self.hybrid
            .await_readable(consistency, key, end, timeout)
            .await
    }

    fn placement_changed(&self, shard: &ShardKey, placement: &Placement) {
        self.hybrid.placement_changed(shard, placement);
    }

    async fn handle(
        &self,
        from: Option<&str>,
        caller: PeerCapabilities,
        request: InternalMessage,
    ) -> InternalMessage {
        self.hybrid.handle(from, caller, request).await
    }
}

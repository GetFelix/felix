//! The replication engine as the broker sees it: what a write waits on before
//! it is acknowledged, and how far a reader may see.
//!
//! Callers move behind [`Engine`] one path at a time, with the protocol in
//! [`crate::quorum`] as its implementation ([`Hybrid`]). See
//! `docs/replication-engine.md`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::ShardKey;
use crate::quorum::{QuorumMarks, ShardServing, Unreadable};

/// What a durable write and a read ask of a shard's replication.
pub trait Engine: Send + Sync {
    /// Wait until the batch in `outcome`, written at `generation`, is
    /// committed: held by enough of the shard's replicas that no later leader
    /// can lose it. Returns at once for a stream that does not acknowledge by
    /// its replicas, or when `shard` is `None` (no replica set).
    ///
    /// An error means this broker cannot say the batch is committed, not that
    /// it is lost.
    fn await_commit(
        &self,
        handle: &felix_broker::StreamHandle,
        shard: Option<&ShardKey>,
        outcome: &felix_broker::PublishOutcome,
        generation: u64,
        timeout: Duration,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// The end of what a reader of `shard` may see, or `None` when its reads
    /// are not bounded by replication.
    fn read_bound(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        shard: &ShardKey,
    ) -> Result<Option<u64>, Unreadable>;
}

/// Felix's own replication protocol: the quorum marks the driver publishes,
/// read against how this broker serves each shard.
pub struct Hybrid<S: ?Sized> {
    marks: Option<Arc<QuorumMarks>>,
    serving: Option<Arc<S>>,
}

impl<S: ?Sized> Hybrid<S> {
    /// `None` for either outside a cluster, where nothing is replicated.
    pub fn new(marks: Option<Arc<QuorumMarks>>, serving: Option<Arc<S>>) -> Self {
        Self { marks, serving }
    }

    /// The quorum marks, for the waits that are not behind [`Engine`] yet
    /// (caches and counters).
    pub fn marks(&self) -> Option<&QuorumMarks> {
        self.marks.as_deref()
    }
}

impl<S: ?Sized> Default for Hybrid<S> {
    /// For tests only: an engine with no marks and no router, so a `Quorum`
    /// wait has nothing to wait on and returns at once. Production code builds
    /// it with [`Hybrid::new`].
    fn default() -> Self {
        Self::new(None, None)
    }
}

impl<S: ShardServing + ?Sized> Engine for Hybrid<S> {
    async fn await_commit(
        &self,
        handle: &felix_broker::StreamHandle,
        shard: Option<&ShardKey>,
        outcome: &felix_broker::PublishOutcome,
        generation: u64,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        crate::quorum::await_quorum(
            handle,
            shard,
            outcome,
            generation,
            self.marks.as_deref(),
            self.serving.as_deref(),
            timeout,
        )
        .await
    }

    fn read_bound(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        shard: &ShardKey,
    ) -> Result<Option<u64>, Unreadable> {
        crate::quorum::read_bound(
            consistency,
            shard,
            self.marks.as_deref(),
            self.serving.as_deref(),
        )
    }
}

#[cfg(test)]
mod tests;

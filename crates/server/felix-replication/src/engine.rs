//! The replication engine as the broker sees it: what a write waits on before
//! it is acknowledged, and how far a reader may see.
//!
//! Callers move behind [`Engine`] one path at a time, with the protocol in
//! [`crate::quorum`] as its implementation ([`Hybrid`]). See
//! `docs/replication-engine.md`.

use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

mod host;
mod start;

#[cfg(feature = "raft")]
pub(crate) use host::EngineHost;
pub(crate) use host::Host;
pub use start::{StartDeps, Started, elections, replica_handler};

use crate::ShardKey;
use crate::peer::PeerRequester;
use crate::quorum::{Access, CacheWrite, QuorumError, QuorumMarks, ShardServing, Unreadable};
use crate::replica::ReplicaHandler;
use felix_wire::internal::{ErrorCode, ForwardPublishError, InternalMessage, PeerCapabilities};

/// Why the engine cannot say a write is committed.
pub use crate::quorum::QuorumError as CommitError;

/// Where a write landed: the offset one past its last record, and the
/// generation it was written at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    pub end: u64,
    pub generation: u64,
}

impl Written {
    /// Where the batch in `outcome`, written at `generation`, landed. `None`
    /// for an ephemeral stream, which has no offsets.
    pub fn of(outcome: &felix_broker::PublishOutcome, generation: u64) -> Option<Self> {
        let (_, last) = outcome.offsets?;
        Some(Self {
            end: last + 1,
            generation,
        })
    }
}

/// The brokers the control plane placed a shard on, by node id.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Placement {
    pub nodes: Vec<String>,
}

impl Placement {
    /// A placement on `nodes`.
    pub fn new(nodes: Vec<String>) -> Self {
        Self { nodes }
    }
}

/// What a durable write and a read ask of a shard's replication.
pub trait Engine: Send + Sync {
    /// Records of `shard` up to `written.end` are durable on this leader's
    /// log, in the broker's commit order: get them to the other copies. Call
    /// it after the batch is durable. It never waits.
    fn appended(&self, shard: &ShardKey, written: Written);

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

    /// Whether a write to `shard` at `generation` may be acknowledged on
    /// local durability alone, as a `Leader` stream's is. On an error the
    /// caller waits on [`Engine::await_commit`] instead. A refusal never
    /// closes the shard.
    fn admit_local_ack(&self, shard: &ShardKey, generation: u64) -> Result<(), CommitError>;

    /// The end of what a reader of `shard` may see, or `None` when its reads
    /// are not bounded by replication.
    fn read_bound(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        shard: &ShardKey,
    ) -> Result<Option<u64>, Unreadable>;

    /// How far the broker core's readers of stream `shard` (subscribe fanout,
    /// cursor replay) may see. Unlike [`Engine::read_bound`] it also refuses
    /// a broker whose lease no longer lets it serve the shard. Asked only for
    /// `Quorum` streams. See [`crate::quorum::committed_bound`].
    fn committed_bound(&self, shard: &ShardKey) -> felix_broker::ReadBound;

    /// Confirm, after a read of `shard`'s state took its value, that this
    /// broker still leads `shard`. See [`crate::quorum::confirm_state_read`].
    fn confirm_read(&self, shard: &ShardKey) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Where a cache write landed, for [`Engine::await_cache_quorum`]. Call it
    /// inside the write's fence, right before the write.
    fn cache_write_at(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        generation: u64,
    ) -> impl Future<Output = Option<CacheWrite>> + Send;

    /// Wait until a cache write at `written` (or, for a read, the value just
    /// taken) is held by a quorum. See [`crate::quorum::await_cache_quorum`].
    fn await_cache_quorum(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        timeout: Duration,
        access: Access,
        written: Option<CacheWrite>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// As [`Engine::await_cache_quorum`] for a counter on `shard`'s cache.
    /// `end` is the counter log offset after the write, `None` for a read.
    fn await_counter_quorum(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        timeout: Duration,
        end: Option<u64>,
        generation: u64,
        what: &'static str,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Whether a read of this cache or counter may skip the lease check it
    /// would otherwise make before taking its value.
    fn read_skips_lease(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
    ) -> impl Future<Output = bool> + Send;

    /// Wait until the records of `key` up to `end` are safe to show a reader
    /// at `consistency`. See [`crate::quorum::await_readable`].
    fn await_readable(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        key: &ShardKey,
        end: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), QuorumError>> + Send;

    /// The control plane placed `shard` on `placement`. Safe to repeat.
    fn placement_changed(&self, shard: &ShardKey, placement: &Placement);

    /// Answer a peer's replication request, follower side: a batch to store,
    /// a bootstrap or rebuild, a fence, a fetch or a pre-vote. `from` is the
    /// peer's node id when it named itself, `caller` what it advertised.
    /// Anything else is refused as malformed.
    fn handle(
        &self,
        from: Option<&str>,
        caller: PeerCapabilities,
        request: InternalMessage,
    ) -> impl Future<Output = InternalMessage> + Send;
}

/// [`Hybrid::set_replica`] was called a second time.
#[derive(Debug, PartialEq, Eq)]
pub struct ReplicaAlreadySet;

impl std::fmt::Display for ReplicaAlreadySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the replica handler is already set")
    }
}

impl std::error::Error for ReplicaAlreadySet {}

/// Felix's own replication protocol: the quorum marks the driver publishes,
/// read against how this broker serves each shard.
pub struct Hybrid<S: ?Sized> {
    marks: Option<Arc<QuorumMarks>>,
    serving: Option<Arc<S>>,
    replica: OnceLock<ReplicaHandler>,
    driver: OnceLock<DriverWakes>,
}

/// What wakes the driver for a pass.
struct DriverWakes {
    /// Holds the shards that took an append.
    broker: Arc<felix_broker::Broker>,
    routes_changed: Arc<tokio::sync::Notify>,
}

impl<S: ?Sized> Hybrid<S> {
    /// `None` for either outside a cluster, where nothing is replicated.
    pub fn new(marks: Option<Arc<QuorumMarks>>, serving: Option<Arc<S>>) -> Self {
        Self {
            marks,
            serving,
            replica: OnceLock::new(),
            driver: OnceLock::new(),
        }
    }

    /// Wake the driver that ships `broker`'s logs, and that `routes_changed`
    /// already wakes, on [`Engine::appended`] and
    /// [`Engine::placement_changed`]. Until this is called, and on a broker
    /// that runs no driver, those two do nothing. Only the first call counts.
    pub fn wake_driver(
        &self,
        broker: Arc<felix_broker::Broker>,
        routes_changed: Arc<tokio::sync::Notify>,
    ) {
        let _ = self.driver.set(DriverWakes {
            broker,
            routes_changed,
        });
    }

    /// Answer peers' replication requests with `replica` ([`Engine::handle`]).
    ///
    /// The replica needs the broker, which exists only after other users of
    /// the engine are built, so it is set once, later. A second call is an
    /// error and leaves the first replica in place.
    pub fn set_replica(&self, replica: ReplicaHandler) -> Result<(), ReplicaAlreadySet> {
        self.replica.set(replica).map_err(|_| ReplicaAlreadySet)
    }

    /// The shard routing the engine reads leadership from, when there is one.
    pub fn serving(&self) -> Option<&Arc<S>> {
        self.serving.as_ref()
    }

    /// Starts a copy's confirmed read early. See
    /// [`ReplicaHandler::confirmed_prefetch`].
    pub fn confirmed_prefetch(&self) -> Option<crate::confirmed::Prefetch> {
        self.replica.get()?.confirmed_prefetch()
    }

    /// The quorum marks, for the waits that are not behind [`Engine`] yet.
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
    /// The driver ships from its own cursors up to the log's tail, so the
    /// position is not used: the shard is marked for a pass, as the broker
    /// core marks it when a publish becomes durable.
    fn appended(&self, shard: &ShardKey, _written: Written) {
        if let Some(driver) = self.driver.get() {
            crate::quorum::wake_pass(&driver.broker, shard);
        }
    }

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

    /// Always admitted. The hybrid checks the lease only when the write enters
    /// the write fence, so a lease that lapses between entry and ack is still
    /// acknowledged. Raft must check here instead; don't copy this.
    fn admit_local_ack(&self, _shard: &ShardKey, _generation: u64) -> Result<(), CommitError> {
        Ok(())
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

    fn committed_bound(&self, shard: &ShardKey) -> felix_broker::ReadBound {
        match (self.marks.as_deref(), self.serving.as_deref()) {
            (Some(marks), Some(serving)) => crate::quorum::committed_bound(shard, marks, serving),
            _ => felix_broker::ReadBound::Unbounded,
        }
    }

    async fn confirm_read(&self, shard: &ShardKey) -> anyhow::Result<()> {
        crate::quorum::confirm_state_read(shard, self.marks.as_deref(), self.serving.as_deref())
            .await
    }

    async fn cache_write_at(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        generation: u64,
    ) -> Option<CacheWrite> {
        crate::quorum::cache_write_at(
            broker,
            shard,
            generation,
            self.marks.as_deref(),
            self.serving.as_deref(),
        )
        .await
    }

    async fn await_cache_quorum(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        timeout: Duration,
        access: Access,
        written: Option<CacheWrite>,
    ) -> anyhow::Result<()> {
        crate::quorum::await_cache_quorum(
            broker,
            shard,
            self.marks.as_deref(),
            self.serving.as_deref(),
            timeout,
            access,
            written,
        )
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
        crate::quorum::await_counter_quorum(
            broker,
            shard,
            self.marks.as_deref(),
            self.serving.as_deref(),
            timeout,
            end,
            generation,
            what,
        )
        .await
    }

    async fn read_skips_lease(&self, broker: &felix_broker::Broker, shard: &ShardKey) -> bool {
        crate::quorum::read_skips_lease(
            broker,
            shard,
            self.marks.as_deref(),
            self.serving.as_deref(),
        )
        .await
    }

    async fn await_readable(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        key: &ShardKey,
        end: u64,
        timeout: Duration,
    ) -> Result<(), QuorumError> {
        crate::quorum::await_readable(
            consistency,
            key,
            self.marks.as_deref(),
            self.serving.as_deref(),
            end,
            timeout,
        )
        .await
    }

    /// The driver reads the set from the routing table on its next pass, so
    /// this only asks for that pass, as the routing feed does.
    fn placement_changed(&self, _shard: &ShardKey, _placement: &Placement) {
        if let Some(driver) = self.driver.get() {
            driver.routes_changed.notify_one();
        }
    }

    async fn handle(
        &self,
        from: Option<&str>,
        caller: PeerCapabilities,
        request: InternalMessage,
    ) -> InternalMessage {
        use felix_broker::LogKind;
        let Some(replica) = self.replica.get() else {
            return InternalMessage::ForwardPublishError(ForwardPublishError {
                correlation_id: request.correlation_id(),
                code: ErrorCode::Unavailable,
                detail: "replica handler not configured".to_string(),
            });
        };
        match request {
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateMarkedRecords(batch) => {
                replica.apply(from, batch, LogKind::Stream).await
            }
            InternalMessage::ReplicateCacheRecords(batch) => {
                replica.apply(from, batch, LogKind::Cache).await
            }
            InternalMessage::ReplicateGroupRecords(batch) => {
                replica.apply(from, batch, LogKind::GroupCursors).await
            }
            InternalMessage::ReplicateDeadLetterRecords(batch) => {
                replica.apply(from, batch, LogKind::GroupDeadLetters).await
            }
            InternalMessage::ReplicateCounterRecords(batch) => {
                replica.apply(from, batch, LogKind::Counters).await
            }
            InternalMessage::ReplicateBootstrap(request) => {
                replica.bootstrap(from, request, LogKind::Stream).await
            }
            InternalMessage::ReplicateCacheBootstrap(request) => {
                replica.bootstrap(from, request, LogKind::Cache).await
            }
            InternalMessage::ReplicateGroupBootstrap(request) => {
                replica
                    .bootstrap(from, request, LogKind::GroupCursors)
                    .await
            }
            InternalMessage::ReplicateDeadLetterBootstrap(request) => {
                replica
                    .bootstrap(from, request, LogKind::GroupDeadLetters)
                    .await
            }
            InternalMessage::ReplicateCounterBootstrap(request) => {
                replica.bootstrap(from, request, LogKind::Counters).await
            }
            InternalMessage::ReplicateRebuild(request) => replica.rebuild(from, request).await,
            InternalMessage::Fence(request) => replica.fence_from(from, caller, request).await,
            InternalMessage::ReplicateFetch(request) => replica.fetch(from, request).await,
            InternalMessage::PreVote(request) => replica.pre_vote(request).await,
            InternalMessage::SealFence(request) => replica.seal_fence(from, request).await,
            InternalMessage::SealAck(request) => replica.seal_ack(from, request).await,
            // Only a stream shard's batches are taken from a named copy;
            // anything else inside is refused.
            InternalMessage::FromCopy(stamped) => {
                let copy = Some(felix_storage::StorageId::from_bytes(stamped.copy));
                match *stamped.message {
                    InternalMessage::ReplicateRecords(batch)
                    | InternalMessage::ReplicateMarkedRecords(batch) => {
                        replica.apply_from(from, copy, batch, LogKind::Stream).await
                    }
                    InternalMessage::ReplicateGroupRecords(batch) => {
                        replica
                            .apply_from(from, copy, batch, LogKind::GroupCursors)
                            .await
                    }
                    InternalMessage::ReplicateDeadLetterRecords(batch) => {
                        replica
                            .apply_from(from, copy, batch, LogKind::GroupDeadLetters)
                            .await
                    }
                    other => not_replication(&other),
                }
            }
            other => not_replication(&other),
        }
    }
}

fn not_replication(request: &InternalMessage) -> InternalMessage {
    InternalMessage::ForwardPublishError(ForwardPublishError {
        correlation_id: request.correlation_id(),
        code: ErrorCode::Malformed,
        detail: format!("{:?} is not a replication request", request.kind()),
    })
}

/// [`Engine::committed_bound`] as the broker core asks for it
/// (`felix_broker::ReadBounds`).
pub struct CommittedReads<E> {
    engine: Arc<E>,
}

impl<E> CommittedReads<E> {
    pub fn new(engine: Arc<E>) -> Self {
        Self { engine }
    }
}

impl<E> std::fmt::Debug for CommittedReads<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommittedReads").finish_non_exhaustive()
    }
}

impl<E: Engine> felix_broker::ReadBounds for CommittedReads<E> {
    fn stream_bound(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> felix_broker::ReadBound {
        let key = ShardKey {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            kind: crate::ShardKind::Stream,
        };
        self.engine.committed_bound(&key)
    }
}

/// Let `Quorum` reads on `marks` confirm leadership by a round of fences sent
/// through `requester`, each round waiting up to `round_timeout` for a
/// majority, unless `by_lease` keeps them on the lease. Only the first call
/// counts. See [`crate::leadership::ReadIndex`].
pub fn bind_read_rounds<R: PeerRequester + Send + Sync + 'static>(
    marks: &QuorumMarks,
    requester: R,
    broker: Arc<felix_broker::Broker>,
    router: Arc<felix_router::ShardRouter>,
    round_timeout: Duration,
    by_lease: bool,
) {
    marks.set_read_check(
        Arc::new(crate::leadership::ReadIndex::new(
            requester,
            broker,
            router,
            round_timeout,
        )),
        by_lease,
    );
}

#[cfg(test)]
mod tests;

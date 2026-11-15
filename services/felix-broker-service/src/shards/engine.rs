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
/// of it ([`StreamEngine::raft`]), the hybrid's otherwise. A Raft shard is
/// written through [`StreamEngine::publish_on_raft`] and read up to its
/// commit index. Every other [`Engine`] call goes to the hybrid, whose own
/// methods are reached by deref.
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

    #[cfg(not(feature = "raft"))]
    pub fn raft_term(&self, _shard: &ShardKey) -> Option<u64> {
        None
    }
}

/// A publish to a Raft shard refused with nothing proposed.
#[derive(Debug, thiserror::Error)]
pub enum RaftRefusal {
    /// The term routing admitted the write at is not one this broker serves
    /// now: it was deposed, or leads a newer term it has not opened yet.
    #[error("this broker does not serve the shard's Raft term {term}; retry")]
    NotLeading { term: u64 },
    #[error(transparent)]
    Broker(#[from] felix_broker::BrokerError),
}

impl RaftRefusal {
    /// As the client is answered: a term that moved is the shard not ready
    /// here, which a client retries after resolving the route again.
    pub(crate) fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::NotLeading { .. } => {
                crate::serving::quic::client_error::ClientError::unavailable(
                    &super::routing::Reason::NotReady,
                    self.to_string(),
                )
                .into()
            }
            Self::Broker(err) => err.into(),
        }
    }
}

/// A batch proposed to a Raft shard, or an empty one, which proposes nothing.
pub struct RaftPublish(Option<felix_broker::ProposedPublish<CommitError>>);

impl RaftPublish {
    /// Wait for the acknowledgement. Returns the first record's offset.
    pub async fn complete(self) -> Result<Option<u64>, CommitError> {
        Ok(self.complete_offsets().await?.map(|(first, _)| first))
    }

    /// [`Self::complete`], returning the first and the last record's offsets.
    pub async fn complete_offsets(self) -> Result<Option<(u64, u64)>, CommitError> {
        match self.0 {
            Some(proposed) => Ok(proposed.complete().await?.offsets),
            None => Ok(None),
        }
    }
}

/// A head trim on its way to an answer from a Raft shard.
pub struct RaftTrim(felix_broker::ProposedTrim<CommitError>);

impl RaftTrim {
    /// Wait for the trim's commit and its floor in the replay ring.
    pub async fn complete(
        self,
    ) -> Result<felix_broker::TrimOutcome, felix_broker::TrimFailed<CommitError>> {
        self.0.complete().await
    }
}

/// An idempotent batch on its way to an answer from a Raft shard.
#[cfg(feature = "raft")]
pub struct RaftIdempotentPublish(felix_replication::raft::IdempotentPublish);

#[cfg(feature = "raft")]
impl RaftIdempotentPublish {
    /// Wait for the commit of the batch, or of the one the log already
    /// held under its sequence. Returns the first and the last record's
    /// offsets. A batch a leader change cut can have other records between
    /// them.
    pub async fn complete(self) -> Result<Option<(u64, u64)>, CommitError> {
        Ok(self.complete_outcome().await?.outcome.offsets)
    }

    /// [`Self::complete`], saying whether the log already held the batch.
    pub async fn complete_outcome(self) -> Result<felix_broker::IdempotentOutcome, CommitError> {
        self.0.complete().await
    }
}

/// What an idempotent publish waits on when no Raft group can run a shard.
#[cfg(not(feature = "raft"))]
pub enum RaftIdempotentPublish {}

#[cfg(not(feature = "raft"))]
impl RaftIdempotentPublish {
    pub async fn complete(self) -> Result<Option<(u64, u64)>, CommitError> {
        match self {}
    }

    pub async fn complete_outcome(self) -> Result<felix_broker::IdempotentOutcome, CommitError> {
        match self {}
    }
}

impl StreamEngine {
    /// Whether a Raft group runs `shard` on this broker.
    pub fn on_raft(&self, shard: &ShardKey) -> bool {
        #[cfg(feature = "raft")]
        return self.raft(shard).is_some();
        #[cfg(not(feature = "raft"))]
        {
            let _ = shard;
            false
        }
    }

    /// Whether this broker runs stream shards on Raft groups. Another
    /// broker's shard it forwards to may be on one too.
    pub fn runs_raft(&self) -> bool {
        #[cfg(feature = "raft")]
        return self.raft.get().is_some();
        #[cfg(not(feature = "raft"))]
        false
    }

    /// Refuse `what` on a shard a Raft group runs here. Its log takes plain
    /// publishes only, and the hybrid's fence never opens for it.
    pub fn refuse_on_raft(
        &self,
        shard: &ShardKey,
        what: &'static str,
    ) -> Result<(), felix_broker::BrokerError> {
        if self.on_raft(shard) {
            return Err(felix_broker::BrokerError::NotOnEngineStream { what });
        }
        Ok(())
    }

    /// Propose a plain publish to `shard`'s Raft group, at the `term`
    /// routing answered. `None` when the hybrid runs the shard.
    ///
    /// Call it in the shard's publish order: the batch takes its offsets
    /// and its place in the delivery order here. [`RaftPublish::complete`]
    /// answers a `Quorum` stream's publisher at the commit when the stream
    /// fsyncs on commit, and otherwise once a majority of the voters has
    /// written the batch, if that is first. A `Leader` stream's publisher is answered once the
    /// batch is on this leader's log, while the group's leader lease holds,
    /// and at the commit while it does not. Subscribers get the batch at
    /// its commit either way. No write waits on the hybrid's marks or this
    /// broker's lease.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_on_raft(
        &self,
        broker: &felix_broker::Broker,
        handle: &felix_broker::StreamHandle,
        shard: &ShardKey,
        term: u64,
        payloads: Vec<bytes::Bytes>,
        attribution: felix_broker::Attribution,
        timeout: Duration,
    ) -> Option<Result<RaftPublish, RaftRefusal>> {
        #[cfg(feature = "raft")]
        {
            use felix_replication::raft::{AckAt, AnswerAt};
            let raft = self.raft(shard)?;
            // A term is served only once its stream is at the committed
            // tail, and only the term the write was routed at. The group
            // can move on between this check and the proposal, so the term
            // goes with the batch and one written at another is given up.
            if self.raft_term(shard) != Some(term) {
                return Some(Err(RaftRefusal::NotLeading { term }));
            }
            if payloads.is_empty() {
                return Some(Ok(RaftPublish(None)));
            }
            let ack = broker.durable_storage().map_or(AckAt::Flushed, |storage| {
                AckAt::for_fsync(&storage.config().fsync_mode)
            });
            // A `Quorum` stream not fsynced on commit is answered once a
            // majority has written the batch, the hybrid's periodic promise.
            // A copy that lost its unsynced tail to a failed flush (#2469) or
            // a power cut (#2423) restarts under a new copy id and does not
            // vote with its cut log, so the batch is lost only if a majority
            // of copies each lose their unsynced tail that way.
            let answer = match (handle.consistency(), ack) {
                (felix_broker::ConsistencyLevel::Leader, _) => AnswerAt::Lease,
                (felix_broker::ConsistencyLevel::Quorum, AckAt::Written) => AnswerAt::Majority,
                (felix_broker::ConsistencyLevel::Quorum, AckAt::Flushed) => AnswerAt::Commit,
            };
            let proposed = raft.publish_with(
                broker,
                handle,
                shard,
                Some(term),
                payloads,
                attribution,
                ack,
                answer,
                timeout,
            );
            Some(
                proposed
                    .map(|proposed| RaftPublish(Some(proposed)))
                    .map_err(RaftRefusal::Broker),
            )
        }
        #[cfg(not(feature = "raft"))]
        {
            let _ = (broker, handle, shard, term, payloads, attribution, timeout);
            None
        }
    }
}

impl StreamEngine {
    /// Propose a conditional publish to `shard`'s Raft group, at the `term`
    /// routing answered: one record, written only at offset `expected`.
    /// `None` when the hybrid runs the shard.
    ///
    /// The leader's log decides the condition at the entry's final index and
    /// followers copy it, so a refused publish still takes an offset (a blank readers skip) and is
    /// answered [`CommitError::OffsetMismatch`] with the offset after it.
    /// Either answer waits for the commit, whatever the stream's level.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_at_on_raft(
        &self,
        broker: &felix_broker::Broker,
        handle: &felix_broker::StreamHandle,
        shard: &ShardKey,
        term: u64,
        payloads: Vec<bytes::Bytes>,
        attribution: felix_broker::Attribution,
        expected: u64,
        timeout: Duration,
    ) -> Option<Result<RaftPublish, RaftRefusal>> {
        #[cfg(feature = "raft")]
        {
            use felix_replication::raft::AckAt;
            let raft = self.raft(shard)?;
            // As for a plain publish: only the term the write was routed at.
            if self.raft_term(shard) != Some(term) {
                return Some(Err(RaftRefusal::NotLeading { term }));
            }
            let ack = broker.durable_storage().map_or(AckAt::Flushed, |storage| {
                AckAt::for_fsync(&storage.config().fsync_mode)
            });
            let proposed = raft.publish_at(
                broker,
                handle,
                shard,
                Some(term),
                payloads,
                attribution,
                expected,
                ack,
                timeout,
            );
            Some(
                proposed
                    .map(|proposed| RaftPublish(Some(proposed)))
                    .map_err(RaftRefusal::Broker),
            )
        }
        #[cfg(not(feature = "raft"))]
        {
            let _ = (
                broker,
                handle,
                shard,
                term,
                payloads,
                attribution,
                expected,
                timeout,
            );
            None
        }
    }
}

impl StreamEngine {
    /// Propose a head trim to `shard`'s Raft group, at the `term` routing
    /// answered: hide the shard below `before`, only if its next offset is
    /// `expected`. `None` when the hybrid runs the shard.
    ///
    /// Checked and answered as on the hybrid, except that a trim the
    /// leader's log refuses at its final index still takes an offset, as a
    /// conditional publish does. The answer waits for the commit and for
    /// the floor to reach this broker's replay ring.
    #[allow(clippy::too_many_arguments)]
    pub async fn trim_at_on_raft(
        &self,
        broker: &felix_broker::Broker,
        handle: &felix_broker::StreamHandle,
        shard: &ShardKey,
        term: u64,
        before: u64,
        expected: u64,
        headers: bytes::Bytes,
        timeout: Duration,
    ) -> Option<Result<RaftTrim, RaftRefusal>> {
        #[cfg(feature = "raft")]
        {
            use felix_replication::raft::AckAt;
            let raft = self.raft(shard)?;
            if self.raft_term(shard) != Some(term) {
                return Some(Err(RaftRefusal::NotLeading { term }));
            }
            let ack = broker.durable_storage().map_or(AckAt::Flushed, |storage| {
                AckAt::for_fsync(&storage.config().fsync_mode)
            });
            let proposed = raft
                .trim_at(
                    broker,
                    handle,
                    shard,
                    Some(term),
                    before,
                    expected,
                    headers,
                    ack,
                    timeout,
                )
                .await;
            Some(proposed.map(RaftTrim).map_err(RaftRefusal::Broker))
        }
        #[cfg(not(feature = "raft"))]
        {
            let _ = (
                broker, handle, shard, term, before, expected, headers, timeout,
            );
            None
        }
    }
}

impl StreamEngine {
    /// Propose an idempotent producer's batch to `shard`'s Raft group, at
    /// the `term` routing answered. `None` when the hybrid runs the shard.
    ///
    /// The sequence is checked against this broker's log, which holds what
    /// every earlier leader committed, and a batch it already holds is
    /// answered with where it landed. `turn` is the producer's
    /// (`Broker::producer_turn`), first polled in the shard's publish order.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_idempotent_on_raft(
        &self,
        broker: &felix_broker::Broker,
        handle: &felix_broker::StreamHandle,
        shard: &ShardKey,
        term: u64,
        turn: felix_broker::ProducerTurn,
        sequence: u64,
        reuse: felix_broker::SequenceReuse,
        payloads: Vec<bytes::Bytes>,
        attribution: felix_broker::Attribution,
        timeout: Duration,
    ) -> Option<Result<RaftIdempotentPublish, RaftRefusal>> {
        #[cfg(feature = "raft")]
        {
            let raft = self.raft(shard)?;
            // As for a plain publish: only the term the write was routed at.
            if self.raft_term(shard) != Some(term) {
                return Some(Err(RaftRefusal::NotLeading { term }));
            }
            let proposed = raft.publish_idempotent(
                broker,
                handle,
                shard,
                term,
                turn,
                sequence,
                payloads,
                attribution,
                reuse,
                timeout,
            );
            Some(
                proposed
                    .map(RaftIdempotentPublish)
                    .map_err(RaftRefusal::Broker),
            )
        }
        #[cfg(not(feature = "raft"))]
        {
            let _ = (
                broker,
                handle,
                shard,
                term,
                turn,
                sequence,
                reuse,
                payloads,
                attribution,
                timeout,
            );
            None
        }
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
        // One bound for every level: nothing above the commit index is read.
        #[cfg(feature = "raft")]
        if let Some(raft) = self.raft(shard) {
            return raft.read_bound(shard).map(Some);
        }
        self.hybrid.read_bound(consistency, shard)
    }

    fn committed_bound(&self, shard: &ShardKey) -> felix_broker::ReadBound {
        #[cfg(feature = "raft")]
        if let Some(raft) = self.raft(shard) {
            return match raft.read_bound(shard) {
                Ok(bound) => felix_broker::ReadBound::Committed(bound),
                Err(_) => felix_broker::ReadBound::Refused,
            };
        }
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

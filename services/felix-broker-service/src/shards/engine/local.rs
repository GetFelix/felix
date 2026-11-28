//! What the engine answers for a shard no Raft member runs here: an
//! in-memory stream shard on a cluster member, and every shard of a
//! standalone broker without durable storage.
//!
//! None of these shards has a follower, so nothing waits on a majority. A
//! cluster member still answers only for a shard it serves, and a state
//! read only while its lease holds: an in-memory shard's owner is placed by
//! the control plane and must stop once cut off from it.

use felix_broker::{ConsistencyLevel, ReadBound};
use felix_replication::engine::{CommitError, Unreadable};
use felix_wire::internal::{ErrorCode, ForwardPublishError, InternalMessage};

use crate::shards::ShardKey;
use crate::shards::routing::IngressRouter;

/// How this broker serves a shard, as the answers below read it. The ingress
/// router answers it; a trait so the tests can set each answer.
pub(crate) trait Serves: Send + Sync {
    /// The generation this broker serves `key` at, or `None` when it does not.
    fn generation(&self, key: &ShardKey) -> Option<u64>;
    /// Whether `key` is placed with followers.
    fn replicated(&self, key: &ShardKey) -> bool;
    /// Whether this broker's lease is valid right now, against the clock.
    fn lease_valid(&self) -> bool;
    /// Count a read refused at its confirmation because the lease lapsed.
    fn record_ack_refusal(&self);
}

impl Serves for IngressRouter {
    fn generation(&self, key: &ShardKey) -> Option<u64> {
        IngressRouter::generation(self, key)
    }

    fn replicated(&self, key: &ShardKey) -> bool {
        IngressRouter::replicated(self, key)
    }

    fn lease_valid(&self) -> bool {
        self.fence().lease_valid()
    }

    fn record_ack_refusal(&self) {
        crate::cluster::lease::metrics::record_refusal(
            crate::cluster::lease::metrics::BOUNDARY_ACK,
        );
    }
}

/// The answers for shards with no Raft member here. `serving` is `None` on a
/// standalone broker, which has nothing to wait for and nothing to refuse.
pub(crate) struct Local<S: ?Sized> {
    serving: Option<std::sync::Arc<S>>,
}

impl<S: ?Sized> Local<S> {
    pub(crate) fn new(serving: Option<std::sync::Arc<S>>) -> Self {
        Self { serving }
    }

    pub(crate) fn serving(&self) -> Option<&std::sync::Arc<S>> {
        self.serving.as_ref()
    }
}

impl<S: Serves + ?Sized> Local<S> {
    /// A `Quorum` publish with offsets is refused once this broker stopped
    /// serving its shard. A replicated one is never acknowledged: no engine
    /// here could tell when a majority holds it.
    pub(crate) fn await_commit(
        &self,
        consistency: ConsistencyLevel,
        shard: Option<&ShardKey>,
        offsets: Option<(u64, u64)>,
    ) -> Result<(), CommitError> {
        if consistency != ConsistencyLevel::Quorum {
            return Ok(());
        }
        let (Some(shard), Some(serving), Some(_)) = (shard, self.serving.as_deref(), offsets)
        else {
            return Ok(());
        };
        if serving.generation(shard).is_none() {
            return Err(CommitError::LeadershipLost {
                what: "batch",
                detail: "shard ownership changed",
            });
        }
        if serving.replicated(shard) {
            return Err(CommitError::LeadershipLost {
                what: "batch",
                detail: "no engine on this broker replicates the shard",
            });
        }
        Ok(())
    }

    pub(crate) fn read_bound(
        &self,
        consistency: Option<ConsistencyLevel>,
        shard: &ShardKey,
    ) -> Result<Option<u64>, Unreadable> {
        if consistency != Some(ConsistencyLevel::Quorum) {
            return Ok(None);
        }
        let Some(serving) = self.serving.as_deref() else {
            return Ok(None);
        };
        serving.generation(shard).ok_or(Unreadable::NotServed)?;
        if serving.replicated(shard) {
            return Err(Unreadable::Settling);
        }
        Ok(None)
    }

    pub(crate) fn committed_bound(&self, shard: &ShardKey) -> ReadBound {
        let Some(serving) = self.serving.as_deref() else {
            return ReadBound::Unbounded;
        };
        // Serving first: a deposed owner must not read a shard placed with
        // no followers as its own.
        if serving.generation(shard).is_none() {
            return ReadBound::Refused;
        }
        if !serving.replicated(shard) {
            return ReadBound::Unbounded;
        }
        if !serving.lease_valid() {
            return ReadBound::Refused;
        }
        ReadBound::Settling
    }

    /// The lease decides whether this broker still owns the shard after a
    /// state read took its value.
    pub(crate) fn confirm_read(&self) -> Result<(), CommitError> {
        let Some(serving) = self.serving.as_deref() else {
            return Ok(());
        };
        if serving.lease_valid() {
            return Ok(());
        }
        serving.record_ack_refusal();
        Err(CommitError::LeadershipLost {
            what: "read",
            detail: "the lease lapsed",
        })
    }
}

/// A peer's replication request. Shards replicate over Raft, whose messages
/// reach the group runtime on their own path, so none is answered here.
pub(crate) fn refuse_replication(request: &InternalMessage) -> InternalMessage {
    InternalMessage::ForwardPublishError(ForwardPublishError {
        correlation_id: request.correlation_id(),
        code: ErrorCode::Unavailable,
        detail: "replica handler not configured".to_string(),
    })
}

#[cfg(test)]
mod tests;

//! What the engine asks of the broker: the shard's serving state and its
//! write fence, behind one trait.

use std::sync::Arc;

use felix_common::membership::RefusedGeneration;

use crate::ShardKey;
use crate::driver::WriteFence;
use crate::quorum::ShardServing;

/// What any engine needs from the broker about a shard.
pub trait EngineHost: Send + Sync {
    /// The generation this broker serves `key` at, or `None` when it does not
    /// serve it here.
    fn generation(&self, key: &ShardKey) -> Option<u64>;
    /// Whether `key` has followers, so a `Quorum` write has to wait for them.
    fn replicated(&self, key: &ShardKey) -> bool;
    /// Whether `key` is closed with no write still inside.
    fn quiesced(&self, key: &ShardKey) -> bool;
    /// `key` has a newer leader than `generation`: stop taking writes.
    fn deposed(&self, key: &ShardKey, generation: u64);
}

/// The callbacks only the hybrid engine makes: the lease, and the states of a
/// leader that gave a generation up or has not opened it yet.
pub trait HybridHost: EngineHost {
    /// Whether this broker's lease is valid right now, against the clock.
    fn lease_valid(&self) -> bool;
    /// Count a `Quorum` write held back because the lease lapsed while it waited.
    fn record_ack_refusal(&self);
    /// `key`'s writes at `generation` no longer need the lease to get in.
    fn serve_without_lease(&self, key: &ShardKey, generation: u64);
    /// `key`'s readers and group sessions at `generation` no longer need the lease.
    fn sessions_without_lease(&self, key: &ShardKey, generation: u64);
    /// A pass over `key` at `generation` has decided how the shard serves.
    fn passed(&self, key: &ShardKey, generation: u64);
    /// This broker does not lead `key` at `generation`, though the route says it does.
    fn withheld(&self, key: &ShardKey, generation: u64) -> bool;
    /// The generation this broker gave `key` up at.
    fn gave_up_at(&self, key: &ShardKey) -> Option<u64>;
    /// The generation this broker refused to open `key` at, and to whom it was promised.
    fn refused_at(&self, key: &ShardKey) -> Option<RefusedGeneration>;
    /// A probe of every follower found none ahead of this broker's copy, or one.
    fn probed(&self, key: &ShardKey, generation: u64, reclaimable: bool);
    /// Whether this process has opened `key` at `generation` or a later one.
    fn opened_since(&self, key: &ShardKey, generation: u64) -> bool;
    /// Whether `key` waits at `generation` for a promotion fence.
    fn promoting(&self, key: &ShardKey, generation: u64) -> bool;
}

/// A broker's serving state and its write fence, as one host.
pub struct Host<S: ?Sized, F: ?Sized> {
    serving: Arc<S>,
    fence: Arc<F>,
}

impl<S: ?Sized, F: ?Sized> Host<S, F> {
    pub fn new(serving: Arc<S>, fence: Arc<F>) -> Self {
        Self { serving, fence }
    }
}

impl<S: ShardServing + ?Sized, F: WriteFence + ?Sized> EngineHost for Host<S, F> {
    fn generation(&self, key: &ShardKey) -> Option<u64> {
        self.serving.generation(key)
    }

    fn replicated(&self, key: &ShardKey) -> bool {
        self.serving.replicated(key)
    }

    fn quiesced(&self, key: &ShardKey) -> bool {
        self.fence.quiesced(key)
    }

    fn deposed(&self, key: &ShardKey, generation: u64) {
        self.fence.deposed(key, generation);
    }
}

impl<S: ShardServing + ?Sized, F: WriteFence + ?Sized> HybridHost for Host<S, F> {
    fn lease_valid(&self) -> bool {
        self.serving.lease_valid()
    }

    fn record_ack_refusal(&self) {
        self.serving.record_ack_refusal();
    }

    fn serve_without_lease(&self, key: &ShardKey, generation: u64) {
        self.fence.serve_without_lease(key, generation);
    }

    fn sessions_without_lease(&self, key: &ShardKey, generation: u64) {
        self.fence.sessions_without_lease(key, generation);
    }

    fn passed(&self, key: &ShardKey, generation: u64) {
        self.fence.passed(key, generation);
    }

    fn withheld(&self, key: &ShardKey, generation: u64) -> bool {
        self.fence.withheld(key, generation)
    }

    fn gave_up_at(&self, key: &ShardKey) -> Option<u64> {
        self.fence.gave_up_at(key)
    }

    fn refused_at(&self, key: &ShardKey) -> Option<RefusedGeneration> {
        self.fence.refused_at(key)
    }

    fn probed(&self, key: &ShardKey, generation: u64, reclaimable: bool) {
        self.fence.probed(key, generation, reclaimable);
    }

    fn opened_since(&self, key: &ShardKey, generation: u64) -> bool {
        self.fence.opened_since(key, generation)
    }

    fn promoting(&self, key: &ShardKey, generation: u64) -> bool {
        self.fence.promoting(key, generation)
    }
}

/// The driver takes the host as its fence, so the fence-side callbacks it
/// makes go through the host.
impl<S: ShardServing + ?Sized, F: WriteFence + ?Sized> WriteFence for Host<S, F> {
    fn quiesced(&self, key: &ShardKey) -> bool {
        EngineHost::quiesced(self, key)
    }

    fn serve_without_lease(&self, key: &ShardKey, generation: u64) {
        HybridHost::serve_without_lease(self, key, generation);
    }

    fn sessions_without_lease(&self, key: &ShardKey, generation: u64) {
        HybridHost::sessions_without_lease(self, key, generation);
    }

    fn passed(&self, key: &ShardKey, generation: u64) {
        HybridHost::passed(self, key, generation);
    }

    fn deposed(&self, key: &ShardKey, generation: u64) {
        EngineHost::deposed(self, key, generation);
    }

    fn withheld(&self, key: &ShardKey, generation: u64) -> bool {
        HybridHost::withheld(self, key, generation)
    }

    fn gave_up_at(&self, key: &ShardKey) -> Option<u64> {
        HybridHost::gave_up_at(self, key)
    }

    fn refused_at(&self, key: &ShardKey) -> Option<RefusedGeneration> {
        HybridHost::refused_at(self, key)
    }

    fn probed(&self, key: &ShardKey, generation: u64, reclaimable: bool) {
        HybridHost::probed(self, key, generation, reclaimable);
    }

    fn opened_since(&self, key: &ShardKey, generation: u64) -> bool {
        HybridHost::opened_since(self, key, generation)
    }

    fn promoting(&self, key: &ShardKey, generation: u64) -> bool {
        HybridHost::promoting(self, key, generation)
    }
}

/// And the serving side, for the quorum waits.
impl<S: ShardServing + ?Sized, F: WriteFence + ?Sized> ShardServing for Host<S, F> {
    fn replicated(&self, key: &ShardKey) -> bool {
        EngineHost::replicated(self, key)
    }

    fn generation(&self, key: &ShardKey) -> Option<u64> {
        EngineHost::generation(self, key)
    }

    fn lease_valid(&self) -> bool {
        HybridHost::lease_valid(self)
    }

    fn record_ack_refusal(&self) {
        HybridHost::record_ack_refusal(self);
    }
}

#[cfg(test)]
mod tests;

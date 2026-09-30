//! What placement may assume about how much of a shard's log each replica
//! holds.
use crate::model::{HaltedCopy, ShardKey};

/// Which replicas hold enough of a shard's log to lead it.
///
/// Promotion is gated on this, and the gate is the difference between a failover
/// and data loss: a replica that holds nothing can be promoted perfectly well
/// and will serve an empty shard.
pub trait CaughtUp {
    /// Whether `node_id` is within the catch-up bound for `key`.
    fn is_caught_up(&self, key: &ShardKey, node_id: &str) -> bool;

    /// How far `node_id` had got, as last reported.
    ///
    /// Used to choose *between* caught-up replicas. "Caught up" is only ever
    /// true of the tail it was measured against, so a report made before the
    /// leader's last writes can call two replicas level when one holds more.
    /// Preferring the higher offset picks the replica a quorum-acknowledged
    /// record is guaranteed to be on.
    ///
    /// `None` means nothing is known, which orders below any known offset.
    fn reported_offset(&self, _key: &ShardKey, _node_id: &str) -> Option<u64> {
        None
    }

    /// How many records `node_id` was behind the leader's tail, as last
    /// reported. `None` when the leader did not say what its tail was.
    fn lag_records(&self, _key: &ShardKey, _node_id: &str) -> Option<u64> {
        None
    }

    /// The leader's own tail, as last reported. `None` when the leader did not
    /// say.
    fn leader_offset(&self, _key: &ShardKey) -> Option<u64> {
        None
    }

    /// Whether the leader of `key` has reported, at exactly `generation`, that
    /// it has stopped serving and its log will not grow. A report from an
    /// earlier generation describes a leader that was still writing.
    fn is_drained(&self, _key: &ShardKey, _generation: u64) -> bool {
        false
    }

    /// The generation the report for `key` was made at, if there is a fresh
    /// one.
    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        None
    }

    /// The store's clock these reports are read as of. Move timeouts are
    /// measured on it, the clock a move's start is stamped with; without it
    /// no move times out.
    fn as_of_millis(&self) -> Option<u64> {
        None
    }

    /// Whether the last report for `key` names `node_id`'s copy halted,
    /// whatever generation it was reported at. A node that has just been
    /// dropped from the set for a halt is still listed; see
    /// [`ReplicaReport::carry_halts`](crate::model::ReplicaReport::carry_halts).
    fn halted(&self, _key: &ShardKey, _node_id: &str) -> Option<&HaltedCopy> {
        None
    }

    /// [`Self::halted`], only where the leader at `generation` reported it:
    /// a member of that generation's replica set that has stopped following.
    fn halted_at(&self, key: &ShardKey, node_id: &str, generation: u64) -> Option<&HaltedCopy> {
        self.halted(key, node_id)
            .filter(|halt| halt.generation == generation)
    }

    /// [`Self::halted`], where the halt still speaks for a member of the set
    /// at `generation`: reported at it, or carried into it from one of the
    /// generations just before and not yet contradicted.
    fn halted_member(&self, key: &ShardKey, node_id: &str, generation: u64) -> Option<&HaltedCopy> {
        self.halted(key, node_id)
            .filter(|halt| halt.holds_at(generation))
    }
}

/// Nothing is caught up.
///
/// What the cluster can honestly report until records are actually replicated
/// (#112). With this, promotion never fires and placement behaves exactly as it
/// did — which is correct, because a promotion today would hand the shard to a
/// broker holding none of it.
pub struct NothingCaughtUp;

impl CaughtUp for NothingCaughtUp {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        false
    }
}

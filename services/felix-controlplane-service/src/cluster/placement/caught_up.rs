//! What placement may assume about how much of a shard's log each replica
//! holds.
use std::collections::BTreeMap;

use crate::model::{HaltedCopy, ShardKey};

/// Which replicas hold enough of a shard's log to lead it.
///
/// Promotion is gated on this, and the gate is the difference between a failover
/// and data loss: a replica that holds nothing can be promoted perfectly well
/// and will serve an empty shard.
pub trait CaughtUp {
    /// Whether `node_id` is within the catch-up bound for `key`. From a
    /// given-up leader's report, only on the evidence a reclaim needs: see
    /// [`Self::reclaimable`].
    fn is_caught_up(&self, key: &ShardKey, node_id: &str) -> bool;

    /// The node that made the report `key` is judged by, when the report
    /// says. `None` from a control plane that predates it.
    fn reported_by(&self, _key: &ShardKey) -> Option<&str> {
        None
    }

    /// Whether `leader`, the leader of `key`, is registered with another
    /// storage identity than the one the shard's last report names for it:
    /// it came back after a crash that may have cost it records it
    /// acknowledged, or on a wiped disk. Its copy can be short of what it
    /// reported, so it does not keep the shard. False when either side has
    /// no identity.
    fn leader_replaced(&self, _key: &ShardKey, _leader: &str) -> bool {
        false
    }

    /// The storage identity a promotion of `key` hands the new leader for the
    /// old set's members, `old_set`, by node id, so its fence counts a member
    /// only from that copy: the one the report names, the old leader's own
    /// included, and an empty one, which nothing matches, for a member the
    /// report names none for but that is registered with one. Empty when
    /// there is no report or it names none.
    fn report_storage_ids(&self, _key: &ShardKey, _old_set: &[&str]) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    /// Whether `leader` reported at `generation` that it gave the shard up:
    /// back from a crash that may have cost it records, it leads nothing
    /// from its copy, and its report says where every follower's copy ends
    /// instead (`felix_replication::reclaim`).
    fn leader_gave_up(&self, _key: &ShardKey, _generation: u64, _leader: &str) -> bool {
        false
    }

    /// Whether `leader`, which gave the shard up, may lead it again from its
    /// copy: a fresh report it made since it last registered, under the
    /// storage identity it registered with, measures every one of
    /// `followers` under the identity and incarnation each is registered at
    /// now, and every one is behind `leader`. A follower level with it or
    /// ahead is promoted instead.
    fn reclaimable(&self, _key: &ShardKey, _leader: &str, _followers: &[&str]) -> bool {
        false
    }

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

    /// How far `node_id` had got with a cache's counter log, as last
    /// reported. Only read as progress for a copy being restored. `None` for
    /// streams and from leaders that do not say.
    fn reported_counter_offset(&self, _key: &ShardKey, _node_id: &str) -> Option<u64> {
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

    /// When this instance last saw `node_id`'s reported position in `key`
    /// move while a restore copied it in. `None` when it has not watched that
    /// copy, and then no restore is given up for want of progress: a pass
    /// without the history must not mistake a moving copy for a stuck one.
    fn progressed_at_millis(&self, _key: &ShardKey, _node_id: &str) -> Option<u64> {
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

    /// Whether `follower` said, recently enough to act on, that it cannot
    /// reach `leader`, the leader of `key` at `generation`. Only for a shard
    /// whose deposed leader is kept out without the lease: the fleet must
    /// have enabled what that takes, and `leader` must have reported at
    /// `generation` that it serves nothing on its lease. For any other the
    /// followers' word decides nothing, and this is `false`.
    fn suspects(&self, _key: &ShardKey, _generation: u64, _follower: &str, _leader: &str) -> bool {
        false
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

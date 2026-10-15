//! Deterministic shard placement.
//!
//! **Bounded-load rendezvous hashing.** Score every eligible node against the
//! shard and take the highest — but skip a node already carrying its balanced
//! share, so the shard spills to the next node under its share. Rendezvous is
//! chosen over a consistent-hash ring because it needs no ring state, no
//! virtual-node tuning, and removing a node only moves the shards that node
//! held; the load bound is added because plain highest-random-weight balances
//! only in the limit of many keys, and a cluster starts at a handful of shards
//! (24 over three brokers skewed 11/5/8) where the bound is the difference
//! between even use and single-node saturation. See `choose`.
//!
//! Everything here is a pure function of a metadata snapshot. That is what makes
//! "the same snapshot always yields the same placement" testable rather than
//! hoped for, and it keeps the algorithm out of the store.
//!
//! **Moves.** A shard whose leader is alive is moved, not reassigned: the
//! destination is staged as a replica, caught up, and made leader only after
//! the old leader has stopped and said so. Each step is an assignment write,
//! so any instance resumes a half-done move from the store. See `move_step`
//! and `docs/replication-design.md`. Two triggers: a draining node gives up
//! what it leads, and a node over its share gives shards to one under it,
//! bounded by `MovePolicy`. An operator may also start or cancel a move, and
//! pause placement's own (`operator`).
//!
//! **Who places.** The holder of the placement lease runs the timed passes
//! (`spawn_reconciler`), and a woken pass runs wherever it was woken. Every
//! write, an operator's included, is conditional on the shard's generation
//! and on the placement token read before the pass decided, so the move
//! limits hold however many instances write. See `docs/control-plane.md`.
//!
//! **Zones.** A shard's copies are spread across the zones brokers register,
//! and a move never narrows that spread when it can avoid it; see `zones`.
//!
//! `NodeCapacity::weight` is ignored: weighted rendezvous needs a logarithm,
//! and floating point that must agree bit-for-bit across instances is a bad
//! foundation for a decision that has to be identical everywhere.
mod caught_up;
mod decision;
mod metrics;
mod moves;
mod operator;
mod plan;
mod progress;
mod reconciler;
mod rendezvous;
mod replica_positions;
mod restore;
mod wakes;
mod zones;

pub use caught_up::{CaughtUp, NothingCaughtUp};
pub use decision::{Blocked, Decision, MoveStep, Unplaceable};
pub(crate) use metrics::{MOVE_DURATION_BUCKETS, MOVE_FENCE_BUCKETS};
pub use metrics::{SHARD_MOVE_DURATION_SECONDS, SHARD_MOVE_FENCE_SECONDS};
pub use moves::{
    DEFAULT_FENCE_MAX_LAG_RECORDS, DEFAULT_MAX_CONCURRENT_MOVES, DEFAULT_MOVE_TIMEOUT_MILLIS,
    DEFAULT_PLACEMENT_SETTLE_MILLIS, DEFAULT_RESTORE_AFTER_MILLIS, DEFAULT_RESTORE_STALL_MILLIS,
    MovePolicy,
};
pub use operator::{
    Catalog, OperatorError, OperatorStep, Refused, ZoneImpact, abandon_log, cancel_move,
    preview_operator, run_operator, start_move,
};
pub use plan::{Plan, ShardPlan, assignment_for, plan, plan_with};
pub use reconciler::{
    FAILOVERS_PER_PASS, PLACEMENT_LEASE_HELD, PLACEMENT_LEASE_INTERVALS,
    PLACEMENT_LEASE_TAKEOVERS_TOTAL, PLACEMENT_WRITES_FENCED_TOTAL, PlacementRead,
    RECONCILE_FAILURES_TOTAL, ReconcileOutcome, SHARD_ASSIGNMENT_WRITE_CONFLICTS_TOTAL,
    SHARD_MOVE_STEPS_TOTAL, SHARD_MOVES_TIMED_OUT_TOTAL, SHARD_MOVES_WAITING,
    SHARD_REPLICAS_HALTED, SHARD_REPLICAS_MISSING, SHARDS_PLACED_TOTAL, SHARDS_UNDER_REPLICATED,
    SHARDS_UNPLACEABLE, SHARDS_ZONE_UNSPREAD, reconcile_once, spawn_reconciler,
};
pub use replica_positions::ReplicaPositions;
pub use restore::{Replication, replication};
pub use wakes::PlacementWakes;

#[cfg(test)]
mod tests;

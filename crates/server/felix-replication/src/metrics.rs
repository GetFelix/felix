//! Metrics for the leader's side of replication.
//!
//! Unlabelled by shard on purpose. A label per shard is a label per stream per
//! tenant, which is unbounded by design in a multi-tenant broker; these answer
//! "is replication healthy on this node" and the shard in question is in the
//! log line that accompanies a halt.

/// Replication batches this broker shipped as a leader, by `outcome`.
pub const SHIPPED_TOTAL: &str = "felix_broker_replication_shipped_total";

/// How far the slowest follower is behind, in records, across every shard this
/// broker leads.
///
/// This is the `ConsistencyLevel::Leader` loss window. Records acknowledged to
/// a client but not yet on a follower are the ones a total loss of this broker
/// would take with it.
pub const LAG_RECORDS: &str = "felix_broker_replication_lag_records";

/// Followers replication has stopped for, because their log diverged or this
/// broker was superseded. Not a rate -- any non-zero value is worth waking
/// someone for.
pub const HALTED: &str = "felix_broker_replication_halted";

pub const OUTCOME_OK: &str = "ok";
/// The follower asked to be sent a different offset. Ordinary during catch-up.
pub const OUTCOME_RESUMED: &str = "resumed";
/// The follower refused for a reason that resolves on its own.
pub const OUTCOME_REFUSED: &str = "refused";
/// The leader could not read its own log.
pub const OUTCOME_READ_FAILED: &str = "read_failed";
pub const OUTCOME_UNREACHABLE: &str = "unreachable";
pub const OUTCOME_TIMEOUT: &str = "timeout";
pub const OUTCOME_DISCONNECTED: &str = "disconnected";
/// The two logs disagree about bytes both sides hold.
pub const OUTCOME_DIVERGED: &str = "diverged";
/// The follower knows a newer generation than this broker is shipping at.
pub const OUTCOME_FENCED: &str = "fenced";
/// The follower needs records retention has removed from the leader. It cannot
/// be caught up by shipping and needs its history transferred.
pub const OUTCOME_NEEDS_BOOTSTRAP: &str = "needs_bootstrap";
/// The follower took a log placed at this leader's oldest surviving offset and
/// replication resumed. Once per replica per shard.
pub const OUTCOME_BOOTSTRAPPED: &str = "bootstrapped";

/// The leader told a halted follower to discard its copy and start again.
pub const OUTCOME_REBUILD_STARTED: &str = "started";
/// A rebuilt follower reached the leader's tail.
pub const OUTCOME_REBUILD_COMPLETED: &str = "completed";
/// The follower would not, or could not, rebuild; the halt stands.
pub const OUTCOME_REBUILD_REFUSED: &str = "refused";

/// Bytes shipped to a move's destination under `FELIX_SHARD_MOVE_BYTES_PER_SEC`.
/// Beside `felix_broker_replication_shipped_total`, it says how much of this
/// broker's shipping is move copies being held to the limit.
pub const MOVE_THROTTLED_BYTES_TOTAL: &str = "felix_broker_replication_move_throttled_bytes_total";

pub const REBUILDS_TOTAL: &str = "felix_broker_replication_rebuilds_total";
/// Followers this broker is rebuilding right now, across every shard it leads.
pub const REBUILDING: &str = "felix_broker_replication_rebuilding";

/// Quorum marks held back because the replica report did not reach the control
/// plane.
///
/// Worth alerting on: `Quorum` publishes are timing out on those shards, and
/// nothing else in the broker looks unwell — the replication itself succeeded.
pub const MARKS_WITHHELD: &str = "felix_broker_quorum_marks_withheld_total";

/// Drained reports held back because a follower level on the shard's log was
/// still behind on a log that rides it, by `log`.
///
/// A planned move waits on this. Rising steadily means a move is stuck on
/// shipping group state or counters to its destination; the log line names
/// the shard and the follower.
pub const DRAIN_WITHHELD: &str = "felix_broker_replication_drain_withheld_total";

pub const LOG_GROUP_CURSORS: &str = "group_cursors";
pub const LOG_DEAD_LETTERS: &str = "dead_letters";
pub const LOG_COUNTERS: &str = "counters";

/// How many shards' reports shared one control-plane request.
///
/// One means the batching found nothing to batch, which is correct for a
/// broker leading a single shard and a warning sign for one leading hundreds.
pub const REPORTS_PER_REQUEST: &str = "felix_broker_replica_reports_per_request";

/// Publishes to a `Quorum` stream that did not reach a majority, by `reason`.
///
/// The `ok` case is not counted here: an acknowledged publish is already
/// counted on the publish path, and a second counter for the same event only
/// invites the two to disagree.
pub const QUORUM_FAILED_TOTAL: &str = "felix_broker_publish_quorum_failed_total";

/// No majority within the budget. The records are durable on this broker and
/// may yet reach one; this broker simply cannot say that they have.
pub const QUORUM_TIMED_OUT: &str = "timed_out";
/// Leadership moved before the batch reached a majority.
pub const QUORUM_NOT_LEADING: &str = "not_leading";

pub fn record_move_throttled(bytes: u64) {
    metrics::counter!(MOVE_THROTTLED_BYTES_TOTAL).increment(bytes);
}

pub fn record_rebuild(outcome: &'static str) {
    metrics::counter!(REBUILDS_TOTAL, "outcome" => outcome).increment(1);
}

pub fn record_rebuilding(count: usize) {
    metrics::gauge!(REBUILDING).set(count as f64);
}

pub fn record_shipped(outcome: &'static str) {
    metrics::counter!(SHIPPED_TOTAL, "outcome" => outcome).increment(1);
}

pub fn record_lag(records: u64) {
    metrics::gauge!(LAG_RECORDS).set(records as f64);
}

pub fn record_halted(count: usize) {
    metrics::gauge!(HALTED).set(count as f64);
}

pub fn record_mark_withheld() {
    metrics::counter!(MARKS_WITHHELD).increment(1);
}

pub fn record_drain_withheld(log: &'static str) {
    metrics::counter!(DRAIN_WITHHELD, "log" => log).increment(1);
}

pub fn record_quorum(reason: &'static str) {
    metrics::counter!(QUORUM_FAILED_TOTAL, "reason" => reason).increment(1);
}

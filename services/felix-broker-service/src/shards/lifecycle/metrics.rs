//! Metrics for local shard ownership.
//!
//! The question: is this broker serving what the cluster thinks it serves? A
//! shard stuck in `opening` is one the control plane believes is live; a shard
//! in `failed` is one nobody is serving.
use std::collections::HashMap;

use crate::shards::lifecycle::Phase;

/// Shards this broker holds, by phase. Bounded at six series.
pub const PHASE: &str = "felix_broker_shard_phase";
/// Local ownership transitions, by direction.
pub const TRANSITIONS_TOTAL: &str = "felix_broker_shard_transitions_total";
/// Events ignored for naming a generation at or below the one already acted on.
pub const STALE_EVENTS_TOTAL: &str = "felix_broker_shard_stale_events_total";
/// Local logs that could not be opened. Non-zero means a shard the cluster
/// believes is placed here is not being served.
pub const OPEN_FAILURES_TOTAL: &str = "felix_broker_shard_open_failures_total";

/// Seconds from this broker first seeing itself named as a shard's
/// destination to serving it: the whole move, copy included.
pub const MOVE_SECONDS: &str = "felix_broker_shard_move_seconds";
/// Seconds from this broker seeing the old leader fenced to serving the shard:
/// the switch-over, during which nobody serves it.
pub const SWITCHOVER_SECONDS: &str = "felix_broker_shard_switchover_seconds";

/// Bucket bounds for [`MOVE_SECONDS`]. A move includes the copy, so the tail
/// is long.
pub const MOVE_BUCKETS: &[f64] = &[
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
];
/// Bucket bounds for [`SWITCHOVER_SECONDS`].
pub const SWITCHOVER_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

const ALL_PHASES: [Phase; 7] = [
    Phase::Unassigned,
    Phase::Opening,
    Phase::Fencing,
    Phase::Active,
    Phase::Draining,
    Phase::Closed,
    Phase::Failed,
];

pub fn record_transition(from: Phase, to: Phase) {
    metrics::counter!(TRANSITIONS_TOTAL, "from" => from.label(), "to" => to.label()).increment(1);
}

pub fn record_stale_event() {
    metrics::counter!(STALE_EVENTS_TOTAL).increment(1);
}

/// A shard moved here is now served. `switchover` is absent when this broker
/// never saw the fence, for instance because it restarted during the move.
pub fn record_move_arrived(total: std::time::Duration, switchover: Option<std::time::Duration>) {
    metrics::histogram!(MOVE_SECONDS).record(total.as_secs_f64());
    if let Some(switchover) = switchover {
        metrics::histogram!(SWITCHOVER_SECONDS).record(switchover.as_secs_f64());
    }
}

pub fn record_open_failure() {
    metrics::counter!(OPEN_FAILURES_TOTAL).increment(1);
}

/// Publish the phase census.
///
/// Every phase is written, including the empty ones: a gauge that stops being
/// set keeps its last value, and "no shards are failed any more" is exactly the
/// fact an operator needs to see.
pub fn record_phase_counts(counts: HashMap<Phase, usize>) {
    for phase in ALL_PHASES {
        metrics::gauge!(PHASE, "phase" => phase.label())
            .set(counts.get(&phase).copied().unwrap_or(0) as f64);
    }
}

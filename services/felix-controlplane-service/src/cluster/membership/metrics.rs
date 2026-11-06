//! Prometheus metrics for cluster membership, in one place.
//!
//! Three questions come up when a cluster misbehaves, and each has an answer
//! here:
//!
//! * *Is the fleet the size I expect?* `felix_node_count` by lifecycle and
//!   region, refreshed from the same store read the node listing serves, so the
//!   dashboard and the API cannot disagree.
//! * *Did a broker just die, or was it taken out on purpose?*
//!   `felix_node_transitions_total{from,to}`. `live -> down` is a failure;
//!   `draining -> left` is a deploy.
//! * *Is this a bad broker or a bad network?* The broker-side split between
//!   rejected and unavailable, in the broker crate's `membership::metrics`.
//!
//! Every label here is bounded. Lifecycle has four values and region has as
//! many as an operator configures; neither grows with fleet size, and `node_id`
//! appears on nothing.
use std::collections::HashSet;
use std::sync::Mutex;

use crate::model::NodeLifecycle;

/// Registered brokers, by lifecycle and region.
pub const NODE_COUNT: &str = "felix_node_count";
/// Lifecycle moves, by direction. `live -> down` is the failure signal.
pub const NODE_TRANSITIONS_TOTAL: &str = "felix_node_transitions_total";
/// Registration attempts by outcome: `new`, `restart`, or `rejected`.
pub const NODE_REGISTRATIONS_TOTAL: &str = "felix_node_registrations_total";
/// Nodes moved to `down` by the expiry sweep.
pub const NODE_EXPIRY_TOTAL: &str = "felix_node_expiry_total";
/// Expiry sweeps that failed outright. Non-zero means liveness is stale.
pub const NODE_EXPIRY_FAILURES_TOTAL: &str = "felix_node_expiry_failures_total";
/// Membership changes published to the changefeed, by op.
pub const NODE_CHANGES_TOTAL: &str = "felix_node_changes_total";

pub const LIFECYCLES: [NodeLifecycle; 4] = [
    NodeLifecycle::Live,
    NodeLifecycle::Draining,
    NodeLifecycle::Down,
    NodeLifecycle::Left,
];

pub fn lifecycle_label(lifecycle: NodeLifecycle) -> &'static str {
    match lifecycle {
        NodeLifecycle::Live => "live",
        NodeLifecycle::Draining => "draining",
        NodeLifecycle::Down => "down",
        NodeLifecycle::Left => "left",
    }
}

/// Regions this process has ever reported, so one that empties can be zeroed.
///
/// A gauge keeps its last value forever otherwise: drain a region to nothing and
/// the dashboard would still show its final count, which is precisely the moment
/// an operator most needs the truth.
static SEEN_REGIONS: Mutex<Option<HashSet<String>>> = Mutex::new(None);

/// Record a lifecycle move.
///
/// Bounded at sixteen series, and callers skip no-op moves, so a repeated expiry
/// sweep does not inflate the failure signal.
pub fn record_transition(from: NodeLifecycle, to: NodeLifecycle) {
    if from == to {
        return;
    }
    metrics::counter!(
        NODE_TRANSITIONS_TOTAL,
        "from" => lifecycle_label(from),
        "to" => lifecycle_label(to),
    )
    .increment(1);
}

/// Record a registration outcome.
///
/// `new` and `restart` are separated because a broker restarting is routine
/// while a stream of `new` registrations means identities are not stable across
/// restarts, which breaks placement.
pub fn record_registration(outcome: &'static str) {
    metrics::counter!(NODE_REGISTRATIONS_TOTAL, "outcome" => outcome).increment(1);
}

/// Publish the fleet census.
///
/// Takes the whole node list rather than a delta so the gauge is a statement
/// about current state, not an accumulation that can drift from it.
pub fn publish_census(nodes: &[crate::model::Node]) {
    let mut regions: HashSet<String> = HashSet::new();
    for node in nodes {
        regions.insert(node.spec.region.clone());
    }

    let mut seen = SEEN_REGIONS.lock().unwrap_or_else(|err| err.into_inner());
    let seen = seen.get_or_insert_with(HashSet::new);
    // Regions that have emptied since the last pass still need a zero written.
    let to_report: HashSet<String> = seen.union(&regions).cloned().collect();
    seen.extend(regions);

    for region in &to_report {
        for lifecycle in LIFECYCLES {
            let count = nodes
                .iter()
                .filter(|node| &node.spec.region == region && node.status.lifecycle == lifecycle)
                .count();
            metrics::gauge!(
                NODE_COUNT,
                "lifecycle" => lifecycle_label(lifecycle),
                "region" => region.clone(),
            )
            .set(count as f64);
        }
    }
}

#[cfg(test)]
mod tests;

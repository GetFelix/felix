//! Metrics for the broker's shard-ownership watch.
//!
//! The question these answer is whether this broker's ownership view can still
//! be trusted. A watch that is failing, or resnapshotting repeatedly, is a
//! broker acting on a picture of the cluster that may already be wrong.

/// Checkpoint the watch has reached. Compare against the control plane's
/// `next_seq` to see lag directly.
pub const CHECKPOINT: &str = "felix_broker_shard_watch_checkpoint";
/// Snapshots taken. Rises on start, and on every forced resync after.
pub const SNAPSHOTS_TOTAL: &str = "felix_broker_shard_watch_snapshots_total";
/// Assignments held after the last snapshot.
pub const ASSIGNMENTS: &str = "felix_broker_shard_assignments";
/// Changes applied.
pub const APPLIED_TOTAL: &str = "felix_broker_shard_changes_applied_total";
/// Changes ignored for carrying a generation at or below the one held. Routine
/// in small numbers -- it is what makes duplicate delivery harmless.
pub const STALE_TOTAL: &str = "felix_broker_shard_changes_stale_total";
/// Forced resyncs, by `reason`: `gap_in_history` or `sequence_reset`.
pub const RESYNCS_TOTAL: &str = "felix_broker_shard_watch_resyncs_total";
/// Polls that failed outright.
pub const FAILURES_TOTAL: &str = "felix_broker_shard_watch_failures_total";
/// Confirmed assignment reads for a copy with no promise, by `outcome`:
/// `assigned`, `unassigned`, `not_served` or `unavailable`. `not_served`
/// means the control plane cannot confirm a read, and the routing view
/// decides alone.
pub const CONFIRMED_READS_TOTAL: &str = "felix_broker_shard_confirmed_reads_total";

/// Nodes in the address book this broker can forward to.
///
/// Zero while assignments are known is the shape of a cluster that cannot
/// forward: every remote shard resolves to "owner unavailable" because the id
/// has no address behind it.
pub const CATALOG_NODES: &str = "felix_broker_node_catalog_nodes";
/// Node-catalog refreshes that failed. The previous catalog is kept, so this
/// rising while `CATALOG_NODES` holds steady means routes are going stale
/// rather than disappearing.
pub const CATALOG_REFRESH_FAILURES_TOTAL: &str = "felix_broker_node_catalog_refresh_failures_total";

pub fn record_checkpoint(seq: u64) {
    metrics::gauge!(CHECKPOINT).set(seq as f64);
}

pub fn record_snapshot(assignments: usize) {
    metrics::counter!(SNAPSHOTS_TOTAL).increment(1);
    metrics::gauge!(ASSIGNMENTS).set(assignments as f64);
}

pub fn record_applied(count: usize) {
    metrics::counter!(APPLIED_TOTAL).increment(count as u64);
}

pub fn record_stale_change() {
    metrics::counter!(STALE_TOTAL).increment(1);
}

pub fn record_failure() {
    metrics::counter!(FAILURES_TOTAL).increment(1);
}

pub fn record_confirmed_read(outcome: &'static str) {
    metrics::counter!(CONFIRMED_READS_TOTAL, "outcome" => outcome).increment(1);
}

pub fn record_resync(reason: super::Resync) {
    let label = match reason {
        super::Resync::GapInHistory => "gap_in_history",
        super::Resync::SequenceReset => "sequence_reset",
    };
    metrics::counter!(RESYNCS_TOTAL, "reason" => label).increment(1);
}

pub fn set_catalog_nodes(count: usize) {
    metrics::gauge!(CATALOG_NODES).set(count as f64);
}

pub fn record_catalog_refresh_failure() {
    metrics::counter!(CATALOG_REFRESH_FAILURES_TOTAL).increment(1);
}

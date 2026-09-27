//! Lease metrics.
//!
//! `felix_broker_lease_held` is the one to alert on. A broker that is up,
//! answering `/ready`, and holding no lease is a broker serving nothing — which
//! looks identical to a healthy idle broker from every other signal.

/// 1 while this broker may serve the shards it leads, 0 otherwise.
pub const LEASE_HELD: &str = "felix_broker_lease_held";
/// Times a lease lapsed. Each one is a window in which this broker's shards were
/// unavailable here and not yet reassigned elsewhere.
pub const LEASE_EXPIRIES_TOTAL: &str = "felix_broker_lease_expiries_total";
/// Requests refused because the lease was invalid, by `boundary`: `admission`,
/// `commit` or `ack` for writes, `read` for a subscribe, watch or cache read.
/// A non-zero `commit` count means requests are outliving their lease inside
/// the broker — the queue is deeper than the margin.
pub const LEASE_REFUSALS_TOTAL: &str = "felix_broker_lease_refusals_total";

pub const BOUNDARY_ADMISSION: &str = "admission";
pub const BOUNDARY_COMMIT: &str = "commit";
/// A `Quorum` write a majority held, not acknowledged because the lease
/// lapsed while it waited.
pub const BOUNDARY_ACK: &str = "ack";
/// A read of a shard this broker leads, refused because another broker may
/// be leading it by now.
pub const BOUNDARY_READ: &str = "read";

pub fn set_held(held: bool) {
    metrics::gauge!(LEASE_HELD).set(if held { 1.0 } else { 0.0 });
}

pub fn record_expiry() {
    metrics::counter!(LEASE_EXPIRIES_TOTAL).increment(1);
}

pub fn record_refusal(boundary: &'static str) {
    metrics::counter!(LEASE_REFUSALS_TOTAL, "boundary" => boundary).increment(1);
}

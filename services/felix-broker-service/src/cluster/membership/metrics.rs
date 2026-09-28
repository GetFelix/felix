//! Broker-side membership metrics, in one place.
//!
//! The question these answer is the one a broker can answer and the control
//! plane cannot: *is this broker's view of its own membership healthy?* A
//! control plane that has marked a node down cannot say whether the node knows.
//!
//! Failures are split by kind because the two have different responses.
//! `rejected` means the control plane answered and said no — a duplicate
//! address, a superseded incarnation — and no amount of retrying fixes it.
//! `unavailable` means nothing answered, which is a network or a deployment
//! still coming up. Collapsing them into one counter makes a misconfigured
//! broker look like a flaky network.
//!
//! No metric here carries `node_id`. Each broker exports its own series, and
//! the fleet view belongs to the control plane's `felix_node_count`.

/// Heartbeats the control plane accepted.
pub const HEARTBEATS_TOTAL: &str = "felix_broker_heartbeats_total";
/// Heartbeats that did not land, by `kind`: `rejected` or `unavailable`.
pub const HEARTBEAT_FAILURES_TOTAL: &str = "felix_broker_heartbeat_failures_total";
/// Seconds since the last accepted heartbeat.
///
/// Alert on this crossing the control plane's expiry timeout: it is the earliest
/// point at which a broker knows it is about to be declared down, and it fires
/// even when the control plane is the thing that is unreachable.
pub const HEARTBEAT_AGE_SECONDS: &str = "felix_broker_heartbeat_age_seconds";
/// 1 while the control plane considers this broker placeable, 0 otherwise.
pub const MEMBERSHIP_LIVE: &str = "felix_broker_membership_live";
/// Registration attempts, by `outcome`: `registered`, `rejected`, `unavailable`.
pub const REGISTRATIONS_TOTAL: &str = "felix_broker_membership_registrations_total";

/// 1 for each fleet feature (`feature` label) this broker has enabled, 0 for
/// one it reported that no operator has finalized yet.
pub const FLEET_FEATURE_ENABLED: &str = "felix_broker_fleet_feature_enabled";

/// Node-credential refreshes, by `outcome`: `ok`, `unavailable`.
///
/// Worth an alert on: a broker whose refreshes are failing is still serving
/// and still looks healthy, right up to the moment its credential expires and
/// it leaves the cluster. The failures are the only warning.
pub const CREDENTIAL_REFRESHES_TOTAL: &str = "felix_broker_credential_refreshes_total";

/// Node credentials adopted from a rotated token file, by `outcome`:
/// `ok`, `rejected`.
pub const CREDENTIAL_ROTATIONS_TOTAL: &str = "felix_broker_credential_rotations_total";

/// Seconds until the node credential expires, or `-1` when it does not say.
///
/// The number to alert on, and the one the refresh and rotation counters cannot
/// give you: they say whether renewal is *failing*, not how long is left before
/// that matters. Crossing zero is not a degraded broker -- the heartbeat
/// carries this token and the heartbeat is the lease renewal, so an expired
/// credential is a broker that stops serving the shards it leads.
///
/// `-1` rather than absent for a token with no `exp`, so the series exists and
/// a missing series stays unambiguous: it means the broker is not reporting.
pub const CREDENTIAL_EXPIRES_IN_SECONDS: &str = "felix_broker_credential_expires_in_seconds";

/// Why a membership call did not succeed.
pub const KIND_REJECTED: &str = "rejected";
pub const KIND_UNAVAILABLE: &str = "unavailable";
pub const KIND_OK: &str = "ok";

pub fn record_heartbeat_success() {
    metrics::counter!(HEARTBEATS_TOTAL).increment(1);
    metrics::gauge!(HEARTBEAT_AGE_SECONDS).set(0.0);
}

pub fn record_heartbeat_failure(kind: &'static str) {
    metrics::counter!(HEARTBEAT_FAILURES_TOTAL, "kind" => kind).increment(1);
}

/// Report how stale this broker's own liveness is.
pub fn record_heartbeat_age(age: std::time::Duration) {
    metrics::gauge!(HEARTBEAT_AGE_SECONDS).set(age.as_secs_f64());
}

pub fn record_credential_refresh(outcome: &'static str) {
    metrics::counter!(CREDENTIAL_REFRESHES_TOTAL, "outcome" => outcome).increment(1);
}

/// Bounded by the features this build implements, each a fixed name.
pub fn record_fleet_feature(feature: &str, enabled: bool) {
    metrics::gauge!(FLEET_FEATURE_ENABLED, "feature" => feature.to_string()).set(if enabled {
        1.0
    } else {
        0.0
    });
}

pub fn record_registration(outcome: &'static str) {
    metrics::counter!(REGISTRATIONS_TOTAL, "outcome" => outcome).increment(1);
}

/// Track whether the cluster still counts this broker as placeable.
pub fn record_membership_live(live: bool) {
    metrics::gauge!(MEMBERSHIP_LIVE).set(if live { 1.0 } else { 0.0 });
}

pub fn record_credential_rotation(outcome: &'static str) {
    metrics::counter!(CREDENTIAL_ROTATIONS_TOTAL, "outcome" => outcome).increment(1);
}

/// Publish how long the credential has left. Negative means already expired.
pub fn record_credential_expiry(remaining_secs: i64) {
    metrics::gauge!(CREDENTIAL_EXPIRES_IN_SECONDS).set(remaining_secs as f64);
}

/// Publish that the credential does not say when it expires.
pub fn record_credential_expiry_unknown() {
    metrics::gauge!(CREDENTIAL_EXPIRES_IN_SECONDS).set(-1.0);
}

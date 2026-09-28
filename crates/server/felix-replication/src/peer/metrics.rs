//! Metrics for the broker-internal transport.
//!
//! These answer one question a broker can answer about itself and the control
//! plane cannot: *can this broker reach the peers it needs to forward to?* A
//! healthy cluster in the catalog says nothing about whether any given broker's
//! connections to it are up.
//!
//! Outcomes are split by kind rather than collapsed into a failure count,
//! because they call for different responses: a `handshake` failure is
//! configuration or a version mismatch, `unreachable` is the network, and
//! `timeout` is a peer that accepted the request and did not answer.

/// Live peer connections this broker holds, as a gauge.
pub const CONNECTIONS: &str = "felix_broker_peer_connections";
/// Connection attempts, by `outcome`: `connected`, `unreachable`, `handshake`.
pub const CONNECT_ATTEMPTS_TOTAL: &str = "felix_broker_peer_connect_attempts_total";
/// Connections that dropped after being established, by `reason`.
pub const CONNECTION_LOSSES_TOTAL: &str = "felix_broker_peer_connection_losses_total";
/// Reconnect attempts made after a loss, i.e. redials that were not the first.
pub const RECONNECTS_TOTAL: &str = "felix_broker_peer_reconnects_total";
/// Multiplexed request streams open across all peers, as a gauge.
pub const STREAMS: &str = "felix_broker_peer_streams";
/// Requests sent to peers, by `outcome`.
pub const REQUESTS_TOTAL: &str = "felix_broker_peer_requests_total";
/// Requests refused before being sent because the peer was already at its
/// in-flight limit. Distinct from a failure: nothing was attempted, and the
/// caller can retry elsewhere or shed.
pub const REQUESTS_SHED_TOTAL: &str = "felix_broker_peer_requests_shed_total";
/// Round-trip latency of a forwarded request, in seconds.
pub const REQUEST_SECONDS: &str = "felix_broker_peer_request_seconds";
/// Requests this broker served for a peer, by `outcome`.
pub const SERVED_TOTAL: &str = "felix_broker_peer_served_total";
/// Inbound peer connections refused, by `reason`: `alpn`, `handshake`.
pub const INBOUND_REJECTED_TOTAL: &str = "felix_broker_peer_inbound_rejected_total";

pub const OUTCOME_CONNECTED: &str = "connected";
pub const OUTCOME_UNREACHABLE: &str = "unreachable";
pub const OUTCOME_HANDSHAKE: &str = "handshake";
pub const OUTCOME_OK: &str = "ok";
pub const OUTCOME_ERROR: &str = "error";
/// A frame kind this build does not know, refused without dropping the stream.
/// Expected during a rolling upgrade and nowhere else — a steady non-zero rate
/// means a peer is sending something this broker will never serve.
pub const OUTCOME_UNSUPPORTED: &str = "unsupported_kind";
pub const OUTCOME_TIMEOUT: &str = "timeout";
pub const OUTCOME_DISCONNECTED: &str = "disconnected";
/// This broker was asked for a shard it does not own, and said so.
pub const OUTCOME_NOT_LEADER: &str = "not_leader";
/// This broker refused before applying anything.
pub const OUTCOME_REFUSED: &str = "refused";
/// The request carried no credential, or one that does not allow the write.
pub const OUTCOME_UNAUTHORIZED: &str = "unauthorized";

/// Publishes this broker forwarded to an owner, by `outcome`.
pub const FORWARDS_TOTAL: &str = "felix_broker_forwards_total";
/// Forward attempts that were not the first: a redirect followed, or a
/// retryable refusal retried. Rising steadily means the routing view is
/// churning, not that anything is broken.
pub const FORWARD_RETRIES_TOTAL: &str = "felix_broker_forward_retries_total";

/// The attempt budget ran out while the owner kept refusing or moving.
pub const OUTCOME_EXHAUSTED: &str = "exhausted";
/// The batch was sent and its answer never arrived. Alert on this: it is the
/// only outcome where the broker cannot say whether the write landed.
pub const OUTCOME_INDETERMINATE: &str = "indeterminate";

/// Replication batches this broker stored as a follower, by `outcome`.
///
/// The failure outcomes are separated because they need different responses.
/// `gap` is ordinary during catch-up and self-repairing. `conflict` and
/// `fenced` are not: the first means two logs have diverged, the second that a
/// superseded leader is still shipping. Either one standing is worth waking
/// someone for.
pub const REPLICATED_TOTAL: &str = "felix_broker_replicated_total";

/// The sender named an epoch older than this broker's, so it is no longer the
/// leader.
pub const OUTCOME_FENCED: &str = "fenced";
/// This broker's routing view has not caught up with the epoch the sender
/// named. Transient by nature.
pub const OUTCOME_BEHIND: &str = "behind";
/// The batch starts past this broker's tail. The leader resumes from the offset
/// in the answer.
pub const OUTCOME_GAP: &str = "gap";
/// The batch disagrees with bytes already stored.
pub const OUTCOME_CONFLICT: &str = "conflict";
/// A divergent suffix from a previous generation was dropped and replication
/// resumed. Not an error — but worth watching, because a steady rate means
/// leadership is changing more often than it should.
pub const OUTCOME_TRUNCATED: &str = "truncated";
/// The batch did not survive the trip.
pub const OUTCOME_CORRUPT: &str = "corrupt";
/// This broker took a promoted leader's fence.
pub const OUTCOME_FENCE_TAKEN: &str = "fence_taken";
/// A fence at the generation this replica had already accepted: the leader
/// confirming, for a read, that nobody newer has reached it.
pub const OUTCOME_FENCE_CONFIRMED: &str = "fence_confirmed";
/// This broker placed a shard log to begin where the leader's surviving log
/// begins. Rare and deliberate: it happens once per replica per shard.
pub const OUTCOME_BOOTSTRAPPED: &str = "bootstrapped";
/// This broker discarded its copy of a shard at the leader's request.
pub const OUTCOME_REBUILT: &str = "rebuilt";
/// A truncation or rebuild was refused because it would have discarded
/// records this broker knows are committed. It means a leader holds a log
/// that disagrees with an acknowledged record, so it always needs a person.
pub const OUTCOME_BELOW_COMMIT: &str = "below_commit";

pub fn record_connect_attempt(outcome: &'static str) {
    metrics::counter!(CONNECT_ATTEMPTS_TOTAL, "outcome" => outcome).increment(1);
}

pub fn record_reconnect() {
    metrics::counter!(RECONNECTS_TOTAL).increment(1);
}

pub fn record_connection_loss(reason: &'static str) {
    metrics::counter!(CONNECTION_LOSSES_TOTAL, "reason" => reason).increment(1);
}

pub fn set_connections(count: usize) {
    metrics::gauge!(CONNECTIONS).set(count as f64);
}

pub fn set_streams(count: usize) {
    metrics::gauge!(STREAMS).set(count as f64);
}

pub fn record_request(outcome: &'static str, elapsed: std::time::Duration) {
    metrics::counter!(REQUESTS_TOTAL, "outcome" => outcome).increment(1);
    metrics::histogram!(REQUEST_SECONDS, "outcome" => outcome).record(elapsed.as_secs_f64());
}

pub fn record_request_shed() {
    metrics::counter!(REQUESTS_SHED_TOTAL).increment(1);
}

pub fn record_served(outcome: &'static str) {
    metrics::counter!(SERVED_TOTAL, "outcome" => outcome).increment(1);
}

pub fn record_inbound_rejected(reason: &'static str) {
    metrics::counter!(INBOUND_REJECTED_TOTAL, "reason" => reason).increment(1);
}

pub fn record_forward(outcome: &'static str) {
    metrics::counter!(FORWARDS_TOTAL, "outcome" => outcome).increment(1);
}

pub fn record_forward_retry() {
    metrics::counter!(FORWARD_RETRIES_TOTAL).increment(1);
}

pub fn record_replicated(outcome: &'static str) {
    metrics::counter!(REPLICATED_TOTAL, "outcome" => outcome).increment(1);
}

//! The 0.8 baseline: the flag and feature bits every 0.8 peer must offer.
//!
//! 0.8 starts fresh, with no upgrade from 0.7, so the bits that existed when
//! it was cut are no longer optional. A broker refuses an `Auth` that does not
//! offer all of [`CLIENT_BASELINE_FLAGS`], [`CLIENT_BASELINE_FEATURES`] and
//! [`CLIENT_BASELINE_FEATURES_HI`], and a client refuses an `AuthOk` that does
//! not advertise the broker baseline. Neither side keeps a fallback for a peer
//! without one of these bits.
//!
//! The values are frozen literals rather than derived from `KNOWN_*`: a bit
//! added after 0.8 is negotiated as before, and must never join the baseline,
//! or a 0.8 peer would be refused by a newer one.

use crate::client::features::*;
use crate::client::flags::*;
use crate::client::message::Message;

/// Every flag bit defined at 0.8 apart from the two a client asks for only
/// when its application wants them (`FLAG_EVENT_BATCH_PUBLISHER` and
/// `FLAG_EVENT_BATCH_TIMESTAMPS`).
pub const CLIENT_BASELINE_FLAGS: u16 = FLAG_BINARY_PUBLISH_BATCH
    | FLAG_BINARY_EVENT_BATCH
    | FLAG_BINARY_EVENT_BATCH_SHARED
    | FLAG_BINARY_PUBLISH_ACKED
    | FLAG_BINARY_PUBLISH_ACK
    | FLAG_EVENT_BATCH_OFFSETS
    | FLAG_BINARY_PUBLISH_KEYED
    | FLAG_BINARY_PUBLISH_ACK_OWNER
    | FLAG_BINARY_PUBLISH_IDEMPOTENT
    | FLAG_BINARY_PUBLISH_ACK_CODE
    | FLAG_BINARY_PUBLISH_ACK_DETAIL
    | FLAG_EVENT_BATCH_SKIPPED
    | FLAG_BINARY_PUBLISH_ACK_OFFSET;

/// Every first-word feature bit defined at 0.8 apart from the three a client
/// asks for only when its application wants them (`FEATURE_ACK_ON_COMMIT`,
/// `FEATURE_GROUP_PUBLISHER` and `FEATURE_RECORD_TIMESTAMPS`).
pub const CLIENT_BASELINE_FEATURES: u32 = FEATURE_TOPOLOGY
    | FEATURE_REDIRECT
    | FEATURE_CACHE_DELETE
    | FEATURE_CONSUMER_GROUP
    | FEATURE_GROUP_DEAD_LETTERS
    | FEATURE_STREAM_SHARDS
    | FEATURE_CACHE_WATCH
    | FEATURE_CACHE_WATCH_RETAINED
    | FEATURE_COUNTERS
    | FEATURE_IDEMPOTENT_PRODUCER
    | FEATURE_CACHE_SHARDS
    | FEATURE_ERROR_CODES
    | FEATURE_SHARD_MOVED
    | FEATURE_UNSUPPORTED
    | FEATURE_SEQUENCE_REUSED
    | FEATURE_PUBLISH_PIPELINE
    | FEATURE_ATOMIC_COMMIT
    | FEATURE_STREAM_PUBLISH_WINDOW
    | FEATURE_SHARD_OWNERS
    | FEATURE_GROUP_CONSUMER
    | FEATURE_SUBSCRIPTION_LAGGED
    | FEATURE_GROUP_SKIPPED
    | FEATURE_GROUP_ADMIN
    | FEATURE_CACHE_CONDITIONAL
    | FEATURE_GROUP_CLAIM_CONTROL
    | FEATURE_PUBLISH_CONDITIONAL
    | FEATURE_STREAM_READ
    | FEATURE_SUBSCRIBE_QUEUE;

/// Every extended feature bit defined at 0.8.
pub const CLIENT_BASELINE_FEATURES_HI: u32 = FEATURE_INSPECT
    | FEATURE_STREAM_DELETED
    | FEATURE_PUBLISH_WIDTH
    | FEATURE_EXPLICIT_SHARD
    | FEATURE_HEAD_TRIM
    | FEATURE_LEASE_TOKEN
    | FEATURE_PUBLISH_ANSWER_ON_COMPLETION
    | FEATURE_SUBSCRIBE_END
    | FEATURE_ACK_DURABILITY;

/// The flag bits every 0.8 broker advertises: every one defined at 0.8.
pub const BROKER_BASELINE_FLAGS: u16 =
    CLIENT_BASELINE_FLAGS | FLAG_EVENT_BATCH_PUBLISHER | FLAG_EVENT_BATCH_TIMESTAMPS;

/// The first-word feature bits every 0.8 broker advertises, whatever it is
/// configured with. Cluster, durable-storage, log-backed-cache and
/// publish-window features are left out: a broker without the thing they
/// describe does not offer them, and a client still checks each one.
pub const BROKER_BASELINE_FEATURES: u32 = FEATURE_CACHE_DELETE
    | FEATURE_STREAM_SHARDS
    | FEATURE_IDEMPOTENT_PRODUCER
    | FEATURE_CACHE_SHARDS
    | FEATURE_ERROR_CODES
    | FEATURE_SHARD_MOVED
    | FEATURE_UNSUPPORTED
    | FEATURE_SEQUENCE_REUSED
    | FEATURE_ATOMIC_COMMIT
    | FEATURE_SHARD_OWNERS
    | FEATURE_ACK_ON_COMMIT
    | FEATURE_SUBSCRIPTION_LAGGED
    | FEATURE_CACHE_CONDITIONAL
    | FEATURE_RECORD_TIMESTAMPS
    | FEATURE_PUBLISH_CONDITIONAL
    | FEATURE_STREAM_READ
    | FEATURE_SUBSCRIBE_QUEUE;

/// The extended feature bits every 0.8 broker advertises: all but
/// `FEATURE_LEASE_TOKEN`, which needs a lease key, and
/// `FEATURE_PUBLISH_ANSWER_ON_COMPLETION`, which needs a publish window.
pub const BROKER_BASELINE_FEATURES_HI: u32 = FEATURE_INSPECT
    | FEATURE_STREAM_DELETED
    | FEATURE_PUBLISH_WIDTH
    | FEATURE_EXPLICIT_SHARD
    | FEATURE_HEAD_TRIM
    | FEATURE_SUBSCRIBE_END
    | FEATURE_ACK_DURABILITY;

/// The baseline bits an offer (or advertisement) lacks, as
/// `(flags, features, features_hi)`. All zero means it meets the baseline.
///
/// Absent fields count as zero, so an `Auth` from a client that predates
/// negotiation lacks every bit. The extended word counts only behind
/// [`FEATURE_EXTENDED`], as it does everywhere else.
pub(crate) fn missing_baseline(
    (flags, features, features_hi): (Option<u16>, Option<u32>, Option<u32>),
    (want_flags, want_features, want_features_hi): (u16, u32, u32),
) -> (u16, u32, u32) {
    let flags = flags.unwrap_or(0);
    let features = features.unwrap_or(0);
    let features_hi = match supports_feature(features, FEATURE_EXTENDED) {
        true => features_hi.unwrap_or(0),
        false => 0,
    };
    (
        want_flags & !flags,
        want_features & !features,
        want_features_hi & !features_hi,
    )
}

/// What a client lacks of [`CLIENT_BASELINE_FLAGS`] and the feature
/// baselines, given the fields of its `Auth`.
pub fn client_missing_baseline(
    flags: Option<u16>,
    features: Option<u32>,
    features_hi: Option<u32>,
) -> (u16, u32, u32) {
    missing_baseline(
        (flags, features, features_hi),
        (
            CLIENT_BASELINE_FLAGS,
            CLIENT_BASELINE_FEATURES,
            CLIENT_BASELINE_FEATURES_HI,
        ),
    )
}

/// What a broker lacks of the broker baselines, given the fields of its
/// `AuthOk`.
pub fn broker_missing_baseline(
    flags: u16,
    features: Option<u32>,
    features_hi: Option<u32>,
) -> (u16, u32, u32) {
    missing_baseline(
        (Some(flags), features, features_hi),
        (
            BROKER_BASELINE_FLAGS,
            BROKER_BASELINE_FEATURES,
            BROKER_BASELINE_FEATURES_HI,
        ),
    )
}

/// An `Auth` offering exactly the client baseline, for peers and tests that
/// build the handshake by hand.
pub fn baseline_auth(tenant_id: impl Into<String>, token: impl Into<String>) -> Message {
    let (client_features, client_features_hi) =
        offer_features(CLIENT_BASELINE_FEATURES, CLIENT_BASELINE_FEATURES_HI);
    Message::Auth {
        tenant_id: tenant_id.into(),
        token: token.into(),
        client_flags: Some(CLIENT_BASELINE_FLAGS),
        client_features: Some(client_features),
        client_features_hi,
        client_flags_ext: None,
    }
}

/// An `AuthOk` advertising exactly the broker baseline, for stand-in brokers.
pub fn baseline_auth_ok() -> Message {
    let (server_features, server_features_hi) =
        offer_features(BROKER_BASELINE_FEATURES, BROKER_BASELINE_FEATURES_HI);
    Message::AuthOk {
        server_flags: BROKER_BASELINE_FLAGS,
        server_features: Some(server_features),
        server_features_hi,
        server_flags_ext: None,
        listener_ports: None,
        publish_window: None,
    }
}

#[cfg(test)]
mod tests;

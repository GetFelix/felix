//! Wire format for framing Felix protocol messages.
//!
//! Defines the frame header layout, message enums, and JSON/binary encoders
//! used by broker and client transports to communicate over QUIC.
//!
//! # Design notes
//! The format balances readability (JSON control frames) with throughput
//! (binary batch frames) while enforcing size limits for safety.
//!
//! # Module layout
//! - The client protocol: [`Frame`] and its header flags, the feature bits,
//!   the [`Message`] enum and its JSON codec, the hand-rolled [`text`] writer
//!   for the publish-batch hot path, and the [`binary`] batch codec. All but
//!   `text` and `binary` are re-exported at the crate root.
//! - [`internal`]: the protocol brokers speak to each other.
//! - [`routing`]: which shard a routing key belongs to.

mod client;
mod error;

pub mod internal;
pub mod routing;

pub use client::error_code::{ErrorCode, ErrorDetail, RetryClass, shard_unavailable_reason};
pub use client::features::{
    FEATURE_ACK_ON_COMMIT, FEATURE_ATOMIC_COMMIT, FEATURE_CACHE_CONDITIONAL, FEATURE_CACHE_DELETE,
    FEATURE_CACHE_SHARDS, FEATURE_CACHE_WATCH, FEATURE_CACHE_WATCH_RETAINED,
    FEATURE_CONSUMER_GROUP, FEATURE_COUNTERS, FEATURE_ERROR_CODES, FEATURE_EXTENDED,
    FEATURE_GROUP_ADMIN, FEATURE_GROUP_CLAIM_CONTROL, FEATURE_GROUP_CONSUMER,
    FEATURE_GROUP_DEAD_LETTERS, FEATURE_GROUP_PUBLISHER, FEATURE_GROUP_SKIPPED,
    FEATURE_IDEMPOTENT_PRODUCER, FEATURE_INSPECT, FEATURE_PUBLISH_CONDITIONAL,
    FEATURE_PUBLISH_PIPELINE, FEATURE_RECORD_TIMESTAMPS, FEATURE_REDIRECT, FEATURE_SEQUENCE_REUSED,
    FEATURE_SHARD_MOVED, FEATURE_SHARD_OWNERS, FEATURE_STREAM_PUBLISH_WINDOW, FEATURE_STREAM_READ,
    FEATURE_STREAM_SHARDS, FEATURE_SUBSCRIBE_QUEUE, FEATURE_SUBSCRIPTION_LAGGED, FEATURE_TOPOLOGY,
    FEATURE_UNSUPPORTED, KNOWN_FEATURES, KNOWN_FEATURES_HI, answer_features, offer_features,
    peer_features_hi, supports_feature,
};
pub use client::flags::{
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_BINARY_PUBLISH_ACK,
    FLAG_BINARY_PUBLISH_ACK_CODE, FLAG_BINARY_PUBLISH_ACK_DETAIL, FLAG_BINARY_PUBLISH_ACK_OFFSET,
    FLAG_BINARY_PUBLISH_ACK_OWNER, FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH,
    FLAG_BINARY_PUBLISH_IDEMPOTENT, FLAG_BINARY_PUBLISH_KEYED, FLAG_EVENT_BATCH_OFFSETS,
    FLAG_EVENT_BATCH_PUBLISHER, FLAG_EVENT_BATCH_SKIPPED, FLAG_EVENT_BATCH_TIMESTAMPS, KNOWN_FLAGS,
    ORIGINAL_V1_FLAGS, has_unknown_flags, supports,
};
pub use client::frame::{CLIENT_ALPN, Frame, FrameHeader, MAGIC, VERSION};
pub use client::message::{
    AckMode, BrokerEndpoint, CacheCondition, CursorErrorReason, GroupRecord, InspectedAssignment,
    InspectedFence, InspectedLease, InspectedReplica, InspectedSubscription, Message,
    PublishRefusalReason, ShardInspection, ShardKind, ShardOwner, StartPosition, StateChange,
    StreamRecord, SubscriptionCursor, SubscriptionFilter, UnknownRequest,
};
pub use client::{binary, text};
pub use error::{Error, Result};

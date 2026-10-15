//! The protocol brokers speak to each other.
//!
//! Deliberately separate from the client protocol, and not a superset of it. A
//! client able to send `ForwardPublish` could write to a shard on a broker that
//! never checked ownership; a broker accepting client frames on its internal
//! listener would treat a peer as an authenticated publisher. The distinct magic
//! below is what makes a misdirected connection fail loudly rather than parse
//! into something plausible — it is not the security boundary, which is the
//! separate listener and peer credential.
//!
//! Flows, correlation rules, and the error table live in
//! `docs/internal-protocol.md`.

mod codec;
mod error_code;
mod fence;
mod forward;
mod handshake;
mod header;
mod message;
mod replicate;

pub use error_code::ErrorCode;
pub use fence::{Fence, FenceOk, PeerCapabilities, Ping, Pong, ReplicateFetch};
pub use forward::{
    AckMode, CacheOpKind, ForwardCacheCondition, ForwardCacheError, ForwardCacheOk, ForwardCacheOp,
    ForwardCacheOutcome, ForwardPublish, ForwardPublishError, ForwardPublishOk, NotLeader,
};
pub use handshake::{Hello, HelloOk};
pub use header::{
    FrameEnvelope, INTERNAL_MAGIC, INTERNAL_VERSION, InternalHeader, Kind, MAX_BODY_BYTES,
    correlation_id_in,
};
pub use message::{InternalMessage, ShardRef};
pub use replicate::{
    GenerationStart, ProducerMark, ReplicaLog, ReplicateBootstrap, ReplicateError, ReplicateOk,
    ReplicateRebuild, ReplicateRecords, batch_checksum,
};

/// Longest identifier (tenant, namespace, stream, node id) accepted.
pub const MAX_IDENT_BYTES: usize = 512;
/// Longest credential a forwarded request may carry. A Felix token is a JWT
/// whose size is its permission list; this leaves room for a long one without
/// letting a peer spend the body limit on it.
pub const MAX_CREDENTIAL_BYTES: usize = 16 * 1024;

/// Most payloads one forwarded batch may carry.
pub const MAX_BATCH_PAYLOADS: usize = 65_536;

#[cfg(test)]
mod tests;

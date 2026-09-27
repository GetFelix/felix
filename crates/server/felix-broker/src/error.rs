//! The broker's error type.

/// Why a broker operation failed.
#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("topic capacity too large")]
    CapacityTooLarge,
    #[error("cursor too old (oldest {oldest}, requested {requested})")]
    CursorTooOld { oldest: u64, requested: u64 },
    /// A resume asked to start past the end of the stream.
    ///
    /// Distinct from `CursorTooOld` because the remedy is the opposite: the
    /// client is ahead of the log, not behind it, and silently starting at the
    /// tail would deliver records it explicitly skipped past.
    #[error("cursor is past the tail (tail {tail}, requested {requested})")]
    CursorInFuture { requested: u64, tail: u64 },
    #[error("stream not found: tenant={tenant_id} namespace={namespace} stream={stream}")]
    StreamNotFound {
        tenant_id: String,
        namespace: String,
        stream: String,
    },
    #[error("stream handle {0} is no longer active")]
    StreamHandleInactive(u64),
    #[error(
        "changing stream durability requires removal and recreation: tenant={tenant_id} namespace={namespace} stream={stream} current={current} requested={requested}"
    )]
    DurabilityChangeRequiresRecreate {
        tenant_id: String,
        namespace: String,
        stream: String,
        current: bool,
        requested: bool,
    },
    #[error("tenant not found: {0}")]
    TenantNotFound(String),
    #[error("namespace not found: tenant={tenant_id} namespace={namespace}")]
    NamespaceNotFound {
        tenant_id: String,
        namespace: String,
    },
    /// Historical replay was requested for a stream that keeps no history on
    /// disk. Its records live only in the bounded in-memory replay ring, which
    /// is reachable through `subscribe_with_cursor`.
    #[error("stream {tenant_id}/{namespace}/{stream} is not durable and has no persisted history")]
    StreamNotDurable {
        tenant_id: String,
        namespace: String,
        stream: String,
    },
    /// The stream asks for durability but this broker has none configured.
    ///
    /// Separate from [`BrokerError::Storage`] because the two want opposite
    /// handling: this is a static misconfiguration that will not resolve on
    /// retry and affects only the stream naming it, while a storage failure
    /// means the disk is in an unknown state and must never be shrugged off.
    #[error(
        "stream {tenant_id}/{namespace}/{stream} is marked durable but this broker has no durable storage configured"
    )]
    DurableStorageNotConfigured {
        tenant_id: String,
        namespace: String,
        stream: String,
    },
    /// Durable storage refused or failed a write, or a log failed to recover.
    /// A publish that hits this is never acknowledged: the record is not on
    /// disk, so claiming otherwise would be the one failure mode durability
    /// exists to prevent.
    #[error("durable storage error: {0}")]
    Storage(String),
    /// An idempotent batch skipped ahead of the sequence this broker expected.
    /// What was skipped is not here, so continuing past it would leave a hole
    /// the producer believes is filled.
    #[error("sequence gap: expected {expected}")]
    SequenceGap { expected: u64 },
    /// An idempotent batch from a producer this broker holds no sequence for,
    /// and not its first. Nothing can be checked against.
    #[error("unknown producer {producer_id}: nothing to check its sequence against")]
    UnknownProducer { producer_id: u64 },
    /// An idempotent batch re-sent from further back than this broker
    /// remembers, so whether it was appended cannot be told.
    #[error("sequence {sequence} is older than the window this broker keeps")]
    SequenceExpired { sequence: u64 },
    /// A consumer settled a group offset this broker never handed out. `next`
    /// is the lowest offset not yet handed out; nothing at or above it can be
    /// acknowledged or handed back.
    #[error("offset {offset} was not handed out by this group (next is {next})")]
    GroupOffsetNotHandedOut { offset: u64, next: u64 },
    /// The shard's log was reset (this broker became a follower, or its log
    /// was rebuilt) while the publish waited for its turn. Nothing reached
    /// the ring or a subscriber, but the records were written to the old log,
    /// so whether they survive is unknown.
    #[error("the shard's log was reset before the publish at offset {first_offset} completed")]
    PublishSuperseded { first_offset: u64 },
    /// This broker cannot say what is committed on the shard, so it serves no
    /// read that depends on it. Retry: here once it can, or on the shard's
    /// owner.
    #[error("reads of {stream} shard {shard} are unavailable here: {reason}")]
    NotReadable {
        stream: String,
        shard: u32,
        reason: NotReadable,
    },
}

/// Why [`BrokerError::NotReadable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NotReadable {
    /// The shard was just taken and has no committed mark yet.
    #[error("no committed mark yet for this leadership")]
    Settling,
    /// The lease lapsed or the shard is led elsewhere.
    #[error("this broker is not serving the shard")]
    Refused,
}

/// Shorthand for results carrying a [`BrokerError`].
pub type Result<T> = std::result::Result<T, BrokerError>;

#[cfg(test)]
mod tests;

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
    /// The disk or quota under durable storage is full. Nothing was written,
    /// so the same request can succeed once space is freed.
    #[error("durable storage is full: {0}")]
    StorageFull(String),
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
    /// An idempotent batch under a sequence this broker already holds a
    /// different batch for. It is not a re-send, so it was not written, and
    /// answering it as a duplicate would report records as written that are
    /// not. Only returned to a caller that asked for [`SequenceReuse::Refuse`].
    ///
    /// [`SequenceReuse::Refuse`]: crate::SequenceReuse::Refuse
    #[error("sequence {sequence} already holds a batch with different payloads")]
    SequenceReused { sequence: u64 },
    /// A consumer settled a group offset this broker never handed out. `next`
    /// is the lowest offset not yet handed out; nothing at or above it can be
    /// acknowledged or handed back.
    #[error("offset {offset} was not handed out by this group (next is {next})")]
    GroupOffsetNotHandedOut { offset: u64, next: u64 },
    /// A consumer asked to extend a group claim that no longer stands: it
    /// lapsed, the record was handed out again, or the group's state was
    /// rebuilt since. The record is owed to the group, not to this consumer.
    #[error("the claim on offset {offset} no longer stands")]
    GroupClaimLapsed { offset: u64 },
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
    /// A commit record's payload does not decode.
    #[error("malformed commit record: {0}")]
    MalformedCommit(String),
    /// Commits and their state live in the shard's log, so a stream without
    /// one has nowhere to put them.
    #[error("an atomic commit needs a durable stream")]
    CommitNeedsDurableStream,
    /// The shard's state view could not be rebuilt because commits kept
    /// landing while it read. Retry.
    #[error("the shard's state is being rebuilt; retry")]
    StateViewBusy,
    /// The shard's state cannot be read here yet, for the reason given.
    #[error("the shard's state is not readable here: {0}")]
    StateNotReadable(NotReadable),
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

impl From<felix_storage::StorageError> for BrokerError {
    fn from(err: felix_storage::StorageError) -> Self {
        match err {
            felix_storage::StorageError::Full(_) => BrokerError::StorageFull(err.to_string()),
            other => BrokerError::Storage(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests;

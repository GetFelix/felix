//! Head trims: hiding the oldest records of a durable stream shard.

/// What a conditional head trim did.
///
/// A stale expected offset is an answer, as it is for a conditional publish.
/// Every other refusal is an error: a [`crate::BrokerError`] whose code says
/// which (`feature_not_enabled`, `stream_has_commits`, `invalid_request`,
/// `forbidden`), or [`TrimError::Unsupported`] when the broker was never
/// asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionalTrim {
    /// The trim is durable (on a `Quorum` stream, on a majority). Reads of
    /// the shard below `oldest` are refused.
    Trimmed {
        /// Where the trim record landed, so the shard's next offset is one
        /// past it. `None` when the floor already stood at or past `before`
        /// and nothing was written.
        offset: Option<u64>,
        /// The oldest offset reads of the shard start at now.
        oldest: u64,
    },
    /// Not trimmed, because the shard's next offset is `tail`. Nothing was
    /// written. After a trim that timed out, this can mean the trim landed.
    Refused { tail: u64 },
}

/// Why a trim was refused before it was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TrimError {
    /// The broker did not advertise `FEATURE_HEAD_TRIM`, so it has no
    /// `trim_before`.
    #[error("this broker does not support head trims")]
    Unsupported,
}

//! The errors every storage operation reports.
//!
//! [`StorageError`] is what the crate returns. [`Corruption`] is the part of it
//! that describes bytes on disk that did not decode, and is kept separate
//! because decoders produce it without knowing which file they were reading.

mod corruption;

pub use corruption::{Corruption, CorruptionKind, CorruptionSite};

use std::fmt;

/// Why a storage operation failed.
#[derive(Debug)]
pub enum StorageError {
    Unsupported(&'static str),
    /// The log was asked to open with settings that cannot work. Raised at open
    /// time, never on the append path.
    InvalidConfig(&'static str),
    InvalidRange,
    /// The requested offset was discarded by retention or truncation. Distinct
    /// from an empty range, which is a valid answer for a reader that has caught
    /// up with the tail.
    Trimmed {
        requested: u64,
        oldest: u64,
    },
    NotFound,
    /// On-disk bytes did not decode. Carries the specific invariant that was
    /// violated plus the shard/segment/position it was found at, because
    /// "corruption detected" is not enough to act on at 3am.
    Corruption(Corruption),
    /// A durable append could not be acknowledged. Distinct from `Io` so callers
    /// can tell "the write never happened" from "the write may have happened but
    /// we could not confirm it".
    SyncFailed(String),
    /// A truncation or rebuild would discard records below the offset this
    /// log knows is committed. Refused rather than performed: those records
    /// were acknowledged on a majority, and this copy may be the last one.
    BelowCommit {
        offset: u64,
        commit: u64,
    },
    /// A restore asked for a point this copy of the log cannot reach: its
    /// records end before `offset`, or retention or compaction already
    /// dropped the records below it. Either way the copy does not hold the
    /// log as it stood at `offset`, and padding or emptying it would pass off
    /// an incomplete backup as a good one.
    OutsideLog {
        offset: u64,
        base: u64,
        tail: u64,
    },
    /// The log was closed, because the shard it holds moved away. Names the
    /// shard. Reopening it through its provider gives a working log.
    Closed(String),
    /// A shard directory holds a different shard than the one being opened:
    /// two keys whose directory names collide. Refused rather than mixing
    /// two shards' records in one log.
    ShardMismatch {
        dir: String,
        expected: String,
        found: String,
    },
    Io(std::io::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Unsupported(feature) => write!(f, "unsupported: {feature}"),
            StorageError::InvalidConfig(detail) => write!(f, "invalid configuration: {detail}"),
            StorageError::InvalidRange => write!(f, "invalid range"),
            StorageError::Trimmed { requested, oldest } => write!(
                f,
                "offset {requested} is no longer available; the log starts at {oldest}"
            ),
            StorageError::NotFound => write!(f, "not found"),
            StorageError::Corruption(detail) => write!(f, "corruption detected: {detail}"),
            StorageError::SyncFailed(detail) => write!(f, "durability sync failed: {detail}"),
            StorageError::BelowCommit { offset, commit } => write!(
                f,
                "refusing to discard records from offset {offset}: everything below \
                 {commit} is committed"
            ),
            StorageError::OutsideLog { offset, base, tail } => write!(
                f,
                "offset {offset} is outside this log, which holds [{base}, {tail})"
            ),
            StorageError::Closed(shard) => write!(f, "the log for {shard} is closed"),
            StorageError::ShardMismatch {
                dir,
                expected,
                found,
            } => write!(
                f,
                "shard directory {dir} belongs to {found}, not {expected}; refusing to open it"
            ),
            StorageError::Io(err) => write!(f, "io error: {err}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<Corruption> for StorageError {
    fn from(err: Corruption) -> Self {
        StorageError::Corruption(err)
    }
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::Io(err)
    }
}

/// A result whose error is a [`StorageError`].
pub type Result<T> = std::result::Result<T, StorageError>;

#[cfg(test)]
mod tests;

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
    /// The requested offset was discarded by retention or truncation, or is
    /// below the log's head floor. Distinct from an empty range, which is a
    /// valid answer for a reader that has caught up with the tail.
    Trimmed {
        requested: u64,
        /// Where reads may start: the higher of the oldest segment's base and
        /// the head floor.
        oldest: u64,
        /// The headers of the trim record that set the head floor, when the
        /// log has one.
        trim_headers: Option<bytes::Bytes>,
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
    /// An earlier flush on this log failed, so the log refuses every append,
    /// commit, flush and state write from then on. The kernel may have
    /// dropped the pages that flush could not write, and a retry would
    /// report success for them. Names the shard and the first failure.
    /// Lasts until the log is reopened. A reopen is only known to read back
    /// just what reached the disk after a reboot: see "A failed fsync
    /// poisons the log" in `docs/durable-storage.md`.
    Poisoned {
        shard: String,
        reason: String,
    },
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
    /// A record would be stamped at `generation`, below `floor`, the
    /// generation of the record before it. A log's generations never fall,
    /// so the record's writer and this log disagree about what precedes it:
    /// refused, never raised to the floor, which would claim a generation
    /// that did not write the record.
    GenerationBelowFloor {
        offset: u64,
        generation: u64,
        floor: u64,
    },
    /// A v9 segment needs the generation that wrote each record, and this
    /// append cannot name it honestly: a copied batch whose sender did not
    /// say, or a leader whose log accepted a generation other than the one it
    /// leads at.
    WriterUnknown(&'static str),
    /// The device or the quota is out of space. Nothing was written, so the
    /// same request can succeed once space is freed.
    Full(std::io::Error),
    Io(std::io::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Unsupported(feature) => write!(f, "unsupported: {feature}"),
            StorageError::InvalidConfig(detail) => write!(f, "invalid configuration: {detail}"),
            StorageError::InvalidRange => write!(f, "invalid range"),
            StorageError::Trimmed {
                requested, oldest, ..
            } => write!(
                f,
                "offset {requested} is no longer available; the log starts at {oldest}"
            ),
            StorageError::NotFound => write!(f, "not found"),
            StorageError::Corruption(detail) => write!(f, "corruption detected: {detail}"),
            StorageError::SyncFailed(detail) => write!(f, "durability sync failed: {detail}"),
            StorageError::Poisoned { shard, reason } => write!(
                f,
                "{shard}: {reason}; the log is poisoned and refuses writes until it is reopened"
            ),
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
            StorageError::GenerationBelowFloor {
                offset,
                generation,
                floor,
            } => write!(
                f,
                "the record at {offset} was written at generation {generation}, below {floor}, the generation of the record before it"
            ),
            StorageError::WriterUnknown(detail) => {
                write!(
                    f,
                    "cannot name the generation that wrote the record: {detail}"
                )
            }
            StorageError::Full(err) => write!(f, "storage full: {err}"),
            StorageError::Io(err) => write!(f, "io error: {err}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Full(err) | StorageError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<Corruption> for StorageError {
    fn from(err: Corruption) -> Self {
        StorageError::Corruption(err)
    }
}

impl StorageError {
    /// Whether this is a flush that failed, however it was wrapped. The log
    /// it happened on must stop: see [`StorageError::Poisoned`].
    pub fn is_sync_failure(&self) -> bool {
        match self {
            StorageError::SyncFailed(_) => true,
            StorageError::Io(err) | StorageError::Full(err) => crate::io::SyncFailure::is(err),
            _ => false,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        // Before the full-disk case: a flush that failed for want of space
        // may have dropped pages like any other.
        if crate::io::SyncFailure::is(&err) {
            return StorageError::SyncFailed(err.to_string());
        }
        if matches!(
            err.kind(),
            std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
        ) {
            metrics::counter!(crate::metrics_names::STORAGE_FULL_TOTAL).increment(1);
            return StorageError::Full(err);
        }
        StorageError::Io(err)
    }
}

/// A result whose error is a [`StorageError`].
pub type Result<T> = std::result::Result<T, StorageError>;

#[cfg(test)]
mod tests;

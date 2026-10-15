//! What conditional cache writes answer, and a value with its version.

use bytes::Bytes;

/// The answer to [`crate::Client::cache_put_if`] or
/// [`crate::Client::cache_delete_if`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheConditionResult {
    /// The condition held and the write was made.
    pub applied: bool,
    /// After an applied put, the version it wrote. Otherwise the key's
    /// current version, `None` when it has no live entry.
    pub version: Option<u64>,
}

/// A cache value and the version a conditional write compares against.
///
/// A key's version changes on every write to it and is never reused, so
/// reading the same version twice means nothing was written in between.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub value: Bytes,
    pub version: u64,
}

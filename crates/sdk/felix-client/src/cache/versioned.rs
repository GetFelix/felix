//! What conditional cache writes answer, and a value with its version.

use bytes::Bytes;

/// The answer to [`crate::Connection::cache_put_if`] or
/// [`crate::Connection::cache_delete_if`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheConditionResult {
    /// The condition held and the write was made.
    pub applied: bool,
    /// After an applied put, the version it wrote. Otherwise the key's
    /// current version, `None` when it has no live entry.
    pub version: Option<u64>,
}

/// The answer to [`crate::Connection::cache_put_if_lease`]: the conditional
/// write's result and, when it applied, a lease token for its writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseWrite {
    pub result: CacheConditionResult,
    /// Present when the write applied and the broker minted a token.
    pub token: Option<LeaseToken>,
    /// Why an applied write came back without a token. The write stands.
    pub token_refused: Option<String>,
}

/// A token for whoever holds a lease, granting what the caller's lease
/// templates name for the key. Present it with
/// [`crate::Connection::with_identity_token`]. A bearer secret: `Debug`
/// leaves it out.
#[derive(Clone, PartialEq, Eq)]
pub struct LeaseToken {
    pub token: String,
    /// When the token stops working, in seconds since the Unix epoch.
    pub expires_at: u64,
}

impl std::fmt::Debug for LeaseToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseToken")
            .field("token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
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

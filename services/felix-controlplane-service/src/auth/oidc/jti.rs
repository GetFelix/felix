//! The record of upstream `jti` values already accepted, for issuers that ask
//! for single-use tokens.
//!
//! Held in memory by each control plane replica. A replay sent to another
//! replica, or to this one before it started, finds no record, so single use
//! holds per replica. The start time closes the restart case: a token issued
//! before this process started is refused, since its `jti` may have been
//! used before the record was lost. Spanning replicas would need the record
//! replicated through the metadata log, one write per exchange.
use std::collections::HashMap;
use std::sync::Mutex;

use chrono::Utc;

use super::OidcError;

/// The most `jti` values held at once. Each lives until its token's `exp`
/// (lifetime-bounded issuers keep it to minutes), so this is the exchange
/// rate times the lifetime. Past it new tokens are refused, not old
/// records dropped, which would let a replay through.
pub(super) const MAX_TRACKED_JTIS: usize = 100_000;

#[derive(Debug)]
pub(super) struct JtiStore {
    started_at: i64,
    cap: usize,
    seen: Mutex<HashMap<(String, String), i64>>,
}

impl JtiStore {
    pub(super) fn new() -> Self {
        Self::with_cap(MAX_TRACKED_JTIS)
    }

    pub(super) fn with_cap(cap: usize) -> Self {
        Self {
            started_at: Utc::now().timestamp(),
            cap,
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// Record `jti` until `until` (unix seconds), or refuse it.
    ///
    /// # Errors
    /// [`OidcError::TokenReplayed`] when it is held or the token was issued
    /// before this process started, [`OidcError::ReplayStoreFull`] when the
    /// record is full of unexpired entries.
    pub(super) fn claim(
        &self,
        issuer: &str,
        jti: &str,
        issued_at: i64,
        until: i64,
    ) -> Result<(), OidcError> {
        if issued_at < self.started_at {
            return Err(OidcError::TokenReplayed);
        }
        let now = Utc::now().timestamp();
        let mut seen = self.seen.lock().unwrap_or_else(|err| err.into_inner());
        if seen.len() >= self.cap {
            seen.retain(|_, until| *until > now);
        }
        let key = (issuer.to_string(), jti.to_string());
        if seen.get(&key).is_some_and(|until| *until > now) {
            return Err(OidcError::TokenReplayed);
        }
        if seen.len() >= self.cap && !seen.contains_key(&key) {
            return Err(OidcError::ReplayStoreFull);
        }
        seen.insert(key, until);
        Ok(())
    }
}

//! The record of upstream `jti` values already accepted, for issuers that ask
//! for single-use tokens.
//!
//! Held in memory by each control plane replica. A replay sent to another
//! replica, or to this one before it started, finds no record, so single use
//! holds per replica. The start time closes the restart case: a token issued
//! before this process started is refused, since its `jti` may have been
//! used before the record was lost.
//!
//! Each issuer has its own record and cap, so one issuer filling its own
//! cannot make another tenant's exchanges fail. An issuer's tokens live no
//! longer than its lifetime bound (required with single use), which keeps
//! the record small. Spanning replicas would need the record
//! replicated through the metadata log, one write per exchange.
use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use chrono::Utc;

use super::OidcError;

/// The most `jti` values held at once for one issuer. Each lives until its
/// token's `exp`, so this is the issuer's exchange rate times its lifetime
/// bound. Past it the issuer's new tokens are refused, not old records
/// dropped, which would let a replay through.
pub(super) const MAX_TRACKED_JTIS_PER_ISSUER: usize = 10_000;

/// The longest token lifetime accepted from an issuer that asks for single
/// use, in seconds.
pub(super) const MAX_SINGLE_USE_LIFETIME: u64 = 600;

#[derive(Debug, Default)]
struct IssuerJtis {
    until: HashMap<String, i64>,
    /// The same entries ordered by expiry, so expired ones are dropped from
    /// the front without scanning.
    by_expiry: BTreeSet<(i64, String)>,
}

#[derive(Debug)]
pub(super) struct JtiStore {
    started_at: i64,
    cap: usize,
    seen: Mutex<HashMap<String, IssuerJtis>>,
}

impl JtiStore {
    pub(super) fn new() -> Self {
        Self::with_cap(MAX_TRACKED_JTIS_PER_ISSUER)
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
    /// issuer's record is full of unexpired entries.
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
        let record = seen.entry(issuer.to_string()).or_default();
        while let Some((expires, key)) = record.by_expiry.first().cloned() {
            if expires > now {
                break;
            }
            record.by_expiry.remove(&(expires, key.clone()));
            record.until.remove(&key);
        }
        if record.until.contains_key(jti) {
            return Err(OidcError::TokenReplayed);
        }
        if record.until.len() >= self.cap {
            return Err(OidcError::ReplayStoreFull);
        }
        record.until.insert(jti.to_string(), until);
        record.by_expiry.insert((until, jti.to_string()));
        Ok(())
    }
}

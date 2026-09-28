//! The broker's own credential, and keeping it current.
//!
//! A broker reads its node token once and then uses it for every control-plane
//! call it will ever make — heartbeat, node catalog, shard-assignment feed,
//! replication reports. When that token expires those calls start answering
//! `401` and the broker falls out of the cluster without anything having gone
//! wrong with the broker. The real-network suite watched exactly that: a 900s
//! token, and brokers deregistering fifteen minutes in.
//!
//! The answer is not a longer token — a long-lived bearer secret is what the
//! short default exists to avoid. It is refreshing before expiry, which means
//! the credential has to be a thing that can *change* while requests are using
//! it. Hence [`NodeCredential`]: one holder every caller reads through, swapped
//! atomically.
pub mod refresh;
pub mod rotate;

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use serde::Deserialize;

/// The bearer token every control-plane call reads through.
///
/// Cloning shares the holder rather than copying the token, which is the whole
/// point: a refresh has to be visible to callers that were handed this before
/// it happened. A caller holds the value only for the length of one request,
/// so a swap is never torn — the request in flight finishes with the token it
/// started with, and the next one picks up the new one.
#[derive(Clone)]
pub struct NodeCredential {
    token: Arc<ArcSwap<String>>,
}

impl std::fmt::Debug for NodeCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the token itself. This ends up in tracing output and in any
        // `{:?}` of a config that carries one.
        f.write_str("NodeCredential(<redacted>)")
    }
}

impl NodeCredential {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: Arc::new(ArcSwap::from_pointee(token.into())),
        }
    }

    /// The token to present right now.
    pub fn bearer(&self) -> Arc<String> {
        self.token.load_full()
    }

    /// Install a freshly minted token. Every holder sees it on its next call.
    pub fn replace(&self, token: String) {
        self.token.store(Arc::new(token));
    }
}

impl felix_replication::reporter::Credential for NodeCredential {
    fn bearer(&self) -> Arc<String> {
        NodeCredential::bearer(self)
    }
}

/// What a Felix access token says about itself.
///
/// Read from the payload **without verifying the signature**, and that is
/// sound because nothing here is a trust decision: the claims choose a URL to
/// call and a moment to wake up. A forged `tid` sends the refresh somewhere it
/// will be refused; a forged `exp` makes the broker refresh at the wrong time.
/// Neither grants anything. The control plane verifies for real, which is the
/// only place it matters.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AccessClaims {
    /// The tenant the token was minted for — which tenant's refresh endpoint
    /// to call.
    pub tid: String,
    /// Expiry, Unix seconds.
    pub exp: i64,
}

/// Read `tid` and `exp` out of a JWT payload.
///
/// Deliberately not a verification: see [`AccessClaims`].
pub fn read_claims(token: &str) -> Option<AccessClaims> {
    use base64::Engine;
    let mut parts = token.split('.');
    let (_header, payload) = (parts.next()?, parts.next()?);
    // A JWT has exactly three parts; anything else is not one, and guessing
    // which piece is the payload would be worse than declining.
    parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice(&decoded).ok()
}

/// When to refresh a token that expires at `exp`, given the time now.
///
/// Two thirds of the way through what is left, not at the deadline. A refresh
/// scheduled for the moment of expiry has no room for the request itself, let
/// alone a retry, so the first transient failure is an outage. Two thirds
/// leaves a third of the lifetime to retry in, which at the 900s default is
/// five minutes of failures before anything is at risk.
///
/// Clamped below so a token already past two thirds — one read at startup, or
/// one whose refresh failed for a while — is retried soon rather than
/// immediately and in a spin.
pub fn refresh_delay(now_secs: i64, exp_secs: i64) -> Duration {
    const FLOOR: Duration = Duration::from_secs(5);
    let remaining = exp_secs.saturating_sub(now_secs);
    if remaining <= 0 {
        return FLOOR;
    }
    Duration::from_secs((remaining as u64) * 2 / 3).max(FLOOR)
}

/// Seconds since the epoch, or 0 if the clock is before it.
///
/// Shared by the refresh loop and the rotation watcher: both compare a token's
/// `exp` against now, and two readings of the clock that could disagree would
/// be worse than one that is occasionally coarse.
pub(crate) fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0)
}

/// Publish how long `credential` has left, for the metric an operator alerts on.
///
/// Called at startup so the series exists before the refresh loop's first pass,
/// which on a long-lived token is hours away — a gauge that only appears once
/// something has already happened is not one you can alert on.
pub fn report_expiry(credential: &NodeCredential) {
    match read_claims(&credential.bearer()) {
        Some(claims) => {
            crate::cluster::membership::metrics::record_credential_expiry(claims.exp - now_secs())
        }
        None => crate::cluster::membership::metrics::record_credential_expiry_unknown(),
    }
}

#[cfg(test)]
mod tests;

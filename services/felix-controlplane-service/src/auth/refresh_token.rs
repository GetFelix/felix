//! Refresh tokens: what one is, and what the store must do with it.
//!
//! The access token stays short-lived because a leaked bearer token is only as
//! dangerous as the time it stays valid. That is the right trade for a caller
//! that can re-exchange freely, and the wrong one for anything long-running —
//! a broker holding a 900s node token falls out of the cluster in fifteen
//! minutes. A refresh token is how something stays authenticated without
//! either standing IdP credentials or a long-TTL bearer secret.
//!
//! Three properties make that safe, and all three live here rather than in the
//! endpoint, because a store that implements them differently is a store that
//! silently weakens them:
//!
//! - **The secret is never stored.** Only its hash, so a database read does not
//!   yield usable credentials.
//! - **Every refresh is single-use.** Using one mints its replacement and
//!   spends it, which bounds how long a stolen token is worth anything.
//! - **A replay revokes the family.** Single-use makes theft *detectable*:
//!   nobody legitimately presents a spent token, so seeing one means two
//!   parties hold the chain and neither should keep it.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A refresh token as the store holds it.
///
/// Note what is absent: the secret. The record carries its hash, and the only
/// place the secret exists in plaintext is the response that hands it to the
/// caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshToken {
    /// Public half, carried in the wire token and used to find this record.
    pub token_id: String,
    pub tenant_id: String,
    /// Who the refreshed access token will be minted for.
    pub principal_id: String,
    /// The IdP group claims this principal presented at exchange, scoped by
    /// issuer (`{issuer}#{group}`).
    ///
    /// Kept because RBAC is re-evaluated on every refresh and group-derived
    /// grants cannot be recomputed without them. They are claims, not
    /// permissions: a refresh re-runs the policy against them rather than
    /// reusing whatever the last token was granted, so a revoked role takes
    /// effect at the next refresh instead of at the next re-exchange.
    pub groups: Vec<String>,
    /// SHA-256 of the secret half, hex encoded.
    pub secret_hash: String,
    /// The rotation chain this token belongs to. A replay revokes all of it.
    pub family_id: String,
    pub issued_at_secs: i64,
    pub expires_at_secs: i64,
    /// Spent by a refresh. A spent token presented again is a replay.
    pub used: bool,
    pub revoked: bool,
    /// The narrowing the exchange applied, re-applied on every refresh so a
    /// refresh cannot widen what the exchange asked for. `None` on records
    /// written before it was recorded, which refresh to full RBAC rights.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub narrowing: Option<Narrowing>,
}

/// What an exchange asked for, kept with its refresh chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Narrowing {
    /// Actions the token was limited to, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested: Option<Vec<String>>,
    /// Resource hints the token was limited to, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<String>>,
    /// The audience the exchange minted for. A refresh keeps it.
    pub audience: String,
}

impl RefreshToken {
    pub fn is_live(&self, now_secs: i64) -> bool {
        !self.used && !self.revoked && self.expires_at_secs > now_secs
    }
}

/// What happened when a refresh token was presented.
///
/// `Replayed` is deliberately distinct from `Unusable`. Collapsing them would
/// lose the one signal single-use rotation exists to produce: a spent token
/// coming back is not a client mistake, it is evidence that two parties hold
/// the chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefreshTokenTake {
    /// Live, and now spent. The caller may mint against it exactly once.
    Taken(Box<RefreshToken>),
    /// Already spent. The family it belongs to must be revoked.
    Replayed(Box<RefreshToken>),
    /// Unknown, revoked, or expired — nothing to act on beyond refusing.
    Unusable,
}

/// The secret half's hash, as the store holds it.
pub fn hash_secret(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    hex::encode(digest)
}

/// Split a presented token into its public and secret halves.
///
/// The wire form is `<token_id>.<secret>`, opaque to the client. Anything else
/// is not a refresh token, which is a refusal rather than an error: an
/// unparseable token must take the same path as a wrong one, or the difference
/// between them is observable.
pub fn split(presented: &str) -> Option<(&str, &str)> {
    let (token_id, secret) = presented.split_once('.')?;
    (!token_id.is_empty() && !secret.is_empty()).then_some((token_id, secret))
}

/// Join the two halves into the form a client presents.
pub fn join(token_id: &str, secret: &str) -> String {
    format!("{token_id}.{secret}")
}

#[cfg(test)]
mod tests;

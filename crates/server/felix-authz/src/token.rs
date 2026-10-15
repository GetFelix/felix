//! Felix token minting and verification, shared across the broker and the
//! control plane.
//!
//! EdDSA (Ed25519) only — RSA and HS algorithms are rejected rather than
//! supported, because the caller supplies the token and accepting a second
//! algorithm means accepting whichever is weakest. `iss`, `aud`, `tid` and the
//! signature are all checked before a token is trusted.
//!
//! Private keys never leave this module serialized; PKCS8 DER is built in
//! memory only to hand to `jsonwebtoken`. `kid` drives rotation and cache
//! lookup and is not a secret.
//!
//! [`FelixTokenIssuer`] mints; [`FelixTokenVerifier`] verifies; both need a
//! [`TenantKeyStore`] for key access.
//!
//! ```rust
//! use felix_authz::{FelixTokenIssuer, TenantId, TenantKeyMaterial, Jwks};
//! use jsonwebtoken::Algorithm;
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! let mut store = HashMap::new();
//! store.insert("t1".to_string(), TenantKeyMaterial {
//!     kid: "k1".to_string(),
//!     alg: Algorithm::EdDSA,
//!     private_key: [1u8; 32],
//!     public_key: [2u8; 32],
//!     jwks: Jwks { keys: vec![] },
//! });
//! let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", Duration::from_secs(60), Arc::new(store));
//! let _ = issuer.mint(&TenantId::new("t1"), "principal", vec![]);
//! ```

mod issuer;
mod key_cache;
mod keys;
mod verifier;

pub use issuer::FelixTokenIssuer;
pub use key_cache::TenantKeyCache;
pub use keys::{TenantKeyMaterial, TenantKeyStore, TenantSigningKey, TenantVerificationKey};
pub use verifier::FelixTokenVerifier;

use serde::{Deserialize, Serialize};

/// Claims carried by Felix-issued JWTs. `tid` and `perms` are what the broker
/// enforces with, so both are checked, never trusted as-is.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FelixClaims {
    pub iss: String,
    pub aud: String,
    pub sub: String,
    pub tid: String,
    pub exp: i64,
    pub iat: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jti: Option<String>,
    pub perms: Vec<String>,
    /// The party acting for `sub` (RFC 8693 `act`), on a token the control
    /// plane minted by delegation. `sub` is still who the token authorizes;
    /// `act` is who may present it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub act: Option<Actor>,
}

/// An RFC 8693 actor claim: the principal a delegated token was issued to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    pub sub: String,
}

#[cfg(test)]
mod tests;

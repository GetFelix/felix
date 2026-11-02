//! Minimal JWKS types for publishing Ed25519 public keys.
//!
//! Only public key material belongs here. Felix signs with OKP/Ed25519
//! (`alg = EdDSA`) and nothing else. Note `x` is base64url, not plain base64.

use serde::{Deserialize, Serialize};

/// The key set served to verifiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwks {
    pub keys: Vec<Jwk>,
}

/// One Ed25519 public key in JWK form: `kty = "OKP"`, `alg = "EdDSA"`,
/// `crv = "Ed25519"`, with the base64url public key in `x`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwk {
    pub kty: String,
    pub kid: String,
    pub alg: String,
    #[serde(rename = "use")]
    pub use_field: KeyUse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crv: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x: Option<String>,
}

/// Intended use for a JWK. Felix only signs, so `Sig` is the only variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyUse {
    Sig,
}

#[cfg(test)]
mod tests;

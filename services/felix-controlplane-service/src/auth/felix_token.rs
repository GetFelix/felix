//! Minting and verification of Felix JWTs — tenant-scoped access tokens the
//! broker and control-plane APIs accept.
//!
//! Felix tokens are **always EdDSA (Ed25519)**, never RSA or HS variants, and
//! verification rejects anything else outright. That is a deliberate narrowing:
//! an attacker supplies the token, so accepting a second algorithm means
//! accepting whichever is weakest. `iss`, `aud` and `tid` are mandatory and
//! checked before a token is trusted.
//!
//! A signing key is a 32-byte Ed25519 seed with its derived public key and a
//! `kid` for rotation. `kid` is not a secret; the seed is, and it must not
//! leave the control-plane trust boundary or reach a log. Public keys are
//! published via JWKS elsewhere.
//!
//! Verification tries the tenant's current key and then its previous ones, so a
//! rotation does not invalidate tokens already in flight. The key cache is
//! behind an `RwLock`: many concurrent reads, writes only on rotation or first
//! use.
//!
//! ```rust
//! use felix_controlplane_service::auth::felix_token::{mint_token, SigningKey, TenantSigningKeys};
//! use ed25519_dalek::SigningKey as Ed25519SigningKey;
//! use jsonwebtoken::Algorithm;
//! use std::time::Duration;
//!
//! let seed = [1u8; 32];
//! let ed_key = Ed25519SigningKey::from_bytes(&seed);
//! let keys = TenantSigningKeys {
//!     current: SigningKey {
//!         kid: "k1".to_string(),
//!         alg: Algorithm::EdDSA,
//!         private_key: seed,
//!         public_key: ed_key.verifying_key().to_bytes(),
//!     },
//!     previous: vec![],
//! };
//! let _ = mint_token(&keys, "tenant-a", "principal", vec![], Duration::from_secs(60));
//! ```
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

const ED25519_KEY_LEN: usize = 32;

/// `iss` of every Felix token.
pub const ISSUER: &str = "felix-auth";
/// `aud` of a token for brokers: what a client presents on connect.
pub const BROKER_AUDIENCE: &str = "felix-broker";
/// `aud` of a token for the control plane's API.
///
/// Separate from [`BROKER_AUDIENCE`] because a broker sees every client
/// token it is shown, and must not be able to replay one against the admin
/// API.
pub const CONTROLPLANE_AUDIENCE: &str = "felix-controlplane";

/// Claims carried by Felix-issued JWTs. `tid` and `perms` are what the broker
/// authorizes with.
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
    /// Who may present this token for `sub` (RFC 8693 `act`). Set only on a
    /// token minted by delegation (`/token/delegate`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub act: Option<Actor>,
    /// Who may have this token delegated to them (RFC 8693 `may_act`). Set
    /// when the exchange that started the token's chain named an actor;
    /// `/token/delegate` refuses a caller it does not name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub may_act: Option<Actor>,
}

/// An RFC 8693 actor claim: the principal a delegated token was issued to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    pub sub: String,
}

/// A tenant signing key: raw 32-byte Ed25519 seed, its derived public key,
/// and the `kid` used for rotation. Never serialize or log `private_key`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SigningKey {
    pub kid: String,
    pub alg: Algorithm,
    pub private_key: [u8; ED25519_KEY_LEN],
    pub public_key: [u8; ED25519_KEY_LEN],
}

impl SigningKey {
    /// Check that the key is EdDSA and that the stored public key matches the
    /// seed — the mismatch guards against corrupted storage or botched
    /// rotation data.
    ///
    /// # Errors
    /// `TokenError::Key` on either failure.
    pub fn validate(&self) -> Result<(), TokenError> {
        if self.alg != Algorithm::EdDSA {
            return Err(TokenError::Key(format!(
                "invalid Felix signing algorithm: {:?}",
                self.alg
            )));
        }
        let signing_key = Ed25519SigningKey::from_bytes(&self.private_key);
        let expected = signing_key.verifying_key().to_bytes();
        if expected != self.public_key {
            return Err(TokenError::Key(
                "Ed25519 public key does not match private seed".to_string(),
            ));
        }
        Ok(())
    }
}

/// A tenant's current signing key plus previous keys that still verify
/// in-flight tokens after rotation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TenantSigningKeys {
    pub current: SigningKey,
    pub previous: Vec<SigningKey>,
}

impl TenantSigningKeys {
    /// Validate every key in the set.
    ///
    /// # Errors
    /// `TokenError::Key` if any key is non-EdDSA or its public key doesn't
    /// match its seed.
    pub fn validate(&self) -> Result<(), TokenError> {
        self.current.validate()?;
        for key in &self.previous {
            key.validate()?;
        }
        Ok(())
    }

    /// All keys, current first — the order verification tries them in.
    pub fn all_keys(&self) -> impl Iterator<Item = &SigningKey> {
        std::iter::once(&self.current).chain(self.previous.iter())
    }
}

/// Errors from token minting or verification. Messages never include private
/// key material.
#[derive(Debug)]
pub enum TokenError {
    Jwt(jsonwebtoken::errors::Error),
    Key(String),
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenError::Jwt(err) => write!(f, "jwt error: {err}"),
            TokenError::Key(err) => write!(f, "key error: {err}"),
        }
    }
}

impl std::error::Error for TokenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TokenError::Jwt(err) => Some(err),
            TokenError::Key(_) => None,
        }
    }
}

impl From<jsonwebtoken::errors::Error> for TokenError {
    fn from(value: jsonwebtoken::errors::Error) -> Self {
        TokenError::Jwt(value)
    }
}

/// [`mint_token_for`] with the broker audience.
///
/// # Errors
/// As [`mint_token_for`].
pub fn mint_token(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    principal_id: &str,
    perms: Vec<String>,
    ttl: Duration,
) -> Result<String, TokenError> {
    mint_token_for(keys, tenant_id, principal_id, perms, ttl, BROKER_AUDIENCE)
}

/// Mint a Felix EdDSA token for `tenant_id` / `principal_id`, valid for `ttl`,
/// for `audience` ([`BROKER_AUDIENCE`] or [`CONTROLPLANE_AUDIENCE`]).
///
/// The algorithm is pinned to EdDSA and the claims always carry `iss`, `aud`
/// and `tid`. Uses a cached encoding key so repeated mints do not redo the
/// PKCS8 conversion. Never log the returned token.
///
/// # Errors
/// `TokenError::Key` if key validation fails, `TokenError::Jwt` if encoding does.
pub fn mint_token_for(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    principal_id: &str,
    perms: Vec<String>,
    ttl: Duration,
    audience: &str,
) -> Result<String, TokenError> {
    mint_token_may_act(keys, tenant_id, principal_id, perms, ttl, audience, None)
}

/// [`mint_token_for`], naming `may_act` as the one principal that may have
/// the token delegated to it.
///
/// # Errors
/// As [`mint_token_for`].
pub fn mint_token_may_act(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    principal_id: &str,
    perms: Vec<String>,
    ttl: Duration,
    audience: &str,
    may_act: Option<&str>,
) -> Result<String, TokenError> {
    let now = now_epoch_seconds();
    let claims = FelixClaims {
        iss: ISSUER.to_string(),
        aud: audience.to_string(),
        sub: principal_id.to_string(),
        tid: tenant_id.to_string(),
        exp: now + ttl.as_secs() as i64,
        iat: now,
        jti: None,
        perms,
        act: None,
        may_act: may_act.map(|sub| Actor {
            sub: sub.to_string(),
        }),
    };
    sign(keys, tenant_id, &claims)
}

/// Mint a broker token for `subject` that names `actor` as who may present
/// it, expiring at `exp` (seconds since the epoch). The caller decides the
/// permissions and expiry; see `auth::delegate`.
///
/// # Errors
/// As [`mint_token_for`].
pub fn mint_delegated_token(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    subject: &str,
    actor: &str,
    perms: Vec<String>,
    exp: i64,
) -> Result<String, TokenError> {
    let claims = FelixClaims {
        iss: ISSUER.to_string(),
        aud: BROKER_AUDIENCE.to_string(),
        sub: subject.to_string(),
        tid: tenant_id.to_string(),
        exp,
        iat: now_epoch_seconds(),
        jti: None,
        perms,
        act: Some(Actor {
            sub: actor.to_string(),
        }),
        may_act: None,
    };
    sign(keys, tenant_id, &claims)
}

fn sign(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    claims: &FelixClaims,
) -> Result<String, TokenError> {
    keys.validate()?;
    let mut header = Header::new(keys.current.alg);
    header.kid = Some(keys.current.kid.clone());
    let encoding_key = key_cache().encoding_key(tenant_id, &keys.current)?;
    Ok(jsonwebtoken::encode(&header, claims, &encoding_key)?)
}

/// [`verify_token_for`] accepting only the broker audience.
///
/// # Errors
/// As [`verify_token_for`].
pub fn verify_token(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    token: &str,
    leeway: u64,
) -> Result<FelixClaims, TokenError> {
    verify_token_for(keys, tenant_id, token, leeway, &[BROKER_AUDIENCE])
}

/// Verify a Felix token against a tenant's signing keys, accepting any of
/// `audiences`.
///
/// Keys are tried in `kid`-preferred order so the common case is one signature
/// check even mid-rotation. After the signature passes, `tid` must still name
/// the expected tenant — the signature only proves who signed, not which
/// tenant the token is for.
///
/// # Errors
/// `TokenError::Jwt` for validation/decoding failures (including a `tid`
/// mismatch), `TokenError::Key` if key validation fails.
pub fn verify_token_for(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    token: &str,
    leeway: u64,
    audiences: &[&str],
) -> Result<FelixClaims, TokenError> {
    keys.validate()?;
    let header = jsonwebtoken::decode_header(token)?;
    let mut ordered_keys = Vec::new();
    if let Some(kid) = header.kid.as_deref() {
        if let Some(found) = keys.all_keys().find(|entry| entry.kid == kid) {
            ordered_keys.push(found);
            for entry in keys.all_keys() {
                if entry.kid != kid {
                    ordered_keys.push(entry);
                }
            }
        } else {
            ordered_keys.extend(keys.all_keys());
        }
    } else {
        ordered_keys.extend(keys.all_keys());
    }

    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.set_audience(audiences);
    validation.set_issuer(&[ISSUER]);
    validation.leeway = leeway;
    let mut last_err = None;
    for key in ordered_keys {
        let decoding_key = key_cache().decoding_key(tenant_id, key)?;
        match jsonwebtoken::decode::<FelixClaims>(token, &decoding_key, &validation) {
            Ok(token) => {
                if token.claims.tid != tenant_id {
                    return Err(TokenError::Jwt(jsonwebtoken::errors::Error::from(
                        jsonwebtoken::errors::ErrorKind::InvalidToken,
                    )));
                }
                return Ok(token.claims);
            }
            Err(err) => last_err = Some(err),
        }
    }
    // Return the last JWT error rather than a generic one; it carries the
    // actual reason verification failed.
    Err(TokenError::Jwt(last_err.unwrap_or_else(|| {
        jsonwebtoken::errors::Error::from(jsonwebtoken::errors::ErrorKind::InvalidToken)
    })))
}

fn now_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}

#[derive(Clone, Default)]
struct FelixKeyCache {
    encoding: Arc<RwLock<HashMap<String, EncodingKey>>>,
    decoding: Arc<RwLock<HashMap<String, DecodingKey>>>,
}

impl FelixKeyCache {
    fn invalidate_tenant(&self, tenant_id: &str) {
        // Entries are keyed "tenant:kid", so a prefix match catches them all.
        let mut prefix = String::with_capacity(tenant_id.len() + 1);
        prefix.push_str(tenant_id);
        prefix.push(':');
        if let Ok(mut map) = self.encoding.write() {
            map.retain(|key, _| !key.starts_with(&prefix));
        }
        if let Ok(mut map) = self.decoding.write() {
            map.retain(|key, _| !key.starts_with(&prefix));
        }
    }

    fn encoding_key(&self, tenant_id: &str, key: &SigningKey) -> Result<EncodingKey, TokenError> {
        let cache_key = cache_key(tenant_id, &key.kid);
        if let Ok(map) = self.encoding.read()
            && let Some(found) = map.get(&cache_key)
        {
            return Ok(found.clone());
        }
        // jsonwebtoken wants PKCS8 DER for EdDSA; the database stores raw
        // seeds, so the conversion happens here, in memory only.
        let signing_key = Ed25519SigningKey::from_bytes(&key.private_key);
        let der = signing_key
            .to_pkcs8_der()
            .map_err(|err| TokenError::Key(format!("encode Ed25519 key: {err}")))?;
        let encoding_key = EncodingKey::from_ed_der(der.as_bytes());
        if let Ok(mut map) = self.encoding.write() {
            map.insert(cache_key, encoding_key.clone());
        }
        Ok(encoding_key)
    }

    fn decoding_key(&self, tenant_id: &str, key: &SigningKey) -> Result<DecodingKey, TokenError> {
        let cache_key = cache_key(tenant_id, &key.kid);
        if let Ok(map) = self.decoding.read()
            && let Some(found) = map.get(&cache_key)
        {
            return Ok(found.clone());
        }
        let x = URL_SAFE_NO_PAD.encode(key.public_key);
        let decoding_key = DecodingKey::from_ed_components(&x).map_err(TokenError::Jwt)?;
        if let Ok(mut map) = self.decoding.write() {
            map.insert(cache_key, decoding_key.clone());
        }
        Ok(decoding_key)
    }
}

static KEY_CACHE: OnceLock<FelixKeyCache> = OnceLock::new();

fn key_cache() -> &'static FelixKeyCache {
    KEY_CACHE.get_or_init(FelixKeyCache::default)
}

/// Drop cached key material for a tenant. Call after key rotation, or the
/// caches keep minting and verifying with the old keys.
pub fn invalidate_tenant_cache(tenant_id: &str) {
    key_cache().invalidate_tenant(tenant_id);
}

fn cache_key(tenant_id: &str, kid: &str) -> String {
    let mut key = String::with_capacity(tenant_id.len() + 1 + kid.len());
    key.push_str(tenant_id);
    key.push(':');
    key.push_str(kid);
    key
}

#[cfg(test)]
mod tests;

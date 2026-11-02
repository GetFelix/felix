//! Lease tokens: a token a broker mints for whoever wins a conditional write
//! to a lease cache key, granting what the caller's lease templates name for
//! that key.
//!
//! Winning the write is the holder check, so no control-plane call sits on the
//! claim path. Every broker holds the same key file, since the owner of the
//! key mints and any broker may verify. `docs/auth.md` ("Lease tokens")
//! describes the whole flow.
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use ed25519_dalek::pkcs8::EncodePrivateKey;
use felix_authz::{
    Action, AuthzError, AuthzResult, CacheScope, FelixClaims, LeaseClaim, Namespace,
    PermissionPattern, TenantId, is_lease_template,
};
use felix_wire::internal::ForwardLease;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};

use crate::serving::auth::AuthContext;

/// The `iss` of a lease token. Distinct from the control plane's, so one can
/// never pass for the other.
pub const LEASE_ISSUER: &str = "felix-lease";
/// A lease token's `kid` starts with this, which routes it to the lease keys.
const KID_PREFIX: &str = "lease-";
/// Clock skew allowed on a lease token's `exp`, here and on session end.
/// Small, since the token is short-lived by design.
pub(crate) const LEEWAY_SECS: i64 = 2;
/// Longest lease token lifetime unless `FELIX_LEASE_TOKEN_MAX_TTL_SECONDS`
/// says otherwise.
pub const DEFAULT_MAX_TTL: Duration = Duration::from_secs(300);
const KEY_FILE_ENV: &str = "FELIX_LEASE_TOKEN_KEY_FILE";
const AUDIENCE: &str = "felix-broker";
const MAX_TTL_ENV: &str = "FELIX_LEASE_TOKEN_MAX_TTL_SECONDS";

/// Mints and verifies lease tokens with the shared lease key.
///
/// The derived `jsonwebtoken` keys are built once from the key file and never
/// cached per tenant: the tenant of an `Auth` is the client's, and a cache
/// keyed by it would grow with every junk frame.
pub struct LeaseTokens {
    /// The first key's `kid` and encoding key.
    signing: (String, EncodingKey),
    /// Every key's `kid` and decoding key, in file order.
    verifying: Vec<(String, DecodingKey)>,
    max_ttl: Duration,
}

impl LeaseTokens {
    /// Read `FELIX_LEASE_TOKEN_KEY_FILE` and
    /// `FELIX_LEASE_TOKEN_MAX_TTL_SECONDS`. `None` without a key file: the
    /// broker then neither mints nor accepts lease tokens.
    ///
    /// # Errors
    /// An unreadable or malformed key file, or a TTL that is not a positive
    /// whole number of seconds.
    pub fn from_env() -> Result<Option<Self>> {
        let Some(path) = std::env::var(KEY_FILE_ENV)
            .ok()
            .filter(|path| !path.trim().is_empty())
        else {
            return Ok(None);
        };
        let max_ttl = match std::env::var(MAX_TTL_ENV) {
            Ok(value) => match value.trim().parse::<u64>() {
                Ok(seconds) if seconds > 0 => Duration::from_secs(seconds),
                _ => bail!("{MAX_TTL_ENV} must be a positive number of seconds"),
            },
            Err(_) => DEFAULT_MAX_TTL,
        };
        Self::from_key_file(Path::new(path.trim()), max_ttl).map(Some)
    }

    /// Load the keys from `path`; see [`Self::from_keys`] for the format.
    ///
    /// # Errors
    /// As [`Self::from_keys`], or an unreadable file.
    pub fn from_key_file(path: &Path, max_ttl: Duration) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read {KEY_FILE_ENV} {}", path.display()))?;
        Self::from_keys(&text, max_ttl)
            .with_context(|| format!("{KEY_FILE_ENV} {}", path.display()))
    }

    /// Keys from the key file's text: one base64 (standard or URL-safe,
    /// padding optional) 32-byte Ed25519 seed per line, blank lines and `#`
    /// comments ignored. The first key signs and every key verifies, so a
    /// rotation adds the new key everywhere, then moves it first, then drops
    /// the old one.
    ///
    /// # Errors
    /// No key, or a line that is not a 32-byte seed. The error names the line,
    /// never its contents.
    pub fn from_keys(text: &str, max_ttl: Duration) -> Result<Self> {
        let mut keys = Vec::new();
        for (index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let seed = decode_seed(line)
                .with_context(|| format!("line {} is not a base64 32-byte seed", index + 1))?;
            keys.push(LeaseKey::from_seed(seed));
        }
        let Some(first) = keys.first() else {
            bail!("no lease token key");
        };
        let signing = (first.kid.clone(), first.encoding_key()?);
        let verifying = keys
            .iter()
            .map(|key| Ok((key.kid.clone(), key.decoding_key()?)))
            .collect::<Result<_>>()?;
        Ok(Self {
            signing,
            verifying,
            max_ttl,
        })
    }

    /// Verify a lease token for `tenant_id`, and check that its claims are
    /// what a broker mints and nothing more, so a token signed with a leaked
    /// key still cannot carry a wildcard, a template, a non-data action, a
    /// long life or another tenant.
    ///
    /// # Errors
    /// A bad signature, issuer, audience or tenant, an expired token, or
    /// claims no broker would mint.
    pub(crate) fn verify(&self, tenant_id: &TenantId, token: &str) -> AuthzResult<FelixClaims> {
        let kid = jsonwebtoken::decode_header(token)?.kid;
        let (_, key) = self
            .verifying
            .iter()
            .find(|(known, _)| Some(known) == kid.as_ref())
            .ok_or_else(|| AuthzError::Key("unknown lease key".to_string()))?;
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_audience(&[AUDIENCE]);
        validation.set_issuer(&[LEASE_ISSUER]);
        validation.leeway = LEEWAY_SECS.unsigned_abs();
        let claims = jsonwebtoken::decode::<FelixClaims>(token, key, &validation)?.claims;
        if claims.tid != tenant_id.as_str() {
            return Err(AuthzError::TenantMismatch {
                expected: tenant_id.to_string(),
                actual: claims.tid,
            });
        }
        check_minted_shape(&claims, self.max_ttl)?;
        Ok(claims)
    }

    /// Sign `claims` as a lease token. Treat the result as a secret.
    fn sign(&self, mut claims: FelixClaims) -> AuthzResult<String> {
        claims.iss = LEASE_ISSUER.to_string();
        claims.aud = AUDIENCE.to_string();
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(self.signing.0.clone());
        Ok(jsonwebtoken::encode(&header, &claims, &self.signing.1)?)
    }
}

/// Refuse claims a broker would never mint: no lease claim, issued in the
/// future, living longer than the maximum, or a permission that is not an
/// exact stream or cache key data grant in the token's own tenant.
fn check_minted_shape(claims: &FelixClaims, max_ttl: Duration) -> AuthzResult<()> {
    let refuse = |why: &str| Err(AuthzError::Key(format!("not a lease token: {why}")));
    if claims.lease.is_none() {
        return refuse("no lease claim");
    }
    let now = secs(SystemTime::now());
    if claims.iat > now.saturating_add(LEEWAY_SECS) {
        return refuse("issued in the future");
    }
    if claims.exp.saturating_sub(now) > secs_of(max_ttl).saturating_add(LEEWAY_SECS) {
        return refuse("lives longer than a lease token may");
    }
    for perm in &claims.perms {
        let pattern = PermissionPattern::parse(perm)?;
        let resource = pattern.resource_pattern.as_str();
        let (kind, segments) = match pattern.action {
            Action::StreamPublish | Action::StreamSubscribe => ("stream:", 3),
            Action::CacheRead | Action::CacheWrite => ("cache:", 4),
            _ => return refuse("a permission that is not data access"),
        };
        let Some(rest) = resource.strip_prefix(kind) else {
            return refuse("a permission on the wrong kind of object");
        };
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() != segments
            || parts[0] != claims.tid
            || parts.iter().any(|part| part.is_empty())
            || resource.contains('*')
            || is_lease_template(resource)
        {
            return refuse("a permission that is not one exact stream or cache key");
        }
    }
    Ok(())
}

/// Whether `token` names a lease key in its header. A token whose header does
/// not parse is not one, and fails wherever it goes.
pub(crate) fn is_lease_token(token: &str) -> bool {
    jsonwebtoken::decode_header(token)
        .ok()
        .and_then(|header| header.kid)
        .is_some_and(|kid| kid.starts_with(KID_PREFIX))
}

/// A mint planned before the write it rides on: the caller may have a lease
/// token for `key`, and this is what it would grant.
#[derive(Clone)]
pub(crate) struct LeaseMint {
    tokens: Arc<LeaseTokens>,
    tenant_id: String,
    subject: String,
    issued_at: i64,
    caller_expires_at: i64,
    namespace: String,
    cache: String,
    key: String,
    perms: Vec<String>,
}

impl LeaseMint {
    /// Check that `caller` may have a lease token for `key` of this cache,
    /// before anything is written.
    ///
    /// # Errors
    /// Why not: the caller's own token is a lease or delegated token, it holds
    /// no template naming this cache, or `key` cannot expand one.
    pub(crate) fn plan(
        tokens: &Arc<LeaseTokens>,
        caller: &AuthContext,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Self, LeaseRefusal> {
        let refused = |refusal: LeaseRefusal| {
            metrics::counter!("felix_lease_token_refusals_total", "reason" => refusal.label())
                .increment(1);
            refusal
        };
        // A lease token minting another would let a holder renew past its
        // lease; a delegated token would let a gateway mint for its users.
        if caller.lease_token {
            return Err(refused(LeaseRefusal::LeaseToken));
        }
        if caller.actor.is_some() {
            return Err(refused(LeaseRefusal::Delegated));
        }
        let perms = caller
            .matcher
            .lease_grants(
                &TenantId::new(&caller.tenant_id),
                &Namespace::new(namespace),
                &CacheScope::new(cache),
                key,
            )
            .map_err(|err| refused(LeaseRefusal::Key(err.to_string())))?;
        if perms.is_empty() {
            return Err(refused(LeaseRefusal::NoTemplate));
        }
        Ok(Self {
            tokens: Arc::clone(tokens),
            tenant_id: caller.tenant_id.clone(),
            subject: caller.subject.clone(),
            issued_at: caller.issued_at,
            caller_expires_at: caller.expires_at,
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            perms,
        })
    }

    /// Mint the token for the write that made `version`. `entry_expires_at`
    /// is when the written entry expires, if it does.
    ///
    /// The token lasts as long as the entry, but no longer than the caller's
    /// own token or the broker's maximum.
    pub(crate) fn mint(&self, version: u64, entry_expires_at: Option<SystemTime>) -> ForwardLease {
        let now = secs(SystemTime::now());
        let mut exp = self
            .caller_expires_at
            .min(now.saturating_add(secs_of(self.tokens.max_ttl)));
        if let Some(entry) = entry_expires_at {
            exp = exp.min(secs(entry));
        }
        if exp <= now {
            metrics::counter!("felix_lease_token_refusals_total", "reason" => "expired")
                .increment(1);
            return ForwardLease::Refused(
                "the lease token would already be expired: the caller's token or the lease \
                 entry ends now"
                    .to_string(),
            );
        }
        let claims = FelixClaims {
            iss: String::new(),
            aud: String::new(),
            sub: self.subject.clone(),
            tid: self.tenant_id.clone(),
            exp,
            // The caller's, so revoking the caller's principal also revokes
            // every lease token minted from its token. Capped at now, since a
            // control-plane clock ahead of this one would otherwise mint a
            // token verification refuses as issued in the future.
            iat: self.issued_at.min(now),
            jti: Some(uuid::Uuid::new_v4().simple().to_string()),
            perms: self.perms.clone(),
            act: None,
            lease: Some(LeaseClaim {
                cache: format!("{}/{}", self.namespace, self.cache),
                key: self.key.clone(),
                version,
            }),
        };
        match self.tokens.sign(claims) {
            Ok(token) => {
                tracing::info!(
                    tenant_id = %self.tenant_id,
                    subject = %self.subject,
                    namespace = %self.namespace,
                    cache = %self.cache,
                    key = %self.key,
                    version,
                    exp,
                    perms = ?self.perms,
                    "lease token issued",
                );
                metrics::counter!("felix_lease_tokens_issued_total").increment(1);
                ForwardLease::Minted {
                    token,
                    expires_at: exp.unsigned_abs(),
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "could not sign a lease token");
                metrics::counter!("felix_lease_token_refusals_total", "reason" => "signing")
                    .increment(1);
                ForwardLease::Refused("the broker could not sign a lease token".to_string())
            }
        }
    }
}

/// Why a caller may not have a lease token. Nothing is written when one is
/// refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LeaseRefusal {
    LeaseToken,
    Delegated,
    NoTemplate,
    Key(String),
}

impl LeaseRefusal {
    fn label(&self) -> &'static str {
        match self {
            Self::LeaseToken => "lease_token",
            Self::Delegated => "delegated",
            Self::NoTemplate => "no_template",
            Self::Key(_) => "invalid_key",
        }
    }
}

impl std::fmt::Display for LeaseRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LeaseToken => f.write_str("a lease token cannot mint another"),
            Self::Delegated => f.write_str("a delegated token cannot mint a lease token"),
            Self::NoTemplate => f.write_str("the token holds no lease template naming this cache"),
            Self::Key(detail) => write!(f, "no lease token for this key: {detail}"),
        }
    }
}

/// One lease key: the seed and its public key.
#[derive(Clone)]
struct LeaseKey {
    kid: String,
    seed: [u8; 32],
    public_key: [u8; 32],
}

impl LeaseKey {
    fn from_seed(seed: [u8; 32]) -> Self {
        let public_key = ed25519_dalek::SigningKey::from_bytes(&seed)
            .verifying_key()
            .to_bytes();
        // The public key's first bytes are fingerprint enough to tell a
        // handful of keys apart, and are not secret.
        let kid = format!("{KID_PREFIX}{}", URL_SAFE_NO_PAD.encode(&public_key[..9]));
        Self {
            kid,
            seed,
            public_key,
        }
    }

    fn encoding_key(&self) -> Result<EncodingKey> {
        // jsonwebtoken wants PKCS8 DER; it is built in memory only.
        let der = ed25519_dalek::SigningKey::from_bytes(&self.seed)
            .to_pkcs8_der()
            .map_err(|err| anyhow::anyhow!("encode a lease key: {err}"))?;
        Ok(EncodingKey::from_ed_der(der.as_bytes()))
    }

    fn decoding_key(&self) -> Result<DecodingKey> {
        Ok(DecodingKey::from_ed_components(
            &URL_SAFE_NO_PAD.encode(self.public_key),
        )?)
    }
}

fn decode_seed(line: &str) -> Result<[u8; 32]> {
    let bytes = [&STANDARD, &STANDARD_NO_PAD, &URL_SAFE, &URL_SAFE_NO_PAD]
        .into_iter()
        .find_map(|engine| engine.decode(line).ok())
        .context("not base64")?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| anyhow::anyhow!("{} bytes, not 32", bytes.len()))
}

fn secs(at: SystemTime) -> i64 {
    secs_of(at.duration_since(UNIX_EPOCH).unwrap_or_default())
}

fn secs_of(duration: Duration) -> i64 {
    i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests;

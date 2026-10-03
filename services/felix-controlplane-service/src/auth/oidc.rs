//! Validation of upstream IdP tokens -- the boundary between an external
//! identity provider and Felix's own authorization.
//!
//! RS256 and ES256 are accepted by default, and the allowlist can narrow or
//! widen that. Accepting both opens no algorithm confusion: the key comes from
//! the issuer's JWKS and its type must match the header's `alg`, and HS* and
//! `none` are always refused. Felix's own EdDSA tokens are a separate path
//! entirely.
//!
//! Claims are decoded once *without* verification purely to find the issuer, so
//! the right JWKS can be fetched. Everything else -- issuer, audience,
//! signature -- is checked only after that, against configuration.
//!
//! Discovery documents and JWKS are cached with a TTL and refreshed on demand,
//! in a `DashMap` so concurrent tasks share them without a global lock. A JWKS
//! is fetched at most once every 30 seconds, because an unknown `kid`
//! forces a refresh before any signature has been checked.
//!
//! Construct an [`UpstreamOidcValidator`] and call
//! [`UpstreamOidcValidator::validate`].
mod keys;

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use dashmap::DashMap;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde_json::Value;

use crate::auth::idp_registry::IdpIssuerConfig;
use keys::{
    CachedDiscovery, CachedJwks, JWKS_REFRESH_FLOOR, JwksFetchGate, ensure_jwk_matches_algorithm,
    find_jwk,
};

/// Upper bound on one discovery or JWKS request. Refreshes of a JWKS queue
/// behind each other, so a hung IdP must not hold them forever.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Validates upstream OIDC bearer tokens, with cached discovery documents and
/// JWKS. RS256 and ES256 by default; see [`DEFAULT_ALLOWED_ALGORITHMS`].
#[derive(Debug, Clone)]
pub struct UpstreamOidcValidator {
    client: reqwest::Client,
    jwks_cache: Arc<DashMap<String, CachedJwks>>,
    jwks_fetches: Arc<DashMap<String, JwksFetchGate>>,
    jwks_refresh_floor: Duration,
    discovery_cache: Arc<DashMap<String, CachedDiscovery>>,
    jwks_ttl: Duration,
    discovery_ttl: Duration,
    clock_skew_seconds: u64,
    allowed_algorithms: Arc<Vec<Algorithm>>,
    allow_insecure_http: bool,
    allow_private: bool,
}

/// The upstream algorithms accepted when none are configured.
///
/// RS256 is the one every OIDC provider must support and most sign with by
/// default. Allowing it next to ES256 opens no algorithm confusion: HMAC is
/// never accepted, the key comes from the issuer's JWKS by `kid`, and its type
/// is checked against the header's `alg` before verification.
pub(crate) const DEFAULT_ALLOWED_ALGORITHMS: [Algorithm; 2] = [Algorithm::ES256, Algorithm::RS256];

impl Default for UpstreamOidcValidator {
    fn default() -> Self {
        Self::new(Duration::from_secs(3600), Duration::from_secs(3600), 60)
    }
}

impl UpstreamOidcValidator {
    /// A validator accepting [`DEFAULT_ALLOWED_ALGORITHMS`], with the given
    /// cache TTLs and clock skew. Shorter TTLs reduce exposure to stale keys
    /// at the cost of more fetches.
    pub fn new(jwks_ttl: Duration, discovery_ttl: Duration, clock_skew_seconds: u64) -> Self {
        Self::new_with_allowed_algorithms(
            jwks_ttl,
            discovery_ttl,
            clock_skew_seconds,
            DEFAULT_ALLOWED_ALGORITHMS.to_vec(),
        )
    }

    /// Create a new validator with an explicit upstream JWT algorithm allowlist.
    ///
    /// The allowlist is checked against the JWT header `alg` before any key
    /// lookup or signature verification. If empty, [`DEFAULT_ALLOWED_ALGORITHMS`].
    pub fn new_with_allowed_algorithms(
        jwks_ttl: Duration,
        discovery_ttl: Duration,
        clock_skew_seconds: u64,
        mut allowed_algorithms: Vec<Algorithm>,
    ) -> Self {
        if allowed_algorithms.is_empty() {
            allowed_algorithms.extend(DEFAULT_ALLOWED_ALGORITHMS);
        }
        allowed_algorithms.sort_unstable_by_key(|alg| *alg as u8);
        allowed_algorithms.dedup();
        let allow_private = crate::auth::idp_registry::allow_private_idp_from_env();
        // No redirects: the URL checks run on the URL we were given, and a
        // redirect would send the fetch past them to wherever the IdP says.
        let mut client = reqwest::Client::builder()
            .timeout(FETCH_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none());
        if !allow_private {
            client = client.dns_resolver(Arc::new(PublicOnlyResolver));
        }
        Self {
            client: client
                .build()
                .expect("an HTTP client with default TLS settings"),
            jwks_cache: Arc::new(DashMap::new()),
            jwks_fetches: Arc::new(DashMap::new()),
            jwks_refresh_floor: JWKS_REFRESH_FLOOR,
            discovery_cache: Arc::new(DashMap::new()),
            jwks_ttl,
            discovery_ttl,
            clock_skew_seconds,
            allowed_algorithms: Arc::new(allowed_algorithms),
            allow_insecure_http: crate::auth::idp_registry::allow_insecure_http_from_env(),
            allow_private,
        }
    }

    /// Whether IdP URLs may name private addresses. Read once, from
    /// [`ALLOW_PRIVATE_IDP_ENV`](crate::auth::idp_registry::ALLOW_PRIVATE_IDP_ENV).
    pub fn allow_private(&self) -> bool {
        self.allow_private
    }

    /// Whether discovery and JWKS may be fetched over plain HTTP from a
    /// non-loopback host. Read once, from
    /// [`ALLOW_INSECURE_HTTP_ENV`](crate::auth::idp_registry::ALLOW_INSECURE_HTTP_ENV).
    pub fn allow_insecure_http(&self) -> bool {
        self.allow_insecure_http
    }

    /// Validate an upstream bearer token against the tenant's configured
    /// issuers: allowlisted algorithm, resolved JWKS, then signature and
    /// `iss`/`aud`/`iat` checks.
    ///
    /// # Errors
    /// An [`OidcError`] naming the specific failure; issuer-not-configured is
    /// distinguished so the caller can answer 403 instead of 401.
    pub async fn validate(
        &self,
        token: &str,
        issuers: &[IdpIssuerConfig],
    ) -> Result<ValidatedToken, OidcError> {
        // Algorithm check first — this is also what keeps EdDSA Felix tokens
        // out of the upstream path.
        let header = decode_header(token)?;
        if !self.is_algorithm_allowed(header.alg) {
            return Err(OidcError::UnsupportedAlgorithm);
        }
        let kid = header.kid.as_deref().ok_or(OidcError::MissingKeyId)?;

        // Unverified decode, purely to find which issuer's JWKS to fetch;
        // nothing from it is trusted until the signature check below.
        let unsafe_claims = decode_unverified_claims(token)?;
        let issuer = extract_string_claim(&unsafe_claims, "iss").ok_or(OidcError::MissingIssuer)?;
        let issuer_cfg = issuers
            .iter()
            .find(|cfg| cfg.issuer == issuer)
            .ok_or(OidcError::IssuerNotAllowed)?;

        // On a `kid` miss, refresh once and retry — the miss usually means the
        // IdP rotated keys since our cached fetch. The refresh is rate-limited
        // per URL, since nothing about this token has been verified yet.
        let jwks_url = self.resolve_jwks_url(&issuer, issuer_cfg).await?;
        let jwks = self.get_jwks(&jwks_url).await?;
        let decoding_key = match find_jwk(&jwks, kid) {
            Some(key) => {
                ensure_jwk_matches_algorithm(key, header.alg)?;
                DecodingKey::from_jwk(key)?
            }
            None => {
                let refreshed = self.refresh_jwks(&jwks_url).await?;
                let key = find_jwk(&refreshed, kid).ok_or(OidcError::JwksKeyNotFound)?;
                ensure_jwk_matches_algorithm(key, header.alg)?;
                DecodingKey::from_jwk(key)?
            }
        };
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[issuer_cfg.issuer.as_str()]);
        validation.set_audience(&issuer_cfg.audiences);
        validation
            .required_spec_claims
            .extend(["iss".to_string(), "aud".to_string()]);
        validation.leeway = self.clock_skew_seconds;

        let token = decode::<Value>(token, &decoding_key, &validation)?;
        validate_iat(&token.claims, self.clock_skew_seconds)?;
        let subject = extract_string_claim(&token.claims, &issuer_cfg.claim_mappings.subject_claim)
            .ok_or(OidcError::MissingSubject)?;
        let groups = extract_groups_claim(
            &token.claims,
            issuer_cfg.claim_mappings.groups_claim.as_deref(),
        );

        Ok(ValidatedToken {
            issuer,
            subject,
            groups,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_jwks_refresh_floor(mut self, floor: Duration) -> Self {
        self.jwks_refresh_floor = floor;
        self
    }

    fn is_algorithm_allowed(&self, alg: Algorithm) -> bool {
        self.allowed_algorithms.contains(&alg)
    }
}

/// The identity extracted from a verified upstream token — just enough to
/// derive a Felix principal.
#[derive(Debug, Clone)]
pub struct ValidatedToken {
    pub issuer: String,
    pub subject: String,
    pub groups: Vec<String>,
}

/// Upstream validation failures. Messages never include token contents.
#[derive(Debug, thiserror::Error)]
pub enum OidcError {
    #[error("missing issuer")]
    MissingIssuer,
    #[error("issuer not allowed")]
    IssuerNotAllowed,
    #[error("missing subject")]
    MissingSubject,
    #[error("missing key id")]
    MissingKeyId,
    #[error("unsupported algorithm")]
    UnsupportedAlgorithm,
    #[error("invalid jwk: {0}")]
    InvalidJwk(String),
    #[error("jwks key not found")]
    JwksKeyNotFound,
    #[error("jwks unavailable")]
    JwksUnavailable,
    #[error("IdP URL not allowed: {0}")]
    UrlNotAllowed(String),
    #[error("discovery document names a different issuer")]
    DiscoveryIssuerMismatch,
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("jwt error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("invalid claim: {0}")]
    InvalidClaim(String),
}

/// Resolves IdP hostnames, refusing any that lead to a private address.
///
/// Checked here rather than on the URL because a hostname can resolve anywhere,
/// and checking the addresses actually connected to leaves no window for the
/// answer to change between check and fetch.
struct PublicOnlyResolver;

impl reqwest::dns::Resolve for PublicOnlyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let addrs: Vec<std::net::SocketAddr> =
                tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            if addrs
                .iter()
                .any(|addr| crate::auth::idp_registry::is_private_address(addr.ip()))
            {
                return Err(
                    format!("IdP host {} resolves to a private address", name.as_str()).into(),
                );
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

// Unverified decode, only ever used to locate the issuer before the real
// signature check.
fn decode_unverified_claims(token: &str) -> Result<Value, OidcError> {
    let mut parts = token.split('.');
    let _header = parts.next();
    let payload = parts
        .next()
        .ok_or_else(|| OidcError::InvalidClaim("token format".to_string()))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| OidcError::InvalidClaim("token payload".to_string()))?;
    serde_json::from_slice(&bytes)
        .map_err(|err| OidcError::InvalidClaim(format!("token payload: {err}")))
}

fn extract_string_claim(claims: &Value, name: &str) -> Option<String> {
    claims
        .get(name)
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn validate_iat(claims: &Value, leeway_seconds: u64) -> Result<(), OidcError> {
    let iat = claims
        .get("iat")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| OidcError::InvalidClaim("iat".to_string()))?;
    let now = Utc::now().timestamp();
    let leeway = leeway_seconds as i64;
    if iat > now + leeway {
        return Err(OidcError::InvalidClaim("iat in future".to_string()));
    }
    Ok(())
}

// IdPs encode groups as either a string or an array of strings.
fn extract_groups_claim(claims: &Value, name: Option<&str>) -> Vec<String> {
    let Some(name) = name else {
        return Vec::new();
    };
    let Some(value) = claims.get(name) else {
        return Vec::new();
    };
    if let Some(values) = value.as_array() {
        return values
            .iter()
            .filter_map(|item| item.as_str().map(|val| val.to_string()))
            .collect();
    }
    if let Some(value) = value.as_str() {
        return vec![value.to_string()];
    }
    Vec::new()
}

#[cfg(test)]
mod tests;

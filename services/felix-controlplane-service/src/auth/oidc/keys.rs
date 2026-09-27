//! Finding the key a token was signed with: discovery, the JWKS behind it,
//! both cached with a TTL, and the checks that the key fits the algorithm.
use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::Algorithm;
use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, JwkSet, KeyAlgorithm};
use serde::Deserialize;

use super::{OidcError, UpstreamOidcValidator};
use crate::auth::idp_registry::{IdpIssuerConfig, check_fetch_url};

#[derive(Debug, Clone)]
pub(super) struct CachedJwks {
    pub(super) jwks: JwkSet,
    pub(super) expires_at: Instant,
}

/// The shortest gap between two fetches of the same JWKS.
///
/// A `kid` miss refreshes the JWKS, and reaching one takes only a token that
/// names an allowlisted issuer, not a valid signature. Without a floor every
/// forged token would cost a request to the tenant's IdP. It is also how long a
/// key the IdP rotates in can go unrecognised, so it stays short.
pub(super) const JWKS_REFRESH_FLOOR: Duration = Duration::from_secs(30);

/// When this JWKS URL was last fetched. Held across the fetch, so concurrent
/// refreshes queue and the ones behind find the result already cached.
pub(super) type JwksFetchGate = Arc<tokio::sync::Mutex<Option<Instant>>>;

#[derive(Debug, Clone)]
pub(super) struct CachedDiscovery {
    pub(super) jwks_url: String,
    pub(super) expires_at: Instant,
}

#[derive(Debug, Deserialize)]
struct DiscoveryDocument {
    issuer: String,
    jwks_uri: String,
}

impl UpstreamOidcValidator {
    pub(super) async fn resolve_jwks_url(
        &self,
        issuer: &str,
        issuer_cfg: &IdpIssuerConfig,
    ) -> Result<String, OidcError> {
        // Checked here as well as when the issuer is stored: a config written
        // before the rule existed, or a discovery document, can name any URL.
        let allowed = |url: &str| {
            check_fetch_url(url, self.allow_insecure_http).map_err(OidcError::UrlNotAllowed)
        };
        // An explicit JWKS URL skips discovery entirely.
        if let Some(url) = &issuer_cfg.jwks_url {
            allowed(url)?;
            return Ok(url.to_string());
        }
        let discovery_url = issuer_cfg.discovery_url();
        allowed(&discovery_url)?;

        if let Some(entry) = self.discovery_cache.get(&discovery_url)
            && entry.expires_at > Instant::now()
        {
            return Ok(entry.jwks_url.clone());
        }

        let doc: DiscoveryDocument = self
            .client
            .get(&discovery_url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        // OIDC Discovery 1.0 section 4.3: the document must be for the issuer
        // it was fetched for, or its keys are someone else's.
        if doc.issuer != issuer {
            return Err(OidcError::DiscoveryIssuerMismatch);
        }
        allowed(&doc.jwks_uri)?;
        self.discovery_cache.insert(
            discovery_url,
            CachedDiscovery {
                jwks_url: doc.jwks_uri.clone(),
                expires_at: Instant::now() + self.discovery_ttl,
            },
        );
        Ok(doc.jwks_uri)
    }

    pub(super) async fn get_jwks(&self, jwks_url: &str) -> Result<JwkSet, OidcError> {
        if let Some(entry) = self.jwks_cache.get(jwks_url)
            && entry.expires_at > Instant::now()
        {
            return Ok(entry.jwks.clone());
        }
        self.refresh_jwks(jwks_url).await
    }

    pub(super) async fn refresh_jwks(&self, jwks_url: &str) -> Result<JwkSet, OidcError> {
        let gate = self
            .jwks_fetches
            .entry(jwks_url.to_string())
            .or_default()
            .clone();
        let mut last_fetch = gate.lock().await;
        if last_fetch.is_some_and(|at| at.elapsed() < self.jwks_refresh_floor) {
            // Fetched recently, possibly by whoever held the gate before us.
            // An expired entry is still the IdP's latest answer.
            return self
                .jwks_cache
                .get(jwks_url)
                .map(|entry| entry.jwks.clone())
                .ok_or(OidcError::JwksUnavailable);
        }
        // Stamped before the fetch so a failing IdP is not retried per request.
        *last_fetch = Some(Instant::now());
        let jwks: JwkSet = self
            .client
            .get(jwks_url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        self.jwks_cache.insert(
            jwks_url.to_string(),
            CachedJwks {
                jwks: jwks.clone(),
                expires_at: Instant::now() + self.jwks_ttl,
            },
        );
        Ok(jwks)
    }
}

pub(super) fn ensure_jwk_matches_algorithm(
    jwk: &jsonwebtoken::jwk::Jwk,
    alg: Algorithm,
) -> Result<(), OidcError> {
    // `alg` is OPTIONAL in a JWK (RFC 7517 §4.4), and major IdPs — Microsoft
    // Entra among them — publish signing keys without it. Requiring it here
    // rejected every token those IdPs issue. When the member is present it must
    // match the token's algorithm; when absent, the key-type/params check below
    // is what binds the key to the algorithm (an RSA key cannot verify an EC
    // token or vice versa, and the header `alg` is already allowlisted upstream).
    if let Some(key_alg) = jwk.common.key_algorithm {
        let expected = expected_key_algorithm(alg)
            .ok_or_else(|| OidcError::InvalidJwk("unsupported algorithm".to_string()))?;
        if key_alg != expected {
            return Err(OidcError::InvalidJwk("alg mismatch".to_string()));
        }
    }

    match (&jwk.algorithm, alg) {
        (AlgorithmParameters::EllipticCurve(params), Algorithm::ES256) => {
            if params.curve != EllipticCurve::P256 {
                return Err(OidcError::InvalidJwk("unexpected EC curve".to_string()));
            }
            if params.x.is_empty() || params.y.is_empty() {
                return Err(OidcError::InvalidJwk("missing EC coordinates".to_string()));
            }
            Ok(())
        }
        (AlgorithmParameters::RSA(params), Algorithm::RS256)
        | (AlgorithmParameters::RSA(params), Algorithm::RS384)
        | (AlgorithmParameters::RSA(params), Algorithm::RS512)
        | (AlgorithmParameters::RSA(params), Algorithm::PS256)
        | (AlgorithmParameters::RSA(params), Algorithm::PS384)
        | (AlgorithmParameters::RSA(params), Algorithm::PS512) => {
            if params.n.is_empty() || params.e.is_empty() {
                return Err(OidcError::InvalidJwk(
                    "missing RSA modulus/exponent".to_string(),
                ));
            }
            Ok(())
        }
        _ => Err(OidcError::InvalidJwk("kty mismatch".to_string())),
    }
}

pub(super) fn expected_key_algorithm(alg: Algorithm) -> Option<KeyAlgorithm> {
    match alg {
        Algorithm::ES256 => Some(KeyAlgorithm::ES256),
        Algorithm::RS256 => Some(KeyAlgorithm::RS256),
        Algorithm::RS384 => Some(KeyAlgorithm::RS384),
        Algorithm::RS512 => Some(KeyAlgorithm::RS512),
        Algorithm::PS256 => Some(KeyAlgorithm::PS256),
        Algorithm::PS384 => Some(KeyAlgorithm::PS384),
        Algorithm::PS512 => Some(KeyAlgorithm::PS512),
        _ => None,
    }
}

pub(super) fn find_jwk<'a>(jwks: &'a JwkSet, kid: &str) -> Option<&'a jsonwebtoken::jwk::Jwk> {
    jwks.keys
        .iter()
        .find(|key| key.common.key_id.as_deref() == Some(kid))
}

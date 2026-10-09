//! Broker-side authentication: JWKS caching and Felix token verification.
//!
//! Felix tokens are EdDSA (Ed25519); RSA is never accepted here. JWKS fetched
//! from the control plane carries public key material only, and is treated as
//! untrusted input — key shape and length are checked before use. `kid` guides
//! which key is tried first, but verification still tries all of them, so a
//! rotation in progress does not reject valid tokens.
//!
//! The JWKS cache is a `DashMap`; invalidation is per-tenant and coordinated
//! with refresh. Nothing here logs a token or a key.
//!
//! Construct [`BrokerAuth`] with the control-plane URL and call
//! [`BrokerAuth::authenticate`] to get an [`AuthContext`].

// A constant signing key: anything it signs is forgeable, so it is only
// compiled in when asked for.
#[cfg(feature = "demo")]
pub mod demo;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use dashmap::DashMap;
use felix_authz::{
    AuthzError, AuthzResult, FelixTokenVerifier, Jwks, PermissionMatcher, TenantId, TenantKeyCache,
    TenantKeyStore, TenantVerificationKey,
};
use felix_broker::Broker;
use jsonwebtoken::Algorithm;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// Verifies Felix tokens against control-plane JWKS. Cloning shares the
/// verifier and key store.
#[derive(Clone)]
pub struct BrokerAuth {
    verifier: Arc<FelixTokenVerifier>,
    key_store: Arc<ControlPlaneKeyStore>,
    /// Require a token's subject to match the client certificate, when the
    /// client presented one (`FELIX_TLS_CLIENT_CERT_BIND_SUBJECT`).
    bind_subject: bool,
}

impl BrokerAuth {
    pub fn new(controlplane_url: String) -> Self {
        // The verifier and the JWKS store share one key cache, so a JWKS
        // refresh invalidates the derived decoding keys with it.
        let key_cache = Arc::new(TenantKeyCache::default());
        let key_store = Arc::new(ControlPlaneKeyStore::new(
            controlplane_url,
            key_cache.clone(),
        ));
        let verifier = Arc::new(
            FelixTokenVerifier::new("felix-auth", "felix-broker", 60, key_store.clone())
                .with_cache(key_cache),
        );
        Self {
            verifier,
            key_store,
            bind_subject: false,
        }
    }

    /// Build from an existing key store, reusing its key cache.
    pub fn with_key_store(key_store: Arc<ControlPlaneKeyStore>) -> Self {
        let key_cache = key_store.key_cache.clone();
        let verifier = Arc::new(
            FelixTokenVerifier::new("felix-auth", "felix-broker", 60, key_store.clone())
                .with_cache(key_cache),
        );
        Self {
            verifier,
            key_store,
            bind_subject: false,
        }
    }

    /// Require every token presented over a connection with a client
    /// certificate to name that certificate's identity as its subject, or as
    /// its actor (`act`) on a token the control plane minted by delegation.
    pub fn with_subject_binding(mut self, bind_subject: bool) -> Self {
        self.bind_subject = bind_subject;
        self
    }

    /// [`Self::authenticate`] for a client on a connection that may carry a
    /// certificate, checking the token's subject against it when this broker
    /// binds the two.
    pub async fn authenticate_peer(
        &self,
        tenant_id: &str,
        token: &str,
        peer_certs: Option<&[rustls::pki_types::CertificateDer<'_>]>,
    ) -> Result<AuthContext> {
        let context = self.authenticate(tenant_id, token).await?;
        if self.bind_subject {
            crate::serving::tls::check_subject_binding(
                peer_certs,
                &context.subject,
                context.actor.as_deref(),
            )
            .map_err(|reason| anyhow::anyhow!("token refused: {reason}"))?;
        }
        Ok(context)
    }

    /// Verify a token for a tenant and return an [`AuthContext`] with a
    /// precomputed permission matcher. Fetches JWKS if not already cached.
    ///
    /// # Errors
    /// JWKS fetch failures (control plane unreachable) and token verification
    /// failures.
    pub async fn authenticate(&self, tenant_id: &str, token: &str) -> Result<AuthContext> {
        ensure_jwks_cached(&self.key_store, tenant_id).await?;
        let tenant = TenantId::new(tenant_id);
        let claims = self.verifier.verify(&tenant, token)?;
        let matcher = PermissionMatcher::from_strings(&claims.perms)?;
        Ok(AuthContext {
            tenant_id: tenant_id.to_string(),
            matcher,
            token: token.to_string(),
            publisher: publisher_of(&claims.sub),
            subject: claims.sub,
            actor: claims.act.map(|actor| actor.sub),
        })
    }
}

/// Tenant scope plus the permission matcher from a verified token.
#[derive(Clone)]
pub struct AuthContext {
    pub tenant_id: String,
    pub matcher: PermissionMatcher,
    /// The token itself, kept so a request this broker forwards carries it
    /// and the owner can verify it again. Never logged.
    pub token: String,
    /// The principal the token was issued to (`sub`).
    pub subject: String,
    /// Who may present the token for `subject` (`act.sub`), when the control
    /// plane minted it by delegation. Only subject binding reads it.
    pub actor: Option<String>,
    /// `subject` as it is recorded on what this connection publishes. `None`
    /// when it is too long to record.
    pub publisher: Option<bytes::Bytes>,
}

impl AuthContext {
    /// This connection as a publisher.
    pub(crate) fn publishing_as(&self) -> crate::serving::quic::handlers::publish::PublishAs {
        crate::serving::quic::handlers::publish::PublishAs {
            credential: self.token.clone(),
            publisher: self.publisher.clone(),
        }
    }
}

/// A subject as a record's publisher: what is recorded is a principal id,
/// bounded, never the token. A longer subject is not recorded rather than
/// cut, since a shortened one could name a different principal.
fn publisher_of(subject: &str) -> Option<bytes::Bytes> {
    if subject.len() > felix_wire::binary::MAX_PUBLISHER_BYTES {
        tracing::warn!(
            len = subject.len(),
            "token subject too long to record as a publisher"
        );
        return None;
    }
    Some(bytes::Bytes::copy_from_slice(subject.as_bytes()))
}

/// How long one JWKS request may take, connect included.
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const JWKS_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a failed fetch is remembered. Short, so a tenant created a moment
/// after a failed auth is not locked out for long.
const JWKS_MISS_TTL: Duration = Duration::from_secs(5);
/// Remembered failures, bounded because the tenant ids are the client's.
const JWKS_MISS_CACHE_MAX: usize = 4096;
/// JWKS requests in flight at once, across every tenant.
const JWKS_FETCH_CONCURRENCY: usize = 8;
/// Longer tenant ids are refused without a lookup.
const TENANT_ID_MAX_BYTES: usize = 256;

/// The tenants this broker has learned from the control plane.
///
/// Consulted before a JWKS fetch, so an `Auth` naming a tenant the control
/// plane never told us about costs no request. Until the first catalog sync
/// lands, `seeded` is not cancelled and every tenant is let through to the
/// fetch, as before.
#[derive(Clone)]
pub struct TenantCatalog {
    broker: Arc<Broker>,
    seeded: CancellationToken,
}

impl TenantCatalog {
    /// `seeded` must be cancelled once the catalog has been applied.
    pub fn new(broker: Arc<Broker>, seeded: CancellationToken) -> Self {
        Self { broker, seeded }
    }

    async fn rules_out(&self, tenant_id: &str) -> bool {
        self.seeded.is_cancelled() && !self.broker.tenant_exists(tenant_id).await
    }
}

/// Fetches and caches per-tenant JWKS from the control plane, and exposes the
/// verification keys to `felix_authz`. Holds public keys only.
///
/// The tenant id comes from an unauthenticated `Auth`, so a fetch is guarded:
/// the local catalog is asked first, concurrent misses for one tenant share a
/// single request, failures are remembered for [`JWKS_MISS_TTL`], and at most
/// [`JWKS_FETCH_CONCURRENCY`] requests run at once, each with a timeout.
#[derive(Clone)]
pub struct ControlPlaneKeyStore {
    base_url: String,
    client: reqwest::Client,
    cache: Arc<DashMap<String, CachedJwks>>,
    misses: Arc<DashMap<String, Instant>>,
    inflight: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    fetches: Arc<Semaphore>,
    ttl: Duration,
    key_cache: Arc<TenantKeyCache>,
    catalog: Option<TenantCatalog>,
}

#[derive(Clone)]
struct CachedJwks {
    jwks: Jwks,
    expires_at: Instant,
}

impl ControlPlaneKeyStore {
    pub fn new(base_url: String, key_cache: Arc<TenantKeyCache>) -> Self {
        let client = crate::cluster::controlplane_http::builder()
            .timeout(JWKS_FETCH_TIMEOUT)
            .connect_timeout(JWKS_CONNECT_TIMEOUT)
            .build()
            .expect("build jwks http client");
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
            cache: Arc::new(DashMap::new()),
            misses: Arc::new(DashMap::new()),
            inflight: Arc::new(DashMap::new()),
            fetches: Arc::new(Semaphore::new(JWKS_FETCH_CONCURRENCY)),
            ttl: Duration::from_secs(3600),
            key_cache,
            catalog: None,
        }
    }

    /// Refuse to fetch for tenants `catalog` does not know, once it is seeded.
    pub fn with_tenant_catalog(mut self, catalog: TenantCatalog) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Cache JWKS for a tenant, replacing any existing entry and invalidating
    /// the derived decoding keys so a rotation takes effect immediately.
    pub fn insert_jwks(&self, tenant_id: &TenantId, jwks: Jwks) {
        self.cache.insert(
            tenant_id.to_string(),
            CachedJwks {
                jwks,
                expires_at: Instant::now() + self.ttl,
            },
        );
        self.misses.remove(tenant_id.as_str());
        self.key_cache.invalidate_tenant(tenant_id);
    }

    /// Fetch JWKS from the control plane and cache it.
    ///
    /// # Errors
    /// An implausible tenant id, a timeout, or a network, status or JSON
    /// failure.
    pub async fn refresh(&self, tenant_id: &TenantId) -> Result<Jwks> {
        check_tenant_id(tenant_id.as_str())?;
        let url = self.jwks_url(tenant_id.as_str())?;
        let _permit = tokio::time::timeout(JWKS_FETCH_TIMEOUT, self.fetches.acquire())
            .await
            .context("wait for a jwks fetch slot")?
            .expect("jwks fetch semaphore is never closed");
        // The URL is deliberately not logged: a deployment may embed
        // credentials in the base URL.
        let jwks: Jwks = self
            .client
            .get(url)
            .send()
            .await
            .context("fetch jwks")?
            .error_for_status()
            .context("fetch jwks")?
            .json()
            .await
            .context("decode jwks")?;
        self.insert_jwks(tenant_id, jwks.clone());
        Ok(jwks)
    }

    /// Make sure JWKS for `tenant_id` is cached, fetching it if not.
    async fn ensure_cached(&self, tenant_id: &str) -> Result<()> {
        let tenant = TenantId::new(tenant_id);
        if self.cached_jwks(&tenant).is_some() {
            return Ok(());
        }
        check_tenant_id(tenant_id)?;
        if self.recent_miss(tenant_id) {
            anyhow::bail!("jwks unavailable for tenant (recent fetch failed)");
        }
        if let Some(catalog) = &self.catalog
            && catalog.rules_out(tenant_id).await
        {
            anyhow::bail!("unknown tenant");
        }
        // One request per tenant however many streams ask at once: the
        // others wait here and find the answer in a cache.
        let flight = Arc::clone(&self.inflight.entry(tenant_id.to_string()).or_default());
        let result = {
            let _guard = flight.lock().await;
            if self.cached_jwks(&tenant).is_some() {
                Ok(())
            } else if self.recent_miss(tenant_id) {
                Err(anyhow::anyhow!(
                    "jwks unavailable for tenant (recent fetch failed)"
                ))
            } else {
                let fetched = self.refresh(&tenant).await.map(|_| ());
                if fetched.is_err() {
                    self.remember_miss(tenant_id);
                }
                fetched
            }
        };
        self.inflight
            .remove_if(tenant_id, |_, current| Arc::ptr_eq(current, &flight));
        result
    }

    fn jwks_url(&self, tenant_id: &str) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(&self.base_url).context("parse control-plane url")?;
        // Pushed as a path segment, so the tenant id is percent-encoded and a
        // `/`, `?` or `#` in it cannot reach another route.
        url.path_segments_mut()
            .map_err(|()| anyhow::anyhow!("control-plane url cannot take a path"))?
            .pop_if_empty()
            .extend(["v1", "tenants", tenant_id, ".well-known", "jwks.json"]);
        Ok(url)
    }

    fn recent_miss(&self, tenant_id: &str) -> bool {
        self.misses
            .get(tenant_id)
            .is_some_and(|expires_at| *expires_at > Instant::now())
    }

    fn remember_miss(&self, tenant_id: &str) {
        if self.misses.len() >= JWKS_MISS_CACHE_MAX {
            let now = Instant::now();
            self.misses.retain(|_, expires_at| *expires_at > now);
            // Still full of live entries: skip it. The other guards still
            // hold, and the map stays bounded.
            if self.misses.len() >= JWKS_MISS_CACHE_MAX {
                return;
            }
        }
        self.misses
            .insert(tenant_id.to_string(), Instant::now() + JWKS_MISS_TTL);
    }

    fn cached_jwks(&self, tenant_id: &TenantId) -> Option<Jwks> {
        self.cache.get(tenant_id.as_str()).and_then(|entry| {
            if entry.expires_at > Instant::now() {
                Some(entry.jwks.clone())
            } else {
                None
            }
        })
    }
}

/// Refuse a tenant id no control plane could have issued, before it goes
/// anywhere near a URL.
fn check_tenant_id(tenant_id: &str) -> Result<()> {
    if tenant_id.is_empty()
        || tenant_id.len() > TENANT_ID_MAX_BYTES
        || tenant_id == "."
        || tenant_id == ".."
        || tenant_id.chars().any(char::is_control)
    {
        anyhow::bail!("invalid tenant id");
    }
    Ok(())
}

impl TenantKeyStore for ControlPlaneKeyStore {
    fn current_signing_key(
        &self,
        _tenant_id: &TenantId,
    ) -> AuthzResult<felix_authz::TenantSigningKey> {
        // Brokers never mint Felix tokens, so there is no signing key to give.
        Err(AuthzError::MissingSigningKey("broker".to_string()))
    }

    fn verification_keys(&self, tenant_id: &TenantId) -> AuthzResult<Vec<TenantVerificationKey>> {
        // Cache only — this runs on the auth path and must not block on
        // network IO. `ensure_jwks_cached` fills the cache beforehand.
        let jwks = self
            .cached_jwks(tenant_id)
            .ok_or_else(|| AuthzError::MissingJwks(tenant_id.to_string()))?;
        jwks_to_keys(&jwks)
    }

    fn jwks(&self, tenant_id: &TenantId) -> AuthzResult<Jwks> {
        self.cached_jwks(tenant_id)
            .ok_or_else(|| AuthzError::MissingJwks(tenant_id.to_string()))
    }
}

/// Fetch JWKS for a tenant only if the cache is missing or expired.
///
/// # Errors
/// Network or decode failures from the fetch, a fetch for this tenant that
/// failed in the last few seconds, or a tenant the local catalog rules out.
pub async fn ensure_jwks_cached(key_store: &ControlPlaneKeyStore, tenant_id: &str) -> Result<()> {
    key_store.ensure_cached(tenant_id).await
}

fn jwks_to_keys(jwks: &Jwks) -> AuthzResult<Vec<TenantVerificationKey>> {
    let mut keys = Vec::new();
    for key in &jwks.keys {
        let public_key = key
            .x
            .as_ref()
            .ok_or_else(|| AuthzError::Key("missing jwk x".to_string()))?;
        let decoded = URL_SAFE_NO_PAD
            .decode(public_key.as_bytes())
            .map_err(|err| AuthzError::Key(format!("invalid jwk x: {err}")))?;
        let key_bytes: [u8; 32] = decoded
            .as_slice()
            .try_into()
            .map_err(|_| AuthzError::Key("invalid Ed25519 public key length".to_string()))?;
        // The algorithm is pinned to EdDSA regardless of what the JWKS claims;
        // honoring a JWKS-supplied alg would reopen the RSA/HS downgrade.
        keys.push(TenantVerificationKey {
            kid: key.kid.clone(),
            alg: Algorithm::EdDSA,
            public_key: key_bytes,
        });
    }
    Ok(keys)
}

#[cfg(test)]
mod tests;

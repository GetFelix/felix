//! Broker-side authentication: JWKS caching and Felix token verification.
//!
//! Felix tokens are EdDSA (Ed25519); RSA is never accepted here. JWKS fetched
//! from the control plane carries public key material only, and is treated as
//! untrusted input — key shape and length are checked before use. `kid` guides
//! which key is tried first, but verification still tries all of them, so a
//! rotation in progress does not reject valid tokens.
//!
//! The JWKS cache is a `DashMap`; invalidation is per-tenant and coordinated
//! with refresh. Requests read the cache and a background task renews it, so
//! the control plane is off the request path for every tenant already cached
//! (`docs/auth.md`, "Keeping tenant keys current"). Nothing here logs a token
//! or a key.
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

use crate::serving::lease::{LeaseTokens, is_lease_token};
use crate::serving::revocation::{Revocable, Revocations, Revoked};

/// Verifies Felix tokens against control-plane JWKS, and lease tokens against
/// the lease key. Cloning shares the verifiers and key store.
#[derive(Clone)]
pub struct BrokerAuth {
    verifier: Arc<FelixTokenVerifier>,
    key_store: Arc<ControlPlaneKeyStore>,
    /// Require a token's subject to match the client certificate, when the
    /// client presented one (`FELIX_TLS_CLIENT_CERT_BIND_SUBJECT`).
    bind_subject: bool,
    /// Without it this broker neither mints nor accepts lease tokens.
    lease: Option<Arc<LeaseTokens>>,
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
            lease: None,
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
            lease: None,
        }
    }

    /// Require every token presented over a connection with a client
    /// certificate to name that certificate's identity as its subject, or as
    /// its actor (`act`) on a token the control plane minted by delegation.
    pub fn with_subject_binding(mut self, bind_subject: bool) -> Self {
        self.bind_subject = bind_subject;
        self
    }

    /// Mint and accept lease tokens with these keys.
    pub fn with_lease_tokens(mut self, lease: Option<Arc<LeaseTokens>>) -> Self {
        self.lease = lease;
        self
    }

    /// The lease keys, when this broker has them.
    pub(crate) fn lease_tokens(&self) -> Option<&Arc<LeaseTokens>> {
        self.lease.as_ref()
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
            .map_err(|reason| {
                anyhow::Error::new(CredentialRefused(format!("token refused: {reason}")))
            })?;
        }
        Ok(context)
    }

    /// Verify a token for a tenant and return an [`AuthContext`] with a
    /// precomputed permission matcher.
    ///
    /// Verifies against the cached JWKS without asking the control plane. It
    /// fetches only for a tenant with no usable keys cached, and once (paced)
    /// for a token whose `kid` the cached set does not have.
    ///
    /// # Errors
    /// JWKS fetch failures (control plane unreachable) and token verification
    /// failures, and [`Revoked`] for a token issued before its principal was
    /// revoked.
    pub async fn authenticate(&self, tenant_id: &str, token: &str) -> Result<AuthContext> {
        let tenant = TenantId::new(tenant_id);
        // The lease keys and the control plane's are disjoint, so a token
        // whose kid points at the wrong set only fails.
        let lease_token = is_lease_token(token);
        let claims = if lease_token {
            // The JWKS path checks this before any lookup; the lease path
            // must too, since the tenant id is the client's.
            check_tenant_id(tenant_id)?;
            let lease = self.lease.as_ref().ok_or_else(|| {
                AuthzError::Key("this broker does not accept lease tokens".to_string())
            })?;
            lease.verify(&tenant, token)?
        } else {
            self.key_store.ensure_cached(tenant_id).await?;
            match self.verifier.verify(&tenant, token) {
                Ok(claims) => claims,
                // A kid the cached set lacks is how a newly staged key first
                // shows up, so it is worth one fetch before refusing.
                Err(err) => {
                    if !self.key_store.refetch_for_kid(&tenant, token).await {
                        return Err(err.into());
                    }
                    self.verifier.verify(&tenant, token)?
                }
            }
        };
        let matcher = PermissionMatcher::from_strings(&claims.perms)?;
        let actor = claims.act.map(|actor| actor.sub);
        let mut revocable = Revocable::new(
            Arc::clone(&self.key_store.revocations),
            tenant_id.to_string(),
            claims.sub.clone(),
            actor.clone(),
            claims.iat,
        );
        if lease_token {
            revocable = revocable.ending_at(
                claims
                    .exp
                    .saturating_add(crate::serving::lease::LEEWAY_SECS),
            );
        }
        if revocable.is_revoked() {
            return Err(anyhow::Error::new(Revoked));
        }
        Ok(AuthContext {
            tenant_id: tenant_id.to_string(),
            matcher,
            token: token.to_string(),
            publisher: publisher_of(&claims.sub),
            subject: claims.sub,
            actor,
            issued_at: claims.iat,
            expires_at: claims.exp,
            lease_token,
            revocable,
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
    /// The token's `iat` and `exp`, which bound a lease token minted from it.
    pub issued_at: i64,
    pub expires_at: i64,
    /// A lease token, whose sessions end at its `exp`.
    pub lease_token: bool,
    /// What ends this session when its principal is revoked.
    pub revocable: Revocable,
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

/// How long one JWKS fetch may take, waiting for a fetch slot included.
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const JWKS_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a failed fetch is remembered. Short, so a tenant created a moment
/// after a failed auth is not locked out for long.
const JWKS_MISS_TTL: Duration = Duration::from_secs(5);
/// Remembered failures, bounded because the tenant ids are the client's.
const JWKS_MISS_CACHE_MAX: usize = 4096;
/// JWKS requests in flight at once, across every tenant.
const JWKS_FETCH_CONCURRENCY: usize = 8;
/// Background renewals in flight at once. Below [`JWKS_FETCH_CONCURRENCY`],
/// so a request for a tenant this broker has not seen always finds a slot.
const JWKS_RENEW_CONCURRENCY: usize = 4;
/// How long after a successful fetch the renewer fetches a tenant's JWKS
/// again. While the control plane answers, a retired key stops verifying
/// within about this long.
const JWKS_RENEW_EVERY: Duration = Duration::from_secs(10 * 60);
/// How long cached keys keep verifying with no successful fetch. This bounds
/// how long a control-plane outage can keep a retired key working; past it,
/// the tenant's tokens are refused until the control plane answers again.
const JWKS_MAX_STALENESS: Duration = Duration::from_secs(24 * 60 * 60);
/// How often the renewer looks for tenants that are due.
const JWKS_RENEW_TICK: Duration = Duration::from_secs(15);
/// The wait before retrying a failed renewal, doubling up to the cap.
const JWKS_RETRY_MIN: Duration = Duration::from_secs(15);
const JWKS_RETRY_MAX: Duration = Duration::from_secs(5 * 60);
/// The shortest gap between a tenant's fetches that an unknown `kid` may
/// cause, so tokens with made-up kids cannot drive requests.
const JWKS_KID_REFETCH_GAP: Duration = Duration::from_secs(10);
/// How often a run of failed renewals is warned about again.
const JWKS_WARN_EVERY: Duration = Duration::from_secs(60);
/// Renewals that failed, for any reason.
const RENEW_ERRORS_TOTAL: &str = "felix_jwks_renew_errors_total";
/// Seconds since the oldest cached JWKS was fetched.
const OLDEST_AGE_GAUGE: &str = "felix_jwks_oldest_age_seconds";
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
/// Requests verify against the cache and do not wait on the control plane.
/// [`Self::renew_until`] fetches each cached tenant's keys again in the
/// background, and a failed renewal leaves the cached keys in use until
/// [`JWKS_MAX_STALENESS`] after the last successful fetch.
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
    renew_every: Duration,
    max_staleness: Duration,
    key_cache: Arc<TenantKeyCache>,
    catalog: Option<TenantCatalog>,
    revocations: Arc<Revocations>,
}

#[derive(Clone)]
struct CachedJwks {
    jwks: Jwks,
    /// When the control plane last answered with it.
    fetched_at: Instant,
    /// Past this the keys are not used: [`JWKS_MAX_STALENESS`] after
    /// `fetched_at`.
    expires_at: Instant,
    /// When the renewer fetches it next.
    renew_at: Instant,
    /// The last fetch tried, answered or not. Paces unknown-kid fetches.
    attempted_at: Instant,
    /// Renewals that failed in a row.
    failures: u32,
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
            renew_every: JWKS_RENEW_EVERY,
            max_staleness: JWKS_MAX_STALENESS,
            key_cache,
            catalog: None,
            revocations: Arc::new(Revocations::default()),
        }
    }

    /// The revocations every token verified against this store is checked
    /// against. The control-plane sync fills it.
    pub fn revocations(&self) -> &Arc<Revocations> {
        &self.revocations
    }

    /// Refuse to fetch for tenants `catalog` does not know, once it is seeded.
    pub fn with_tenant_catalog(mut self, catalog: TenantCatalog) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Cache JWKS for a tenant, replacing any existing entry. A key set that
    /// differs from the cached one also drops the derived decoding keys, so a
    /// rotation takes effect immediately.
    pub fn insert_jwks(&self, tenant_id: &TenantId, jwks: Jwks) {
        let now = Instant::now();
        let fresh = CachedJwks {
            jwks,
            fetched_at: now,
            expires_at: now + self.max_staleness,
            renew_at: now + self.renew_every,
            attempted_at: now,
            failures: 0,
        };
        // A renewal usually brings back the same keys. Invalidating then would
        // take the decoding-key cache's write lock for nothing.
        let changed = match self.cache.insert(tenant_id.to_string(), fresh.clone()) {
            Some(previous) => previous.jwks != fresh.jwks,
            None => true,
        };
        self.misses.remove(tenant_id.as_str());
        if changed {
            self.key_cache.invalidate_tenant(tenant_id);
        }
    }

    /// Fetch JWKS from the control plane and cache it.
    ///
    /// # Errors
    /// An implausible tenant id, a timeout, or a network, status or JSON
    /// failure.
    pub async fn refresh(&self, tenant_id: &TenantId) -> Result<Jwks> {
        check_tenant_id(tenant_id.as_str())?;
        let url = self.jwks_url(tenant_id.as_str())?;
        // One deadline for the slot and the request, so a caller holding a
        // stream's pre-auth permit waits at most this long.
        let fetched = match tokio::time::timeout(JWKS_FETCH_TIMEOUT, self.fetch(url)).await {
            Ok(fetched) => fetched,
            Err(_) => Err(anyhow::anyhow!("jwks fetch timed out")),
        };
        match fetched {
            Ok(jwks) => {
                self.insert_jwks(tenant_id, jwks.clone());
                Ok(jwks)
            }
            Err(err) => {
                if let Some(mut entry) = self.cache.get_mut(tenant_id.as_str()) {
                    entry.attempted_at = Instant::now();
                }
                Err(err)
            }
        }
    }

    async fn fetch(&self, url: reqwest::Url) -> Result<Jwks> {
        let _permit = self
            .fetches
            .acquire()
            .await
            .expect("jwks fetch semaphore is never closed");
        // The URL is deliberately not logged: a deployment may embed
        // credentials in the base URL.
        self.client
            .get(url)
            .send()
            .await
            .context("fetch jwks")?
            .error_for_status()
            .context("fetch jwks")?
            .json()
            .await
            .context("decode jwks")
    }

    /// Make sure usable JWKS for `tenant_id` is cached, fetching it if not.
    ///
    /// Keys past their renewal time but within [`JWKS_MAX_STALENESS`] count
    /// as usable: renewing them is the renewer's job, not the request's.
    pub(crate) async fn ensure_cached(&self, tenant_id: &str) -> Result<()> {
        if self.has_keys(tenant_id) {
            return Ok(());
        }
        check_tenant_id(tenant_id)?;
        if self.recent_miss(tenant_id) {
            anyhow::bail!("jwks unavailable for tenant (recent fetch failed)");
        }
        if let Some(catalog) = &self.catalog
            && catalog.rules_out(tenant_id).await
        {
            return Err(anyhow::Error::new(CredentialRefused(
                "unknown tenant".to_string(),
            )));
        }
        // One request per tenant however many streams ask at once: the
        // others wait here and find the answer in a cache.
        let tenant = TenantId::new(tenant_id);
        let flight = self.join_flight(tenant_id);
        let result = {
            let _guard = flight.lock().await;
            if self.has_keys(tenant_id) {
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
        self.leave_flight(tenant_id, &flight);
        result
    }

    /// Fetch a tenant's JWKS again because `token` names a `kid` the cached
    /// set lacks, and say whether the set now has it.
    ///
    /// Concurrent callers share one fetch, and a tenant is fetched this way at
    /// most once per [`JWKS_KID_REFETCH_GAP`]. A failed fetch keeps the cached
    /// keys.
    pub(crate) async fn refetch_for_kid(&self, tenant: &TenantId, token: &str) -> bool {
        let Some(kid) = jsonwebtoken::decode_header(token)
            .ok()
            .and_then(|header| header.kid)
        else {
            return false;
        };
        // Another request's fetch may have brought it since this one
        // verified. When the kid was known all along, the retry fails the
        // same way, which costs one more signature check on a bad token.
        if self.knows_kid(tenant.as_str(), &kid) {
            return true;
        }
        if !self.kid_refetch_due(tenant.as_str(), &kid) {
            return false;
        }
        let flight = self.join_flight(tenant.as_str());
        let found = {
            let _guard = flight.lock().await;
            // A fetch that finished while this one waited may have brought it.
            if self.knows_kid(tenant.as_str(), &kid) {
                true
            } else if !self.kid_refetch_due(tenant.as_str(), &kid) {
                false
            } else {
                match self.refresh(tenant).await {
                    Ok(jwks) => jwks.keys.iter().any(|key| key.kid == kid),
                    Err(err) => {
                        tracing::debug!(error = %err, "jwks fetch for an unknown kid failed");
                        false
                    }
                }
            }
        };
        self.leave_flight(tenant.as_str(), &flight);
        found
    }

    /// Fetch JWKS for every tenant in `tenants` that is not cached yet,
    /// giving up after `budget`. A failure is logged, not returned.
    ///
    /// Keys fetched only on demand leave a broker unable to verify any token
    /// of a tenant it has not served yet while the control plane is down,
    /// and that includes the credential on a write a peer forwards to it.
    pub(crate) async fn warm(&self, tenants: Vec<String>, budget: Duration) {
        use futures::StreamExt;
        let fetches = futures::stream::iter(tenants).for_each_concurrent(
            JWKS_FETCH_CONCURRENCY,
            |tenant_id| async move {
                if let Err(err) = self.ensure_cached(&tenant_id).await {
                    tracing::warn!(tenant_id, error = %err, "could not fetch a tenant's jwks ahead of use");
                }
            },
        );
        if tokio::time::timeout(budget, fetches).await.is_err() {
            tracing::warn!(?budget, "gave up fetching tenant jwks ahead of use");
        }
    }

    /// Renew cached JWKS in the background until `shutdown`: each tenant
    /// [`JWKS_RENEW_EVERY`] after its last fetch, and sooner again after a
    /// failure, with backoff.
    pub(crate) async fn renew_until(self: Arc<Self>, shutdown: CancellationToken) {
        let mut warned_at: Option<Instant> = None;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(JWKS_RENEW_TICK) => {}
            }
            let (failed, oldest) = self.renew_due().await;
            if failed == 0 {
                warned_at = None;
                continue;
            }
            let now = Instant::now();
            if warned_at.is_none_or(|at| now.duration_since(at) >= JWKS_WARN_EVERY) {
                warned_at = Some(now);
                tracing::warn!(
                    failed,
                    oldest_secs = oldest.as_secs(),
                    max_staleness_secs = self.max_staleness.as_secs(),
                    "could not renew tenant jwks; verifying with the cached keys until they \
                     reach the maximum staleness"
                );
            }
        }
    }

    /// One pass of the renewer: fetch every tenant that is due. Returns how
    /// many fetches failed and the age of the oldest cached set before it.
    async fn renew_due(&self) -> (usize, Duration) {
        use futures::StreamExt;
        let now = Instant::now();
        let mut oldest = Duration::ZERO;
        let due: Vec<String> = self
            .cache
            .iter()
            .filter_map(|entry| {
                oldest = oldest.max(now.saturating_duration_since(entry.fetched_at));
                (entry.renew_at <= now).then(|| entry.key().clone())
            })
            .collect();
        metrics::gauge!(OLDEST_AGE_GAUGE).set(oldest.as_secs_f64());
        let failed = std::sync::atomic::AtomicUsize::new(0);
        futures::stream::iter(due)
            .for_each_concurrent(JWKS_RENEW_CONCURRENCY, |tenant_id| {
                let failed = &failed;
                async move {
                    if !self.renew(&tenant_id).await {
                        failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            })
            .await;
        (failed.into_inner(), oldest)
    }

    /// Renew one tenant's JWKS. `false` when the fetch failed.
    async fn renew(&self, tenant_id: &str) -> bool {
        // A tenant the control plane removed has no keys to renew. Dropping
        // them stops its tokens verifying here.
        if let Some(catalog) = &self.catalog
            && catalog.rules_out(tenant_id).await
        {
            let tenant = TenantId::new(tenant_id);
            self.cache.remove(tenant_id);
            self.key_cache.invalidate_tenant(&tenant);
            return true;
        }
        let flight = self.join_flight(tenant_id);
        let renewed = {
            let _guard = flight.lock().await;
            // A request's fetch may have renewed it while this waited.
            let due = self
                .cache
                .get(tenant_id)
                .is_some_and(|entry| entry.renew_at <= Instant::now());
            if due {
                match self.refresh(&TenantId::new(tenant_id)).await {
                    Ok(_) => true,
                    Err(err) => {
                        metrics::counter!(RENEW_ERRORS_TOTAL).increment(1);
                        tracing::debug!(tenant_id, error = %err, "jwks renewal failed");
                        self.back_off(tenant_id);
                        false
                    }
                }
            } else {
                true
            }
        };
        self.leave_flight(tenant_id, &flight);
        renewed
    }

    fn back_off(&self, tenant_id: &str) {
        if let Some(mut entry) = self.cache.get_mut(tenant_id) {
            entry.failures = entry.failures.saturating_add(1);
            let doublings = entry.failures.min(6) - 1;
            let wait = JWKS_RETRY_MIN
                .saturating_mul(1 << doublings)
                .min(JWKS_RETRY_MAX);
            entry.renew_at = Instant::now() + wait;
        }
    }

    fn join_flight(&self, tenant_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.inflight.entry(tenant_id.to_string()).or_default())
    }

    fn leave_flight(&self, tenant_id: &str, flight: &Arc<tokio::sync::Mutex<()>>) {
        self.inflight
            .remove_if(tenant_id, |_, current| Arc::ptr_eq(current, flight));
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

    fn has_keys(&self, tenant_id: &str) -> bool {
        self.cache
            .get(tenant_id)
            .is_some_and(|entry| entry.expires_at > Instant::now())
    }

    fn knows_kid(&self, tenant_id: &str, kid: &str) -> bool {
        self.cache.get(tenant_id).is_some_and(|entry| {
            entry.expires_at > Instant::now() && entry.jwks.keys.iter().any(|key| key.kid == kid)
        })
    }

    /// Whether an unknown `kid` may cause a fetch now: the tenant's keys are
    /// cached, lack `kid`, and were not fetched in the last
    /// [`JWKS_KID_REFETCH_GAP`].
    fn kid_refetch_due(&self, tenant_id: &str, kid: &str) -> bool {
        self.cache.get(tenant_id).is_some_and(|entry| {
            entry.attempted_at.elapsed() >= JWKS_KID_REFETCH_GAP
                && !entry.jwks.keys.iter().any(|key| key.kid == kid)
        })
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

/// A credential the broker looked at and refused, as opposed to one it could
/// not check. Listeners that tell a device "wrong credentials" from "try
/// again" read this.
#[derive(Debug)]
pub struct CredentialRefused(pub String);

impl std::fmt::Display for CredentialRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CredentialRefused {}

/// Refuse a tenant id no control plane could have issued, before it goes
/// anywhere near a URL.
fn check_tenant_id(tenant_id: &str) -> Result<()> {
    if tenant_id.is_empty()
        || tenant_id.len() > TENANT_ID_MAX_BYTES
        || tenant_id == "."
        || tenant_id == ".."
        || tenant_id.chars().any(char::is_control)
    {
        return Err(anyhow::Error::new(CredentialRefused(
            "invalid tenant id".to_string(),
        )));
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

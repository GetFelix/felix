use axum::{Json, Router, routing::get};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::json;
use tokio::net::TcpListener;

use super::*;

#[test]
fn jwks_to_keys_rejects_invalid_components() {
    let jwks = Jwks {
        keys: vec![felix_authz::Jwk {
            kty: "OKP".to_string(),
            kid: "k1".to_string(),
            alg: "EdDSA".to_string(),
            use_field: felix_authz::KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some("not-base64".to_string()),
        }],
    };
    assert!(matches!(jwks_to_keys(&jwks), Err(AuthzError::Key(_))));
}

#[test]
fn jwks_to_keys_accepts_valid_ed25519_key() {
    let x = URL_SAFE_NO_PAD.encode(vec![9u8; 32]);
    let jwks = Jwks {
        keys: vec![felix_authz::Jwk {
            kty: "OKP".to_string(),
            kid: "k1".to_string(),
            alg: "EdDSA".to_string(),
            use_field: felix_authz::KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    };
    let keys = jwks_to_keys(&jwks).expect("jwks to keys");
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].kid, "k1");
    assert_eq!(keys[0].alg, Algorithm::EdDSA);
    assert_eq!(keys[0].public_key, [9u8; 32]);
}

#[tokio::test]
async fn ensure_jwks_cached_refreshes_when_expired() -> Result<()> {
    let x = URL_SAFE_NO_PAD.encode(vec![7u8; 32]);
    let jwks_body = json!({
        "keys": [{
            "kty": "OKP",
            "kid": "k1",
            "alg": "EdDSA",
            "use": "sig",
            "crv": "Ed25519",
            "x": x,
        }]
    });

    let app = Router::new().route(
        "/v1/tenants/t1/.well-known/jwks.json",
        get(move || async move { Json(jwks_body.clone()) }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server =
        tokio::spawn(async move { axum::serve(listener, app).await.context("serve jwks") });

    let mut key_store = ControlPlaneKeyStore::new(
        format!("http://{addr}"),
        Arc::new(TenantKeyCache::default()),
    );
    let tenant = TenantId::new("t1");
    // Past the maximum staleness the moment it lands.
    key_store.max_staleness = Duration::ZERO;
    key_store.insert_jwks(&tenant, Jwks { keys: vec![] });
    key_store.max_staleness = JWKS_MAX_STALENESS;

    ensure_jwks_cached(&key_store, "t1").await?;
    let cached = key_store.cached_jwks(&tenant).expect("cached jwks");
    assert_eq!(cached.keys.len(), 1);

    server.abort();
    Ok(())
}

#[tokio::test]
async fn ensure_jwks_cached_uses_existing_cache() -> Result<()> {
    let key_store = ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    );
    let tenant = TenantId::new("t1");
    let x = URL_SAFE_NO_PAD.encode(vec![1; 32]);
    key_store.insert_jwks(
        &tenant,
        Jwks {
            keys: vec![felix_authz::Jwk {
                kty: "OKP".to_string(),
                kid: "k1".to_string(),
                alg: "EdDSA".to_string(),
                use_field: felix_authz::KeyUse::Sig,
                crv: Some("Ed25519".to_string()),
                x: Some(x),
            }],
        },
    );
    ensure_jwks_cached(&key_store, "t1").await?;
    Ok(())
}

#[tokio::test]
async fn ensure_jwks_cached_fetch_fails_when_missing() {
    let key_store = ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    );
    let result = ensure_jwks_cached(&key_store, "t1").await;
    assert!(result.is_err());
}

#[test]
fn verification_keys_missing_jwks_returns_error() {
    let key_store = ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    );
    let tenant = TenantId::new("t1");
    assert!(matches!(
        key_store.verification_keys(&tenant),
        Err(AuthzError::MissingJwks(_))
    ));
}

#[test]
fn jwks_missing_returns_error() {
    let key_store = ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    );
    let tenant = TenantId::new("t1");
    assert!(matches!(
        key_store.jwks(&tenant),
        Err(AuthzError::MissingJwks(_))
    ));
}

#[test]
fn cached_jwks_expires_after_the_maximum_staleness() {
    let mut key_store = ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    );
    let tenant = TenantId::new("t1");
    key_store.max_staleness = Duration::ZERO;
    key_store.insert_jwks(&tenant, Jwks { keys: vec![] });
    assert!(key_store.cached_jwks(&tenant).is_none());
}

#[test]
fn current_signing_key_is_unavailable_for_broker() {
    let key_store = ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    );
    let tenant = TenantId::new("t1");
    assert!(matches!(
        key_store.current_signing_key(&tenant),
        Err(AuthzError::MissingSigningKey(_))
    ));
}

#[test]
fn broker_auth_new_builds_shared_store_and_normalizes_url() {
    let auth = BrokerAuth::new("http://controlplane/".to_string());
    assert_eq!(auth.key_store.base_url, "http://controlplane");

    let clone = auth.clone();
    assert!(Arc::ptr_eq(&auth.key_store, &clone.key_store));
    assert!(Arc::ptr_eq(&auth.verifier, &clone.verifier));
}

/// A stub control plane that counts requests and records their paths.
struct StubControlPlane {
    addr: std::net::SocketAddr,
    hits: Arc<std::sync::atomic::AtomicUsize>,
    paths: Arc<parking_lot::Mutex<Vec<String>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for StubControlPlane {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[derive(Clone, Copy)]
enum StubAnswer {
    Jwks { delay: Duration },
    NotFound,
    Hang,
}

async fn stub_control_plane(answer: StubAnswer) -> Result<StubControlPlane> {
    use axum::http::{StatusCode, Uri};
    use axum::response::IntoResponse;

    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let paths = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let (handler_hits, handler_paths) = (Arc::clone(&hits), Arc::clone(&paths));
    let app = Router::new().fallback(move |uri: Uri| {
        let hits = Arc::clone(&handler_hits);
        let paths = Arc::clone(&handler_paths);
        async move {
            hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            paths.lock().push(uri.path().to_string());
            match answer {
                StubAnswer::Jwks { delay } => {
                    tokio::time::sleep(delay).await;
                    let x = URL_SAFE_NO_PAD.encode([3u8; 32]);
                    Json(json!({"keys": [{
                        "kty": "OKP", "kid": "k1", "alg": "EdDSA", "use": "sig",
                        "crv": "Ed25519", "x": x,
                    }]}))
                    .into_response()
                }
                StubAnswer::NotFound => StatusCode::NOT_FOUND.into_response(),
                StubAnswer::Hang => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    StatusCode::OK.into_response()
                }
            }
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(StubControlPlane {
        addr,
        hits,
        paths,
        server,
    })
}

fn store_for(stub: &StubControlPlane) -> ControlPlaneKeyStore {
    ControlPlaneKeyStore::new(
        format!("http://{}/", stub.addr),
        Arc::new(TenantKeyCache::default()),
    )
}

fn hits(stub: &StubControlPlane) -> usize {
    stub.hits.load(std::sync::atomic::Ordering::SeqCst)
}

#[tokio::test]
async fn tenant_id_is_percent_encoded_into_one_path_segment() -> Result<()> {
    let stub = stub_control_plane(StubAnswer::NotFound).await?;
    let key_store = store_for(&stub);
    let _ = ensure_jwks_cached(&key_store, "a/b?c#d %").await;
    assert_eq!(
        stub.paths.lock().as_slice(),
        ["/v1/tenants/a%2Fb%3Fc%23d%20%25/.well-known/jwks.json"]
    );
    Ok(())
}

#[tokio::test]
async fn implausible_tenant_ids_are_refused_without_a_request() -> Result<()> {
    let stub = stub_control_plane(StubAnswer::NotFound).await?;
    let key_store = store_for(&stub);
    let too_long = "t".repeat(TENANT_ID_MAX_BYTES + 1);
    for tenant in ["", ".", "..", "a\nb", too_long.as_str()] {
        assert!(
            ensure_jwks_cached(&key_store, tenant).await.is_err(),
            "{tenant:?}"
        );
    }
    assert_eq!(hits(&stub), 0);
    Ok(())
}

#[tokio::test]
async fn concurrent_misses_for_one_tenant_share_one_fetch() -> Result<()> {
    let stub = stub_control_plane(StubAnswer::Jwks {
        delay: Duration::from_millis(200),
    })
    .await?;
    let key_store = store_for(&stub);
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let key_store = key_store.clone();
        tasks.push(tokio::spawn(async move {
            ensure_jwks_cached(&key_store, "t1").await
        }));
    }
    for task in tasks {
        task.await??;
    }
    assert_eq!(hits(&stub), 1);
    assert!(key_store.inflight.is_empty(), "flights are cleaned up");
    Ok(())
}

#[tokio::test]
async fn a_failed_fetch_is_remembered_briefly() -> Result<()> {
    let stub = stub_control_plane(StubAnswer::NotFound).await?;
    let key_store = store_for(&stub);
    assert!(ensure_jwks_cached(&key_store, "ghost").await.is_err());
    assert!(ensure_jwks_cached(&key_store, "ghost").await.is_err());
    assert_eq!(hits(&stub), 1);

    // Once the miss expires, the next auth asks again.
    key_store
        .misses
        .insert("ghost".to_string(), Instant::now() - Duration::from_secs(1));
    assert!(ensure_jwks_cached(&key_store, "ghost").await.is_err());
    assert_eq!(hits(&stub), 2);
    Ok(())
}

#[test]
fn remembered_misses_stay_bounded() {
    let key_store = ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    );
    for i in 0..(JWKS_MISS_CACHE_MAX + 100) {
        key_store.remember_miss(&format!("t{i}"));
    }
    assert_eq!(key_store.misses.len(), JWKS_MISS_CACHE_MAX);
}

#[tokio::test]
async fn a_seeded_catalog_rules_out_unknown_tenants_before_any_request() -> Result<()> {
    let stub = stub_control_plane(StubAnswer::Jwks {
        delay: Duration::ZERO,
    })
    .await?;
    let broker = Arc::new(Broker::new(felix_storage::EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    let seeded = CancellationToken::new();
    let key_store =
        store_for(&stub).with_tenant_catalog(TenantCatalog::new(broker, seeded.clone()));

    // Before the catalog is seeded it cannot tell unknown from not yet synced.
    ensure_jwks_cached(&key_store, "late").await?;
    assert_eq!(hits(&stub), 1);

    seeded.cancel();
    let err = ensure_jwks_cached(&key_store, "ghost")
        .await
        .expect_err("unknown tenant");
    assert!(err.to_string().contains("unknown tenant"), "{err}");
    assert_eq!(hits(&stub), 1);

    ensure_jwks_cached(&key_store, "t1").await?;
    assert_eq!(hits(&stub), 2);
    Ok(())
}

#[tokio::test]
async fn a_control_plane_that_never_answers_times_out() -> Result<()> {
    let stub = stub_control_plane(StubAnswer::Hang).await?;
    let key_store = store_for(&stub);
    let result = tokio::time::timeout(
        JWKS_FETCH_TIMEOUT + Duration::from_secs(5),
        ensure_jwks_cached(&key_store, "t1"),
    )
    .await
    .context("the fetch gives up on its own")?;
    assert!(result.is_err());
    Ok(())
}

/// A lease token skips the JWKS fetch, so its tenant id gets the same check
/// before any key is touched: the id is the client's, and nothing it names
/// may be kept.
#[tokio::test]
async fn a_lease_token_for_an_implausible_tenant_is_refused_before_verifying() {
    let lease = crate::serving::lease::LeaseTokens::from_keys(
        &base64::engine::general_purpose::STANDARD.encode([4u8; 32]),
        crate::serving::lease::DEFAULT_MAX_TTL,
    )
    .expect("keys");
    let auth =
        BrokerAuth::new("http://127.0.0.1:1".to_string()).with_lease_tokens(Some(Arc::new(lease)));
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"EdDSA","kid":"lease-junk"}"#);
    let junk = format!("{header}.e30.c2ln");
    for tenant in ["x".repeat(TENANT_ID_MAX_BYTES + 1), "..".to_string()] {
        let err = auth
            .authenticate(&tenant, &junk)
            .await
            .err()
            .expect("refused");
        assert!(err.to_string().contains("invalid tenant id"), "{err:#}");
    }
}

/// A control plane whose tenant JWKS can be changed or taken down mid-test.
struct KeyServer {
    addr: std::net::SocketAddr,
    hits: Arc<std::sync::atomic::AtomicUsize>,
    /// `None` answers 503, like a control plane that is down.
    jwks: Arc<parking_lot::Mutex<Option<Jwks>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for KeyServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl KeyServer {
    async fn start(jwks: Jwks) -> Result<Self> {
        use axum::http::StatusCode;
        use axum::response::IntoResponse;

        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let served = Arc::new(parking_lot::Mutex::new(Some(jwks)));
        let (handler_hits, handler_jwks) = (Arc::clone(&hits), Arc::clone(&served));
        let app = Router::new().fallback(move || {
            let hits = Arc::clone(&handler_hits);
            let jwks = handler_jwks.lock().clone();
            async move {
                hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Slow enough that concurrent callers overlap the fetch.
                tokio::time::sleep(Duration::from_millis(50)).await;
                match jwks {
                    Some(jwks) => Json(jwks).into_response(),
                    None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
                }
            }
        });
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            addr,
            hits,
            jwks: served,
            server,
        })
    }

    fn serve(&self, jwks: Option<Jwks>) {
        *self.jwks.lock() = jwks;
    }

    fn hits(&self) -> usize {
        self.hits.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn store(&self) -> ControlPlaneKeyStore {
        ControlPlaneKeyStore::new(
            format!("http://{}", self.addr),
            Arc::new(TenantKeyCache::default()),
        )
    }
}

/// A tenant signing key: `seed` is the private key, `kid` its name.
struct Signer {
    kid: &'static str,
    seed: u8,
}

impl Signer {
    fn public_key(&self) -> [u8; 32] {
        ed25519_dalek::SigningKey::from_bytes(&[self.seed; 32])
            .verifying_key()
            .to_bytes()
    }

    fn jwk(&self) -> felix_authz::Jwk {
        felix_authz::Jwk {
            kty: "OKP".to_string(),
            kid: self.kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: felix_authz::KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(URL_SAFE_NO_PAD.encode(self.public_key())),
        }
    }

    fn mint(&self, tenant_id: &str) -> String {
        let material = felix_authz::TenantKeyMaterial {
            kid: self.kid.to_string(),
            alg: Algorithm::EdDSA,
            private_key: [self.seed; 32],
            public_key: self.public_key(),
            jwks: Jwks {
                keys: vec![self.jwk()],
            },
        };
        let keys = std::collections::HashMap::from([(tenant_id.to_string(), material)]);
        felix_authz::FelixTokenIssuer::new(
            "felix-auth",
            "felix-broker",
            Duration::from_secs(900),
            Arc::new(keys),
        )
        .mint(&TenantId::new(tenant_id), "p:test", vec![])
        .expect("mint")
    }
}

const K1: Signer = Signer {
    kid: "k1",
    seed: 11,
};
const K2: Signer = Signer {
    kid: "k2",
    seed: 12,
};
const K3: Signer = Signer {
    kid: "k3",
    seed: 13,
};

fn key_set(signers: &[&Signer]) -> Jwks {
    Jwks {
        keys: signers.iter().map(|signer| signer.jwk()).collect(),
    }
}

/// Make the next unknown-kid fetch for `tenant_id` due, as if the last fetch
/// were long ago.
fn age_last_attempt(key_store: &ControlPlaneKeyStore, tenant_id: &str) {
    let mut entry = key_store.cache.get_mut(tenant_id).expect("cached");
    entry.attempted_at = Instant::now()
        .checked_sub(JWKS_KID_REFETCH_GAP)
        .expect("uptime");
}

/// The regression: keys used to expire an hour after a fetch, and the next
/// auth fetched them inline, so a control plane that was down at that moment
/// refused every token of the tenant.
#[tokio::test]
async fn existing_tenants_authenticate_while_the_control_plane_is_down_past_renewal() -> Result<()>
{
    let cp = KeyServer::start(key_set(&[&K1])).await?;
    let mut store = cp.store();
    store.renew_every = Duration::from_millis(100);
    let store = Arc::new(store);
    let auth = BrokerAuth::with_key_store(Arc::clone(&store));
    let token = K1.mint("t1");
    auth.authenticate("t1", &token).await?;
    assert_eq!(cp.hits(), 1);

    cp.serve(None);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (_, failed, _) = store.renew_due().await;
    assert_eq!(
        failed, 1,
        "the renewal reached the control plane and failed"
    );
    assert_eq!(cp.hits(), 2);

    auth.authenticate("t1", &token)
        .await
        .context("a renewal that failed keeps the cached keys")?;
    assert_eq!(cp.hits(), 2, "the request did not ask the control plane");

    // Backed off: the next pass does not ask again straight away.
    store.renew_due().await;
    assert_eq!(cp.hits(), 2);
    Ok(())
}

#[tokio::test]
async fn keys_past_the_maximum_staleness_are_not_used() -> Result<()> {
    let cp = KeyServer::start(key_set(&[&K1])).await?;
    let mut store = cp.store();
    store.max_staleness = Duration::from_millis(500);
    let auth = BrokerAuth::with_key_store(Arc::new(store));
    let token = K1.mint("t1");
    auth.authenticate("t1", &token).await?;

    cp.serve(None);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(auth.authenticate("t1", &token).await.is_err());

    // A control plane that answers again restores them.
    cp.serve(Some(key_set(&[&K1])));
    auth.key_store.misses.clear();
    auth.authenticate("t1", &token).await?;
    Ok(())
}

#[tokio::test]
async fn a_renewal_drops_a_key_the_control_plane_retired() -> Result<()> {
    let cp = KeyServer::start(key_set(&[&K1, &K2])).await?;
    let mut store = cp.store();
    store.renew_every = Duration::from_millis(100);
    let store = Arc::new(store);
    let auth = BrokerAuth::with_key_store(Arc::clone(&store));
    let old = K1.mint("t1");
    auth.authenticate("t1", &old).await?;

    cp.serve(Some(key_set(&[&K2])));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(store.renew_due().await.1, 0);
    assert!(auth.authenticate("t1", &old).await.is_err());
    auth.authenticate("t1", &K2.mint("t1")).await?;
    Ok(())
}

#[tokio::test]
async fn an_unknown_kid_fetches_the_rotated_set_once() -> Result<()> {
    let cp = KeyServer::start(key_set(&[&K1])).await?;
    let auth = Arc::new(BrokerAuth::with_key_store(Arc::new(cp.store())));
    auth.authenticate("t1", &K1.mint("t1")).await?;
    assert_eq!(cp.hits(), 1);

    // k2 is staged after this broker cached the set.
    cp.serve(Some(key_set(&[&K1, &K2])));
    age_last_attempt(&auth.key_store, "t1");
    let rotated = K2.mint("t1");
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let (auth, token) = (Arc::clone(&auth), rotated.clone());
        tasks.push(tokio::spawn(async move {
            auth.authenticate("t1", &token).await.map(|_| ())
        }));
    }
    for task in tasks {
        task.await??;
    }
    assert_eq!(cp.hits(), 2, "concurrent requests shared one fetch");

    // A kid the control plane does not have either is refused, and paced:
    // straight after a fetch it does not cause another.
    let unknown = K3.mint("t1");
    assert!(auth.authenticate("t1", &unknown).await.is_err());
    assert_eq!(cp.hits(), 2);
    age_last_attempt(&auth.key_store, "t1");
    assert!(auth.authenticate("t1", &unknown).await.is_err());
    assert_eq!(cp.hits(), 3);
    assert!(auth.authenticate("t1", &unknown).await.is_err());
    assert_eq!(cp.hits(), 3);

    // A failed fetch keeps the keys it had.
    cp.serve(None);
    age_last_attempt(&auth.key_store, "t1");
    assert!(auth.authenticate("t1", &unknown).await.is_err());
    assert_eq!(cp.hits(), 4);
    auth.authenticate("t1", &rotated).await?;
    Ok(())
}

#[tokio::test]
async fn a_tenant_never_seen_is_fetched_once_for_concurrent_auths() -> Result<()> {
    let cp = KeyServer::start(key_set(&[&K1])).await?;
    let auth = Arc::new(BrokerAuth::with_key_store(Arc::new(cp.store())));
    let token = K1.mint("t1");
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let (auth, token) = (Arc::clone(&auth), token.clone());
        tasks.push(tokio::spawn(async move {
            auth.authenticate("t1", &token).await.map(|_| ())
        }));
    }
    for task in tasks {
        task.await??;
    }
    assert_eq!(cp.hits(), 1);
    Ok(())
}

#[tokio::test]
async fn the_renewer_drops_a_tenant_the_catalog_no_longer_has() -> Result<()> {
    let cp = KeyServer::start(key_set(&[&K1])).await?;
    let broker = Arc::new(Broker::new(felix_storage::EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    let seeded = CancellationToken::new();
    seeded.cancel();
    let mut store = cp
        .store()
        .with_tenant_catalog(TenantCatalog::new(Arc::clone(&broker), seeded));
    // Not due: removal does not wait for the tenant's next renewal.
    store.renew_every = Duration::from_secs(3600);
    let store = Arc::new(store);
    let auth = BrokerAuth::with_key_store(Arc::clone(&store));
    let token = K1.mint("t1");
    auth.authenticate("t1", &token).await?;

    broker.remove_tenant("t1").await?;
    store.renew_due().await;
    assert!(store.cached_jwks(&TenantId::new("t1")).is_none());
    assert!(auth.authenticate("t1", &token).await.is_err());
    Ok(())
}

#[tokio::test]
async fn the_spawned_renewer_drops_a_retired_key_and_stops_on_shutdown() -> Result<()> {
    let cp = KeyServer::start(key_set(&[&K1, &K2])).await?;
    let mut store = cp.store();
    store.renew_every = Duration::from_millis(100);
    store.renew_tick = Duration::from_millis(20);
    let store = Arc::new(store);
    let auth = BrokerAuth::with_key_store(Arc::clone(&store));
    let old = K1.mint("t1");
    auth.authenticate("t1", &old).await?;

    let shutdown = CancellationToken::new();
    let renewer = tokio::spawn(Arc::clone(&store).renew_until(shutdown.clone()));
    // A failing stretch first: the cached keys keep verifying.
    cp.serve(None);
    tokio::time::sleep(Duration::from_millis(300)).await;
    auth.authenticate("t1", &old).await?;

    cp.serve(Some(key_set(&[&K2])));
    // Backoff after the failures is at least 15 s, so step the renewal along.
    store.cache.get_mut("t1").expect("cached").renew_at = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(10);
    while auth.authenticate("t1", &old).await.is_ok() {
        assert!(Instant::now() < deadline, "the renewer never dropped k1");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), renewer)
        .await
        .context("the renewer stops on shutdown")??;
    Ok(())
}

#[tokio::test]
async fn unknown_kid_fetches_share_one_budget_across_tenants() -> Result<()> {
    let cp = KeyServer::start(key_set(&[&K1])).await?;
    let auth = BrokerAuth::with_key_store(Arc::new(cp.store()));
    let tenants: Vec<String> = (0..10).map(|i| format!("t{i}")).collect();
    for tenant in &tenants {
        auth.authenticate(tenant, &K1.mint(tenant)).await?;
    }
    let before = cp.hits();
    for tenant in &tenants {
        age_last_attempt(&auth.key_store, tenant);
        assert!(auth.authenticate(tenant, &K3.mint(tenant)).await.is_err());
    }
    assert_eq!(
        cp.hits() - before,
        JWKS_KID_REFETCH_BURST as usize,
        "made-up kids across tenants stop at the broker-wide budget"
    );
    Ok(())
}

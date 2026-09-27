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

    let key_store = ControlPlaneKeyStore::new(
        format!("http://{addr}"),
        Arc::new(TenantKeyCache::default()),
    );
    let tenant = TenantId::new("t1");
    key_store.cache.insert(
        tenant.to_string(),
        CachedJwks {
            jwks: Jwks { keys: vec![] },
            expires_at: Instant::now() - Duration::from_secs(1),
        },
    );

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
fn cached_jwks_expires_after_ttl() {
    let key_store = ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    );
    let tenant = TenantId::new("t1");
    key_store.cache.insert(
        tenant.to_string(),
        CachedJwks {
            jwks: Jwks { keys: vec![] },
            expires_at: Instant::now() - Duration::from_secs(5),
        },
    );
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

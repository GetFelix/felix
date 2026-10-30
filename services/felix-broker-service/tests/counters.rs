//! A counter as a log semantic, end to end.
//!
//! The storage tests prove the fold; this proves the wiring: a client's add
//! goes over QUIC, is routed like a cache key, answers with the sum including
//! itself, and the sum outlives the broker that accumulated it.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{FelixTokenIssuer, Jwks, TenantId, TenantKeyCache, TenantKeyMaterial};
use felix_broker::{Broker, CacheMetadata};
use felix_broker_service::serving::{auth::BrokerAuth, quic};
use felix_client::{ClientConfig, Connection};
use felix_storage::log::{FsyncMode, LogConfig};
use felix_storage::{CounterStore, LogCache};
use felix_transport::{QuicServer, TransportConfig};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

const DEMO_PRIVATE_KEY: [u8; 32] = [42u8; 32];
const CACHE: &str = "metrics";

struct DemoAuthBundle {
    auth: Arc<BrokerAuth>,
    tokens: HashMap<String, String>,
}

struct Running {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    tokens: HashMap<String, String>,
    server: Arc<QuicServer>,
    task: tokio::task::JoinHandle<Result<()>>,
}

fn log_config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn start(root: &std::path::Path) -> Result<Running> {
    let cache = LogCache::open(root.join("caches"), log_config()).context("open the cache log")?;
    let counters =
        CounterStore::open(root.join("counters"), log_config()).context("open counters")?;
    let broker = Arc::new(Broker::new(Box::new(cache)).with_counters(Arc::new(counters)));
    start_broker(broker).await
}

/// A broker with no durable storage: no counter store, and the feature is not
/// advertised.
async fn start_ephemeral() -> Result<Running> {
    let broker = Arc::new(Broker::new(felix_storage::EphemeralCache::new().into()));
    start_broker(broker).await
}

async fn start_broker(broker: Arc<Broker>) -> Result<Running> {
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", CACHE, CacheMetadata::default())
        .await?;

    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let demo_auth = demo_auth_for_tenants(&["t1"], Duration::from_secs(900))?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let task = tokio::spawn(quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        demo_auth.auth,
    ));

    Ok(Running {
        addr,
        cert,
        tokens: demo_auth.tokens,
        server,
        task,
    })
}

impl Running {
    async fn client(&self) -> Result<Connection> {
        let mut config = build_client_config(self.cert.clone())?;
        config.auth_tenant_id = Some("t1".to_string());
        config.auth_token = self.tokens.get("t1").cloned();
        Connection::connect(self.addr, "localhost", config).await
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
        drop(self.server);
    }
}

/// **The counter contract over the wire.** Each add answers with the sum
/// including itself — one round trip to increment and know where you stand —
/// deltas may be negative, and never-written stays distinct from zero.
#[tokio::test]
async fn an_add_answers_with_the_sum_including_it() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    assert_eq!(
        client.counter_get("t1", "default", CACHE, "views").await?,
        None,
        "a counter never written is absent, not zero",
    );
    assert_eq!(
        client
            .counter_add("t1", "default", CACHE, "views", 5)
            .await?,
        5
    );
    assert_eq!(
        client
            .counter_add("t1", "default", CACHE, "views", -2)
            .await?,
        3
    );
    assert_eq!(
        client.counter_get("t1", "default", CACHE, "views").await?,
        Some(3)
    );
    // Cancelled out is still an answer.
    assert_eq!(
        client
            .counter_add("t1", "default", CACHE, "views", -3)
            .await?,
        0
    );
    assert_eq!(
        client.counter_get("t1", "default", CACHE, "views").await?,
        Some(0)
    );

    running.stop().await;
    Ok(())
}

/// A counter and a cache value may share a key and are unrelated: same scope,
/// same routing, different stores.
#[tokio::test]
async fn a_counter_and_a_cache_value_sharing_a_key_are_unrelated() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    client
        .cache_put("t1", "default", CACHE, "k", b"cached".to_vec().into(), None)
        .await?;
    client.counter_add("t1", "default", CACHE, "k", 7).await?;

    assert_eq!(
        client
            .cache_get("t1", "default", CACHE, "k")
            .await?
            .as_deref(),
        Some(&b"cached"[..]),
        "the counter overwrote the cache value",
    );
    assert_eq!(
        client.counter_get("t1", "default", CACHE, "k").await?,
        Some(7),
        "the cache value shadowed the counter",
    );
    client.cache_delete("t1", "default", CACHE, "k").await?;
    assert_eq!(
        client.counter_get("t1", "default", CACHE, "k").await?,
        Some(7),
        "deleting the cache value deleted the counter",
    );

    running.stop().await;
    Ok(())
}

/// **The sum survives a restart.** The fold is rebuilt from the log; nothing
/// in memory is the source of truth.
#[tokio::test]
async fn a_counter_survives_a_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;

    let running = start(dir.path()).await?;
    let client = running.client().await?;
    for delta in [10, 20, 12] {
        client
            .counter_add("t1", "default", CACHE, "views", delta)
            .await?;
    }
    drop(client);
    running.stop().await;

    let restarted = start(dir.path()).await?;
    assert_eq!(
        restarted
            .client()
            .await?
            .counter_get("t1", "default", CACHE, "views")
            .await?,
        Some(42),
        "the counter lost its sum across a restart",
    );
    restarted.stop().await;
    Ok(())
}

/// A broker with no durable storage does not advertise the feature, and the
/// client refuses before sending anything — probing would cost the connection.
#[tokio::test]
async fn a_broker_without_durable_storage_does_not_offer_counters() -> Result<()> {
    let running = start_ephemeral().await?;
    let client = running.client().await?;

    let err = client
        .counter_add("t1", "default", CACHE, "views", 1)
        .await
        .expect_err("a counter on an ephemeral broker must be refused client-side");
    assert!(
        err.to_string().contains("does not support counters"),
        "{err}"
    );

    running.stop().await;
    Ok(())
}

fn demo_auth_for_tenants(tenants: &[&str], ttl: Duration) -> Result<DemoAuthBundle> {
    let mut key_store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    let mut tokens = HashMap::new();
    let mut jwks_per_tenant = HashMap::new();

    for tenant in tenants {
        let signing_key = Ed25519SigningKey::from_bytes(&DEMO_PRIVATE_KEY);
        let public_key = signing_key.verifying_key().to_bytes();
        let jwks = build_demo_jwks("demo-k1", &public_key)?;
        key_store.insert(
            (*tenant).to_string(),
            TenantKeyMaterial {
                kid: "demo-k1".to_string(),
                alg: jsonwebtoken::Algorithm::EdDSA,
                private_key: DEMO_PRIVATE_KEY,
                public_key,
                jwks: jwks.clone(),
            },
        );
        jwks_per_tenant.insert((*tenant).to_string(), jwks);
    }

    let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", ttl, Arc::new(key_store));
    for tenant in tenants {
        let perms = vec![
            format!("tenant.manage:tenant:{tenant}"),
            format!("ns.manage:namespace:{tenant}/*"),
            format!("cache.read:cache:{tenant}/*/*"),
            format!("cache.write:cache:{tenant}/*/*"),
            format!("stream.publish:stream:{tenant}/*/*"),
            format!("stream.subscribe:stream:{tenant}/*/*"),
        ];
        tokens.insert(
            (*tenant).to_string(),
            issuer.mint(&TenantId::new(*tenant), "p:demo", perms)?,
        );
    }

    let key_store = Arc::new(
        felix_broker_service::serving::auth::ControlPlaneKeyStore::new(
            "http://127.0.0.1".to_string(),
            Arc::new(TenantKeyCache::default()),
        ),
    );
    for (tenant, jwks) in jwks_per_tenant {
        key_store.insert_jwks(&TenantId::new(&tenant), jwks);
    }

    Ok(DemoAuthBundle {
        auth: Arc::new(BrokerAuth::with_key_store(key_store)),
        tokens,
    })
}

fn build_demo_jwks(kid: &str, public_key: &[u8; 32]) -> Result<Jwks> {
    Ok(Jwks {
        keys: vec![felix_authz::Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: felix_authz::KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(URL_SAFE_NO_PAD.encode(public_key)),
        }],
    })
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    Ok((
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?,
        cert_der,
    ))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    ClientConfig::from_env_or_yaml(
        QuinnClientConfig::with_root_certificates(Arc::new(roots))?,
        None,
    )
}

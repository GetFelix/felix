//! A publish handler must not hold a span guard across an `.await`.
//!
//! The registry keeps entered spans on a per-thread stack. A guard held across
//! an await exits on whichever worker resumed the task, leaving a stale id on
//! the stack of the worker that entered it. The next span created on that
//! worker picks the stale id up as its parent and, if the span has meanwhile
//! closed, the registry panics with "tried to clone a span that already
//! closed" -- killing the handler. A slow `on_close` (an OTLP layer exporting
//! the span) widens that window enough for it to happen under load.
//!
//! This installs a layer that counts exits on a different thread than the
//! matching enter, and drives many concurrent binary batch publishes through a
//! one-slot ingress queue so the handler really does suspend and migrate.
//!
//! Run with `cargo test -p felix-broker-service --test tracing_span_guard`.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::ThreadId;
use std::time::Duration;

use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, StreamMetadata};
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_client::{ClientConfig, Connection};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use jsonwebtoken::Algorithm;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tracing::span;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const TEST_PRIVATE_KEY: [u8; 32] = [10u8; 32];

static CROSS_THREAD_EXITS: AtomicUsize = AtomicUsize::new(0);

struct EnteredOn(ThreadId);

/// Flags a span exited on another thread than it was entered on, and closes
/// slowly the way an exporting layer does.
struct GuardCheck;

impl<S> Layer<S> for GuardCheck
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_enter(&self, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            span.extensions_mut()
                .replace(EnteredOn(std::thread::current().id()));
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(EnteredOn(thread)) = span.extensions().get::<EnteredOn>()
            && *thread != std::thread::current().id()
        {
            CROSS_THREAD_EXITS.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn on_close(&self, _id: span::Id, _ctx: Context<'_, S>) {
        std::thread::sleep(Duration::from_micros(50));
    }
}

fn auth_fixture(tenant_id: &str) -> (String, Arc<BrokerAuth>) {
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let jwks = Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: "k1".to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(URL_SAFE_NO_PAD.encode(public_key)),
        }],
    };
    let mut key_materials = std::collections::HashMap::new();
    key_materials.insert(
        tenant_id.to_string(),
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TEST_PRIVATE_KEY,
            public_key,
            jwks: jwks.clone(),
        },
    );
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(key_materials),
    );
    let token = issuer
        .mint(
            &TenantId::new(tenant_id),
            "p:test",
            vec!["stream.publish:stream:t1/*/*".to_string()],
        )
        .expect("mint token");
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    (token, Arc::new(BrokerAuth::with_key_store(key_store)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_span_never_exits_on_another_thread() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(GuardCheck)
        .init();

    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;

    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der: CertificateDer<'static> = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    // One ingress slot with waiting admission, so enqueue really suspends.
    let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
    config.pub_queue_depth = 1;
    config.pub_workers_per_conn = 1;
    config.pub_ingress_wait = true;
    let (token, auth) = auth_fixture("t1");
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        auth,
    ));

    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut publishers = Vec::new();
    for _ in 0..16 {
        let mut client_config = ClientConfig::from_env_or_yaml(quinn.clone(), None)?;
        client_config.auth_tenant_id = Some("t1".to_string());
        client_config.auth_token = Some(token.clone());
        publishers.push(tokio::spawn(async move {
            let client = Connection::connect(addr, "localhost", client_config).await?;
            let publisher = client.publisher().await?;
            let payloads = vec![vec![7u8; 256]; 32];
            for _ in 0..200 {
                publisher
                    .publish_batch_binary("t1", "default", "orders", &payloads)
                    .await?;
            }
            anyhow::Ok(())
        }));
    }
    for publisher in publishers {
        publisher.await??;
    }
    server_task.abort();

    assert_eq!(
        CROSS_THREAD_EXITS.load(Ordering::Relaxed),
        0,
        "a span guard was held across an await and exited on another worker"
    );
    Ok(())
}

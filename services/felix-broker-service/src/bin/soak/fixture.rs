//! Auth and transport fixtures, and an in-process broker to run load against.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
    TenantKeyStore,
};
use felix_broker::{Broker, StreamMetadata};
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_client::ClientConfig;
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use jsonwebtoken::Algorithm;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

// Fixed keypair so runs are deterministic and need no control plane.
pub(crate) const TEST_PRIVATE_KEY: [u8; 32] = [37u8; 32];

// Drain budget the child broker is configured with. Kept short so a restart
// cycle is quick, and passed to the child via the same env var production uses
// so the soak exercises the real configuration path. The per-connection grace
// inside the broker is derived from this, so the child's own deadline must be
// this value — not a separate constant that could be smaller than the grace it
// is meant to bound.
pub(crate) const CHILD_DRAIN_BUDGET_MS: u64 = 6_000;

pub(crate) const CHILD_DRAIN_DEADLINE: Duration = Duration::from_millis(CHILD_DRAIN_BUDGET_MS);

pub(crate) const TENANT: &str = "t1";

pub(crate) const NAMESPACE: &str = "default";

pub(crate) const STREAM: &str = "soak";

pub(crate) struct AuthFixture {
    pub(crate) token: String,
    pub(crate) broker_auth: Arc<BrokerAuth>,
}

// Mirrors the conformance runner's fixture: a deterministic Ed25519 keypair and
// an in-memory JWKS, so the soak needs no control plane while still going
// through the real token-verification path.
pub(crate) fn build_auth_fixture() -> Result<AuthFixture> {
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
    let mut keys = HashMap::new();
    keys.insert(
        TENANT.to_string(),
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TEST_PRIVATE_KEY,
            public_key,
            jwks: jwks.clone(),
        },
    );
    let key_store: Arc<dyn TenantKeyStore> = Arc::new(keys);
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(3600),
        key_store,
    );
    let token = issuer.mint(
        &TenantId::new(TENANT),
        "soak",
        vec![
            format!("stream.publish:stream:{TENANT}/{NAMESPACE}/*"),
            format!("stream.subscribe:stream:{TENANT}/{NAMESPACE}/*"),
        ],
    )?;

    let cp_store = Arc::new(ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    cp_store.insert_jwks(&TenantId::new(TENANT), jwks);
    Ok(AuthFixture {
        token,
        broker_auth: Arc::new(BrokerAuth::with_key_store(cp_store)),
    })
}

pub(crate) fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    Ok((
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?,
        cert_der,
    ))
}

pub(crate) fn client_config(
    cert: &CertificateDer<'static>,
    auth: &AuthFixture,
) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone())?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(TENANT.to_string());
    config.auth_token = Some(auth.token.clone());
    Ok(config)
}

/// A running in-process broker plus the handles needed to drain it.
pub(crate) struct BrokerHarness {
    pub(crate) addr: SocketAddr,
    pub(crate) cert: CertificateDer<'static>,
    pub(crate) accept_shutdown: CancellationToken,
    pub(crate) connections: TaskTracker,
    pub(crate) accept_task: tokio::task::JoinHandle<()>,
    pub(crate) _server: Arc<QuicServer>,
}

pub(crate) async fn start_broker(auth: &AuthFixture) -> Result<BrokerHarness> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant(TENANT).await?;
    broker.register_namespace(TENANT, NAMESPACE).await?;
    broker
        .register_stream(TENANT, NAMESPACE, STREAM, StreamMetadata::default())
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let limit =
        felix_broker_service::serving::quic::ConnectionLimit::new(config.max_client_connections);
    let accept_shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    let accept_task = {
        let server = Arc::clone(&server);
        let accept_shutdown = accept_shutdown.clone();
        let connections = connections.clone();
        let auth = Arc::clone(&auth.broker_auth);
        tokio::spawn(async move {
            let limits =
                felix_broker_service::serving::limits::ListenerLimits::from_config(&config);
            if let Err(err) = felix_broker_service::serving::quic::serve_with_shutdown(
                server,
                broker,
                config,
                auth,
                accept_shutdown,
                connections,
                // The soak harness is a single-node broker: nothing to route
                // against, and no peers to forward to.
                Default::default(),
                limit,
                limits,
            )
            .await
            {
                eprintln!("accept loop exited: {err}");
            }
        })
    };

    Ok(BrokerHarness {
        addr,
        cert,
        accept_shutdown,
        connections,
        accept_task,
        _server: server,
    })
}

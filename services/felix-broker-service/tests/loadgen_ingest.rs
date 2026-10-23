//! A fire-and-forget loadgen ingest run ends with every published record
//! accepted by the broker.
//!
//! The perf campaign compares bytes sent with bytes accepted to prove a run
//! lossless. Dropping the connection at the deadline cancelled publishes still
//! queued at the broker, so sent exceeded accepted by a few batches per
//! connection; the run now finishes its publishers instead.
//!
//! Run with `cargo test -p felix-broker-service loadgen_ingest`.
use std::sync::Arc;
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
use felix_client::ClientConfig;
use felix_loadgen::{Common, IngestOptions, Scenario};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use jsonwebtoken::Algorithm;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;

const TEST_PRIVATE_KEY: [u8; 32] = [9u8; 32];

struct AuthFixture {
    tenant_id: String,
    token: String,
    auth: Arc<BrokerAuth>,
}

fn auth_fixture(tenant_id: &str, perms: Vec<String>) -> AuthFixture {
    // Build a deterministic Ed25519 keypair and JWKS for repeatable auth tests.
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let jwks = jwks_from_public_key(&public_key, "k1");
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
    // Mint a Felix token to authenticate the QUIC client.
    let token = issuer
        .mint(&TenantId::new(tenant_id), "p:test", perms)
        .expect("mint token");

    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    // Inject JWKS directly to avoid network dependencies in tests.
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    let auth = Arc::new(BrokerAuth::with_key_store(key_store));
    AuthFixture {
        tenant_id: tenant_id.to_string(),
        token,
        auth,
    }
}

fn jwks_from_public_key(public_key: &[u8], kid: &str) -> Jwks {
    // Encode Ed25519 public key into JWK `x` using base64url.
    let x = URL_SAFE_NO_PAD.encode(public_key);
    Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    }
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    // Self-signed cert is sufficient for loopback QUIC tests.
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    Ok((server_config, cert_der))
}

fn build_quinn_client_config(cert: CertificateDer<'static>) -> Result<QuinnClientConfig> {
    // Trust the test server certificate to avoid TLS validation failures.
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    Ok(QuinnClientConfig::with_root_certificates(Arc::new(roots))?)
}

fn build_client_config(cert: CertificateDer<'static>, auth: &AuthFixture) -> Result<ClientConfig> {
    // Embed auth token in client config for automated auth handshake.
    let quinn = build_quinn_client_config(cert)?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(auth.tenant_id.clone());
    config.auth_token = Some(auth.token.clone());
    Ok(config)
}

/// Records the run's readiness pre-flight publishes before the measured part.
const PREFLIGHT_RECORDS: u64 = 50;

#[tokio::test]
#[serial]
async fn fire_and_forget_ingest_publishes_exactly_what_the_broker_accepts() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
        // Without it the broker drops unacked publishes when its queue is full.
        std::env::set_var("FELIX_PUB_INGRESS_WAIT", "true");
    }
    let dir = tempfile::tempdir()?;
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..Default::default()
        },
    )?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let auth = auth_fixture("t1", vec!["stream.publish:stream:t1/*/*".to_string()]);
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let mut common = Common::new(
        vec![addr],
        "t1".to_string(),
        "default".to_string(),
        auth.token.clone(),
    );
    common.client_config = Some(build_client_config(cert, &auth)?);
    common.payload_bytes = 4096;
    common.concurrency = 4;
    common.batch = 16;
    common.prose = false;
    let result = felix_loadgen::run(
        &common,
        &Scenario::Ingest {
            stream: "orders".to_string(),
            options: IngestOptions {
                shards: Some(1),
                duration: Some(Duration::from_secs(2)),
                ..Default::default()
            },
        },
    )
    .await?;

    let published = result["published"].as_u64().expect("published");
    assert!(published > 0, "the run published nothing: {result}");
    // The broker applies a stream's queued publishes after the client's FIN
    // is answered, so wait for the tail to stop moving.
    let mut tail = 0;
    let mut still = 0;
    for _ in 0..600 {
        let now = broker
            .cursor_tail("t1", "default", "orders", 0)
            .await?
            .next_seq();
        still = if now == tail { still + 1 } else { 0 };
        tail = now;
        if still >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let accepted = tail - PREFLIGHT_RECORDS;
    let payload = result["payload_bytes"].as_u64().expect("payload_bytes");
    assert_eq!(
        published * payload,
        accepted * payload,
        "sent bytes must equal accepted bytes: {result}"
    );
    assert_eq!(result["cut_off_records"], 0, "{result}");
    assert_eq!(result["finish_failed"], 0, "{result}");
    assert!(result["finish_ms"].is_number(), "{result}");

    server_task.abort();
    Ok(())
}

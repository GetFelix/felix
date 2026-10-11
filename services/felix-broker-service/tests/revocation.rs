//! Revoking a principal ends what its tokens already opened.
//!
//! The control-plane sync is stood in for by writing to the key store's
//! revocations directly, which is all the sync does with what it reads.
//!
//! Run with `cargo test -p felix-broker-service --test revocation`.
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
use felix_client::{BrokerError, Client, ClientConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::ErrorCode;
use jsonwebtoken::Algorithm;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;
use tokio::time::timeout;

const TEST_PRIVATE_KEY: [u8; 32] = [7u8; 32];
const PRINCIPAL: &str = "p:test";

struct Fixture {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    token: String,
    key_store: Arc<ControlPlaneKeyStore>,
    server_task: tokio::task::JoinHandle<Result<()>>,
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn start() -> Result<Fixture> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;

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
        "t1".to_string(),
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
    let token = issuer.mint(
        &TenantId::new("t1"),
        PRINCIPAL,
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    )?;
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new("t1"), jwks);
    let auth = Arc::new(BrokerAuth::with_key_store(Arc::clone(&key_store)));

    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        server, broker, config, auth,
    ));
    Ok(Fixture {
        addr,
        cert: cert_der,
        token,
        key_store,
        server_task,
    })
}

fn client_config(fixture: &Fixture) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(fixture.cert.clone())?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some("t1".to_string());
    config.auth_token = Some(fixture.token.clone());
    Ok(config)
}

fn is_revoked(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<BrokerError>()
            .is_some_and(|broker| broker.code == ErrorCode::Revoked)
    })
}

#[tokio::test]
#[serial]
async fn revoking_a_principal_ends_its_open_subscription() -> Result<()> {
    let fixture = start().await?;
    let client = Client::connect(fixture.addr, "localhost", client_config(&fixture)?).await?;
    let mut sub = client.subscribe("t1", "default", "orders").await?;
    let publisher = client.publisher().await?;
    publisher
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"before".to_vec()],
            felix_wire::AckMode::None,
        )
        .await?;
    let first = timeout(Duration::from_secs(2), sub.next_event())
        .await??
        .expect("an event before the revocation");
    assert_eq!(first.payload.as_ref(), b"before");

    fixture
        .key_store
        .revocations()
        .revoke("t1", PRINCIPAL, now_secs());

    let ended = timeout(Duration::from_secs(2), sub.next_event()).await?;
    let Err(err) = ended else {
        anyhow::bail!("the subscription should end with an error, not a clean close");
    };
    assert!(is_revoked(&err), "expected the revoked code, got {err:#}");

    fixture.server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn revoking_a_principal_ends_its_publish_stream_and_refuses_its_token() -> Result<()> {
    let fixture = start().await?;
    let client = Client::connect(fixture.addr, "localhost", client_config(&fixture)?).await?;
    let publisher = client.publisher().await?;
    publisher
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"before".to_vec()],
            felix_wire::AckMode::PerBatch,
        )
        .await?;

    fixture
        .key_store
        .revocations()
        .revoke("t1", PRINCIPAL, now_secs());

    // The open stream is ended; what the next publish sees is that end.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let result = publisher
            .publish_batch(
                "t1",
                "default",
                "orders",
                vec![b"after".to_vec()],
                felix_wire::AckMode::PerBatch,
            )
            .await;
        if result.is_err() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a revoked publish stream kept accepting"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // And the same token opens nothing new, with a code that says why.
    let err = match client.subscribe("t1", "default", "orders").await {
        Ok(_) => anyhow::bail!("a revoked token opened a subscription"),
        Err(err) => err,
    };
    assert!(is_revoked(&err), "expected the revoked code, got {err:#}");

    fixture.server_task.abort();
    Ok(())
}

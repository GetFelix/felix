//! Text publish-batch latency integration test.
//!
//! Exercises the broker and client over QUIC using JSON (text) frames to ensure
//! large publish batches do not drop and that auth/JWKS wiring works end-to-end.
use std::sync::Arc;

use anyhow::{Context, Result};
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
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::time::{Duration, timeout};

// Static test keys keep JWT/JWKS generation deterministic for the test run.
const TEST_PRIVATE_KEY: [u8; 32] = [12u8; 32];

struct AuthFixture {
    tenant_id: String,
    token: String,
    auth: Arc<BrokerAuth>,
}

fn auth_fixture(tenant_id: &str) -> AuthFixture {
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
    let token = issuer
        .mint(
            &TenantId::new(tenant_id),
            "p:test",
            vec![
                "stream.publish:stream:t1/*/*".to_string(),
                "stream.subscribe:stream:t1/*/*".to_string(),
            ],
        )
        .expect("mint token");

    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    let auth = Arc::new(BrokerAuth::with_key_store(key_store));
    AuthFixture {
        tenant_id: tenant_id.to_string(),
        token,
        auth,
    }
}

fn jwks_from_public_key(public_key: &[u8], kid: &str) -> Jwks {
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

#[tokio::test]
async fn text_publish_batch_large_payload_no_drop() -> Result<()> {
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(6000)?
            .with_log_capacity(6000)?,
    );
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "latency", StreamMetadata::default())
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let mut broker_config = felix_broker_service::config::BrokerConfig::from_env()?;
    broker_config.subscriber_lane_queue_policy = felix_broker::SubQueuePolicy::Block;
    broker_config.subscriber_lane_queue_depth = 8192;
    let auth = auth_fixture("t1");
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        broker,
        broker_config,
        Arc::clone(&auth.auth),
    ));

    let client = Connection::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let mut sub = client.subscribe("t1", "default", "latency").await?;
    let publisher = client.publisher().await?;

    let payload = large_payload();
    let total = 5000usize;
    let batch_size = 64usize;
    let mut remaining = total;
    while remaining > 0 {
        let count = remaining.min(batch_size);
        let payloads = (0..count).map(|_| payload.clone()).collect::<Vec<_>>();
        // The JSON encoding on purpose -- this test exists to measure it against the binary one, so it has to keep reaching the deprecated call.
        #[allow(deprecated)]
        publisher
            .publish_batch_json(
                "t1",
                "default",
                "latency",
                payloads,
                felix_wire::AckMode::None,
            )
            .await?;
        remaining -= count;
    }

    let recv_all = async {
        let mut received = 0usize;
        while received < total {
            if let Some(_event) = sub.next_event().await? {
                received += 1;
            } else {
                break;
            }
        }
        Result::<usize>::Ok(received)
    };
    let received = timeout(Duration::from_secs(10), recv_all)
        .await
        .context("receive timeout")??;

    let broker_counters = felix_broker_service::serving::quic::frame_counters_snapshot();
    let client_counters = felix_client::frame_counters_snapshot();
    assert_eq!(received, total);
    assert_eq!(broker_counters.frames_in_err, 0);
    assert_eq!(client_counters.frames_in_err, 0);

    server_task.abort();
    Ok(())
}

fn large_payload() -> Vec<u8> {
    let mut payload = Vec::with_capacity(4096);
    for _ in 0..16 {
        for byte in 0u8..=255 {
            payload.push(byte);
        }
    }
    payload
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_client_config(cert: CertificateDer<'static>, auth: &AuthFixture) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.client_sub_queue_policy = felix_client::ClientSubQueuePolicy::Block;
    config.client_sub_queue_capacity = 8192;
    config.auth_tenant_id = Some(auth.tenant_id.clone());
    config.auth_token = Some(auth.token.clone());
    Ok(config)
}

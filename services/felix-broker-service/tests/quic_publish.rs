//! QUIC publish integration tests for broker control streams.
//!
//! Validate publish-path behavior over real QUIC connections, including:
//! - auth enforcement and stream existence errors
//! - ack modes and commit-ack paths
//! - binary batch handling and decode failures
//! - control-stream error responses for malformed requests
//!
//! These tests use ephemeral QUIC servers and in-memory broker state.
//!
//! - Felix tokens are EdDSA and verified via JWKS.
//! - Publish ordering is preserved per stream.
//!
//! - Test keys are fixtures only and must not be logged in production.
//! - No database or token secrets are written to logs.
//!
//! - Tests are serialized to avoid port collisions and shared state races.
//!
//! Run with `cargo test -p felix-broker-service quic_publish`.
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, StreamMetadata};
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_client::{Client, ClientConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, FLAG_BINARY_PUBLISH_BATCH, FrameHeader, Message};
use jsonwebtoken::Algorithm;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

const TEST_PRIVATE_KEY: [u8; 32] = [10u8; 32];

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

#[tokio::test]
#[serial]
async fn quic_publish_unauthorized_and_stream_missing() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let auth = auth_fixture("t1", vec!["stream.subscribe:stream:t1/*/*".to_string()]);
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client =
        Client::connect(addr, "localhost", build_client_config(cert.clone(), &auth)?).await?;
    let publisher = client.publisher().await?;
    let err = publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("forbidden publish");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("forbidden")
            || err_msg.contains("unauthorized")
            || err_msg.contains("auth failed")
            || err_msg.contains("publish failed: None"),
        "unexpected publish auth error: {err_msg}"
    );

    let auth = auth_fixture("t1", vec!["stream.publish:stream:t1/*/*".to_string()]);
    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let publisher = client.publisher().await?;
    let err = publisher
        .publish(
            "t1",
            "default",
            "missing",
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("missing stream");
    assert!(err.to_string().contains("stream not found"));

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn quic_publish_ack_and_batch_success() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
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

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let publisher = client.publisher().await?;
    // Use AckMode::None here to avoid coupling this test to control-stream ack timing;
    // ack paths are covered by commit-ack and explicit error tests.
    publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"payload".to_vec(),
            AckMode::None,
        )
        .await?;
    publisher
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"a".to_vec(), b"b".to_vec()],
            AckMode::None,
        )
        .await?;

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn quic_publish_commit_ack_ok() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "true");
        std::env::set_var("FELIX_ACK_WAIT_TIMEOUT_MS", "1000");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
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

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let publisher = client.publisher().await?;
    publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    server_task.abort();
    Ok(())
}

// The client's in-flight budget (4 MiB) is smaller than the frame cap
// (16 MiB); a payload between the two must still be publishable.
#[tokio::test]
#[serial]
async fn quic_publish_larger_than_the_inflight_budget() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;
    let mut sub = broker.subscribe("t1", "default", "orders", 0).await?;

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

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let payload = vec![7u8; 6 * 1024 * 1024];
    client
        .publisher()
        .await?
        .publish(
            "t1",
            "default",
            "orders",
            payload.clone(),
            AckMode::PerMessage,
        )
        .await?;
    let got = timeout(Duration::from_secs(10), sub.recv())
        .await
        .context("record not delivered")?
        .context("subscription closed")?;
    assert_eq!(got.len(), payload.len());

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn quic_publish_binary_decode_error_closes_stream() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
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
        config.clone(),
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    felix_broker_service::serving::quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            // Legacy handshake: no capabilities offered, so the broker
            // answers with a plain `Ok`.
            client_flags: None,
            client_features: None,
        },
    )
    .await?;
    let mut frame_scratch = bytes::BytesMut::with_capacity(1024);
    let response = felix_broker_service::serving::quic::read_message_limited(
        &mut recv,
        config.max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(matches!(response, Some(Message::Ok)));

    // Send a malformed binary publish batch payload to exercise decode errors.
    let header = FrameHeader::new(FLAG_BINARY_PUBLISH_BATCH, 2);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    header.encode_into(&mut header_bytes);
    send.write_all(&header_bytes).await?;
    send.write_all(&[0, 5]).await?;
    send.flush().await?;

    let close = timeout(
        Duration::from_millis(200),
        felix_broker_service::serving::quic::read_message_limited(
            &mut recv,
            config.max_frame_bytes,
            &mut frame_scratch,
        ),
    )
    .await;
    assert!(close.is_ok());

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn quic_publish_missing_request_id_returns_error() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
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
        config.clone(),
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    felix_broker_service::serving::quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            // Legacy handshake: no capabilities offered, so the broker
            // answers with a plain `Ok`.
            client_flags: None,
            client_features: None,
        },
    )
    .await?;
    let mut frame_scratch = bytes::BytesMut::with_capacity(1024);
    let response = felix_broker_service::serving::quic::read_message_limited(
        &mut recv,
        config.max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(matches!(response, Some(Message::Ok)));

    // Acked publish without request_id should yield an error on the control stream.
    felix_broker_service::serving::quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            payload: b"payload".to_vec(),
            request_id: None,
            ack: Some(AckMode::PerMessage),
            key: None,
        },
    )
    .await?;
    let response = felix_broker_service::serving::quic::read_message_limited(
        &mut recv,
        config.max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    match response {
        Some(Message::Error { message, .. }) => {
            assert!(message.contains("missing request_id"));
        }
        other => anyhow::bail!("unexpected response: {other:?}"),
    }

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn quic_publish_binary_batch_success() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
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

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let publisher = client.publisher().await?;
    // Binary batch publish exercises the FLAG_BINARY_PUBLISH_BATCH fast path.
    publisher
        .publish_batch_binary(
            "t1",
            "default",
            "orders",
            &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        )
        .await?;

    server_task.abort();
    Ok(())
}

// Binary acked publish: the frame carries FLAG_BINARY_PUBLISH_ACKED with a
// request_id prefix, and the broker answers with a binary ack frame rather than
// a JSON PublishOk. Both ack modes and both ack-on-commit settings are covered,
// because commit acks are emitted from a different task (the ack waiter) than
// enqueue acks, and each has to pick the binary encoding independently.
#[tokio::test]
#[serial]
async fn quic_publish_binary_acked_success() -> Result<()> {
    for ack_on_commit in ["false", "true"] {
        for ack_mode in [AckMode::PerMessage, AckMode::PerBatch] {
            unsafe {
                std::env::set_var("FELIX_ACK_ON_COMMIT", ack_on_commit);
            }
            let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
            broker.register_tenant("t1").await?;
            broker.register_namespace("t1", "default").await?;
            broker
                .register_stream("t1", "default", "orders", StreamMetadata::default())
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

            let client =
                Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
            let publisher = client.publisher().await?;
            // The other half of the negotiation contract: against a broker that
            // does advertise, the client must actually take the binary path
            // rather than defensively staying on JSON.
            assert_eq!(
                publisher.negotiated_server_flags(),
                felix_wire::KNOWN_FLAGS,
                "broker should advertise its full flag set during auth"
            );
            assert!(
                felix_wire::supports(
                    publisher.negotiated_server_flags(),
                    felix_wire::FLAG_BINARY_PUBLISH_ACKED
                ),
                "acked binary publishes require the negotiated 0x0008 bit"
            );
            // `publish_batch` with a non-None ack mode now takes the binary path.
            publisher
                .publish_batch(
                    "t1",
                    "default",
                    "orders",
                    vec![b"a".to_vec(), b"b".to_vec()],
                    ack_mode,
                )
                .await
                .with_context(|| {
                    format!("acked binary publish (commit={ack_on_commit}, ack={ack_mode:?})")
                })?;
            // A single acked publish is a one-item acked batch on the wire.
            publisher
                .publish("t1", "default", "orders", b"c".to_vec(), ack_mode)
                .await
                .with_context(|| {
                    format!("acked binary single (commit={ack_on_commit}, ack={ack_mode:?})")
                })?;

            server_task.abort();
        }
    }
    Ok(())
}

// An acked publish to a stream that was never registered must come back as a
// failed ack, not a hang and not a torn-down stream: the client is synchronously
// blocked waiting for this frame.
#[tokio::test]
#[serial]
async fn quic_publish_binary_acked_unknown_stream_returns_error() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

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

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let publisher = client.publisher().await?;
    let err = publisher
        .publish_batch(
            "t1",
            "default",
            "never-registered",
            vec![b"a".to_vec()],
            AckMode::PerBatch,
        )
        .await
        .expect_err("publish to an unregistered stream must fail");
    let message = format!("{err:#}");
    assert!(
        message.contains("stream not found"),
        "expected a stream-not-found ack, got: {message}"
    );

    server_task.abort();
    Ok(())
}

// Flag bits select the payload layout, so an undefined bit must be rejected
// rather than masked off and the body misparsed. This is what makes the *next*
// wire extension fail loudly instead of silently producing garbage.
#[tokio::test]
#[serial]
async fn quic_publish_unknown_flag_bit_is_rejected() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
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
        config.clone(),
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    felix_broker_service::serving::quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            // Legacy handshake: no capabilities offered, so the broker
            // answers with a plain `Ok`.
            client_flags: None,
            client_features: None,
        },
    )
    .await?;
    let mut frame_scratch = bytes::BytesMut::with_capacity(1024);
    let response = felix_broker_service::serving::quic::read_message_limited(
        &mut recv,
        config.max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(matches!(response, Some(Message::Ok)));

    // Derived rather than hardcoded: the lowest bit outside `KNOWN_FLAGS`. A
    // literal here becomes a defined bit the moment the next flag lands, and the
    // test then quietly stops testing anything -- which is what 0x0040 did.
    let undefined = !felix_wire::KNOWN_FLAGS & (!felix_wire::KNOWN_FLAGS).wrapping_neg();
    assert_ne!(
        undefined, 0,
        "every flag bit is defined; pick another probe"
    );
    let header = FrameHeader::new(undefined, 2);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    header.encode_into(&mut header_bytes);
    send.write_all(&header_bytes).await?;
    send.write_all(&[0x00, 0x00]).await?;

    let response = felix_broker_service::serving::quic::read_message_limited(
        &mut recv,
        config.max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    match response {
        Some(Message::Error { message, .. }) => {
            assert!(
                message.contains("unsupported frame flags"),
                "unexpected error text: {message}"
            );
        }
        other => anyhow::bail!("expected an unsupported-flags error, got: {other:?}"),
    }

    server_task.abort();
    Ok(())
}

// Wire-level proof that the acked binary path is actually binary in both
// directions. The round-trip tests above would still pass if the broker quietly
// answered in JSON, because the client accepts either; this one asserts the
// response frame's flags directly.
#[tokio::test]
#[serial]
async fn quic_publish_binary_acked_reply_is_a_binary_frame() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
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
        config.clone(),
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    felix_broker_service::serving::quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            // Legacy handshake: no capabilities offered, so the broker
            // answers with a plain `Ok`.
            client_flags: None,
            client_features: None,
        },
    )
    .await?;
    let mut frame_scratch = bytes::BytesMut::with_capacity(1024);
    let response = felix_broker_service::serving::quic::read_message_limited(
        &mut recv,
        config.max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(matches!(response, Some(Message::Ok)));

    let bytes = felix_wire::binary::encode_acked_publish_batch_bytes(
        4242,
        AckMode::PerBatch,
        "t1",
        "default",
        "orders",
        &[b"a".to_vec(), b"b".to_vec()],
    )?;
    // The request frame must carry both bits.
    let request = felix_wire::Frame::decode(bytes.clone())?;
    assert_eq!(
        request.header.flags,
        FLAG_BINARY_PUBLISH_BATCH | felix_wire::FLAG_BINARY_PUBLISH_ACKED
    );
    send.write_all(&bytes).await?;

    let frame = felix_broker_service::serving::quic::read_frame_limited_into(
        &mut recv,
        config.max_frame_bytes,
        &mut frame_scratch,
    )
    .await?
    .context("expected an ack frame")?;
    assert_eq!(
        frame.header.flags,
        felix_wire::FLAG_BINARY_PUBLISH_ACK,
        "broker must answer an acked binary publish with a binary ack frame"
    );
    let ack = felix_wire::binary::decode_publish_ack(&frame)?;
    assert_eq!(ack.request_id, 4242);
    assert_eq!(ack.error, None);

    server_task.abort();
    Ok(())
}

// A batch whose publish failed may still have landed under its sequence, and
// the leader answers that sequence from memory. A different batch sent under
// it next would be acknowledged and silently dropped, so the producer must
// refuse it and only let the same batch go out again.
#[tokio::test]
#[serial]
async fn idempotent_producer_does_not_reuse_a_sequence_in_doubt() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

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

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let producer = client.idempotent_producer().await?;

    // As far as the producer can tell, the first batch failed...
    producer
        .publish("t1", "default", "orders", b"a".to_vec())
        .await
        .expect_err("the stream does not exist yet");

    // ...but it landed under sequence 0, as it would have if only the
    // acknowledgement had been lost.
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;
    let mut sub = broker.subscribe("t1", "default", "orders", 0).await?;
    // The connection remembers the miss for STREAM_CACHE_TTL (2 s).
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    client
        .publisher()
        .await?
        .publish_idempotent_batch(
            "t1",
            "default",
            "orders",
            vec![b"a".to_vec()],
            producer.producer_id(),
            0,
        )
        .await
        .context("out-of-band append")?;

    let err = producer
        .publish("t1", "default", "orders", b"b".to_vec())
        .await
        .expect_err("a different batch under a sequence in doubt must not be sent");
    assert!(
        format!("{err:#}").contains("re-send the same batch"),
        "unexpected error: {err:#}"
    );

    // The same batch again is answered from memory, and the producer moves on.
    producer
        .publish("t1", "default", "orders", b"a".to_vec())
        .await
        .context("re-send")?;
    producer
        .publish("t1", "default", "orders", b"b".to_vec())
        .await
        .context("next")?;

    for expected in [b"a", b"b"] {
        let got = timeout(Duration::from_secs(5), sub.recv())
            .await
            .context("record not delivered")?
            .context("subscription closed")?;
        assert_eq!(got.as_ref(), expected);
    }
    assert!(
        timeout(Duration::from_millis(200), sub.recv())
            .await
            .is_err(),
        "a record landed twice"
    );

    server_task.abort();
    Ok(())
}

/// A broker over a durable single-shard `t1/default/orders`, serving QUIC.
async fn serve_durable_orders(
    dir: &std::path::Path,
) -> Result<(
    Arc<Broker>,
    std::net::SocketAddr,
    CertificateDer<'static>,
    AuthFixture,
    felix_broker_service::config::BrokerConfig,
    tokio::task::JoinHandle<Result<()>>,
)> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let storage = felix_broker::DurableStorage::open(
        dir,
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
        config.clone(),
        Arc::clone(&auth.auth),
    ));
    Ok((broker, addr, cert, auth, config, server_task))
}

/// Every payload `t1/default/orders` holds, in order.
async fn stored_orders(broker: &Broker) -> Result<Vec<Vec<u8>>> {
    let resumed = broker
        .subscribe_from(
            "t1",
            "default",
            "orders",
            0,
            felix_wire::StartPosition::Earliest,
        )
        .await?;
    anyhow::ensure!(
        resumed.history.is_none(),
        "the replay ring should cover the log"
    );
    Ok(resumed
        .backlog
        .into_iter()
        .map(|(_, payload)| payload.to_vec())
        .collect())
}

// A producer that sends a different batch under a sequence the leader already
// holds is refused with a typed reason, not acknowledged: the batch was not
// written, and saying otherwise loses it silently.
#[tokio::test]
#[serial]
async fn a_reused_sequence_is_refused_to_the_rust_client() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (broker, addr, cert, auth, _config, server_task) = serve_durable_orders(dir.path()).await?;

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let producer_id = client.idempotent_producer().await?.producer_id();
    let publisher = client.publisher().await?;
    let send = |payload: &'static [u8]| {
        publisher.publish_idempotent_batch(
            "t1",
            "default",
            "orders",
            vec![payload.to_vec()],
            producer_id,
            0,
        )
    };

    send(b"a").await.context("first")?;
    let err = send(b"b")
        .await
        .expect_err("a different batch under a held sequence was acknowledged");
    let refused = err
        .downcast_ref::<felix_client::PublishRefused>()
        .with_context(|| format!("not a typed refusal: {err:#}"))?;
    assert_eq!(
        refused.reason,
        felix_client::PublishRefusalReason::SequenceReused
    );
    send(b"a")
        .await
        .context("the batch itself is still a re-send")?;

    assert_eq!(stored_orders(&broker).await?, vec![b"a".to_vec()]);
    server_task.abort();
    Ok(())
}

/// Over JSON frames on one connection offering `client_features`: take a
/// producer id, publish `a` under sequence 0, then `b` under the same sequence,
/// and return the answer to `b`.
async fn reuse_a_sequence_over_json(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
    config: &felix_broker_service::config::BrokerConfig,
    client_features: u32,
) -> Result<Option<Message>> {
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut frame_scratch = bytes::BytesMut::with_capacity(1024);
    let mut exchange = async |message: Message| -> Result<Option<Message>> {
        felix_broker_service::serving::quic::write_message(&mut send, message).await?;
        felix_broker_service::serving::quic::read_message_limited(
            &mut recv,
            config.max_frame_bytes,
            &mut frame_scratch,
        )
        .await
    };

    match exchange(Message::Auth {
        tenant_id: auth.tenant_id.clone(),
        token: auth.token.clone(),
        client_flags: Some(felix_wire::ORIGINAL_V1_FLAGS),
        client_features: Some(client_features),
    })
    .await?
    {
        Some(Message::AuthOk {
            server_features, ..
        }) => assert!(felix_wire::supports_feature(
            server_features.unwrap_or(0),
            felix_wire::FEATURE_SEQUENCE_REUSED
        )),
        other => anyhow::bail!("expected AuthOk, got {other:?}"),
    }
    let producer_id = match exchange(Message::ProducerInit { request_id: 1 }).await? {
        Some(Message::ProducerInitOk { producer_id, .. }) => producer_id,
        other => anyhow::bail!("expected ProducerInitOk, got {other:?}"),
    };
    let publish = |request_id: u64, payload: &[u8]| Message::PublishIdempotent {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
        payloads: vec![payload.to_vec()],
        key: None,
        request_id,
        producer_id,
        sequence: 0,
    };
    let first = exchange(publish(2, b"a")).await?;
    anyhow::ensure!(
        matches!(first, Some(Message::PublishOk { request_id: 2 })),
        "first: {first:?}"
    );
    exchange(publish(3, b"b")).await
}

// A client that did not offer `FEATURE_SEQUENCE_REUSED` cannot decode the
// refusal, so it gets the answer it always got: acknowledged, and not written.
// One that offered it over JSON is refused as over binary.
#[tokio::test]
#[serial]
async fn a_reused_sequence_is_refused_over_json_only_when_negotiated() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (broker, addr, cert, auth, config, server_task) = serve_durable_orders(dir.path()).await?;

    let legacy = reuse_a_sequence_over_json(
        addr,
        cert.clone(),
        &auth,
        &config,
        felix_wire::FEATURE_IDEMPOTENT_PRODUCER,
    )
    .await?;
    assert!(
        matches!(legacy, Some(Message::PublishOk { request_id: 3 })),
        "a legacy client should get publish_ok for a reused sequence"
    );

    let negotiated = reuse_a_sequence_over_json(
        addr,
        cert,
        &auth,
        &config,
        felix_wire::FEATURE_IDEMPOTENT_PRODUCER | felix_wire::FEATURE_SEQUENCE_REUSED,
    )
    .await?;
    assert!(
        matches!(
            negotiated,
            Some(Message::PublishRefused {
                request_id: 3,
                reason: felix_wire::PublishRefusalReason::SequenceReused,
                ..
            })
        ),
        "a negotiating client should get publish_refused sequence_reused"
    );

    // One `a` per producer, and no `b`.
    assert_eq!(
        stored_orders(&broker).await?,
        vec![b"a".to_vec(), b"a".to_vec()]
    );
    server_task.abort();
    Ok(())
}

#[path = "quic_publish/pipeline.rs"]
mod pipeline;

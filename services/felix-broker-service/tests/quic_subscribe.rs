//! QUIC subscribe integration tests for broker control/event streams.
//!
//! Validate subscription lifecycle and error handling over QUIC, including:
//! - auth enforcement and missing stream errors
//! - event delivery + cancel cleanup
//! - fanout behavior with multiple subscribers
//! - malformed control frames on the subscribe path
//!
//! These tests use ephemeral QUIC servers and in-memory broker state.
//!
//! - Felix tokens are EdDSA and verified via JWKS.
//! - Subscription ordering is preserved per stream.
//!
//! - Test keys are fixtures only and must not be logged in production.
//! - No database or token secrets are written to logs.
//!
//! - Tests are serialized to avoid port collisions and shared state races.
//!
//! Run with `cargo test -p felix-broker-service quic_subscribe`.
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
use felix_client::{Client, ClientConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::{FrameHeader, Message};
use jsonwebtoken::Algorithm;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

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

#[tokio::test]
#[serial]
async fn quic_subscribe_unauthorized_and_stream_missing() -> Result<()> {
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

    let client =
        Client::connect(addr, "localhost", build_client_config(cert.clone(), &auth)?).await?;
    let err = match client.subscribe("t1", "default", "orders").await {
        Ok(_) => anyhow::bail!("expected unauthorized subscribe"),
        Err(err) => err,
    };
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("forbidden")
            || err_msg.contains("unauthorized")
            || err_msg.contains("auth failed")
            || err_msg.contains("subscribe failed: None"),
        "unexpected subscribe auth error: {err_msg}"
    );

    let auth = auth_fixture("t1", vec!["stream.subscribe:stream:t1/*/*".to_string()]);
    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let err = match client.subscribe("t1", "default", "missing").await {
        Ok(_) => anyhow::bail!("expected missing stream"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("stream not found"));

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn quic_subscribe_batch_receive_and_cancel() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_EVENT_BATCH_MAX_EVENTS", "2");
        std::env::set_var("FELIX_EVENT_BATCH_MAX_BYTES", "1024");
        std::env::set_var("FELIX_EVENT_BATCH_MAX_DELAY_US", "200");
        std::env::set_var("FELIX_FANOUT_BATCH", "4");
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
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let mut sub = client.subscribe("t1", "default", "orders").await?;
    let publisher = client.publisher().await?;

    // Use AckMode::None to avoid control-stream ack timing affecting subscription delivery.
    publisher
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            felix_wire::AckMode::None,
        )
        .await?;

    let mut received = Vec::new();
    while received.len() < 3 {
        let next = timeout(Duration::from_secs(2), sub.next_event()).await??;
        if let Some(event) = next {
            received.push(event.payload.to_vec());
        }
    }
    assert_eq!(received.len(), 3);
    drop(sub);

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn quic_subscribe_fanout_and_drop_cleanup() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_EVENT_BATCH_MAX_EVENTS", "2");
        std::env::set_var("FELIX_EVENT_BATCH_MAX_BYTES", "1024");
        std::env::set_var("FELIX_EVENT_BATCH_MAX_DELAY_US", "200");
        std::env::set_var("FELIX_FANOUT_BATCH", "4");
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
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let mut sub_a = client.subscribe("t1", "default", "orders").await?;
    let mut sub_b = client.subscribe("t1", "default", "orders").await?;
    let publisher = client.publisher().await?;

    // Use AckMode::None to reduce flakiness in coverage runs.
    publisher
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"a".to_vec(), b"b".to_vec()],
            felix_wire::AckMode::None,
        )
        .await?;

    let first_a = timeout(Duration::from_secs(2), sub_a.next_event()).await??;
    let first_b = timeout(Duration::from_secs(2), sub_b.next_event()).await??;
    assert!(first_a.is_some());
    assert!(first_b.is_some());

    // Drop one subscriber and ensure the other keeps receiving events.
    drop(sub_a);

    publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"c".to_vec(),
            felix_wire::AckMode::None,
        )
        .await?;
    let remaining = timeout(Duration::from_secs(2), sub_b.next_event()).await??;
    assert!(remaining.is_some());

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn quic_subscribe_invalid_frame_closes_stream() -> Result<()> {
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
    let auth = auth_fixture("t1", vec!["stream.subscribe:stream:t1/*/*".to_string()]);
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config.clone(),
        Arc::clone(&auth.auth),
    ));

    let client = felix_transport::QuicClient::bind(
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
            client_features_hi: None,
        },
    )
    .await?;
    let mut frame_scratch = felix_broker_service::serving::quic::FrameScratch::new();
    let response = felix_broker_service::serving::quic::read_message_limited(
        &mut recv,
        config.max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(matches!(response, Some(Message::Ok)));

    // Send an invalid JSON frame to trigger decode error handling on the control stream.
    let header = FrameHeader::new(0, 2);
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

/// The acceptance criterion of #177, end to end over QUIC: a client that
/// disconnects, reconnects with the offset it last handled, and resumes must
/// receive every record published in between, exactly once, in order.
///
/// Everything before this test exercised the broker API directly. This is the
/// only one that proves the *wire* carries it: the start position out, the
/// offsets back, and the join between disk history and live delivery.
#[tokio::test]
#[serial]
async fn quic_subscribe_resumes_from_a_checkpointed_offset() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let dir = tempfile::tempdir()?;
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            // Small enough that the run rolls segments, so history is read
            // across a boundary rather than out of one file.
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..Default::default()
        },
    )?;
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_durable_storage(storage)
            // A replay ring far smaller than the record count, so the resume
            // has to come off disk instead of out of memory.
            .with_log_capacity(4)?,
    );
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
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    const TOTAL: usize = 40;
    let client =
        Client::connect(addr, "localhost", build_client_config(cert.clone(), &auth)?).await?;
    let publisher = client.publisher().await?;

    // Phase 1: subscribe live, take the first few, then "crash".
    let mut sub = client.subscribe("t1", "default", "orders").await?;
    // A plain tail subscribe from a client that reads offsets is `latest`:
    // it says where live delivery began, which on an empty stream is 0.
    assert_eq!((sub.start_offset(), sub.live_offset()), (Some(0), Some(0)));
    for i in 0..5usize {
        publisher
            .publish(
                "t1",
                "default",
                "orders",
                format!("v{i:03}").into_bytes(),
                felix_wire::AckMode::None,
            )
            .await?;
    }
    let mut seen = Vec::new();
    let mut checkpoint = None;
    while seen.len() < 5 {
        let Some(event) = timeout(Duration::from_secs(5), sub.next_event()).await?? else {
            continue;
        };
        checkpoint = Some(
            event
                .offset
                .expect("a durable stream must report offsets on live delivery"),
        );
        seen.push(String::from_utf8(event.payload.to_vec())?);
    }
    drop(sub);
    let checkpoint = checkpoint.expect("checkpoint");
    assert_eq!(checkpoint, 4, "five records occupy offsets 0..=4");

    // Phase 2: publish while nobody is subscribed. These are exactly the records
    // a tail-only reconnect loses, and there are far more than the ring holds.
    for i in 5..TOTAL {
        publisher
            .publish(
                "t1",
                "default",
                "orders",
                format!("v{i:03}").into_bytes(),
                felix_wire::AckMode::None,
            )
            .await?;
    }

    // Phase 3: reconnect and resume from the offset after the last one handled.
    let client2 = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let mut resumed = client2
        .subscribe_from(
            "t1",
            "default",
            "orders",
            Some(felix_client::StartPosition::Offset(checkpoint + 1)),
        )
        .await?;
    // Catch-up is exactly what was published while away; everything from the
    // tail at join is live.
    assert_eq!(resumed.start_offset(), Some(checkpoint + 1));
    assert_eq!(resumed.live_offset(), Some(TOTAL as u64));

    while seen.len() < TOTAL {
        let Some(event) = timeout(Duration::from_secs(10), resumed.next_event()).await?? else {
            continue;
        };
        let offset = event.offset.expect("resumed events carry offsets");
        assert_eq!(
            offset as usize,
            seen.len(),
            "offsets must be contiguous and match position",
        );
        seen.push(String::from_utf8(event.payload.to_vec())?);
    }

    let expected: Vec<String> = (0..TOTAL).map(|i| format!("v{i:03}")).collect();
    assert_eq!(
        seen, expected,
        "resume must lose nothing and duplicate nothing"
    );

    // `Latest` joins at the current position: nothing to catch up on.
    let latest = client2
        .subscribe_from(
            "t1",
            "default",
            "orders",
            Some(felix_client::StartPosition::Latest),
        )
        .await?;
    assert_eq!(latest.start_offset(), Some(TOTAL as u64));
    assert_eq!(latest.live_offset(), Some(TOTAL as u64));

    drop(latest);
    drop(resumed);
    server_task.abort();
    Ok(())
}

/// Asking for an offset the log has already passed the end of is a typed
/// protocol error, not a silent restart at the tail.
#[tokio::test]
#[serial]
async fn quic_subscribe_rejects_a_cursor_past_the_tail() -> Result<()> {
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
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let err = client
        .subscribe_from(
            "t1",
            "default",
            "orders",
            Some(felix_client::StartPosition::Offset(9_999)),
        )
        .await
        .err()
        .expect("9999 is far past the tail");
    let typed = err
        .downcast_ref::<felix_client::SubscribeCursorError>()
        .expect("a typed cursor error, not a formatted string");
    assert_eq!(typed.reason, felix_client::CursorErrorReason::InFuture);
    assert_eq!(typed.requested, 9_999);
    assert_eq!(typed.available, 0);

    server_task.abort();
    Ok(())
}

/// A long replay reaches the reader whole. History is read off disk for a
/// subscriber that asked for it, so it must arrive paced by that reader rather
/// than faster than it can take it and then dropped.
#[tokio::test]
#[serial]
async fn quic_subscribe_replays_a_long_history_without_loss() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
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
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    // Far past both the replay ring and every queue between the log and the
    // reader, so most of it comes off disk and none of it fits in a buffer.
    const TOTAL: usize = 5_000;
    let client =
        Client::connect(addr, "localhost", build_client_config(cert.clone(), &auth)?).await?;
    let publisher = client.publisher().await?;
    for chunk in 0..TOTAL / 250 {
        let payloads = (chunk * 250..(chunk + 1) * 250)
            .map(|i| format!("r{i:05}").into_bytes())
            .collect();
        publisher
            .publish_batch(
                "t1",
                "default",
                "orders",
                payloads,
                felix_wire::AckMode::PerBatch,
            )
            .await?;
    }

    let reader = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let mut sub = reader
        .subscribe_from(
            "t1",
            "default",
            "orders",
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await?;
    assert_eq!(sub.live_offset(), Some(TOTAL as u64));

    // The reader takes each event as soon as it is there, and yields between
    // them the way an application doing any work at all does.
    let mut next = 0u64;
    while next < TOTAL as u64 {
        let event = match timeout(Duration::from_secs(5), sub.next_event()).await {
            Ok(event) => event?,
            Err(_) => panic!("replay stalled after {next} of {TOTAL} records"),
        };
        let Some(event) = event else {
            panic!("replay ended after {next} of {TOTAL} records");
        };
        let offset = event.offset.expect("durable events carry offsets");
        assert_eq!(offset, next, "replay skipped from {next} to {offset}");
        assert_eq!(event.payload.as_ref(), format!("r{next:05}").as_bytes());
        next += 1;
        tokio::task::yield_now().await;
    }

    drop(sub);
    server_task.abort();
    Ok(())
}

/// A subscriber that asked is told who published each event, live and on a
/// resume from disk; one that did not is told nothing, and its events are
/// the ones it always got.
#[tokio::test]
#[serial]
async fn quic_subscribe_reports_the_publisher_to_a_client_that_asked() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
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
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_durable_storage(storage)
            // Smaller than the record count, so the resume reads disk too.
            .with_log_capacity(2)?,
    );
    broker.record_publishers_when(|| true);
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "inputs",
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
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let mut asking = build_client_config(cert.clone(), &auth)?;
    asking.publishers = true;
    let asking = Client::connect(addr, "localhost", asking).await?;
    let plain = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let mut told = asking.subscribe("t1", "default", "inputs").await?;
    let mut untold = plain.subscribe("t1", "default", "inputs").await?;

    const TOTAL: usize = 5;
    let publisher = plain.publisher().await?;
    for i in 0..TOTAL {
        publisher
            .publish(
                "t1",
                "default",
                "inputs",
                format!("move-{i}").into_bytes(),
                felix_wire::AckMode::PerMessage,
            )
            .await?;
    }
    for _ in 0..TOTAL {
        let event = timeout(Duration::from_secs(5), told.next_event())
            .await??
            .expect("event");
        assert_eq!(event.publisher.as_deref(), Some("p:test"));
        let event = timeout(Duration::from_secs(5), untold.next_event())
            .await??
            .expect("event");
        assert_eq!(event.publisher, None);
    }

    let mut resumed = asking
        .subscribe_from(
            "t1",
            "default",
            "inputs",
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await?;
    for offset in 0..TOTAL as u64 {
        let event = timeout(Duration::from_secs(5), resumed.next_event())
            .await??
            .expect("event");
        assert_eq!(
            (event.offset, event.publisher.as_deref()),
            (Some(offset), Some("p:test"))
        );
    }

    drop((told, untold, resumed));
    server_task.abort();
    Ok(())
}

/// A subscriber that asked gets each record's append time, the same live and
/// on a resume from disk, and can find an offset by time. One that did not
/// gets the events it always got.
#[tokio::test]
#[serial]
async fn quic_subscribe_reports_record_times_to_a_client_that_asked() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
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
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_durable_storage(storage)
            // Smaller than the record count, so the resume reads disk too.
            .with_log_capacity(2)?,
    );
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "inputs",
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
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let mut asking = build_client_config(cert.clone(), &auth)?;
    asking.timestamps = true;
    let asking = Client::connect(addr, "localhost", asking).await?;
    let plain = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    assert!(plain.supports_offset_for_time());
    let mut told = asking.subscribe("t1", "default", "inputs").await?;
    let mut untold = plain.subscribe("t1", "default", "inputs").await?;

    const TOTAL: usize = 5;
    let before = felix_broker::append_time_now();
    let publisher = plain.publisher().await?;
    for i in 0..TOTAL {
        publisher
            .publish(
                "t1",
                "default",
                "inputs",
                format!("tick-{i}").into_bytes(),
                felix_wire::AckMode::PerMessage,
            )
            .await?;
        // Apart enough that each publish gets a time of its own.
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    let after = felix_broker::append_time_now();
    let mut times = Vec::new();
    for _ in 0..TOTAL {
        let event = timeout(Duration::from_secs(5), told.next_event())
            .await??
            .expect("event");
        let time = event.timestamp_micros.expect("a time");
        assert!((before..=after).contains(&time), "{time} outside the run");
        times.push(time);
        let event = timeout(Duration::from_secs(5), untold.next_event())
            .await??
            .expect("event");
        assert_eq!(event.timestamp_micros, None);
    }
    assert!(times.windows(2).all(|pair| pair[0] < pair[1]), "{times:?}");

    let mut resumed = asking
        .subscribe_from(
            "t1",
            "default",
            "inputs",
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await?;
    for (offset, time) in times.iter().enumerate() {
        let event = timeout(Duration::from_secs(5), resumed.next_event())
            .await??
            .expect("event");
        assert_eq!(
            (event.offset, event.timestamp_micros),
            (Some(offset as u64), Some(*time))
        );
    }

    let lookup = |at| plain.offset_for_time("t1", "default", "inputs", 0, at);
    assert_eq!(lookup(0).await?, Some(0));
    assert_eq!(lookup(times[2]).await?, Some(2));
    assert_eq!(lookup(times[2] + 1).await?, Some(3));
    assert_eq!(lookup(after + 1_000_000).await?, None);

    drop((told, untold, resumed));
    server_task.abort();
    Ok(())
}

/// A subscriber's requested queue capacity is clamped to the broker's range
/// and echoed back; one that asks for nothing hears nothing about it.
#[tokio::test]
#[serial]
async fn quic_subscribe_grants_a_clamped_queue_capacity() -> Result<()> {
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

    let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
    config.subscriber_queue_capacity_max = 64;
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let subscribe_asking = |capacity: Option<u32>| {
        let cert = cert.clone();
        let auth = &auth;
        async move {
            let mut client_config = build_client_config(cert, auth)?;
            client_config.broker_sub_queue_capacity = capacity;
            let client = Client::connect(addr, "localhost", client_config).await?;
            let sub = client.subscribe("t1", "default", "orders").await?;
            Result::<_>::Ok((client, sub))
        }
    };
    let (_big_client, mut big) = subscribe_asking(Some(10_000)).await?;
    let (_tiny_client, tiny) = subscribe_asking(Some(0)).await?;
    let (_mid_client, mid) = subscribe_asking(Some(48)).await?;
    let (plain_client, plain) = subscribe_asking(None).await?;
    assert_eq!(big.queue_capacity(), Some(64), "clamped to the maximum");
    assert_eq!(tiny.queue_capacity(), Some(1), "clamped to one");
    assert_eq!(mid.queue_capacity(), Some(48), "granted as asked");
    assert_eq!(
        plain.queue_capacity(),
        None,
        "nothing asked, nothing echoed"
    );

    plain_client
        .publisher()
        .await?
        .publish(
            "t1",
            "default",
            "orders",
            b"hello".to_vec(),
            felix_wire::AckMode::PerMessage,
        )
        .await?;
    let event = timeout(Duration::from_secs(2), big.next_event())
        .await??
        .expect("event");
    assert_eq!(event.payload.as_ref(), b"hello");

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn quic_stream_read_pages_a_range_without_subscribing() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
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
    for (stream, durable) in [("matches", true), ("chatter", false)] {
        broker
            .register_stream(
                "t1",
                "default",
                stream,
                StreamMetadata {
                    durable,
                    shards: 1,
                    ..Default::default()
                },
            )
            .await?;
    }

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let auth = auth_fixture(
        "t1",
        vec![
            "stream.publish:stream:t1/*/*".to_string(),
            "stream.subscribe:stream:t1/*/*".to_string(),
        ],
    );
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client =
        Client::connect(addr, "localhost", build_client_config(cert.clone(), &auth)?).await?;
    assert!(client.supports_read());
    let publisher = client.publisher().await?;
    for i in 0..10 {
        publisher
            .publish(
                "t1",
                "default",
                "matches",
                format!("move-{i}").into_bytes(),
                felix_wire::AckMode::PerMessage,
            )
            .await?;
    }

    let mut seen = Vec::new();
    let mut from = 2;
    while from < 8 {
        let page = client
            .read("t1", "default", "matches", 0, from, Some(8), 4)
            .await?;
        assert!(page.records.len() <= 4);
        assert!(page.next_offset > from);
        for record in &page.records {
            assert_eq!(
                record.payload.as_ref(),
                format!("move-{}", record.offset).as_bytes()
            );
            assert!(record.timestamp_micros > 0);
        }
        seen.extend(page.records.iter().map(|record| record.offset));
        from = page.next_offset;
    }
    assert_eq!(from, 8);
    assert_eq!(seen, (2..8).collect::<Vec<_>>());
    assert_eq!(
        broker
            .registered_subscribers("t1", "default", "matches", 0)
            .await?,
        0,
        "a read registered a subscriber"
    );

    let tail = client
        .read("t1", "default", "matches", 0, 10, None, 0)
        .await?;
    assert!(tail.records.is_empty());
    assert_eq!(tail.next_offset, 10);
    let past = client
        .read("t1", "default", "matches", 0, 11, None, 0)
        .await
        .expect_err("past the tail");
    let cursor = past
        .downcast_ref::<felix_client::SubscribeCursorError>()
        .expect("a cursor error");
    assert_eq!(cursor.reason, felix_wire::CursorErrorReason::InFuture);
    assert_eq!(cursor.available, 10);
    assert!(
        client
            .read("t1", "default", "chatter", 0, 0, None, 0)
            .await
            .is_err(),
        "an in-memory stream has no log to read"
    );

    // A grant on another stream, as a narrowed token would carry, is not one
    // on this stream; nor is publishing to it.
    for perms in [
        vec!["stream.subscribe:stream:t1/default/other".to_string()],
        vec!["stream.publish:stream:t1/*/*".to_string()],
    ] {
        let narrow = auth_fixture("t1", perms);
        let narrow = Client::connect(
            addr,
            "localhost",
            build_client_config(cert.clone(), &narrow)?,
        )
        .await?;
        let refused = narrow
            .read("t1", "default", "matches", 0, 0, None, 0)
            .await
            .expect_err("no subscribe grant on the stream");
        assert!(refused.to_string().contains("forbidden"), "{refused:#}");
    }

    server_task.abort();
    Ok(())
}

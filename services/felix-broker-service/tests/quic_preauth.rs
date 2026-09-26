//! What an unauthenticated client can cost the broker, over real QUIC.
//!
//! - A connection that authenticates nothing is closed at the auth deadline.
//! - One that authenticates is not.
//! - A stream that has not authenticated cannot declare a large frame.
//! - Connections past the broker-wide cap are refused.
//! - The client's idle event connections survive the deadline.
//!
//! Run with `cargo test -p felix-broker-service --test quic_preauth`.
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_broker::{Broker, StreamMetadata};
use felix_broker_service::config::BrokerConfig;
use felix_broker_service::serving::auth::demo::{DemoAuth, demo_auth_for_tenant};
use felix_broker_service::serving::quic::{read_message_limited, serve, write_message};
use felix_client::{Client, ClientConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicConnection, QuicServer, TransportConfig};
use felix_wire::{FrameHeader, Message};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::time::timeout;

struct Harness {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    demo: DemoAuth,
    server_task: tokio::task::JoinHandle<Result<()>>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

async fn start(config: BrokerConfig) -> Result<Harness> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;

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
    let demo = demo_auth_for_tenant("t1")?;
    let server_task = tokio::spawn(serve(server, broker, config, Arc::clone(&demo.auth)));
    Ok(Harness {
        addr,
        cert: cert_der,
        demo,
        server_task,
    })
}

fn quinn_config(cert: CertificateDer<'static>) -> Result<quinn::ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    Ok(quinn::ClientConfig::with_root_certificates(Arc::new(
        roots,
    ))?)
}

async fn connect(harness: &Harness) -> Result<(QuicClient, QuicConnection)> {
    let client = QuicClient::bind(
        "127.0.0.1:0".parse()?,
        quinn_config(harness.cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(harness.addr, "localhost").await?;
    Ok((client, connection))
}

async fn authenticate(harness: &Harness, connection: &QuicConnection) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_message(
        &mut send,
        Message::Auth {
            tenant_id: harness.demo.tenant_id.clone(),
            token: harness.demo.token.clone(),
            client_flags: Some(felix_wire::KNOWN_FLAGS),
            client_features: None,
        },
    )
    .await?;
    let mut scratch = BytesMut::new();
    let answer = timeout(
        Duration::from_secs(5),
        read_message_limited(&mut recv, 64 * 1024, &mut scratch),
    )
    .await
    .context("auth answered")??;
    assert!(
        matches!(answer, Some(Message::AuthOk { .. })),
        "expected AuthOk, got {answer:?}"
    );
    let _ = send.finish();
    Ok(())
}

fn with_auth_timeout(ms: u64) -> BrokerConfig {
    BrokerConfig {
        auth_timeout_ms: ms,
        ..BrokerConfig::default()
    }
}

#[tokio::test]
async fn unauthenticated_connection_is_closed_at_the_deadline() -> Result<()> {
    let harness = start(with_auth_timeout(300)).await?;
    let (_client, connection) = connect(&harness).await?;

    let reason = timeout(Duration::from_secs(5), connection.closed())
        .await
        .context("the broker closes an idle unauthenticated connection")?;
    match reason {
        quinn::ConnectionError::ApplicationClosed(close) => {
            assert_eq!(close.error_code, quinn::VarInt::from_u32(1));
            assert_eq!(&close.reason[..], b"authentication timeout");
        }
        other => panic!("expected an application close, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn authenticated_connection_outlives_the_deadline() -> Result<()> {
    let harness = start(with_auth_timeout(300)).await?;
    let (_client, connection) = connect(&harness).await?;
    authenticate(&harness, &connection).await?;

    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(
        connection.close_reason().is_none(),
        "closed: {:?}",
        connection.close_reason()
    );
    authenticate(&harness, &connection).await?;
    Ok(())
}

/// The header alone must be enough to refuse a large frame on a stream that
/// has not authenticated: the broker does not wait for the payload.
#[tokio::test]
async fn large_frame_before_auth_is_refused_from_the_header() -> Result<()> {
    let harness = start(BrokerConfig::default()).await?;
    let (_client, connection) = connect(&harness).await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    let mut header = [0u8; FrameHeader::LEN];
    // Under the general 16 MiB cap, over the 64 KiB pre-auth one.
    FrameHeader::new(0, 1024 * 1024).encode_into(&mut header);
    send.write_all(&header).await?;

    let mut buf = [0u8; 64];
    let ended = timeout(Duration::from_secs(3), recv.read(&mut buf))
        .await
        .context("the broker ends the stream without waiting for the payload")?;
    assert!(
        !matches!(ended, Ok(Some(_))),
        "expected no answer, got {ended:?}"
    );
    Ok(())
}

#[tokio::test]
async fn connections_past_the_cap_are_refused() -> Result<()> {
    let harness = start(BrokerConfig {
        max_client_connections: 1,
        ..BrokerConfig::default()
    })
    .await?;
    let (_first_client, first) = connect(&harness).await?;
    authenticate(&harness, &first).await?;

    let refused = timeout(Duration::from_secs(5), connect(&harness))
        .await
        .context("a refusal is prompt")?;
    assert!(refused.is_err(), "a second connection must be refused");

    // Closing the first frees its slot.
    first.close(0u32.into(), b"done");
    let mut admitted = None;
    for _ in 0..50 {
        if let Ok(pair) = connect(&harness).await {
            admitted = Some(pair);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (_client, connection) = admitted.context("a freed slot admits a new connection")?;
    authenticate(&harness, &connection).await?;
    Ok(())
}

/// The client pre-dials event connections it may not use until much later; it
/// authenticates them at connect so the deadline does not close them.
#[tokio::test]
async fn client_event_connections_survive_the_deadline() -> Result<()> {
    let harness = start(with_auth_timeout(300)).await?;
    let mut config = ClientConfig::from_env_or_yaml(quinn_config(harness.cert.clone())?, None)?;
    config.auth_tenant_id = Some(harness.demo.tenant_id.clone());
    config.auth_token = Some(harness.demo.token.clone());
    let client = Client::connect(harness.addr, "localhost", config).await?;

    tokio::time::sleep(Duration::from_millis(900)).await;
    timeout(
        Duration::from_secs(5),
        client.subscribe("t1", "default", "orders"),
    )
    .await
    .context("subscribe answered")??;
    Ok(())
}

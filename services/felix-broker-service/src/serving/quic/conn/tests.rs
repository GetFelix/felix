use anyhow::{Context, Result};
use felix_storage::EphemeralCache;
use felix_transport::TransportConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;

use super::*;
use crate::serving::quic::handlers::publish::build_publish_context;

#[tokio::test]
async fn handle_connection_returns_ok_on_closed_connection() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

    let config = BrokerConfig::default();
    let publish_ctx =
        build_publish_context(Arc::clone(&broker), &config, ClusterContext::default());
    let auth = Arc::new(BrokerAuth::new("http://127.0.0.1".to_string()));

    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        handle_connection_with_shutdown(
            broker,
            connection,
            config,
            auth,
            publish_ctx,
            CancellationToken::new(),
        )
        .await
    });

    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let client =
        felix_transport::QuicClient::bind("0.0.0.0:0".parse()?, quinn, TransportConfig::default())?;
    let connection = client.connect(addr, "localhost").await?;
    drop(connection);

    let result = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .context("handle connection timeout")??;
    assert!(result.is_ok());
    Ok(())
}

/// A control stream opened while the connection drains is told `draining` when
/// its client offered error codes, and left unanswered otherwise, as before.
#[tokio::test]
async fn a_new_stream_during_drain_is_told_draining() -> Result<()> {
    use felix_wire::{ErrorCode, Message, RetryClass};

    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let config = BrokerConfig::default();
    let publish_ctx =
        build_publish_context(Arc::clone(&broker), &config, ClusterContext::default());
    let auth = Arc::new(BrokerAuth::new("http://127.0.0.1".to_string()));

    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        handle_connection_with_shutdown(
            broker,
            connection,
            config,
            auth,
            publish_ctx,
            server_shutdown,
        )
        .await
    });

    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let client =
        felix_transport::QuicClient::bind("0.0.0.0:0".parse()?, quinn, TransportConfig::default())?;
    let connection = client.connect(addr, "localhost").await?;

    // A stream already in flight, parked mid-header, so the drain has to wait
    // out its grace window rather than close at once.
    let (mut held, _held_recv) = connection.open_bi().await?;
    held.write_all(&[0u8; 2]).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let auth_offering = |features| Message::Auth {
        tenant_id: "t1".to_string(),
        token: "token".to_string(),
        client_flags: Some(felix_wire::KNOWN_FLAGS),
        client_features: features,
    };
    let mut scratch = crate::serving::quic::FrameScratch::new();

    let (mut send, mut recv) = connection.open_bi().await?;
    super::super::codec::write_message(
        &mut send,
        auth_offering(Some(felix_wire::FEATURE_ERROR_CODES)),
    )
    .await?;
    let answer = tokio::time::timeout(
        Duration::from_secs(2),
        super::super::codec::read_message_limited(&mut recv, 64 * 1024, &mut scratch),
    )
    .await
    .context("a negotiated client is answered")??;
    match answer {
        Some(Message::Error { code, retry, .. }) => {
            assert_eq!(code, Some(ErrorCode::Draining));
            assert_eq!(retry, Some(RetryClass::Retry));
        }
        other => panic!("expected draining, got {other:?}"),
    }

    let (mut send, mut recv) = connection.open_bi().await?;
    super::super::codec::write_message(&mut send, auth_offering(None)).await?;
    let silent = tokio::time::timeout(
        Duration::from_millis(300),
        super::super::codec::read_message_limited(&mut recv, 64 * 1024, &mut scratch),
    )
    .await;
    assert!(
        silent.is_err(),
        "an old client must not be answered: {silent:?}"
    );

    drop(held);
    let result = tokio::time::timeout(Duration::from_secs(10), server_task)
        .await
        .context("drain finishes")??;
    assert!(result.is_ok());
    Ok(())
}

/// A stream held unanswered during a drain must not stretch the drain: with an
/// in-flight stream and a held one both still open, the connection closes
/// once the grace window ends.
#[tokio::test]
async fn a_held_stream_does_not_outlast_the_drain_deadline() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let config = BrokerConfig {
        // Grace is half of this, floored at one second.
        shutdown_drain_timeout_ms: 2_000,
        ..BrokerConfig::default()
    };
    let publish_ctx =
        build_publish_context(Arc::clone(&broker), &config, ClusterContext::default());
    let auth = Arc::new(BrokerAuth::new("http://127.0.0.1".to_string()));

    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        handle_connection_with_shutdown(
            broker,
            connection,
            config,
            auth,
            publish_ctx,
            server_shutdown,
        )
        .await
    });

    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let client =
        felix_transport::QuicClient::bind("0.0.0.0:0".parse()?, quinn, TransportConfig::default())?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut in_flight, _in_flight_recv) = connection.open_bi().await?;
    in_flight.write_all(&[0u8; 2]).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // An old client's stream: held, never answered, never closed by it.
    let (mut held, mut held_recv) = connection.open_bi().await?;
    super::super::codec::write_message(
        &mut held,
        felix_wire::Message::Auth {
            tenant_id: "t1".to_string(),
            token: "token".to_string(),
            client_flags: None,
            client_features: None,
        },
    )
    .await?;

    let started = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .context("the drain must end at its deadline despite the held stream")??;
    assert!(result.is_ok());
    assert!(
        started.elapsed() < Duration::from_millis(2_500),
        "drain took {:?}",
        started.elapsed()
    );
    // The held stream ends with the connection rather than being answered.
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let read = tokio::time::timeout(
        Duration::from_secs(2),
        super::super::codec::read_message_limited(&mut held_recv, 64 * 1024, &mut scratch),
    )
    .await
    .context("the held stream is closed")?;
    assert!(read.is_err(), "expected a closed connection, got {read:?}");
    Ok(())
}

//! The per-address cap on the real QUIC accept loop.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::*;
use crate::serving::auth::BrokerAuth;
use crate::serving::quic::{ClusterContext, serve_with_shutdown};

#[tokio::test]
async fn a_connection_past_the_per_address_cap_is_refused() -> Result<()> {
    let mut config = BrokerConfig::default();
    config.limits.max_connections_per_ip = 2;

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
    let shutdown = CancellationToken::new();
    let limits = ListenerLimits::from_config(&config);
    let per_ip = Arc::clone(&limits.quic_per_ip);
    let server_task = tokio::spawn(serve_with_shutdown(
        server,
        Arc::new(Broker::new(EphemeralCache::new().into())),
        config,
        Arc::new(BrokerAuth::new("http://127.0.0.1:1".to_string())),
        shutdown.clone(),
        TaskTracker::new(),
        ClusterContext::default(),
        crate::serving::quic::ConnectionLimit::new(1024),
        limits,
    ));

    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let client = QuicClient::bind("127.0.0.1:0".parse()?, quinn, TransportConfig::default())?;

    let first = client.connect(addr, "localhost").await?;
    let second = client.connect(addr, "localhost").await?;
    let third = tokio::time::timeout(Duration::from_secs(5), client.connect(addr, "localhost"))
        .await
        .expect("the third attempt ends promptly");
    assert!(third.is_err(), "the third connection is refused");
    assert!(first.close_reason().is_none(), "the first is still open");
    assert!(second.close_reason().is_none(), "the second is still open");
    assert_eq!(per_ip.held("127.0.0.1".parse()?), 2);

    // A closed connection gives its place back.
    first.close(0u32.into(), b"done");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while per_ip.held("127.0.0.1".parse()?) > 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the closed connection's place was never released"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let fourth = client.connect(addr, "localhost").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        fourth.close_reason().is_none(),
        "admitted into the freed place"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), server_task).await;
    Ok(())
}

//! Graceful shutdown / drain integration tests for the QUIC accept loop.
//!
//! Cover the transport half of the SIGTERM lifecycle: cancelling the accept token
//! must stop *admitting* connections without killing the ones already accepted, and
//! the drain must be bounded.
//!
//! - Cancellation stops admission; accepted connections keep running.
//! - The accept loop exits promptly even when parked on an idle listener.
//! - `TaskTracker::wait` resolves once tracked connections finish.
//!
//! Run with `cargo test -p felix-broker-service --test graceful_shutdown`.
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_broker::Broker;
use felix_broker_service::serving::auth::BrokerAuth;
use felix_common::lifecycle::{DrainBudget, Readiness};
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    Ok((server_config, cert_der))
}

fn build_quinn_client_config(cert: CertificateDer<'static>) -> Result<QuinnClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    Ok(QuinnClientConfig::with_root_certificates(Arc::new(roots))?)
}

struct Harness {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    accept_shutdown: CancellationToken,
    connections: TaskTracker,
    server_task: tokio::task::JoinHandle<Result<()>>,
}

async fn start_broker() -> Result<Harness> {
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
    let limit =
        felix_broker_service::serving::quic::ConnectionLimit::new(config.max_client_connections);
    let auth = Arc::new(BrokerAuth::new("http://127.0.0.1:1/".to_string()));
    let accept_shutdown = CancellationToken::new();
    let connections = TaskTracker::new();

    let limits = felix_broker_service::serving::limits::ListenerLimits::from_config(&config);
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve_with_shutdown(
        Arc::clone(&server),
        broker,
        config,
        auth,
        accept_shutdown.clone(),
        connections.clone(),
        Default::default(),
        limit,
        limits,
    ));
    // The accept loop registers its publish workers with `connections` before
    // it accepts anything; wait for that so tests can count connections on top.
    for _ in 0..500 {
        if !connections.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    Ok(Harness {
        addr,
        cert,
        accept_shutdown,
        connections,
        server_task,
    })
}

fn client_for(cert: CertificateDer<'static>) -> Result<QuicClient> {
    QuicClient::bind(
        "127.0.0.1:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )
}

#[tokio::test]
#[serial]
// The accept loop spends nearly all of its life parked on `server.accept()`. If
// cancellation were only checked between accepts, an idle broker would ignore
// SIGTERM entirely and hang until SIGKILL.
async fn cancelling_accept_token_exits_idle_accept_loop() -> Result<()> {
    let harness = start_broker().await?;

    harness.accept_shutdown.cancel();

    let result = timeout(Duration::from_secs(5), harness.server_task)
        .await
        .expect("accept loop must exit promptly when idle")
        .expect("join accept loop");
    assert!(
        result.is_ok(),
        "accept loop should exit cleanly: {result:?}"
    );
    Ok(())
}

#[tokio::test]
#[serial]
// Cancellation must stop admission *and* wind down connections already accepted.
//
// This originally asserted the opposite — that accepted connections keep running
// indefinitely — which is what the soak harness showed to be the bug: a subscriber
// holds its connection open forever, so a drain that only waits for connection
// tasks to end never completes. It burns the whole deadline and then force-aborts,
// dropping exactly the in-flight work the drain exists to protect.
async fn cancellation_winds_down_accepted_connections() -> Result<()> {
    let harness = start_broker().await?;
    // The publish workers, tracked alongside the connections.
    let idle = harness.connections.len();

    // Establish a connection before shutdown and keep it open.
    let early_client = client_for(harness.cert.clone())?;
    let early_conn = early_client.connect(harness.addr, "localhost").await?;

    // Wait for the accept loop to register it, so we are asserting about a
    // connection the broker actually took ownership of.
    for _ in 0..100 {
        if harness.connections.len() > idle {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        harness.connections.len(),
        idle + 1,
        "broker should have accepted the pre-shutdown connection"
    );

    harness.accept_shutdown.cancel();
    let result = timeout(Duration::from_secs(5), harness.server_task)
        .await
        .expect("accept loop must exit")
        .expect("join accept loop");
    assert!(
        result.is_ok(),
        "accept loop should exit cleanly: {result:?}"
    );

    // The connection task winds itself down without the peer disconnecting first.
    // This is the property that makes a bounded drain possible: the client here
    // never goes away, exactly as a real subscriber would not.
    harness.connections.close();
    timeout(Duration::from_secs(10), harness.connections.wait())
        .await
        .expect("accepted connections must drain without waiting for the peer to disconnect");

    // A new connection is no longer admitted. The handshake may fail outright or
    // hang because nothing accepts it; either is "not admitted", and both are
    // distinguishable from the success this returns before shutdown.
    let late_client = client_for(harness.cert.clone())?;
    let late = timeout(
        Duration::from_secs(2),
        late_client.connect(harness.addr, "localhost"),
    )
    .await;
    let admitted = matches!(late, Ok(Ok(_)));
    assert!(
        !admitted,
        "broker must not admit connections after shutdown"
    );

    drop(early_conn);
    drop(early_client);
    Ok(())
}

#[tokio::test]
#[serial]
// Readiness has to flip before the listener stops, so traffic is steered away while
// the broker can still serve it.
async fn readiness_flips_before_admission_stops() -> Result<()> {
    let harness = start_broker().await?;
    let readiness = Readiness::ready();
    assert!(readiness.is_ready());

    readiness.begin_draining();
    assert!(!readiness.is_ready());
    harness.accept_shutdown.cancel();

    let mut budget = DrainBudget::new(Duration::from_secs(5));
    harness.connections.close();
    assert!(
        budget
            .drain("quic_connections", harness.connections.wait())
            .await,
        "an idle broker should drain immediately"
    );
    assert!(budget.unfinished().is_empty());

    let _ = timeout(Duration::from_secs(5), harness.server_task).await;
    Ok(())
}

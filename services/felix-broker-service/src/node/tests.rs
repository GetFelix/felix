use std::time::Duration;

use super::*;
use crate::serving::tls::ClientTls;

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, prev }
    }

    fn unset(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(value) => unsafe {
                std::env::set_var(self.key, value);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}

// Basic sanity check that TLS config generation succeeds, for both listeners.
#[test]
fn build_server_config_smoke() -> Result<()> {
    let tls = ClientTls::from_config(&Default::default())?;
    let _quic = tls.quic_server_config()?;
    let _kafka = tls.kafka_server_config()?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn run_with_shutdown_starts_and_stops() -> Result<()> {
    let _g1 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", "127.0.0.1:0");
    let _g2 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
    let _g3 = EnvGuard::unset("FELIX_CP_URL");
    let _g4 = EnvGuard::set("FELIX_CONTROLPLANE_URL", "http://127.0.0.1:1");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        run_with_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    let _ = shutdown_tx.send(());
    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("shutdown timeout")?;
    result?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn run_with_shutdown_controlplane_enabled() -> Result<()> {
    let _g1 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", "127.0.0.1:0");
    let _g2 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
    let _g3 = EnvGuard::set("FELIX_CP_URL", "http://127.0.0.1:1");
    let _g4 = EnvGuard::set("FELIX_CP_SYNC_INTERVAL_MS", "1");
    let _g5 = EnvGuard::set("FELIX_CONTROLPLANE_URL", "http://127.0.0.1:1");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        run_with_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    let _ = shutdown_tx.send(());
    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("shutdown timeout")?;
    result?;
    Ok(())
}

/// A broker that cannot serve `/ready` has to exit, so whatever started it
/// can try again, rather than run on unready with nothing reporting why.
#[tokio::test]
#[serial_test::serial]
async fn a_taken_metrics_port_fails_startup() -> Result<()> {
    let taken = std::net::TcpListener::bind("127.0.0.1:0")?;
    let _g1 = EnvGuard::set(
        "FELIX_BROKER_METRICS_BIND",
        &taken.local_addr()?.to_string(),
    );
    let _g2 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
    let _g3 = EnvGuard::unset("FELIX_CP_URL");
    let _g4 = EnvGuard::set("FELIX_CONTROLPLANE_URL", "http://127.0.0.1:1");

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_with_shutdown(std::future::pending::<()>()),
    )
    .await
    .expect("startup should fail, not run on without a metrics listener");
    let err = result.expect_err("the metrics port is taken");
    assert!(
        format!("{err:#}").contains("metrics"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

fn free_tcp() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("free tcp port")
}

fn free_udp() -> std::net::SocketAddr {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .and_then(|s| s.local_addr())
        .expect("free udp port")
}

fn client_trusting(pem: &str) -> Result<felix_transport::QuicClient> {
    use base64::Engine as _;
    let body: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    let der = base64::engine::general_purpose::STANDARD.decode(body)?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from(der))?;
    let config = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    felix_transport::QuicClient::bind(
        "127.0.0.1:0".parse()?,
        config,
        felix_transport::TransportConfig::default(),
    )
}

/// The hold-off's whole point: once `/ready` says draining, a new
/// connection is still admitted, so one a load balancer routed here before
/// it noticed is served rather than refused.
#[tokio::test]
#[serial_test::serial]
async fn the_listener_admits_while_unready_during_the_hold_off() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cert_path = dir.path().join("broker-cert.pem");
    let metrics = free_tcp();
    let quic = free_udp();
    let _g1 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", &metrics.to_string());
    let _g2 = EnvGuard::set("FELIX_QUIC_BIND", &quic.to_string());
    let _g3 = EnvGuard::unset("FELIX_CP_URL");
    let _g4 = EnvGuard::set("FELIX_CONTROLPLANE_URL", "http://127.0.0.1:1");
    let _g5 = EnvGuard::set("FELIX_TLS_CERT_EXPORT", cert_path.to_str().expect("utf-8"));
    let _g6 = EnvGuard::set("FELIX_SHUTDOWN_PREDRAIN_MS", "3000");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(run_with_shutdown(async {
        let _ = shutdown_rx.await;
    }));

    let http = reqwest::Client::new();
    let ready_url = format!("http://{metrics}/ready");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status = http
            .get(&ready_url)
            .send()
            .await
            .ok()
            .map(|r| r.status().as_u16());
        if status == Some(200) && cert_path.exists() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "broker never became ready"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let started = tokio::time::Instant::now();
    let _ = shutdown_tx.send(());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let status = http
            .get(&ready_url)
            .send()
            .await
            .ok()
            .map(|r| r.status().as_u16());
        if status == Some(503) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "never saw /ready report draining while the broker was up: \
             shutdown ran straight through without holding off",
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let client = client_trusting(&std::fs::read_to_string(&cert_path)?)?;
    let connected =
        tokio::time::timeout(Duration::from_secs(2), client.connect(quic, "localhost")).await;
    assert!(
        matches!(connected, Ok(Ok(_))),
        "a connection arriving after readiness flipped was refused during the hold-off",
    );
    drop(connected);
    drop(client);

    tokio::time::timeout(Duration::from_secs(30), handle).await???;
    assert!(
        started.elapsed() >= Duration::from_millis(3000),
        "shutdown finished in {:?}, before the hold-off elapsed",
        started.elapsed(),
    );
    Ok(())
}

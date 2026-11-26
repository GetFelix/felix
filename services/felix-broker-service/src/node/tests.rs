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

/// **A broker waiting on the control plane at startup stays live (#1334).**
/// It opens no storage until the control plane answers, and a liveness probe
/// that found no listener meanwhile would restart it in a loop.
#[tokio::test]
#[serial_test::serial]
async fn live_answers_while_startup_waits_for_the_control_plane() -> Result<()> {
    let data = tempfile::tempdir()?;
    let metrics = free_tcp();
    let _g1 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", &metrics.to_string());
    let _g2 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
    let _g3 = EnvGuard::unset("FELIX_CP_URL");
    // Nothing listens here, so the startup lookup keeps retrying.
    let _g4 = EnvGuard::set("FELIX_CONTROLPLANE_URL", "http://127.0.0.1:1");
    let _g5 = EnvGuard::set("FELIX_NODE_ID", "broker-a");
    let _g6 = EnvGuard::set("FELIX_NODE_ADVERTISE_ADDR", &free_udp().to_string());
    let _g7 = EnvGuard::set("FELIX_NODE_TOKEN", "a-node-token");
    let _g8 = EnvGuard::set(
        "FELIX_DURABLE_STORAGE_DIR",
        data.path().to_str().expect("utf-8"),
    );
    let _g9 = EnvGuard::set("FELIX_INTERNAL_ALLOW_UNAUTHENTICATED", "true");
    let _g10 = EnvGuard::set("FELIX_INTERNAL_BIND", &free_udp().to_string());

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(run_with_shutdown(async {
        let _ = shutdown_rx.await;
    }));

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let live = http.get(format!("http://{metrics}/live")).send().await;
        if matches!(&live, Ok(r) if r.status() == 200) {
            break;
        }
        assert!(!handle.is_finished(), "startup ended: {:?}", handle.await);
        assert!(
            tokio::time::Instant::now() < deadline,
            "/live never answered while startup waited"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let ready = http.get(format!("http://{metrics}/ready")).send().await?;
    assert_eq!(ready.status(), 503);
    assert!(
        !data.path().join("storage-id").exists(),
        "storage was opened before the control plane answered"
    );

    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(5), handle).await???;
    Ok(())
}

/// **A broker offers `CLIENT_CERTS` exactly when it binds subjects.** A
/// binding broker that did not offer it would refuse every forward from a
/// certified client; one that offered it on a client CA alone would be sent
/// envelopes it never checks.
#[test]
fn client_certs_are_offered_exactly_when_the_broker_binds_subjects() {
    use felix_wire::internal::PeerCapabilities;
    let offered = |client_ca: bool, bind_subject: bool| {
        let mut config = crate::config::BrokerConfig {
            peer_transport: Some(felix_replication::peer::PeerTransportConfig::default()),
            ..crate::config::BrokerConfig::default()
        };
        config.client_tls.files = Some(crate::config::ClientTlsFiles {
            cert_path: "cert.pem".to_string(),
            key_path: "key.pem".to_string(),
            client_ca_path: client_ca.then(|| "ca.pem".to_string()),
        });
        config.client_tls.bind_subject = bind_subject;
        offer_client_certs(&mut config);
        let peer = config.peer_transport.expect("peer transport");
        let listener = peer.capabilities().contains(PeerCapabilities::CLIENT_CERTS);
        let dialler = peer
            .outgoing_capabilities()
            .contains(PeerCapabilities::CLIENT_CERTS);
        assert_eq!(listener, dialler);
        listener
    };
    assert!(offered(true, true), "a binding broker must offer it");
    assert!(!offered(true, false), "a client CA alone must not offer it");
    assert!(
        !offered(false, false),
        "a broker without binding must not offer it"
    );
}

/// Point a standalone durable broker at `data`, answering probes on
/// `metrics`. Its control plane does not answer and it has no token, so it
/// opens storage without one and never becomes ready.
fn standalone_durable(data: &std::path::Path, metrics: std::net::SocketAddr) -> Vec<EnvGuard> {
    vec![
        EnvGuard::set("FELIX_BROKER_METRICS_BIND", &metrics.to_string()),
        EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0"),
        EnvGuard::unset("FELIX_CP_URL"),
        EnvGuard::set("FELIX_CONTROLPLANE_URL", "http://127.0.0.1:1"),
        EnvGuard::set("FELIX_DURABLE_STORAGE_DIR", data.to_str().expect("utf-8")),
    ]
}

/// Wait until the broker `handle` runs has opened every store on `data`.
async fn await_opened(data: &std::path::Path, handle: &mut tokio::task::JoinHandle<Result<()>>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    // The counters open last.
    while !data.join("counters").is_dir() {
        if handle.is_finished() {
            panic!("the broker stopped while starting: {:?}", handle.await);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the broker never opened its storage"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    if handle.is_finished() {
        panic!("the broker stopped after opening: {:?}", handle.await);
    }
}

/// The data directory's own files a start writes: the run record, the run
/// token and the storage id.
fn run_files(data: &std::path::Path) -> Vec<(&'static str, Vec<u8>)> {
    [
        felix_storage::run_state::FILE_NAME,
        felix_storage::run_state::witness::TOKEN_FILE_NAME,
        felix_storage::storage_identity::FILE_NAME,
    ]
    .into_iter()
    .map(|name| (name, std::fs::read(data.join(name)).expect(name)))
    .collect()
}

/// **A second broker on a running broker's data directory is refused before
/// it writes anything there** (#3250). Without the lock on the root it
/// begins a run under the first: it rewrites the run record and the run
/// token, and may rotate the storage id, before any log refuses it.
#[tokio::test]
#[serial_test::serial]
async fn a_second_broker_on_the_data_directory_is_refused_before_it_writes() -> Result<()> {
    let data = tempfile::tempdir()?;
    let env = standalone_durable(data.path(), free_tcp());
    let (stop_first, stopped) = tokio::sync::oneshot::channel();
    let mut first = tokio::spawn(run_with_shutdown(async {
        let _ = stopped.await;
    }));
    await_opened(data.path(), &mut first).await;
    let before = run_files(data.path());

    // Its own ports, so only the directory is shared.
    let _metrics = EnvGuard::set("FELIX_BROKER_METRICS_BIND", &free_tcp().to_string());
    let second = tokio::time::timeout(
        Duration::from_secs(10),
        run_with_shutdown(std::future::pending::<()>()),
    )
    .await;
    assert_eq!(
        run_files(data.path()),
        before,
        "the second broker wrote the first one's run files"
    );
    let err = second
        .expect("the second broker started on the directory")
        .expect_err("the second broker started on the directory");
    let text = format!("{err:#}");
    assert!(
        text.contains(&data.path().display().to_string())
            && text.contains("another broker process"),
        "{text}"
    );

    let _ = stop_first.send(());
    tokio::time::timeout(Duration::from_secs(10), first).await???;
    drop(env);
    Ok(())
}

/// **A broker restarts in this process on the directory a stopped one left**:
/// the lock goes with the run, not with the process, so the many tests and
/// the simulator that restart a broker in place are not kept out.
#[tokio::test]
#[serial_test::serial]
async fn a_broker_restarts_in_process_after_a_clean_stop() -> Result<()> {
    let data = tempfile::tempdir()?;
    let mut ids = Vec::new();
    for _ in 0..2 {
        let _env = standalone_durable(data.path(), free_tcp());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let mut run = tokio::spawn(run_with_shutdown(async {
            let _ = stopped.await;
        }));
        await_opened(data.path(), &mut run).await;
        ids.push(std::fs::read(
            data.path().join(felix_storage::storage_identity::FILE_NAME),
        )?);
        let _ = stop.send(());
        tokio::time::timeout(Duration::from_secs(10), run).await???;
    }
    assert_eq!(ids[0], ids[1], "a clean stop kept the storage id");
    Ok(())
}

/// **A start that fails after storage opened lets the data directory go
/// only once it has stopped everything and closed storage** (#3250). Its
/// QUIC port is taken, which fails the start after storage and the Raft
/// shards are up. By the time it returns the stop is recorded as
/// clean, so nothing under the root is still being written, and a restart in
/// this process takes the directory and keeps its storage id.
#[tokio::test]
#[serial_test::serial]
async fn a_start_that_fails_after_storage_opened_lets_go_once_stopped() -> Result<()> {
    let data = tempfile::tempdir()?;
    let taken = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let env = standalone_durable(data.path(), free_tcp());
    let _quic = EnvGuard::set("FELIX_QUIC_BIND", &taken.local_addr()?.to_string());
    let failed = tokio::time::timeout(
        Duration::from_secs(20),
        run_with_shutdown(std::future::pending::<()>()),
    )
    .await
    .expect("the start neither failed nor stopped");
    let err = failed.expect_err("a broker started on a taken QUIC port");
    assert!(format!("{err:#}").contains("bind QUIC listener"), "{err:#}");

    let id = felix_storage::storage_identity::load(data.path())?.expect("a storage id");
    assert_eq!(
        felix_storage::run_state::read(data.path(), id)?,
        felix_storage::run_state::LastRun::Clean,
        "the failed start returned before it closed storage and recorded the stop"
    );

    drop(taken);
    drop(env);
    let _env = standalone_durable(data.path(), free_tcp());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut again = tokio::spawn(run_with_shutdown(async {
        let _ = stopped.await;
    }));
    await_opened(data.path(), &mut again).await;
    assert_eq!(
        felix_storage::storage_identity::load(data.path())?,
        Some(id),
        "the restart after a failed start rotated the storage id"
    );
    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(10), again).await???;
    Ok(())
}

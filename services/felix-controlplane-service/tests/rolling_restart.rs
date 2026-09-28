#![cfg(feature = "pg-tests")]
//! Control-plane high availability, run for real: two control-plane instances over
//! one Postgres, a broker's traffic (heartbeats and the shard-assignment
//! watch) flowing continuously, and every instance restarted in turn — with
//! zero failed calls.
//!
//! The client here plays the load balancer of the supported deployment: it
//! routes each call to an instance whose readiness probe answers 200, and on a
//! connection-level error retries once through the other ready instance —
//! which is a failover, counted separately, never a failure. What counts as a
//! failure is an HTTP error status, or no instance able to serve the call at
//! all: exactly the "metadata outage" the milestone forbids.
//!
//! Needs Postgres (or Docker to start one), like the other pg-tests; skips
//! with a note otherwise. Run with
//! `cargo test -p felix-controlplane-service --features pg-tests --test rolling_restart`.
mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig};
use testcontainers::clients::Cli;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// One-shot HTTP/1.1 over a fresh connection.
///
/// `None` means the connection itself failed — refused, reset, or timed out —
/// which is the case the load-balancer logic treats differently from a served
/// error status.
fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&str>,
) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .ok()?;

    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(bearer) = bearer {
        request.push_str(&format!("Authorization: Bearer {bearer}\r\n"));
    }
    match body {
        Some(body) => request.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )),
        None => request.push_str("\r\n"),
    }
    stream.write_all(request.as_bytes()).ok()?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).ok()?;
    let text = String::from_utf8_lossy(&response).into_owned();
    let status: u16 = text.split_whitespace().nth(1)?.parse().ok()?;
    Some((status, text))
}

/// The JSON payload of a response, tolerant of chunked framing: everything
/// from the first `{` to the last `}`.
fn response_json(raw: &str) -> Option<serde_json::Value> {
    let start = raw.find('{')?;
    let end = raw.rfind('}')?;
    serde_json::from_str(&raw[start..=end]).ok()
}

struct Instance {
    addr: SocketAddr,
    child: std::process::Child,
}

fn spawn_instance(addr: SocketAddr, pg_url: &str) -> Instance {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_felix-controlplane"));
    cmd.env("FELIX_CONTROLPLANE_BIND", addr.to_string())
        .env("FELIX_CONTROLPLANE_METRICS_BIND", "127.0.0.1:0")
        .env("FELIX_CONTROLPLANE_POSTGRES_URL", pg_url)
        .env("FELIX_BOOTSTRAP_ENABLED", "false")
        // Sized to the test's 100ms probe cadence the way production sizes it
        // to the LB's: comfortably above interval x threshold, far below the
        // default so the test does not spend 10s per restart waiting.
        .env("FELIX_SHUTDOWN_PREDRAIN_MS", "1500")
        .env("FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS", "8000")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Instance {
        addr,
        child: cmd.spawn().expect("spawn controlplane"),
    }
}

fn wait_until_ready(instance: &mut Instance, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some((200, _)) = http(instance.addr, "GET", "/v1/system/ready", None, None) {
            return;
        }
        if let Some(status) = instance.child.try_wait().expect("try_wait") {
            panic!(
                "instance on {} exited during startup: {status}",
                instance.addr
            );
        }
        assert!(
            Instant::now() < deadline,
            "instance on {} not ready within {timeout:?}",
            instance.addr
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_exit(child: &mut std::process::Child, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait().expect("try_wait").is_some() {
            return;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill on timeout");
            child.wait().expect("wait after kill");
            panic!("instance did not exit within {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn terminate(child: &std::process::Child) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
}

#[derive(Default)]
struct Traffic {
    calls: AtomicU64,
    failures: AtomicU64,
    failovers: AtomicU64,
    /// Iterations where no instance was ready at all — a metadata outage.
    outages: AtomicU64,
}

/// The broker's side of the conversation, routed like the supported deployment
/// routes it. Runs until `stop`, at the cadence of a chatty broker.
fn traffic_loop(
    addrs: [SocketAddr; 2],
    bearer: String,
    incarnation: u64,
    stop: Arc<AtomicBool>,
    stats: Arc<Traffic>,
) {
    let heartbeat_body = format!("{{\"incarnation\": {incarnation}}}");
    while !stop.load(Ordering::Relaxed) {
        // The load balancer's view: an instance is in rotation while its
        // readiness probe answers 200, and out the moment it does not.
        let ready: Vec<SocketAddr> = addrs
            .iter()
            .copied()
            .filter(|addr| {
                matches!(
                    http(*addr, "GET", "/v1/system/ready", None, None),
                    Some((200, _))
                )
            })
            .collect();
        if ready.is_empty() {
            stats.outages.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }

        for (method, path, body) in [
            (
                "POST",
                "/v1/nodes/broker-1/heartbeat",
                Some(heartbeat_body.as_str()),
            ),
            ("GET", "/v1/shard-assignments/changes?since=0", None),
        ] {
            stats.calls.fetch_add(1, Ordering::Relaxed);
            let mut outcome = http(ready[0], method, path, Some(&bearer), body);
            if outcome.is_none() {
                // The connection died before an answer. A real load balancer
                // does not retry against the rotation it sampled before the
                // call — that snapshot can be stale in both directions across
                // a SIGKILL, which flips no readiness before dying — it
                // re-probes and re-sends through whatever is ready *now*.
                // Only a connection-level failure is retried; a served error
                // status is a failure below, and a retry with nothing ready
                // stays a failure.
                let retry = addrs
                    .iter()
                    .copied()
                    .filter(|addr| *addr != ready[0])
                    .find(|addr| {
                        matches!(
                            http(*addr, "GET", "/v1/system/ready", None, None),
                            Some((200, _))
                        )
                    });
                if let Some(addr) = retry {
                    stats.failovers.fetch_add(1, Ordering::Relaxed);
                    outcome = http(addr, method, path, Some(&bearer), body);
                }
            }
            match outcome {
                Some((200, _)) => {}
                other => {
                    stats.failures.fetch_add(1, Ordering::Relaxed);
                    eprintln!("failed call {method} {path}: {other:?}");
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[tokio::test]
async fn a_rolling_restart_serves_every_watch_and_heartbeat() {
    // --- Postgres, one schema of our own ------------------------------------
    let (base_url, _container) = match std::env::var("FELIX_TEST_DATABASE_URL") {
        Ok(url) if !url.trim().is_empty() => (url, None),
        _ => {
            if !docker_available() {
                eprintln!("skipping rolling_restart: docker not available");
                return;
            }
            let docker = Box::leak(Box::new(Cli::default()));
            // The module's default tag is Postgres 11, which predates the
            // generated columns migration 0009 uses; pin the version the
            // supported deployment path (`task pg:up`) runs.
            let image = testcontainers::RunnableImage::from(
                testcontainers_modules::postgres::Postgres::default(),
            )
            .with_tag("16-alpine")
            .with_container_name(felix_test_container_name());
            let container = docker.run(image);
            let port = container.get_host_port_ipv4(5432);
            (
                format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
                Some(container),
            )
        }
    };

    let schema = format!(
        "felix_rolling_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&base_url)
                .await
            {
                Ok(pool) => {
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        r#"CREATE SCHEMA IF NOT EXISTS "{schema}""#
                    )))
                    .execute(&pool)
                    .await
                    .expect("create schema");
                    pool.close().await;
                    break;
                }
                Err(err) => {
                    assert!(Instant::now() < deadline, "postgres never came up: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let pg_url = format!("{base_url}{separator}options=-csearch_path%3D{schema}");

    // --- Seed: tenant, signing keys, an operator credential -----------------
    let store = felix_controlplane_service::store::postgres::PostgresStore::connect(
        &felix_controlplane_service::config::PostgresConfig {
            url: pg_url.clone(),
            max_connections: 2,
            connect_timeout_ms: 5_000,
            acquire_timeout_ms: 5_000,
        },
        StoreConfig {
            changes_limit: 100,
            change_retention_max_rows: Some(1_000),
        },
    )
    .await
    .expect("connect and migrate");
    store
        .create_tenant(felix_controlplane_service::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("create tenant");
    let keys = felix_controlplane_service::auth::keys::generate_signing_keys().expect("keys");
    store
        .set_tenant_signing_keys("t1", keys.clone())
        .await
        .expect("store keys");
    let bearer = felix_controlplane_service::auth::felix_token::mint_token_for(
        &keys,
        "t1",
        "p:operator",
        vec![
            "node.manage:cluster:*".to_string(),
            "node.view:cluster:*".to_string(),
        ],
        Duration::from_secs(3_600),
        felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )
    .expect("token");
    drop(store);

    // --- Two instances, both ready -----------------------------------------
    let addr_a = common::reserve_port();
    let addr_b = common::reserve_port();
    let mut a = spawn_instance(addr_a, &pg_url);
    let mut b = spawn_instance(addr_b, &pg_url);
    wait_until_ready(&mut a, Duration::from_secs(30));
    wait_until_ready(&mut b, Duration::from_secs(30));

    // --- One broker registers, then its traffic runs for the whole test -----
    let (status, _) = http(
        addr_a,
        "POST",
        "/v1/nodes",
        Some(&bearer),
        Some(r#"{"node_id": "broker-1", "advertise_addr": "10.0.0.4:7000", "region": "local"}"#),
    )
    .expect("registration reaches the control plane");
    assert_eq!(status, 200, "registration");
    let (_, node_raw) = http(addr_b, "GET", "/v1/nodes/broker-1", Some(&bearer), None)
        .expect("read the node back through the other instance");
    let incarnation = response_json(&node_raw)
        .and_then(|node| node["node"]["status"]["incarnation"].as_u64())
        .expect("incarnation");

    let stop = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(Traffic::default());
    let traffic = {
        let stop = Arc::clone(&stop);
        let stats = Arc::clone(&stats);
        let bearer = bearer.clone();
        std::thread::spawn(move || traffic_loop([addr_a, addr_b], bearer, incarnation, stop, stats))
    };

    // --- Rolling restart: every instance, one at a time ---------------------
    for instance in [&mut a, &mut b] {
        terminate(&instance.child);
        wait_for_exit(&mut instance.child, Duration::from_secs(30));
        *instance = spawn_instance(instance.addr, &pg_url);
        wait_until_ready(instance, Duration::from_secs(30));
    }

    // --- And the harder half of the signal: kill one outright ---------------
    a.child.kill().expect("SIGKILL instance a");
    a.child.wait().expect("reap");
    std::thread::sleep(Duration::from_secs(2));
    a = spawn_instance(addr_a, &pg_url);
    wait_until_ready(&mut a, Duration::from_secs(30));

    // A little quiet running at full strength before judging.
    std::thread::sleep(Duration::from_secs(1));
    stop.store(true, Ordering::Relaxed);
    traffic.join().expect("traffic thread");

    let calls = stats.calls.load(Ordering::Relaxed);
    let failures = stats.failures.load(Ordering::Relaxed);
    let failovers = stats.failovers.load(Ordering::Relaxed);
    let outages = stats.outages.load(Ordering::Relaxed);
    eprintln!(
        "rolling restart: {calls} calls, {failures} failures, {failovers} failovers, {outages} outage ticks"
    );

    assert!(calls > 20, "the traffic loop barely ran ({calls} calls)");
    assert_eq!(
        failures, 0,
        "watch/heartbeat calls failed during the restart"
    );
    assert_eq!(outages, 0, "there was a window with no ready instance");

    terminate(&a.child);
    terminate(&b.child);
    wait_for_exit(&mut a.child, Duration::from_secs(30));
    wait_for_exit(&mut b.child, Duration::from_secs(30));
}

/// A name a cleanup can recognise as ours.
///
/// testcontainers sets no labels, so without this the only thing separating a
/// leftover test database from one an operator is running is the image tag —
/// which is not enough to delete on. Unique per container, since a fixed name
/// would collide between concurrent runs.
fn felix_test_container_name() -> String {
    format!(
        "felix-test-pg-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}

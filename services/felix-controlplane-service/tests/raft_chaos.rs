//! What the Raft backend has to survive: the faults only a consensus group
//! can have — run against three real `felix-controlplane` binaries on the raft
//! backend, with **no database anywhere**.
//!
//! The client plays the load balancer of the supported deployment, exactly
//! as the rolling-restart test does: an instance is in rotation while its
//! readiness probe answers 200, a connection that dies before answering is
//! retried through another in-rotation instance (a failover, never a
//! failure), and — new under raft — a moment where *no* instance is ready
//! (an election) is waited out and measured, not failed, as long as it
//! stays inside a bound.
//!
//! Faults, in order: a rolling SIGTERM restart of every member; SIGKILL of
//! the leader specifically; the leader frozen with SIGSTOP past several
//! election timeouts and then thawed (the single-machine stand-in for a
//! partition, the same discipline the broker leader-failover suite uses);
//! and a follower's volume wiped entirely. Throughout, broker-shaped
//! traffic (registration, heartbeats, watch polls) and metadata writes
//! flow, and the verdict at the end is the milestone's own words: **zero
//! failed calls, and no acknowledged write lost** — every tenant create
//! that returned 201 is present on every member.
//!
//! Seeding exercises #340's path too: the signing keys and tenant arrive by
//! proposing an `ImportState` command over the real propose route, which is
//! also what lets this test mint operator tokens locally. That route is on
//! the peer listener and needs the cluster id and peer token, so the test
//! proposes as a peer would.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use felix_controlplane_service::store::export::export_state_from;
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::raft::command::{
    MetaCommand, decode_result, encode_command,
};
use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig};

const CLUSTER_ID: &str = "chaos-cluster";
const PEER_TOKEN: &str = "chaos-peer-token-chaos-peer-token-chaos";

fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&[u8]>,
) -> Option<(u16, String)> {
    // Generous: a write during a failover legitimately waits out an election.
    http_with_timeout(addr, method, path, bearer, body, Duration::from_secs(15))
}

/// Readiness probes use this with a short timeout, like a real load
/// balancer: a frozen process accepts the connection and then stalls, and a
/// probe that waits 15s for it stalls the whole rotation view.
fn http_with_timeout(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&[u8]>,
    read_timeout: Duration,
) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    stream.set_read_timeout(Some(read_timeout)).ok()?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .ok()?;
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(bearer) = bearer {
        request.push_str(&format!("Authorization: Bearer {bearer}\r\n"));
    }
    // Only the peer listener reads it; the peer token rides as the bearer.
    request.push_str(&format!(
        "{}: {CLUSTER_ID}\r\n",
        felix_controlplane_service::raft::CLUSTER_ID_HEADER
    ));
    match body {
        Some(body) => {
            request.push_str(&format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            ));
            stream.write_all(request.as_bytes()).ok()?;
            stream.write_all(body).ok()?;
        }
        None => {
            request.push_str("\r\n");
            stream.write_all(request.as_bytes()).ok()?;
        }
    }
    let mut response = Vec::new();
    stream.read_to_end(&mut response).ok()?;
    let text = String::from_utf8_lossy(&response).into_owned();
    let status: u16 = text.split_whitespace().nth(1)?.parse().ok()?;
    Some((status, text))
}

struct Instance {
    id: u64,
    api: SocketAddr,
    metrics: SocketAddr,
    data_dir: PathBuf,
    child: std::process::Child,
}

fn reserve() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve")
        .local_addr()
        .expect("addr")
}

/// Restart a member of a group that already exists.
fn spawn_instance(
    id: u64,
    api: SocketAddr,
    metrics: SocketAddr,
    data_dir: &PathBuf,
    peers: &str,
) -> std::process::Child {
    spawn_with_state(id, api, metrics, data_dir, peers, "existing")
}

/// Start a member for the first time; `initial` is
/// `FELIX_RAFT_INITIAL_CLUSTER_STATE`. The member's peer listener is its own
/// entry in `peers`.
fn spawn_with_state(
    id: u64,
    api: SocketAddr,
    metrics: SocketAddr,
    data_dir: &PathBuf,
    peers: &str,
    initial: &str,
) -> std::process::Child {
    let peer_bind = peers
        .split(',')
        .find_map(|entry| entry.strip_prefix(&format!("{id}=")))
        .expect("own entry in peers")
        .to_string();
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(
            data_dir
                .parent()
                .unwrap_or(data_dir)
                .join(format!("cp-{id}.log")),
        )
        .expect("open instance log");
    Command::new(env!("CARGO_BIN_EXE_felix-controlplane"))
        .env("FELIX_CONTROLPLANE_BIND", api.to_string())
        .env("FELIX_CONTROLPLANE_METRICS_BIND", metrics.to_string())
        .env("FELIX_RAFT_NODE_ID", id.to_string())
        .env("FELIX_RAFT_DATA_DIR", data_dir)
        .env("FELIX_RAFT_PEERS", peers)
        .env("FELIX_RAFT_BIND_ADDR", peer_bind)
        .env("FELIX_RAFT_CLUSTER_ID", CLUSTER_ID)
        .env("FELIX_RAFT_PEER_TOKEN", PEER_TOKEN)
        .env("FELIX_RAFT_INITIAL_CLUSTER_STATE", initial)
        // Faster consensus so each injected fault costs the test seconds,
        // not the production-default election window.
        .env("FELIX_RAFT_HEARTBEAT_MS", "100")
        .env("FELIX_RAFT_ELECTION_TIMEOUT_MIN_MS", "400")
        .env("FELIX_RAFT_ELECTION_TIMEOUT_MAX_MS", "800")
        .env("FELIX_RAFT_WRITE_TIMEOUT_MS", "8000")
        .env("FELIX_SHUTDOWN_PREDRAIN_MS", "800")
        .env("FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS", "8000")
        .env("RUST_LOG", "info,openraft=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn controlplane")
}

/// Multiplier applied to the setup waits here, from `FELIX_TEST_TIMEOUT_SCALE`.
///
/// What these wait for -- an instance to come up, a process to exit -- is setup,
/// not the thing under test. The subject is that no acknowledged write is lost
/// across restart, kill, freeze and wipe. On a shared runner the setup takes
/// longer for reasons that say nothing about the code, and the failure reports
/// as the durability claim having broken.
///
/// Tunable rather than simply larger: a bigger constant would hide a genuine
/// hang behind a longer wait on machines fast enough to notice. Unset means 1.
fn scale() -> f64 {
    static SCALE: std::sync::LazyLock<f64> = std::sync::LazyLock::new(|| {
        std::env::var("FELIX_TEST_TIMEOUT_SCALE")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|scale| scale.is_finite() && *scale >= 1.0)
            .unwrap_or(1.0)
    });
    *SCALE
}

fn wait_ready(instance: &mut Instance, timeout: Duration) {
    let timeout = timeout.mul_f64(scale());
    let deadline = Instant::now() + timeout;
    loop {
        if let Some((200, _)) = http(instance.api, "GET", "/v1/system/ready", None, None) {
            return;
        }
        if let Some(status) = instance.child.try_wait().expect("try_wait") {
            panic!("instance {} exited during startup: {status}", instance.id);
        }
        assert!(
            Instant::now() < deadline,
            "instance {} not ready within {timeout:?}",
            instance.id
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_exit(child: &mut std::process::Child, timeout: Duration) {
    let timeout = timeout.mul_f64(scale());
    let deadline = Instant::now() + timeout;
    while child.try_wait().expect("try_wait").is_none() {
        assert!(Instant::now() < deadline, "instance did not exit");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn signal(child: &std::process::Child, signal: &str) {
    let status = Command::new("kill")
        .arg(signal)
        .arg(child.id().to_string())
        .status()
        .expect("send signal");
    assert!(status.success());
}

/// Any instance whose readiness probe answers 200.
fn find_ready(instances: &[Instance]) -> SocketAddr {
    let deadline = Instant::now() + Duration::from_secs(15).mul_f64(scale());
    loop {
        if let Some(instance) = instances.iter().find(|instance| {
            matches!(
                http_with_timeout(
                    instance.api,
                    "GET",
                    "/v1/system/ready",
                    None,
                    None,
                    Duration::from_secs(1),
                ),
                Some((200, _))
            )
        }) {
            return instance.api;
        }
        assert!(Instant::now() < deadline, "no instance is ready");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The tenant list `instance` serves, once it includes `tenant_id`.
fn wait_until_applied(
    instance: &Instance,
    bearer: &str,
    tenant_id: &str,
    timeout: Duration,
) -> String {
    let timeout = timeout.mul_f64(scale());
    let deadline = Instant::now() + timeout;
    let needle = format!("\"{tenant_id}\"");
    loop {
        if let Some((200, body)) = http(instance.api, "GET", "/v1/tenants", Some(bearer), None)
            && body.contains(&needle)
        {
            return body;
        }
        if Instant::now() >= deadline {
            let applied = http_with_timeout(
                instance.metrics,
                "GET",
                "/metrics",
                None,
                None,
                Duration::from_secs(1),
            )
            .and_then(|(_, body)| {
                body.lines()
                    .find(|line| line.starts_with("felix_meta_raft_last_applied_index"))
                    .map(str::to_string)
            });
            panic!(
                "instance {} did not apply {tenant_id} within {timeout:?} ({})",
                instance.id,
                applied.as_deref().unwrap_or("applied index unknown")
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// This instance's `(is_leader, term)` gauges, if it answers.
fn leadership_gauges(instance: &Instance) -> Option<(bool, u64)> {
    let (_, body) = http_with_timeout(
        instance.metrics,
        "GET",
        "/metrics",
        None,
        None,
        Duration::from_secs(1),
    )?;
    let gauge = |name: &str| {
        body.lines()
            .find(|line| line.starts_with(name))
            .and_then(|line| line.split_whitespace().last())
            .and_then(|value| value.parse::<f64>().ok())
    };
    Some((
        gauge("felix_meta_raft_is_leader")? == 1.0,
        gauge("felix_meta_raft_term")? as u64,
    ))
}

/// Which instance the group currently follows, from the gauges each
/// instance publishes.
///
/// One `is_leader` reading is not enough: a member just thawed from a freeze
/// still reports the leadership it held before, until it hears the newer
/// term. So the leader must be the only one claiming it at the highest term
/// any member reports, and still be so on a second reading after the gauges
/// have refreshed.
fn find_leader(instances: &[Instance]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(15).mul_f64(scale());
    let sample = || -> Option<usize> {
        let gauges: Vec<Option<(bool, u64)>> = instances.iter().map(leadership_gauges).collect();
        let top_term = gauges.iter().flatten().map(|(_, term)| *term).max()?;
        let mut claimants = gauges
            .iter()
            .enumerate()
            .filter(|(_, gauge)| matches!(gauge, Some((true, term)) if *term == top_term))
            .map(|(index, _)| index);
        let leader = claimants.next()?;
        claimants.next().is_none().then_some(leader)
    };
    loop {
        if let Some(first) = sample() {
            // The gauges publish once a second.
            std::thread::sleep(Duration::from_millis(1_100));
            if sample() == Some(first) {
                return first;
            }
        }
        assert!(Instant::now() < deadline, "no instance reports leadership");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Deterministic signing keys the test holds both halves of, delivered to
/// the group through #340's import path.
fn seed_keys() -> felix_controlplane_service::auth::felix_token::TenantSigningKeys {
    let private = [7u8; 32];
    let signing = ed25519_dalek::SigningKey::from_bytes(&private);
    felix_controlplane_service::auth::felix_token::TenantSigningKeys {
        current: felix_controlplane_service::auth::felix_token::SigningKey {
            kid: "chaos-kid".to_string(),
            alg: jsonwebtoken::Algorithm::EdDSA,
            private_key: private,
            public_key: signing.verifying_key().to_bytes(),
        },
        previous: Vec::new(),
    }
}

#[derive(Default)]
struct Traffic {
    calls: AtomicU64,
    failures: AtomicU64,
    failovers: AtomicU64,
    /// Longest stretch with no ready instance, in milliseconds — elections
    /// are allowed, outages are not.
    longest_gap_ms: AtomicU64,
}

fn traffic_loop(
    apis: Vec<SocketAddr>,
    bearer: String,
    acked_tenants: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    stats: Arc<Traffic>,
    t0: Instant,
) {
    let mut tenant_counter = 0u64;
    let mut gap_started: Option<Instant> = None;
    while !stop.load(Ordering::Relaxed) {
        let ready: Vec<SocketAddr> = apis
            .iter()
            .copied()
            .filter(|addr| {
                matches!(
                    http_with_timeout(
                        *addr,
                        "GET",
                        "/v1/system/ready",
                        None,
                        None,
                        Duration::from_secs(1),
                    ),
                    Some((200, _))
                )
            })
            .collect();
        if ready.is_empty() {
            // An election in progress: every member is briefly honest about
            // not being fit to serve. Wait it out and measure it.
            let started = *gap_started.get_or_insert_with(Instant::now);
            let gap = started.elapsed().as_millis() as u64;
            stats.longest_gap_ms.fetch_max(gap, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }
        gap_started = None;

        let call = |method: &str, path: &str, body: Option<&[u8]>, expect: &[u16]| -> Option<u16> {
            stats.calls.fetch_add(1, Ordering::Relaxed);
            let mut outcome = http(ready[0], method, path, Some(&bearer), body);
            if outcome.is_none() {
                // The connection died before an answer. As in the rolling
                // restart's harness: retry against what is ready *now*, not
                // the rotation sampled before the call — a kill flips no
                // readiness before landing, so that snapshot can name a
                // corpse as the only member. A served error status is still
                // a failure below, and nothing-ready-now stays a failure.
                let retry = apis
                    .iter()
                    .copied()
                    .filter(|addr| *addr != ready[0])
                    .find(|addr| {
                        matches!(
                            http_with_timeout(
                                *addr,
                                "GET",
                                "/v1/system/ready",
                                None,
                                None,
                                Duration::from_secs(1),
                            ),
                            Some((200, _))
                        )
                    });
                if let Some(addr) = retry {
                    stats.failovers.fetch_add(1, Ordering::Relaxed);
                    outcome = http(addr, method, path, Some(&bearer), body);
                }
            }
            match outcome {
                Some((status, _)) if expect.contains(&status) => Some(status),
                // The one ambiguous outcome in the harness, and it is a
                // *success*.
                //
                // A create that committed and then lost its answer comes back
                // as `409 already exists`, and that conflict is an earlier
                // attempt of this same call reporting itself. Every id here is
                // used once (`t-chaos-N`, counting up), so a conflict cannot be
                // anyone else's write.
                //
                // Deliberately not conditioned on the harness having retried.
                // `RaftStore::write` retries a proposal itself when an attempt
                // exceeds its cap, so a command that committed just as the cap
                // expired is re-proposed and answered `409` on the caller's
                // *first* HTTP attempt — no dead connection, no harness retry.
                // Requiring `retried` here scored that as a failed call, which
                // is the window the faults exist to open.
                Some((409, _)) if method == "POST" => Some(409),
                other => {
                    stats.failures.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "FAIL at {:?} {method} {path} via {} : {:?}",
                        t0.elapsed(),
                        ready[0],
                        other.map(|(status, body)| (
                            status,
                            body.chars()
                                .rev()
                                .take(60)
                                .collect::<String>()
                                .chars()
                                .rev()
                                .collect::<String>()
                        ))
                    );
                    for api in &apis {
                        let jwks = http(
                            *api,
                            "GET",
                            "/v1/tenants/t1/.well-known/jwks.json",
                            None,
                            None,
                        )
                        .map(|(status, _)| status);
                        let tenants = http(*api, "GET", "/v1/tenants", Some(&bearer), None)
                            .map(|(_, body)| body.contains("\"t1\""));
                        eprintln!("  probe {api}: jwks={jwks:?} has_t1={tenants:?}");
                    }
                    None
                }
            }
        };

        call(
            "POST",
            "/v1/nodes/broker-1/heartbeat",
            Some(br#"{"incarnation": 0}"#),
            &[200],
        );
        call("GET", "/v1/shard-assignments/changes?since=0", None, &[200]);

        // A metadata write every few ticks; each acknowledged create is a
        // promise the final state must keep.
        if tenant_counter.is_multiple_of(4) {
            let tenant_id = format!("t-chaos-{}", tenant_counter / 4);
            let body = format!(r#"{{"tenant_id": "{tenant_id}", "display_name": "Chaos"}}"#);
            // A 409 from a retry counts as acknowledged, not merely tolerated:
            // the tenant is in the log, so the final state has to still hold it.
            if call("POST", "/v1/tenants", Some(body.as_bytes()), &[201]).is_some() {
                acked_tenants.lock().expect("acked lock").push(tenant_id);
            }
        }
        tenant_counter += 1;
        std::thread::sleep(Duration::from_millis(150));
    }
}

/// Three members on the raft backend, each on its own volume, seeded with
/// tenant `t1` and its signing keys through the import path.
struct Group {
    instances: Vec<Instance>,
    peers: String,
    apis: Vec<SocketAddr>,
    bearer: String,
    _dirs: Vec<tempfile::TempDir>,
}

fn start_group() -> Group {
    // --- Three members, fixed addresses, own volumes ------------------------
    let apis: Vec<SocketAddr> = (0..3).map(|_| reserve()).collect();
    let peer_addrs: Vec<SocketAddr> = (0..3).map(|_| reserve()).collect();
    let metrics: Vec<SocketAddr> = (0..3).map(|_| reserve()).collect();
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let peers = peer_addrs
        .iter()
        .enumerate()
        .map(|(i, addr)| format!("{}={}", i + 1, addr))
        .collect::<Vec<_>>()
        .join(",");

    let mut instances: Vec<Instance> = (0..3)
        .map(|i| Instance {
            id: (i + 1) as u64,
            api: apis[i],
            metrics: metrics[i],
            data_dir: dirs[i].path().to_path_buf(),
            child: spawn_with_state(
                (i + 1) as u64,
                apis[i],
                metrics[i],
                &dirs[i].path().to_path_buf(),
                &peers,
                "new",
            ),
        })
        .collect();
    for instance in &mut instances {
        wait_ready(instance, Duration::from_secs(30));
    }

    // --- Seed through the #340 import path, then mint a bearer locally ------
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let keys = seed_keys();
    let exported = runtime.block_on(async {
        let seed_store = InMemoryStore::new(StoreConfig {
            changes_limit: 100,
            change_retention_max_rows: Some(1_000),
        });
        seed_store
            .create_tenant(felix_controlplane_service::model::Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant One".to_string(),
            })
            .await
            .expect("tenant");
        seed_store
            .set_tenant_signing_keys("t1", keys.clone())
            .await
            .expect("keys");
        export_state_from(&seed_store).await.expect("export")
    });
    let import = encode_command(&MetaCommand::ImportState {
        state: Box::new(exported),
        overwrite: false,
    });
    let (status, body) = http(
        peer_addrs[0],
        "POST",
        "/internal/raft/propose",
        Some(PEER_TOKEN),
        Some(&import),
    )
    .expect("propose import");
    assert_eq!(status, 200, "import proposal: {body}");
    let payload_start = body.find("\r\n\r\n").expect("body split") + 4;
    decode_result(&body.as_bytes()[payload_start..])
        .expect("decodes")
        .expect("import succeeds");

    let bearer = felix_controlplane_service::auth::felix_token::mint_token_for(
        &keys,
        "t1",
        "p:operator",
        vec![
            "node.manage:cluster:*".to_string(),
            "node.view:cluster:*".to_string(),
            // The traffic loop creates tenants, and the checks below list them.
            "tenant.manage:cluster:*".to_string(),
        ],
        Duration::from_secs(3_600),
        felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )
    .expect("token");

    Group {
        instances,
        peers,
        apis,
        bearer,
        _dirs: dirs,
    }
}

#[test]
fn the_group_survives_restart_kill_freeze_and_wipe_without_losing_a_write() {
    let Group {
        mut instances,
        peers,
        apis,
        bearer,
        _dirs,
    } = start_group();

    // The import committed on the leader; this member may be a follower
    // that has not applied it yet. A real broker retries registration with
    // backoff — do the same, briefly.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (status, register_body) = http(
            apis[0],
            "POST",
            "/v1/nodes",
            Some(&bearer),
            Some(
                br#"{"node_id": "broker-1", "advertise_addr": "10.0.0.4:7000", "region": "local"}"#,
            ),
        )
        .expect("register");
        if status == 200 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "registration never succeeded: {register_body}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // --- Traffic on, faults in sequence -------------------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(Traffic::default());
    let acked = Arc::new(Mutex::new(Vec::new()));
    let traffic = {
        let apis = apis.clone();
        let stop = Arc::clone(&stop);
        let stats = Arc::clone(&stats);
        let acked = Arc::clone(&acked);
        let bearer = bearer.clone();
        let t0_for_traffic = Instant::now();
        std::thread::spawn(move || traffic_loop(apis, bearer, acked, stop, stats, t0_for_traffic))
    };

    let t0 = Instant::now();
    // Fault 1: rolling SIGTERM restart of every member.
    #[allow(clippy::needless_range_loop)] // indexes stay readable next to the fault narration
    for i in 0..3 {
        eprintln!("PHASE restart-{} at {:?}", i + 1, t0.elapsed());
        signal(&instances[i].child, "-TERM");
        wait_exit(&mut instances[i].child, Duration::from_secs(30));
        instances[i].child = spawn_instance(
            instances[i].id,
            instances[i].api,
            instances[i].metrics,
            &instances[i].data_dir,
            &peers,
        );
        wait_ready(&mut instances[i], Duration::from_secs(30));
        // Ready is a promise: a restarted member must already hold its
        // committed state (the startup replay gate) before it reports fit.
        let (_, tenants) = http(instances[i].api, "GET", "/v1/tenants", Some(&bearer), None)
            .expect("tenants after restart");
        assert!(
            tenants.contains("\"t1\""),
            "instance {} reported ready before replaying its committed log",
            instances[i].id
        );
    }

    // Fault 2: SIGKILL the leader specifically — no drain, no goodbye.
    let leader = find_leader(&instances);
    eprintln!("PHASE kill-leader idx {} at {:?}", leader, t0.elapsed());
    instances[leader].child.kill().expect("SIGKILL leader");
    instances[leader].child.wait().expect("reap");
    std::thread::sleep(Duration::from_secs(2));
    instances[leader].child = spawn_instance(
        instances[leader].id,
        instances[leader].api,
        instances[leader].metrics,
        &instances[leader].data_dir,
        &peers,
    );
    wait_ready(&mut instances[leader], Duration::from_secs(30));

    // Fault 3: freeze the leader past several election timeouts, then thaw.
    // The single-machine stand-in for a partition: the frozen member still
    // believes it leads when it wakes, and must rejoin as a follower rather
    // than split the group.
    let leader = find_leader(&instances);
    eprintln!("PHASE freeze-leader idx {} at {:?}", leader, t0.elapsed());
    signal(&instances[leader].child, "-STOP");
    std::thread::sleep(Duration::from_secs(4));
    let _ = find_leader(&instances); // someone else leads while it is frozen
    signal(&instances[leader].child, "-CONT");
    wait_ready(&mut instances[leader], Duration::from_secs(30));

    // Fault 4: a follower loses its volume entirely.
    let leader = find_leader(&instances);
    let victim = (0..3).find(|i| *i != leader).expect("a follower");
    eprintln!("PHASE wipe-follower idx {} at {:?}", victim, t0.elapsed());
    instances[victim].child.kill().expect("SIGKILL follower");
    instances[victim].child.wait().expect("reap");
    std::fs::remove_dir_all(&instances[victim].data_dir).expect("wipe volume");
    std::fs::create_dir_all(&instances[victim].data_dir).expect("recreate dir");
    instances[victim].child = spawn_instance(
        instances[victim].id,
        instances[victim].api,
        instances[victim].metrics,
        &instances[victim].data_dir,
        &peers,
    );
    wait_ready(&mut instances[victim], Duration::from_secs(30));

    // A little quiet running at full strength before judging.
    std::thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);
    traffic.join().expect("traffic thread");

    // --- The verdict ---------------------------------------------------------
    let calls = stats.calls.load(Ordering::Relaxed);
    let failures = stats.failures.load(Ordering::Relaxed);
    let failovers = stats.failovers.load(Ordering::Relaxed);
    let longest_gap = stats.longest_gap_ms.load(Ordering::Relaxed);
    let acked = acked.lock().expect("acked lock").clone();
    eprintln!(
        "chaos: {calls} calls, {failures} failures, {failovers} failovers, \
         longest no-ready gap {longest_gap}ms, {} acked writes",
        acked.len()
    );

    if failures > 0 {
        for instance in &instances {
            let log = instance
                .data_dir
                .parent()
                .unwrap_or(&instance.data_dir)
                .join(format!("cp-{}.log", instance.id));
            if let Ok(content) = std::fs::read_to_string(&log) {
                let tail: Vec<&str> = content.lines().rev().take(25).collect();
                eprintln!("--- log tail instance {} ---", instance.id);
                for line in tail.iter().rev() {
                    eprintln!("{line}");
                }
            }
        }
    }
    assert!(calls > 30, "the traffic loop barely ran ({calls} calls)");
    assert_eq!(failures, 0, "calls failed during the faults");
    assert!(
        longest_gap < 5_000,
        "an election gap became an outage: {longest_gap}ms"
    );
    assert!(!acked.is_empty(), "no writes were acknowledged at all");

    // No acknowledged write lost: every 201 is present on every member —
    // including the one rebuilt from a wiped volume.
    //
    // Reads are served from each member's applied state, so a member that is
    // still catching up (the wiped one, or the thawed ex-leader) can simply
    // not have applied an acked write yet. A marker write committed after
    // every acked one fixes that: members apply the log in order, so once a
    // member serves the marker it has applied everything acked before it,
    // and a missing write from then on is a loss, not lag.
    let marker = "t-chaos-marker";
    let (status, body) = http(
        find_ready(&instances),
        "POST",
        "/v1/tenants",
        Some(&bearer),
        Some(format!(r#"{{"tenant_id": "{marker}", "display_name": "Chaos"}}"#).as_bytes()),
    )
    .expect("marker write");
    assert_eq!(status, 201, "marker write: {body}");
    let mut lost = Vec::new();
    for instance in &instances {
        let body = wait_until_applied(instance, &bearer, marker, Duration::from_secs(30));
        for tenant_id in &acked {
            if !body.contains(&format!("\"{tenant_id}\"")) {
                lost.push(format!("instance {} lost {tenant_id}", instance.id));
            }
        }
    }
    assert!(
        lost.is_empty(),
        "acknowledged writes missing from members that applied a later write: {lost:?}"
    );

    for instance in &instances {
        signal(&instance.child, "-TERM");
    }
    for instance in &mut instances {
        wait_exit(&mut instance.child, Duration::from_secs(30));
    }
}

/// A member that lost its volume must not vote until it holds the log again.
///
/// Raft's safety assumes a voter never forgets its vote or its log. Here the
/// leader L and follower B hold `t-x`; A missed it. With B frozen, L comes
/// back empty and A comes back behind: if L's empty log lets it vote, A wins
/// on A+L and truncates `t-x` from B when B returns, though `t-x` was
/// acknowledged.
#[test]
fn a_wiped_member_does_not_vote_a_lagging_member_into_leadership() {
    let Group {
        mut instances,
        peers,
        bearer,
        _dirs,
        ..
    } = start_group();

    // Let t1 settle everywhere and the leadership gauges refresh.
    std::thread::sleep(Duration::from_secs(3));
    let l = find_leader(&instances);
    let followers: Vec<usize> = (0..3).filter(|i| *i != l).collect();
    let (a, b) = (followers[0], followers[1]);
    eprintln!("leader idx {l}, lagging follower idx {a}, up-to-date follower idx {b}");

    // A is down; t-x commits on L and B.
    instances[a].child.kill().expect("kill a");
    instances[a].child.wait().expect("reap a");
    let (status, body) = http(
        instances[l].api,
        "POST",
        "/v1/tenants",
        Some(&bearer),
        Some(br#"{"tenant_id": "t-x", "display_name": "X"}"#),
    )
    .expect("write t-x");
    assert_eq!(status, 201, "{body}");

    // B freezes; L loses its volume and restarts empty; A restarts behind.
    signal(&instances[b].child, "-STOP");
    instances[l].child.kill().expect("kill l");
    instances[l].child.wait().expect("reap l");
    std::fs::remove_dir_all(&instances[l].data_dir).expect("wipe volume");
    std::fs::create_dir_all(&instances[l].data_dir).expect("recreate dir");
    for idx in [l, a] {
        instances[idx].child = spawn_instance(
            instances[idx].id,
            instances[idx].api,
            instances[idx].metrics,
            &instances[idx].data_dir,
            &peers,
        );
    }
    // Several election timeouts with only A and the wiped L running.
    std::thread::sleep(Duration::from_secs(5));
    signal(&instances[b].child, "-CONT");

    // Every member must end up holding t-x. Once a stale leader has
    // truncated it, it never comes back, so a bounded wait is a fair test.
    let deadline = Instant::now() + Duration::from_secs(30).mul_f64(scale());
    let missing = loop {
        let missing: Vec<u64> = instances
            .iter()
            .filter(|instance| {
                !matches!(
                    http(instance.api, "GET", "/v1/tenants", Some(&bearer), None),
                    Some((200, body)) if body.contains("\"t-x\"")
                )
            })
            .map(|instance| instance.id)
            .collect();
        if missing.is_empty() || Instant::now() >= deadline {
            break missing;
        }
        std::thread::sleep(Duration::from_millis(200));
    };

    // Who led whom, for the failure message.
    let gauges: Vec<String> = instances
        .iter()
        .map(|instance| {
            let lines = http_with_timeout(
                instance.metrics,
                "GET",
                "/metrics",
                None,
                None,
                Duration::from_secs(1),
            )
            .map(|(_, body)| {
                body.lines()
                    .filter(|line| {
                        line.starts_with("felix_meta_raft_is_leader")
                            || line.starts_with("felix_meta_raft_term")
                            || line.starts_with("felix_meta_raft_last_log_index")
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            });
            format!("instance {}: {}", instance.id, lines.unwrap_or_default())
        })
        .collect();
    for instance in &instances {
        let _ = Command::new("kill")
            .arg("-KILL")
            .arg(instance.child.id().to_string())
            .status();
    }
    for instance in &mut instances {
        let _ = instance.child.wait();
    }
    assert!(
        missing.is_empty(),
        "acknowledged write t-x missing from instances {missing:?}: {gauges:#?}"
    );
}

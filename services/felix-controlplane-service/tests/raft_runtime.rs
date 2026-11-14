//! The real binary on the Raft backend: a single-member group serving the
//! HTTP API with no external database, and keeping its metadata across a
//! restart — the property the data directory exists to provide. Also that
//! the Raft routes live only on the authenticated peer listener.
mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use felix_controlplane_service::store::export::export_state_from;
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::raft::command::{MetaCommand, encode_command};
use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig};

const CLUSTER_ID: &str = "runtime-cluster";
const PEER_TOKEN: &str = "runtime-peer-token-runtime-peer-token";

fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&[u8]>,
) -> Option<(u16, String)> {
    http_with_headers(addr, method, path, bearer, &[], body)
}

/// A request to the peer listener, carrying the cluster id and peer token.
fn http_as_peer(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> Option<(u16, String)> {
    http_with_headers(
        addr,
        method,
        path,
        Some(PEER_TOKEN),
        &[(
            felix_controlplane_service::raft::CLUSTER_ID_HEADER,
            CLUSTER_ID,
        )],
        Some(body),
    )
}

fn http_with_headers(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(bearer) = bearer {
        request.push_str(&format!("Authorization: Bearer {bearer}\r\n"));
    }
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    let mut payload = Vec::new();
    match body {
        Some(body) => {
            request.push_str(&format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            ));
            payload.extend_from_slice(body);
        }
        None => request.push_str("\r\n"),
    }
    stream.write_all(request.as_bytes()).ok()?;
    stream.write_all(&payload).ok()?;
    let mut response = Vec::new();
    // A route that refuses the request answers and closes without reading the
    // body, so the kernel may reset the connection after the answer arrived.
    // The answer is what the test asks about; only a reset with nothing read
    // is a failure.
    if stream.read_to_end(&mut response).is_err() && response.is_empty() {
        return None;
    }
    let text = String::from_utf8_lossy(&response).into_owned();
    let status: u16 = text.split_whitespace().nth(1)?.parse().ok()?;
    Some((status, text))
}

/// Kills the child when dropped, so a failing assertion does not leave a
/// control plane running after the test.
struct Reaped(std::process::Child);

impl std::ops::Deref for Reaped {
    type Target = std::process::Child;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Reaped {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn command(addr: SocketAddr, peer: SocketAddr, data_dir: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_felix-controlplane"));
    command
        .env("FELIX_CONTROLPLANE_BIND", addr.to_string())
        .env("FELIX_CONTROLPLANE_METRICS_BIND", "127.0.0.1:0")
        // The three raft variables select the backend on their own.
        .env("FELIX_RAFT_NODE_ID", "1")
        .env("FELIX_RAFT_DATA_DIR", data_dir)
        .env("FELIX_RAFT_PEERS", format!("1={peer}"))
        .env("FELIX_RAFT_BIND_ADDR", peer.to_string())
        .env("FELIX_RAFT_CLUSTER_ID", CLUSTER_ID)
        .env("FELIX_RAFT_PEER_TOKEN", PEER_TOKEN)
        .env("FELIX_SHUTDOWN_PREDRAIN_MS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

/// `initial` is `FELIX_RAFT_INITIAL_CLUSTER_STATE`: `new` on the first
/// start of an empty data dir, `existing` after.
fn spawn(
    addr: SocketAddr,
    peer: SocketAddr,
    data_dir: &std::path::Path,
    initial: &str,
) -> std::process::Child {
    command(addr, peer, data_dir)
        .env("FELIX_RAFT_INITIAL_CLUSTER_STATE", initial)
        .spawn()
        .expect("spawn controlplane")
}

fn wait_ready(child: &mut std::process::Child, addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some((200, _)) = http(addr, "GET", "/v1/system/ready", None, None) {
            return;
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("controlplane exited during startup: {status}");
        }
        assert!(Instant::now() < deadline, "never became ready");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn stop(child: &mut std::process::Child) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(30);
    while child.try_wait().expect("try_wait").is_none() {
        assert!(Instant::now() < deadline, "did not exit");
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn the_binary_serves_and_survives_a_restart_with_no_database() {
    let addr = common::reserve_port();
    let peer = common::reserve_port();
    let data_dir = tempfile::tempdir().expect("tempdir");

    let mut first = Reaped(spawn(addr, peer, data_dir.path(), "new"));
    wait_ready(&mut first, addr);

    // The catalog API needs an operator credential, and a fresh binary has no
    // tenant to mint one from. Seed one through the import path, the same
    // way a migration would, then mint against the keys it was given.
    let bearer = seed_operator(addr, peer);

    let (status, _) = http(
        addr,
        "POST",
        "/v1/tenants",
        Some(&bearer),
        Some(br#"{"tenant_id": "t1", "display_name": "Tenant One"}"#),
    )
    .expect("create tenant");
    assert_eq!(status, 201, "create through the HTTP API on raft");

    stop(&mut first);

    // Same data directory: the metadata must come back from the Raft log
    // and snapshot — there is no database holding it.
    let mut second = Reaped(spawn(addr, peer, data_dir.path(), "existing"));
    wait_ready(&mut second, addr);
    let (status, body) =
        http(addr, "GET", "/v1/tenants", Some(&bearer), None).expect("list tenants");
    assert_eq!(status, 200);
    assert!(
        body.contains("\"t1\""),
        "metadata survived the restart without a database: {body}"
    );

    stop(&mut second);
}

/// Import an `ops` tenant with known keys and return an operator bearer.
///
/// On the way, checks that only a peer can do this: the public API port has
/// no Raft routes at all, and the peer port refuses anyone without the
/// cluster id and peer token.
fn seed_operator(addr: SocketAddr, peer: SocketAddr) -> String {
    let private = [3u8; 32];
    let signing = ed25519_dalek::SigningKey::from_bytes(&private);
    let keys = felix_controlplane_service::auth::felix_token::TenantSigningKeys {
        current: felix_controlplane_service::auth::felix_token::SigningKey {
            kid: "runtime-kid".to_string(),
            alg: jsonwebtoken::Algorithm::EdDSA,
            private_key: private,
            public_key: signing.verifying_key().to_bytes(),
        },
        previous: Vec::new(),
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let exported = runtime.block_on(async {
        let seed = InMemoryStore::new(StoreConfig {
            changes_limit: 100,
            change_retention_max_rows: Some(1_000),
        });
        seed.create_tenant(felix_controlplane_service::model::Tenant {
            tenant_id: "ops".to_string(),
            display_name: "Operators".to_string(),
        })
        .await
        .expect("tenant");
        seed.set_tenant_signing_keys("ops", keys.clone())
            .await
            .expect("keys");
        export_state_from(&seed).await.expect("export")
    });
    let import = encode_command(&MetaCommand::ImportState {
        state: Box::new(exported),
        overwrite: false,
        mark_unplaced: false,
    });
    let (status, _) = http(
        addr,
        "POST",
        &felix_controlplane_service::raft::peer_route("propose"),
        None,
        Some(&import),
    )
    .expect("api port");
    assert_eq!(status, 404, "the public API port serves no raft routes");
    let (status, _) = http(
        peer,
        "POST",
        &felix_controlplane_service::raft::peer_route("propose"),
        None,
        Some(&import),
    )
    .expect("peer port");
    assert_eq!(
        status, 403,
        "an anonymous caller on the peer port is refused"
    );
    let (status, body) = http_as_peer(
        peer,
        "POST",
        &felix_controlplane_service::raft::peer_route("propose"),
        &import,
    )
    .expect("propose import");
    assert_eq!(status, 200, "import the operator tenant: {body}");

    felix_controlplane_service::auth::felix_token::mint_token_for(
        &keys,
        "ops",
        "p:operator",
        vec!["tenant.manage:cluster:*".to_string()],
        Duration::from_secs(3_600),
        felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )
    .expect("token")
}

/// The Raft routes can replace the whole store; without a peer token (and
/// no explicit insecure opt-out) the binary does not start at all.
#[test]
fn the_binary_refuses_raft_without_a_peer_token() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let output = command(
        common::reserve_port(),
        common::reserve_port(),
        data_dir.path(),
    )
    .env_remove("FELIX_RAFT_PEER_TOKEN")
    .stderr(Stdio::piped())
    .output()
    .expect("run controlplane");
    assert!(!output.status.success(), "started without a peer token");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("FELIX_RAFT_PEER_TOKEN"), "{stderr}");
}

/// The path of a released control plane binary, one that keeps its store in
/// `raft.redb` (v0.7.1). CI downloads it
/// (`scripts/fetch-released-controlplane.sh`) and sets [`REQUIRE_RELEASED`];
/// elsewhere the test that needs it has nothing to run against and says so.
const RELEASED_BIN: &str = "RELEASED_CONTROLPLANE_BIN";
const REQUIRE_RELEASED: &str = "REQUIRE_RELEASED_CONTROLPLANE_BIN";

/// A member run as a child process, on whichever binary.
struct Member {
    id: u64,
    api: SocketAddr,
    peer: SocketAddr,
    metrics: SocketAddr,
    dir: tempfile::TempDir,
    child: Option<Reaped>,
}

impl Member {
    fn start(&mut self, bin: &std::ffi::OsStr, peers: &str, initial: &str) {
        let child = Command::new(bin)
            .env("FELIX_CONTROLPLANE_BIND", self.api.to_string())
            .env("FELIX_CONTROLPLANE_METRICS_BIND", self.metrics.to_string())
            .env("FELIX_RAFT_NODE_ID", self.id.to_string())
            .env("FELIX_RAFT_DATA_DIR", self.dir.path())
            .env("FELIX_RAFT_PEERS", peers)
            .env("FELIX_RAFT_BIND_ADDR", self.peer.to_string())
            .env("FELIX_RAFT_CLUSTER_ID", CLUSTER_ID)
            .env("FELIX_RAFT_PEER_TOKEN", PEER_TOKEN)
            .env("FELIX_RAFT_INITIAL_CLUSTER_STATE", initial)
            .env("FELIX_SHUTDOWN_PREDRAIN_MS", "0")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn controlplane");
        self.child = Some(Reaped(child));
    }

    fn wait_ready(&mut self) {
        wait_ready(self.child.as_mut().expect("running"), self.api);
    }

    fn stop(&mut self) {
        stop(&mut self.child.take().expect("running"));
    }
}

/// Every file under `dir`, with its length and modification time.
fn listing(dir: &std::path::Path) -> Vec<(std::path::PathBuf, u64, std::time::SystemTime)> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).expect("read_dir") {
            let entry = entry.expect("entry");
            let meta = entry.metadata().expect("metadata");
            if meta.is_dir() {
                pending.push(entry.path());
            }
            found.push((entry.path(), meta.len(), meta.modified().expect("mtime")));
        }
    }
    found.sort();
    found
}

/// A data directory a released control plane wrote holds its Raft state in
/// `raft.redb`, which this build cannot read. Started on it, this build must
/// refuse before it writes anything: starting as a new member would drop
/// the group's metadata without a word.
#[test]
fn this_build_refuses_a_data_dir_a_released_binary_wrote() {
    let Some(released) = std::env::var_os(RELEASED_BIN) else {
        assert!(
            std::env::var_os(REQUIRE_RELEASED).is_none(),
            "{RELEASED_BIN} is not set"
        );
        eprintln!("skipped: {RELEASED_BIN} is not set");
        return;
    };
    let mut member = Member {
        id: 1,
        api: common::reserve_port(),
        peer: common::reserve_port(),
        metrics: common::reserve_port(),
        dir: tempfile::tempdir().expect("data dir"),
        child: None,
    };
    let peers = format!("1={}", member.peer);
    // Ready means the group formed: the release's store holds its membership
    // entries and vote.
    member.start(&released, &peers, "new");
    member.wait_ready();
    member.stop();
    let before = listing(member.dir.path());
    assert!(
        before.iter().any(|(path, ..)| path.ends_with("raft.redb")),
        "the release keeps its store in raft.redb: {before:?}"
    );

    let refused = Command::new(env!("CARGO_BIN_EXE_felix-controlplane"))
        .env("FELIX_CONTROLPLANE_BIND", member.api.to_string())
        .env(
            "FELIX_CONTROLPLANE_METRICS_BIND",
            member.metrics.to_string(),
        )
        .env("FELIX_RAFT_NODE_ID", "1")
        .env("FELIX_RAFT_DATA_DIR", member.dir.path())
        .env("FELIX_RAFT_PEERS", &peers)
        .env("FELIX_RAFT_BIND_ADDR", member.peer.to_string())
        .env("FELIX_RAFT_CLUSTER_ID", CLUSTER_ID)
        .env("FELIX_RAFT_PEER_TOKEN", PEER_TOKEN)
        .env("FELIX_RAFT_INITIAL_CLUSTER_STATE", "new")
        .env("FELIX_SHUTDOWN_PREDRAIN_MS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn controlplane");
    let (status, stderr) = {
        let mut child = Reaped(refused);
        let deadline = Instant::now() + Duration::from_secs(30);
        while child.try_wait().expect("try_wait").is_none() {
            assert!(
                Instant::now() < deadline,
                "this build started on a released data dir"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .expect("stderr")
            .read_to_string(&mut stderr)
            .expect("read stderr");
        (child.try_wait().expect("status").expect("exited"), stderr)
    };
    assert!(!status.success(), "refused with a failure status");
    assert!(stderr.contains("raft.redb"), "{stderr}");
    assert_eq!(listing(member.dir.path()), before, "nothing was written");
}

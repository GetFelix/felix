//! The real binary on the Raft backend: a single-member group serving the
//! HTTP API with no external database, and keeping its metadata across a
//! restart — the property the data directory exists to provide. Also that
//! the Raft routes live only on the authenticated peer listener.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
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
    stream.read_to_end(&mut response).ok()?;
    let text = String::from_utf8_lossy(&response).into_owned();
    let status: u16 = text.split_whitespace().nth(1)?.parse().ok()?;
    Some((status, text))
}

fn reserve() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("addr")
}

fn command(addr: SocketAddr, peer: SocketAddr, data_dir: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_felix-controlplane"));
    command
        .env("FELIX_CONTROLPLANE_BIND", addr.to_string())
        .env("FELIX_CONTROLPLANE_METRICS_BIND", "127.0.0.1:0")
        // The three raft variables select the backend on their own, the same
        // way a postgres URL selects postgres.
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
    let addr = reserve();
    let peer = reserve();
    let data_dir = tempfile::tempdir().expect("tempdir");

    let mut first = spawn(addr, peer, data_dir.path(), "new");
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
    let mut second = spawn(addr, peer, data_dir.path(), "existing");
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
    });
    let (status, _) =
        http(addr, "POST", "/internal/raft/propose", None, Some(&import)).expect("api port");
    assert_eq!(status, 404, "the public API port serves no raft routes");
    let (status, _) =
        http(peer, "POST", "/internal/raft/propose", None, Some(&import)).expect("peer port");
    assert_eq!(
        status, 403,
        "an anonymous caller on the peer port is refused"
    );
    let (status, body) =
        http_as_peer(peer, "POST", "/internal/raft/propose", &import).expect("propose import");
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
    let output = command(reserve(), reserve(), data_dir.path())
        .env_remove("FELIX_RAFT_PEER_TOKEN")
        .stderr(Stdio::piped())
        .output()
        .expect("run controlplane");
    assert!(!output.status.success(), "started without a peer token");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("FELIX_RAFT_PEER_TOKEN"), "{stderr}");
}

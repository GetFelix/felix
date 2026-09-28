//! The metadata Raft group, exercised as a group: election, replication,
//! restart with state, rebuild from snapshot, and learner-first growth.
//!
//! Each node here is the real thing — a `RaftHandle` over the redb store,
//! serving its RPCs on a real HTTP listener — with a toy key/value app
//! standing where the metadata state machine (#338) will stand.
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use felix_controlplane_service::raft::{AppStateMachine, NodeId, RaftHandle, RaftSettings};

/// A deterministic key/value app: `set k v` returns the previous value.
#[derive(Default)]
struct KvApp {
    state: RwLock<BTreeMap<String, String>>,
}

impl KvApp {
    fn get(&self, key: &str) -> Option<String> {
        self.state.read().expect("state lock").get(key).cloned()
    }

    fn len(&self) -> usize {
        self.state.read().expect("state lock").len()
    }
}

#[async_trait::async_trait]
impl AppStateMachine for KvApp {
    async fn apply(&self, command: &[u8]) -> Vec<u8> {
        let (key, value): (String, String) =
            serde_json::from_slice(command).expect("decode command");
        let previous = self.state.write().expect("state lock").insert(key, value);
        previous.map(String::into_bytes).unwrap_or_default()
    }

    async fn snapshot(&self) -> Vec<u8> {
        serde_json::to_vec(&*self.state.read().expect("state lock")).expect("encode snapshot")
    }

    async fn restore(&self, snapshot: &[u8]) {
        let restored: BTreeMap<String, String> =
            serde_json::from_slice(snapshot).expect("decode snapshot");
        *self.state.write().expect("state lock") = restored;
    }
}

fn set(key: &str, value: &str) -> Vec<u8> {
    serde_json::to_vec(&(key, value)).expect("encode command")
}

struct TestNode {
    handle: RaftHandle,
    app: Arc<KvApp>,
    addr: SocketAddr,
    dir: PathBuf,
    server: tokio::task::JoinHandle<()>,
}

/// Election timings shrunk so a test failure is a failure, not a wait.
fn settings(id: NodeId, dir: PathBuf) -> RaftSettings {
    let mut settings = RaftSettings::new(id, dir, peer_security());
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(200), Duration::from_millis(400));
    settings
}

async fn start_node(dir: PathBuf, settings: RaftSettings, addr: Option<SocketAddr>) -> TestNode {
    let app = Arc::new(KvApp::default());
    let handle = RaftHandle::start(settings, Arc::clone(&app) as Arc<dyn AppStateMachine>)
        .await
        .expect("start raft node");
    // A node restarting on its predecessor's address can race the aborted
    // server task still holding the socket; retry briefly rather than flake.
    let bind_addr = addr.unwrap_or("127.0.0.1:0".parse().expect("addr"));
    let listener = {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match tokio::net::TcpListener::bind(bind_addr).await {
                Ok(listener) => break listener,
                Err(err) if Instant::now() < deadline => {
                    let _ = err;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => panic!("bind rpc listener: {err}"),
            }
        }
    };
    let addr = listener.local_addr().expect("local addr");
    let router = handle.rpc_router();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    TestNode {
        handle,
        app,
        addr,
        dir,
        server,
    }
}

impl TestNode {
    async fn stop(&self) {
        let _ = self.handle.shutdown().await;
        self.server.abort();
    }
}

async fn wait_until(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The current leader's handle, waiting out an election if one is running.
async fn leader_of(nodes: &[&TestNode]) -> RaftHandle {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for node in nodes {
            let status = node.handle.status();
            if let Some(leader) = status.leader
                && let Some(found) = nodes.iter().find(|n| n.handle.status().id == leader)
            {
                return found.handle.clone();
            }
        }
        assert!(Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_single_node_group_serves_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = start_node(dir.path().into(), settings(1, dir.path().into()), None).await;

    node.handle
        .initialize(BTreeMap::from([(1, node.addr.to_string())]))
        .await
        .expect("initialize");
    leader_of(&[&node]).await;

    let previous = node.handle.write(set("k", "v1")).await.expect("write");
    assert!(previous.is_empty(), "first write has no previous value");
    let previous = node.handle.write(set("k", "v2")).await.expect("write");
    assert_eq!(
        previous, b"v1",
        "the response is the state machine's answer"
    );
    assert_eq!(node.app.get("k").as_deref(), Some("v2"));

    node.stop().await;
}

/// The acceptance test for the group itself: three members elect, writes
/// reach every state machine, and a member that restarts with its disk
/// rejoins and catches up.
#[tokio::test]
async fn a_three_node_group_replicates_and_survives_restart() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let id = (i + 1) as NodeId;
        nodes.push(start_node(dir.path().into(), settings(id, dir.path().into()), None).await);
    }

    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|node| (node.handle.status().id, node.addr.to_string()))
        .collect();
    nodes[0]
        .handle
        .initialize(members)
        .await
        .expect("initialize");

    let leader = leader_of(&nodes.iter().collect::<Vec<_>>()).await;
    for i in 0..5 {
        leader
            .write(set(&format!("k{i}"), "before"))
            .await
            .expect("write");
    }
    for node in &nodes {
        let app = Arc::clone(&node.app);
        wait_until(
            "all writes on every member",
            Duration::from_secs(10),
            move || app.len() == 5,
        )
        .await;
    }

    // A follower goes away; the group keeps serving without it.
    let leader_id = leader.status().id;
    let follower_index = nodes
        .iter()
        .position(|node| node.handle.status().id != leader_id)
        .expect("a follower exists");
    let follower_addr = nodes[follower_index].addr;
    let follower_dir = nodes[follower_index].dir.clone();
    let follower_id = nodes[follower_index].handle.status().id;
    nodes[follower_index].stop().await;

    for i in 5..8 {
        leader
            .write(set(&format!("k{i}"), "while-away"))
            .await
            .expect("write");
    }

    // Back on the same identity, address, and disk: a restart is a rejoin.
    let restarted = start_node(
        follower_dir.clone(),
        settings(follower_id, follower_dir),
        Some(follower_addr),
    )
    .await;
    let app = Arc::clone(&restarted.app);
    wait_until(
        "the restarted member catches up",
        Duration::from_secs(10),
        move || app.len() == 8,
    )
    .await;
    assert_eq!(restarted.app.get("k7").as_deref(), Some("while-away"));

    restarted.stop().await;
    for node in &nodes {
        node.stop().await;
    }
}

/// A member that lost its disk entirely is rebuilt by snapshot install —
/// the log behind the snapshot is purged, so there is no other way back.
#[tokio::test]
async fn a_wiped_member_is_rebuilt_by_snapshot() {
    let mut tuned = Vec::new();
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    for (i, dir) in dirs.iter().enumerate() {
        let id = (i + 1) as NodeId;
        let mut s = settings(id, dir.path().into());
        // Snapshot early and keep nothing behind it, so catching up from
        // scratch cannot quietly use the log instead.
        s.snapshot_logs_since_last = 5;
        s.logs_kept_behind_snapshot = 0;
        tuned.push(s);
    }
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(start_node(dir.path().into(), tuned[i].clone(), None).await);
    }

    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|node| (node.handle.status().id, node.addr.to_string()))
        .collect();
    nodes[0]
        .handle
        .initialize(members)
        .await
        .expect("initialize");
    let leader = leader_of(&nodes.iter().collect::<Vec<_>>()).await;

    for i in 0..20 {
        leader
            .write(set(&format!("k{i}"), "v"))
            .await
            .expect("write");
    }
    leader.trigger_snapshot().await.expect("snapshot");

    let leader_id = leader.status().id;
    let victim_index = nodes
        .iter()
        .position(|node| node.handle.status().id != leader_id)
        .expect("a follower exists");
    let victim_addr = nodes[victim_index].addr;
    let victim_id = nodes[victim_index].handle.status().id;
    nodes[victim_index].stop().await;

    // The disk is gone: a brand-new directory, same identity and address.
    let fresh = tempfile::tempdir().expect("tempdir");
    let mut fresh_settings = settings(victim_id, fresh.path().into());
    fresh_settings.snapshot_logs_since_last = 5;
    fresh_settings.logs_kept_behind_snapshot = 0;
    let rebuilt = start_node(fresh.path().into(), fresh_settings, Some(victim_addr)).await;

    let app = Arc::clone(&rebuilt.app);
    wait_until(
        "the wiped member is rebuilt",
        Duration::from_secs(15),
        move || app.len() == 20,
    )
    .await;
    assert_eq!(rebuilt.app.get("k19").as_deref(), Some("v"));

    rebuilt.stop().await;
    for node in &nodes {
        node.stop().await;
    }
}

/// Growing the group is learner-first: the newcomer holds the data before
/// it holds a vote, so a join never costs quorum.
#[tokio::test]
async fn a_learner_catches_up_and_then_votes() {
    let dir1 = tempfile::tempdir().expect("tempdir");
    let one = start_node(dir1.path().into(), settings(1, dir1.path().into()), None).await;
    one.handle
        .initialize(BTreeMap::from([(1, one.addr.to_string())]))
        .await
        .expect("initialize");
    leader_of(&[&one]).await;
    for i in 0..3 {
        one.handle
            .write(set(&format!("k{i}"), "v"))
            .await
            .expect("write");
    }

    let dir2 = tempfile::tempdir().expect("tempdir");
    let two = start_node(dir2.path().into(), settings(2, dir2.path().into()), None).await;

    // add_learner blocks until the learner's *log* matches; the state
    // machine applies on the next commit notification, so the data is
    // observed with a short wait rather than instantly.
    one.handle
        .add_learner(2, two.addr.to_string())
        .await
        .expect("add learner");
    assert!(
        !one.handle.status().voters.contains(&2),
        "a learner is not yet a voter"
    );
    let two_app = Arc::clone(&two.app);
    wait_until(
        "the learner holds the data",
        Duration::from_secs(5),
        move || two_app.len() == 3,
    )
    .await;

    one.handle.change_membership([1, 2]).await.expect("promote");
    wait_until(
        "the learner becomes a voter",
        Duration::from_secs(5),
        || one.handle.status().voters.contains(&2),
    )
    .await;

    one.stop().await;
    two.stop().await;
}

/// The Raft routes can replace the whole store, so a caller that does not
/// name this cluster and present its peer token never reaches them — not
/// `propose`, not `vote`.
#[tokio::test]
async fn the_rpc_routes_refuse_callers_without_peer_credentials() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = start_node(dir.path().into(), settings(1, dir.path().into()), None).await;
    node.handle
        .initialize(BTreeMap::from([(1, node.addr.to_string())]))
        .await
        .expect("initialize");
    leader_of(&[&node]).await;

    let anonymous = reqwest::Client::new();
    let propose = format!("http://{}/internal/raft/propose", node.addr);
    let status = |response: reqwest::Response| response.status().as_u16();
    let refused = anonymous
        .post(&propose)
        .body(set("k", "forged"))
        .send()
        .await
        .map(status)
        .expect("send");
    assert_eq!(refused, 403, "no cluster id");
    let refused = anonymous
        .post(&propose)
        .header(
            felix_controlplane_service::raft::CLUSTER_ID_HEADER,
            "test-cluster",
        )
        .body(set("k", "forged"))
        .send()
        .await
        .map(status)
        .expect("send");
    assert_eq!(refused, 401, "no peer token");
    let refused = anonymous
        .post(format!("http://{}/internal/raft/vote", node.addr))
        .header(
            felix_controlplane_service::raft::CLUSTER_ID_HEADER,
            "test-cluster",
        )
        .header("authorization", "Bearer not-the-token-not-the-token-not!!")
        .json(&serde_json::json!({}))
        .send()
        .await
        .map(status)
        .expect("send");
    assert_eq!(refused, 401, "wrong peer token");
    assert_eq!(node.app.get("k"), None, "nothing forged was applied");

    let peer = peer_security()
        .client(Duration::from_secs(5))
        .expect("peer client");
    let accepted = peer
        .post(&propose)
        .body(set("k", "v1"))
        .send()
        .await
        .map(status)
        .expect("send");
    assert_eq!(accepted, 200);
    assert_eq!(node.app.get("k").as_deref(), Some("v1"));

    node.stop().await;
}

/// Empty members that make up a majority used to form a fresh group on
/// their own, which after lost volumes means a new, empty control plane.
/// Now they wait until the operator says this is a first start.
#[tokio::test]
async fn empty_members_form_a_group_only_when_told_to() {
    use felix_controlplane_service::raft::InitialClusterState;

    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let id = (i + 1) as NodeId;
        nodes.push(start_node(dir.path().into(), settings(id, dir.path().into()), None).await);
    }
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|node| (node.handle.status().id, node.addr.to_string()))
        .collect();
    let shutdown = tokio_util::sync::CancellationToken::new();
    for node in &nodes {
        node.handle
            .enter_group(
                members.clone(),
                InitialClusterState::Existing,
                shutdown.clone(),
            )
            .expect("enter group");
    }
    // Many election timeouts, and every member can see every other.
    tokio::time::sleep(Duration::from_secs(3)).await;
    for node in &nodes {
        let status = node.handle.status();
        assert_eq!(status.leader, None, "member {} found a leader", status.id);
        assert!(
            status.voters.is_empty(),
            "member {} formed a group",
            status.id
        );
    }
    shutdown.cancel();
    let addrs: Vec<SocketAddr> = nodes.iter().map(|node| node.addr).collect();
    for node in &nodes {
        node.stop().await;
    }
    drop(nodes);

    // The same empty members, told this is day 0.
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let id = (i + 1) as NodeId;
        nodes.push(
            start_node(
                dir.path().into(),
                settings(id, dir.path().into()),
                Some(addrs[i]),
            )
            .await,
        );
    }
    let shutdown = tokio_util::sync::CancellationToken::new();
    for node in &nodes {
        node.handle
            .enter_group(members.clone(), InitialClusterState::New, shutdown.clone())
            .expect("enter group");
    }
    let refs: Vec<&TestNode> = nodes.iter().collect();
    let leader = leader_of(&refs).await;
    leader.write(set("k", "v1")).await.expect("write");
    shutdown.cancel();
    for node in &nodes {
        node.stop().await;
    }
}

/// Under peer mTLS the members replicate over TLS, and a caller holding the
/// peer token but no certificate from the cluster CA is stopped at the
/// handshake.
#[tokio::test]
async fn peers_replicate_over_mtls_and_refuse_a_caller_without_a_certificate() {
    let pki_dir = tempfile::tempdir().expect("tempdir");
    let tls = peer_pki(pki_dir.path());
    let dirs: Vec<tempfile::TempDir> = (0..2)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let id = (i + 1) as NodeId;
        let mut settings = settings(id, dir.path().into());
        settings.security.tls = Some(tls.clone());
        let app = Arc::new(KvApp::default());
        let handle = RaftHandle::start(settings, Arc::clone(&app) as Arc<dyn AppStateMachine>)
            .await
            .expect("start raft node");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let server = {
            let handle = handle.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                handle
                    .serve_peers(listener, shutdown)
                    .await
                    .expect("serve peers");
            })
        };
        nodes.push((handle, app, port, shutdown, server));
    }
    // `localhost`, the name the certificate carries: peers verify it.
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .enumerate()
        .map(|(i, (_, _, port, _, _))| ((i + 1) as NodeId, format!("localhost:{port}")))
        .collect();
    nodes[0].0.initialize(members).await.expect("initialize");
    let deadline = Instant::now() + Duration::from_secs(10);
    let leader = loop {
        if let Some(leader) = nodes[0].0.status().leader {
            break nodes[(leader - 1) as usize].0.clone();
        }
        assert!(Instant::now() < deadline, "no leader over mTLS");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    leader.write(set("k", "v1")).await.expect("write over mTLS");
    wait_until("replication over mTLS", Duration::from_secs(5), || {
        nodes
            .iter()
            .all(|(_, app, ..)| app.get("k").as_deref() == Some("v1"))
    })
    .await;

    // The token alone is not enough without a certificate.
    let ca = std::fs::read(&tls.ca_path).expect("read ca");
    let no_cert = reqwest::Client::builder()
        .tls_certs_only([reqwest::Certificate::from_pem(&ca).expect("ca")])
        .build()
        .expect("client");
    let result = no_cert
        .get(format!(
            "https://localhost:{}/internal/raft/standing",
            nodes[0].2
        ))
        .header(
            felix_controlplane_service::raft::CLUSTER_ID_HEADER,
            "test-cluster",
        )
        .header("authorization", "Bearer 0123456789abcdef0123456789abcdef")
        .send()
        .await;
    assert!(
        result.is_err(),
        "a caller without a certificate got {result:?}"
    );

    for (handle, _, _, shutdown, server) in nodes {
        let _ = handle.shutdown().await;
        shutdown.cancel();
        server.abort();
    }
}

/// A CA and one certificate for `localhost`, used by every member as both
/// server and client.
fn peer_pki(dir: &std::path::Path) -> felix_controlplane_service::raft::PeerTls {
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");
    let ca = rcgen::Issuer::new(ca_params, ca_key);
    let key = rcgen::KeyPair::generate().expect("peer key");
    let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .expect("peer params")
        .signed_by(&key, &ca)
        .expect("peer cert");
    let path = |name: &str| dir.join(name).to_string_lossy().into_owned();
    std::fs::write(path("ca.pem"), ca_cert.pem()).expect("write ca");
    std::fs::write(path("peer.pem"), cert.pem()).expect("write cert");
    std::fs::write(path("peer.key"), key.serialize_pem()).expect("write key");
    felix_controlplane_service::raft::PeerTls {
        cert_path: path("peer.pem"),
        key_path: path("peer.key"),
        ca_path: path("ca.pem"),
    }
}

/// Peer credentials every member of a test group shares.
fn peer_security() -> felix_controlplane_service::raft::PeerSecurity {
    felix_controlplane_service::raft::PeerSecurity {
        cluster_id: "test-cluster".to_string(),
        token: Some("0123456789abcdef0123456789abcdef".to_string()),
        tls: None,
    }
}

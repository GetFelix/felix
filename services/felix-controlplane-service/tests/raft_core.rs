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

use felix_controlplane_service::raft::{
    AppStateMachine, LeaderService, NodeId, NotLeader, RaftHandle, RaftSettings,
};

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

/// Peer routes a test holds on one member. A held route answers 503, as a
/// member that cannot be reached for it would look.
#[derive(Default)]
struct Gate {
    /// Vote requests: with every member's held, nobody can be elected.
    votes: std::sync::atomic::AtomicBool,
    /// Requests to record a forming member.
    forming: std::sync::atomic::AtomicBool,
}

impl Gate {
    fn holding_votes() -> Arc<Self> {
        let gate = Arc::new(Self::default());
        gate.votes.store(true, std::sync::atomic::Ordering::SeqCst);
        gate
    }

    async fn check(
        axum::extract::State(gate): axum::extract::State<Arc<Self>>,
        request: axum::extract::Request,
        next: axum::middleware::Next,
    ) -> axum::response::Response {
        use axum::response::IntoResponse;
        use std::sync::atomic::Ordering::SeqCst;

        let path = request.uri().path();
        let held = (path.ends_with("/vote") && gate.votes.load(SeqCst))
            || (path.ends_with("/forming") && gate.forming.load(SeqCst));
        if held {
            return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        next.run(request).await
    }
}

/// Election timings shrunk so a test failure is a failure, not a wait.
fn settings(id: NodeId, dir: PathBuf) -> RaftSettings {
    let mut settings = RaftSettings::new(id, dir, peer_security());
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(200), Duration::from_millis(400));
    settings
}

async fn start_node(dir: PathBuf, settings: RaftSettings, addr: Option<SocketAddr>) -> TestNode {
    start_gated(dir, settings, addr, Arc::default()).await
}

async fn start_gated(
    dir: PathBuf,
    settings: RaftSettings,
    addr: Option<SocketAddr>,
    gate: Arc<Gate>,
) -> TestNode {
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
    let router = handle
        .rpc_router()
        .layer(axum::middleware::from_fn_with_state(gate, Gate::check));
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

struct Echo;

#[async_trait::async_trait]
impl LeaderService for Echo {
    async fn handle(&self, request: &[u8]) -> Result<Vec<u8>, NotLeader> {
        Ok(request.to_vec())
    }
}

/// openraft's metrics keep naming a stopped node as leader. The handle must
/// not: it reports no leadership and refuses leader-only requests at once.
#[tokio::test]
async fn a_stopped_leader_stops_claiming_leadership() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = start_node(dir.path().into(), settings(1, dir.path().into()), None).await;
    node.handle.set_leader_service(Arc::new(Echo));
    node.handle
        .initialize(BTreeMap::from([(1, node.addr.to_string())]))
        .await
        .expect("initialize");
    leader_of(&[&node]).await;
    assert_eq!(
        node.handle.ask_leader(b"ping".to_vec()).await.expect("ask"),
        b"ping"
    );

    node.stop().await;
    assert!(
        !node.handle.is_leader(),
        "a stopped member reports leadership"
    );
    assert_eq!(node.handle.status().leader, None);
    let asked = Instant::now();
    assert!(
        node.handle.ask_leader(b"ping".to_vec()).await.is_err(),
        "a stopped member answered a leader-only request",
    );
    assert!(
        asked.elapsed() < Duration::from_secs(1),
        "the refusal waited out the write budget",
    );
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
    let propose = format!(
        "http://{}{}",
        node.addr,
        felix_controlplane_service::raft::peer_route("propose")
    );
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
        .post(format!(
            "http://{}{}",
            node.addr,
            felix_controlplane_service::raft::peer_route("vote")
        ))
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
            .await
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
            .await
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

/// An address nothing listens on, as a member that is down looks. The
/// socket stays bound and never listens: a port given back could go to
/// another test's member, which would answer as this group's.
struct ClosedAddr {
    addr: SocketAddr,
    _bound: tokio::net::TcpSocket,
}

fn closed_addr() -> ClosedAddr {
    let bound = tokio::net::TcpSocket::new_v4().expect("socket");
    bound
        .bind("127.0.0.1:0".parse().expect("addr"))
        .expect("bind");
    ClosedAddr {
        addr: bound.local_addr().expect("addr"),
        _bound: bound,
    }
}

/// Something at a member's address that answers `standing` with `status`
/// and `body`, whoever asks, and records any forming member.
async fn standing_stub(
    status: u16,
    body: &'static str,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = axum::Router::new()
        .route(
            &felix_controlplane_service::raft::peer_route("standing"),
            axum::routing::get(move || async move {
                (
                    axum::http::StatusCode::from_u16(status).expect("status"),
                    body,
                )
            }),
        )
        .route(
            &felix_controlplane_service::raft::peer_route("forming"),
            axum::routing::post(|| async { axum::http::StatusCode::NO_CONTENT }),
        );
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (addr, server)
}

/// Start empty members 1 and 3 of a three-member group whose member 2 is at
/// `second`, and have them enter it as a first start.
async fn first_start_without_the_second(
    dirs: &[tempfile::TempDir; 2],
    addrs: Option<[SocketAddr; 2]>,
    second: SocketAddr,
) -> (
    Vec<TestNode>,
    BTreeMap<NodeId, String>,
    tokio_util::sync::CancellationToken,
) {
    use felix_controlplane_service::raft::InitialClusterState;

    let mut nodes = Vec::new();
    for (i, id) in [1, 3].into_iter().enumerate() {
        let dir: PathBuf = dirs[i].path().into();
        let addr = addrs.map(|addrs| addrs[i]);
        nodes.push(start_node(dir.clone(), settings(id, dir), addr).await);
    }
    let members = BTreeMap::from([
        (1, nodes[0].addr.to_string()),
        (2, second.to_string()),
        (3, nodes[1].addr.to_string()),
    ]);
    let shutdown = tokio_util::sync::CancellationToken::new();
    for node in &nodes {
        node.handle
            .enter_group(members.clone(), InitialClusterState::New, shutdown.clone())
            .await
            .expect("enter group");
    }
    (nodes, members, shutdown)
}

/// Many election timeouts later, no member has a leader or a membership.
async fn assert_no_group_forms(nodes: &[TestNode], why: &str) {
    tokio::time::sleep(Duration::from_secs(3)).await;
    for node in nodes {
        let status = node.handle.status();
        assert_eq!(
            status.leader, None,
            "{why}: member {} found a leader",
            status.id
        );
        assert!(
            status.voters.is_empty(),
            "{why}: member {} formed a group",
            status.id
        );
    }
}

async fn waive_the_second(dir: &std::path::Path) -> anyhow::Result<String> {
    felix_controlplane_service::raft::form_without(
        dir,
        "test-cluster",
        &std::collections::BTreeSet::from([2]),
    )
    .await
}

/// The initial membership every first-boot member writes at index 0 is not
/// a group: a member that decides after a peer has initialized still forms,
/// or that peer could never win its vote.
#[tokio::test]
async fn three_empty_members_form_one_group() {
    use felix_controlplane_service::raft::InitialClusterState;

    // Member 1 decides first, against two peers that answer empty and give
    // no vote, so it initializes and stays at index 0.
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let first = start_node(
        dirs[0].path().into(),
        settings(1, dirs[0].path().into()),
        None,
    )
    .await;
    let (second, second_stub) = standing_stub(200, "{}").await;
    let (third, third_stub) = standing_stub(200, "{}").await;
    let members = BTreeMap::from([
        (1, first.addr.to_string()),
        (2, second.to_string()),
        (3, third.to_string()),
    ]);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let enter = |node: &TestNode| {
        let handle = node.handle.clone();
        let (members, shutdown) = (members.clone(), shutdown.clone());
        async move {
            handle
                .enter_group(members, InitialClusterState::New, shutdown)
                .await
                .expect("enter group");
        }
    };
    enter(&first).await;
    wait_until(
        "the first member to initialize",
        Duration::from_secs(10),
        || !first.handle.status().voters.is_empty(),
    )
    .await;

    // The real members 2 and 3 start after that and ask it.
    let mut nodes = vec![first];
    for (i, (addr, stub)) in [(second, second_stub), (third, third_stub)]
        .into_iter()
        .enumerate()
    {
        stub.abort();
        let dir: PathBuf = dirs[i + 1].path().into();
        let node = start_node(dir.clone(), settings((i + 2) as NodeId, dir), Some(addr)).await;
        enter(&node).await;
        nodes.push(node);
    }
    let refs: Vec<&TestNode> = nodes.iter().collect();
    let leader = leader_of(&refs).await;
    leader.write(set("k", "v1")).await.expect("write");
    wait_until(
        "every member in the one group",
        Duration::from_secs(10),
        || {
            nodes
                .iter()
                .all(|node| node.handle.status().voters == [1, 2, 3] && node.app.get("k").is_some())
        },
    )
    .await;
    shutdown.cancel();
    for node in &nodes {
        node.stop().await;
    }
}

/// A first start acts on answers, not on silence: with one member down the
/// other two are a majority and both empty, and they still do not form.
#[tokio::test]
async fn first_boot_waits_for_every_peer() {
    use felix_controlplane_service::raft::InitialClusterState;

    let dirs = [
        tempfile::tempdir().expect("tempdir"),
        tempfile::tempdir().expect("tempdir"),
    ];
    let closed = closed_addr();
    let second = closed.addr;
    let (mut nodes, members, shutdown) = first_start_without_the_second(&dirs, None, second).await;
    assert_no_group_forms(&nodes, "one member is down").await;

    // The missing member answers, empty like the others.
    drop(closed);
    let dir = tempfile::tempdir().expect("tempdir");
    let late = start_node(
        dir.path().into(),
        settings(2, dir.path().into()),
        Some(second),
    )
    .await;
    late.handle
        .enter_group(members, InitialClusterState::New, shutdown.clone())
        .await
        .expect("enter group");
    nodes.push(late);
    let refs: Vec<&TestNode> = nodes.iter().collect();
    let leader = leader_of(&refs).await;
    leader.write(set("k", "v1")).await.expect("write");
    shutdown.cancel();
    for node in &nodes {
        node.stop().await;
    }
}

/// #2371: two members lose their volumes while the third, which holds the
/// group's writes, is down, and `new` is still set. They used to form a
/// fresh group, and the write was gone when the third came back.
#[tokio::test]
async fn wiped_members_do_not_form_over_a_down_data_holder() {
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
    nodes[0]
        .handle
        .initialize(members.clone())
        .await
        .expect("initialize");
    let refs: Vec<&TestNode> = nodes.iter().collect();
    let leader = leader_of(&refs).await;
    leader.write(set("k", "acknowledged")).await.expect("write");
    wait_until(
        "the write on the second member",
        Duration::from_secs(10),
        || nodes[1].app.get("k").is_some(),
    )
    .await;
    let addrs: Vec<SocketAddr> = nodes.iter().map(|node| node.addr).collect();
    for node in &nodes {
        node.stop().await;
    }
    drop(nodes);

    // Members 1 and 3 come back on empty volumes; member 2 stays down.
    let wiped = [
        tempfile::tempdir().expect("tempdir"),
        tempfile::tempdir().expect("tempdir"),
    ];
    let (nodes, _, shutdown) =
        first_start_without_the_second(&wiped, Some([addrs[0], addrs[2]]), addrs[1]).await;
    assert_no_group_forms(&nodes, "the data holder is down").await;

    // Member 2 returns with its log. The others see a group and still do
    // not form one of their own.
    let holder = start_node(
        dirs[1].path().into(),
        settings(2, dirs[1].path().into()),
        Some(addrs[1]),
    )
    .await;
    holder
        .handle
        .enter_group(members, InitialClusterState::New, shutdown.clone())
        .await
        .expect("enter group");
    tokio::time::sleep(Duration::from_secs(2)).await;
    for node in &nodes {
        let status = node.handle.status();
        assert_ne!(
            status.leader,
            Some(status.id),
            "wiped member {} leads",
            status.id
        );
        assert_eq!(node.app.len(), 0, "wiped member {} has a state", status.id);
    }
    assert_eq!(holder.app.get("k").as_deref(), Some("acknowledged"));
    shutdown.cancel();
    holder.stop().await;
    for node in &nodes {
        node.stop().await;
    }
}

/// Three members that formed on a first start and cannot elect: every vote
/// request is held. Each wrote its mark, each recorded the other two, and
/// no log holds an entry past the initial membership. This is the window
/// of #2374.
async fn formed_and_leaderless(
    dirs: &[tempfile::TempDir],
) -> (
    Vec<TestNode>,
    Vec<Arc<Gate>>,
    BTreeMap<NodeId, String>,
    tokio_util::sync::CancellationToken,
) {
    use felix_controlplane_service::raft::InitialClusterState;

    let mut nodes = Vec::new();
    let mut gates = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let dir: PathBuf = dir.path().into();
        let gate = Gate::holding_votes();
        let settings = settings((i + 1) as NodeId, dir.clone());
        nodes.push(start_gated(dir, settings, None, Arc::clone(&gate)).await);
        gates.push(gate);
    }
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|node| (node.handle.status().id, node.addr.to_string()))
        .collect();
    let shutdown = tokio_util::sync::CancellationToken::new();
    for node in &nodes {
        node.handle
            .enter_group(members.clone(), InitialClusterState::New, shutdown.clone())
            .await
            .expect("enter group");
    }
    wait_until(
        "every member to initialize and record that it reached the vote",
        Duration::from_secs(10),
        || {
            nodes.iter().all(|node| {
                node.handle.status().voters == [1, 2, 3]
                    && node
                        .handle
                        .readiness()
                        .is_err_and(|why| !why.contains("catching up"))
            })
        },
    )
    .await;
    (nodes, gates, members, shutdown)
}

/// Stop `node` and start the same member on an empty volume at the same
/// address, still told this is a first start.
async fn wipe_and_restart(
    node: TestNode,
    dir: &tempfile::TempDir,
    members: &BTreeMap<NodeId, String>,
    shutdown: &tokio_util::sync::CancellationToken,
) -> TestNode {
    use felix_controlplane_service::raft::InitialClusterState;

    let (id, addr) = (node.handle.status().id, node.addr);
    node.stop().await;
    drop(node);
    let dir: PathBuf = dir.path().into();
    let wiped = start_node(dir.clone(), settings(id, dir), Some(addr)).await;
    wiped
        .handle
        .enter_group(members.clone(), InitialClusterState::New, shutdown.clone())
        .await
        .expect("enter group");
    wiped
}

/// Whether `node` refuses a vote request the way a member withholding its
/// vote does.
async fn withholds_its_vote(node: &TestNode) -> bool {
    let vote = if cfg!(felix_segment_store) {
        serde_json::json!({"term": 0, "voted_for": 9, "committed": false})
    } else {
        serde_json::json!({"leader_id": {"term": 0, "node_id": 9}, "committed": false})
    };
    let response = peer_security()
        .client(Duration::from_secs(5))
        .expect("peer client")
        .post(format!(
            "http://{}{}",
            node.addr,
            felix_controlplane_service::raft::peer_route("vote")
        ))
        .json(&serde_json::json!({"vote": vote, "last_log_id": null}))
        .send()
        .await
        .expect("send");
    let status = response.status().as_u16();
    let body = response.text().await.expect("body");
    assert!(
        status == 200 || (status == 503 && body.contains("withheld")),
        "a vote request answered {status}: {body}"
    );
    status == 503
}

/// The members `node` says it recorded as having set out to form the group.
async fn recorded_by(node: &TestNode) -> Vec<NodeId> {
    let standing: serde_json::Value = peer_security()
        .client(Duration::from_secs(5))
        .expect("peer client")
        .get(format!(
            "http://{}{}",
            node.addr,
            felix_controlplane_service::raft::peer_route("standing")
        ))
        .send()
        .await
        .expect("send")
        .json()
        .await
        .expect("standing");
    serde_json::from_value(standing.get("formed").cloned().unwrap_or_default()).unwrap_or_default()
}

/// Many election timeouts later, `wiped` has not initialized, still
/// withholds its vote, and nobody leads.
async fn assert_stays_withheld(wiped: &[&TestNode], all: &[&TestNode]) {
    tokio::time::sleep(Duration::from_secs(3)).await;
    for node in wiped {
        let status = node.handle.status();
        assert!(
            status.voters.is_empty(),
            "wiped member {} formed the group again",
            status.id
        );
        assert!(
            withholds_its_vote(node).await,
            "wiped member {} answers vote requests",
            status.id
        );
    }
    for node in all {
        let status = node.handle.status();
        assert_eq!(status.leader, None, "member {} found a leader", status.id);
    }
}

/// #2374: a member loses its volume after the group formed and before any
/// leader wrote an entry, with `new` still set. Every peer's log is empty,
/// so it used to form again, having forgotten any vote it gave. Its peers
/// recorded it before it first voted, so now it stays withheld.
#[tokio::test]
async fn a_member_wiped_before_the_first_entry_does_not_form_again() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let (mut nodes, _gates, members, shutdown) = formed_and_leaderless(&dirs).await;
    for node in &nodes {
        let others: Vec<NodeId> = (1..=3)
            .filter(|id| *id != node.handle.status().id)
            .collect();
        assert_eq!(recorded_by(node).await, others);
    }

    // A waiver does not apply to a member that lost its volume.
    let empty = tempfile::tempdir().expect("tempdir");
    waive_the_second(empty.path())
        .await
        .expect("write the waiver");
    let third = nodes.pop().expect("third");
    let wiped = wipe_and_restart(third, &empty, &members, &shutdown).await;
    assert_stays_withheld(&[&wiped], &[&nodes[0], &nodes[1], &wiped]).await;
    assert!(
        !empty.path().join("form-without").exists(),
        "a member that lost its volume kept a waiver"
    );

    shutdown.cancel();
    wiped.stop().await;
    for node in &nodes {
        node.stop().await;
    }
}

/// #2407: on the segment store the vote is in `store/log/` and the forming
/// mark is in `store/formation`. A member whose log directory is emptied
/// keeps its mark, and every peer names it. Before the reached flag it went
/// on forming and could vote twice in a term; now it catches up instead.
#[cfg(felix_segment_store)]
#[tokio::test]
async fn a_member_whose_log_was_emptied_does_not_form_again() {
    use felix_controlplane_service::raft::InitialClusterState;

    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let (mut nodes, _gates, members, shutdown) = formed_and_leaderless(&dirs).await;
    let third = nodes.pop().expect("third");
    let (id, addr) = (third.handle.status().id, third.addr);
    third.stop().await;
    drop(third);

    let log_dir = dirs[2].path().join("store").join("log");
    for entry in std::fs::read_dir(&log_dir).expect("read the log dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path).expect("remove");
        } else {
            std::fs::remove_file(&path).expect("remove");
        }
    }
    assert!(
        dirs[2].path().join("store").join("formation").exists(),
        "the formation file went with the log"
    );

    let dir: PathBuf = dirs[2].path().into();
    let emptied = start_node(dir.clone(), settings(id, dir), Some(addr)).await;
    emptied
        .handle
        .enter_group(members, InitialClusterState::New, shutdown.clone())
        .await
        .expect("enter group");
    assert_stays_withheld(&[&emptied], &[&nodes[0], &nodes[1], &emptied]).await;

    shutdown.cancel();
    emptied.stop().await;
    for node in &nodes {
        node.stop().await;
    }
}

/// The same wiped member is not stuck: once the others elect a leader it
/// catches up with what that leader holds, and then votes again.
#[tokio::test]
async fn a_wiped_member_catches_up_once_a_leader_exists() {
    use std::sync::atomic::Ordering::SeqCst;

    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let (mut nodes, gates, members, shutdown) = formed_and_leaderless(&dirs).await;
    let empty = tempfile::tempdir().expect("tempdir");
    let third = nodes.pop().expect("third");
    let wiped = wipe_and_restart(third, &empty, &members, &shutdown).await;
    assert_stays_withheld(&[&wiped], &[&nodes[0], &nodes[1], &wiped]).await;

    // The other two may elect now. They are a majority without the third.
    for gate in &gates {
        gate.votes.store(false, SeqCst);
    }
    let leader = leader_of(&[&nodes[0], &nodes[1]]).await;
    leader.write(set("k", "v1")).await.expect("write");
    wait_until(
        "the wiped member to catch up",
        Duration::from_secs(15),
        || wiped.app.get("k").is_some(),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(15);
    while withholds_its_vote(&wiped).await {
        assert!(
            Instant::now() < deadline,
            "the wiped member never voted again"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // It votes, so it recorded that it reached the vote (#2407): flags bit 1
    // of the formation file.
    #[cfg(felix_segment_store)]
    {
        let formation =
            std::fs::read(empty.path().join("store").join("formation")).expect("formation file");
        assert_eq!(
            formation[7] & 0x02,
            0x02,
            "no reached flag after the catch-up"
        );
    }

    // Its vote counts: with the leader stopped, the other member and the
    // wiped one are the majority.
    let leader_id = leader.status().id;
    let (stopped, kept): (Vec<&TestNode>, Vec<&TestNode>) = nodes
        .iter()
        .partition(|node| node.handle.status().id == leader_id);
    stopped[0].stop().await;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let wrote = match kept[0].handle.status().leader {
            Some(id) if id != leader_id => kept[0].handle.write(set("k", "v2")).await.is_ok(),
            _ => false,
        };
        if wrote {
            break;
        }
        assert!(Instant::now() < deadline, "no write without the old leader");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown.cancel();
    wiped.stop().await;
    kept[0].stop().await;
}

/// The two-volume case of #2374: two members lose their volumes while the
/// third, which formed with them, has an empty log. The wiped two are a
/// majority and every log they can see is empty, but the third recorded
/// them, so neither forms.
#[tokio::test]
async fn two_wiped_members_do_not_form_over_an_empty_third() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let (mut nodes, _gates, members, shutdown) = formed_and_leaderless(&dirs).await;
    let empty = [
        tempfile::tempdir().expect("tempdir"),
        tempfile::tempdir().expect("tempdir"),
    ];
    let third = nodes.pop().expect("third");
    let second = nodes.pop().expect("second");
    let first = nodes.pop().expect("first");
    let wiped = [
        wipe_and_restart(first, &empty[0], &members, &shutdown).await,
        wipe_and_restart(second, &empty[1], &members, &shutdown).await,
    ];
    assert_stays_withheld(&[&wiped[0], &wiped[1]], &[&wiped[0], &wiped[1], &third]).await;
    shutdown.cancel();
    for node in wiped.iter().chain([&third]) {
        node.stop().await;
    }
}

/// The record must not bring back the first-boot deadlock: members 1 and 2
/// have formed and cannot elect, and member 3 starts late. They have
/// initialized and recorded each other, but neither recorded member 3, so
/// it forms with them.
#[tokio::test]
async fn a_late_first_boot_member_forms_against_formed_leaderless_peers() {
    use felix_controlplane_service::raft::InitialClusterState;
    use std::sync::atomic::Ordering::SeqCst;

    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    // Until member 3 starts, something at its address answers empty and
    // records, and gives no vote.
    let (third, third_stub) = standing_stub(200, "{}").await;
    let mut nodes = Vec::new();
    let mut gates = Vec::new();
    for (i, dir) in dirs[..2].iter().enumerate() {
        let dir: PathBuf = dir.path().into();
        let gate = Gate::holding_votes();
        let settings = settings((i + 1) as NodeId, dir.clone());
        nodes.push(start_gated(dir, settings, None, Arc::clone(&gate)).await);
        gates.push(gate);
    }
    let members = BTreeMap::from([
        (1, nodes[0].addr.to_string()),
        (2, nodes[1].addr.to_string()),
        (3, third.to_string()),
    ]);
    let shutdown = tokio_util::sync::CancellationToken::new();
    for node in &nodes {
        node.handle
            .enter_group(members.clone(), InitialClusterState::New, shutdown.clone())
            .await
            .expect("enter group");
    }
    wait_until(
        "the first two to initialize",
        Duration::from_secs(10),
        || {
            nodes
                .iter()
                .all(|node| node.handle.status().voters == [1, 2, 3])
        },
    )
    .await;
    assert_eq!(recorded_by(&nodes[0]).await, [2]);
    assert_eq!(recorded_by(&nodes[1]).await, [1]);

    third_stub.abort();
    let dir: PathBuf = dirs[2].path().into();
    let late = start_node(dir.clone(), settings(3, dir), Some(third)).await;
    late.handle
        .enter_group(members, InitialClusterState::New, shutdown.clone())
        .await
        .expect("enter group");
    wait_until("the late member to form", Duration::from_secs(10), || {
        late.handle.status().voters == [1, 2, 3]
    })
    .await;
    // The vote comes back after `initialize`, once the reached flag is on
    // disk.
    let deadline = Instant::now() + Duration::from_secs(10);
    while withholds_its_vote(&late).await {
        assert!(Instant::now() < deadline, "formed without its vote");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for gate in &gates {
        gate.votes.store(false, SeqCst);
    }
    nodes.push(late);
    let refs: Vec<&TestNode> = nodes.iter().collect();
    let leader = leader_of(&refs).await;
    leader.write(set("k", "v1")).await.expect("write");
    wait_until("the write on every member", Duration::from_secs(10), || {
        nodes.iter().all(|node| node.app.get("k").is_some())
    })
    .await;
    shutdown.cancel();
    for node in &nodes {
        node.stop().await;
    }
}

/// Record, then vote: while one peer has not recorded the forming member,
/// it has not initialized and answers no vote request, although the other
/// peer already holds the record.
#[tokio::test]
async fn no_vote_before_every_peer_records_the_member() {
    use felix_controlplane_service::raft::InitialClusterState;
    use std::sync::atomic::Ordering::SeqCst;

    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    let mut gates = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let dir: PathBuf = dir.path().into();
        let gate = Arc::new(Gate::default());
        let settings = settings((i + 1) as NodeId, dir.clone());
        nodes.push(start_gated(dir, settings, None, Arc::clone(&gate)).await);
        gates.push(gate);
    }
    gates[2].forming.store(true, SeqCst);
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|node| (node.handle.status().id, node.addr.to_string()))
        .collect();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let enter = |node: &TestNode| {
        let handle = node.handle.clone();
        let (members, shutdown) = (members.clone(), shutdown.clone());
        async move {
            handle
                .enter_group(members, InitialClusterState::New, shutdown)
                .await
                .expect("enter group");
        }
    };
    // Members 2 and 3 only answer for now; member 1 sets out to form.
    enter(&nodes[0]).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while recorded_by(&nodes[1]).await != [1] {
        assert!(
            Instant::now() < deadline,
            "member 2 never recorded member 1"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(recorded_by(&nodes[2]).await.is_empty());
    assert!(
        nodes[0].handle.status().voters.is_empty(),
        "initialized before every peer recorded it"
    );
    assert!(
        withholds_its_vote(&nodes[0]).await,
        "votes before every peer recorded it"
    );

    gates[2].forming.store(false, SeqCst);
    wait_until(
        "member 1 to initialize once member 3 recorded it",
        Duration::from_secs(10),
        || nodes[0].handle.status().voters == [1, 2, 3],
    )
    .await;
    assert_eq!(recorded_by(&nodes[2]).await, [1]);
    enter(&nodes[1]).await;
    enter(&nodes[2]).await;
    let refs: Vec<&TestNode> = nodes.iter().collect();
    let leader = leader_of(&refs).await;
    leader.write(set("k", "v1")).await.expect("write");
    shutdown.cancel();
    for node in &nodes {
        node.stop().await;
    }
}

/// Only an answer that decodes as empty counts as empty. A member of
/// another cluster (403) or a body that is not a standing blocks a first
/// start like silence does.
#[tokio::test]
async fn a_403_or_undecodable_answer_blocks_formation() {
    for (status, body) in [(403, "wrong raft cluster id"), (200, "not a standing")] {
        let dirs = [
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        ];
        let (second, stub) = standing_stub(status, body).await;
        let (nodes, _, shutdown) = first_start_without_the_second(&dirs, None, second).await;
        assert_no_group_forms(&nodes, body).await;
        shutdown.cancel();
        for node in &nodes {
            node.stop().await;
        }
        stub.abort();
    }
}

/// With the operator's waiver for a slot whose machine is gone, the other
/// two form without its answer. The waiver is used up by that: it is gone
/// before the group exists, and a member that holds state takes no other.
#[tokio::test]
async fn form_without_forms_once_and_is_consumed() {
    let dirs = [
        tempfile::tempdir().expect("tempdir"),
        tempfile::tempdir().expect("tempdir"),
    ];
    for dir in &dirs {
        waive_the_second(dir.path())
            .await
            .expect("write the waiver");
    }
    let waiver = dirs[0].path().join("form-without");
    let written = std::fs::read(&waiver).expect("read the waiver");
    let closed = closed_addr();
    let (nodes, members, shutdown) = first_start_without_the_second(&dirs, None, closed.addr).await;
    let refs: Vec<&TestNode> = nodes.iter().collect();
    let leader = leader_of(&refs).await;
    leader.write(set("k", "v1")).await.expect("write");
    for dir in &dirs {
        assert!(
            !dir.path().join("form-without").exists(),
            "the waiver outlived the start it was written for"
        );
    }
    // The absent slot is still a voter: the waiver shrinks nothing.
    assert_eq!(leader.status().voters, [1, 2, 3]);
    shutdown.cancel();
    let addr = nodes[0].addr;
    for node in &nodes {
        node.stop().await;
    }
    drop(nodes);

    // The tool refuses a member that has formed, and a waiver left in its
    // directory some other way is removed at start, unused.
    let refused = waive_the_second(dirs[0].path())
        .await
        .expect_err("a member with state");
    assert!(
        format!("{refused:#}").contains("holds raft state"),
        "{refused:#}"
    );
    std::fs::write(&waiver, written).expect("plant a waiver");
    let dir: PathBuf = dirs[0].path().into();
    let node = start_node(dir.clone(), settings(1, dir), Some(addr)).await;
    let shutdown = tokio_util::sync::CancellationToken::new();
    node.handle
        .enter_group(
            members,
            felix_controlplane_service::raft::InitialClusterState::New,
            shutdown.clone(),
        )
        .await
        .expect("enter group");
    assert!(!waiver.exists(), "a member with state kept a waiver");
    shutdown.cancel();
    node.stop().await;
}

/// A waiver excuses silence and nothing else. If the waived slot answers,
/// its answer counts: a refusal or an unreadable body still blocks, and a
/// group that exists is caught up with, not replaced.
#[tokio::test]
async fn form_without_never_overrides_an_answering_peer() {
    for (status, body, consumed) in [
        (403, "wrong raft cluster id", false),
        (200, "not a standing", false),
        (200, r#"{"last_log_index":7}"#, true),
    ] {
        let dirs = [
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        ];
        for dir in &dirs {
            waive_the_second(dir.path())
                .await
                .expect("write the waiver");
        }
        let (second, stub) = standing_stub(status, body).await;
        let (nodes, _, shutdown) = first_start_without_the_second(&dirs, None, second).await;
        assert_no_group_forms(&nodes, body).await;
        for dir in &dirs {
            assert_eq!(
                !dir.path().join("form-without").exists(),
                consumed,
                "waiver after the slot answered {body:?}"
            );
        }
        shutdown.cancel();
        for node in &nodes {
            node.stop().await;
        }
        stub.abort();
    }
}

/// A peer that fails the TLS handshake is up, not silent, so the waiver
/// for its slot does not excuse it. Here slot 2's certificate is from
/// another CA, as a member of some other cluster's would be.
#[tokio::test]
async fn form_without_does_not_excuse_a_failed_tls_handshake() {
    use felix_controlplane_service::raft::InitialClusterState;

    let pki_dir = tempfile::tempdir().expect("tempdir");
    let other_pki_dir = tempfile::tempdir().expect("tempdir");
    let tls = peer_pki(pki_dir.path());
    let other_tls = peer_pki(other_pki_dir.path());
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let id = (i + 1) as NodeId;
        let mut settings = settings(id, dir.path().into());
        settings.security.tls = Some(if id == 2 {
            other_tls.clone()
        } else {
            tls.clone()
        });
        if id != 2 {
            waive_the_second(dir.path())
                .await
                .expect("write the waiver");
        }
        let handle = RaftHandle::start(settings, Arc::new(KvApp::default()))
            .await
            .expect("start raft node");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
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
        nodes.push((handle, port, server));
    }
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .enumerate()
        .map(|(i, (_, port, _))| ((i + 1) as NodeId, format!("localhost:{port}")))
        .collect();
    for (handle, ..) in [&nodes[0], &nodes[2]] {
        handle
            .enter_group(members.clone(), InitialClusterState::New, shutdown.clone())
            .await
            .expect("enter group");
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    for (handle, ..) in [&nodes[0], &nodes[2]] {
        let status = handle.status();
        assert_eq!(status.leader, None, "member {} found a leader", status.id);
        assert!(status.voters.is_empty(), "member {} formed", status.id);
    }
    for dir in [&dirs[0], &dirs[2]] {
        assert!(dir.path().join("form-without").exists(), "waiver used");
    }
    shutdown.cancel();
    for (handle, _, server) in nodes {
        let _ = handle.shutdown().await;
        server.abort();
    }
}

/// The case the waiver is for: two members started, waited on a dead third,
/// and were stopped. Each has a store by then but no Raft state, so the tool
/// accepts them and the next start forms.
#[tokio::test]
async fn form_without_applies_after_a_blocked_first_start() {
    let dirs = [
        tempfile::tempdir().expect("tempdir"),
        tempfile::tempdir().expect("tempdir"),
    ];
    let closed = closed_addr();
    let second = closed.addr;
    let (nodes, _, shutdown) = first_start_without_the_second(&dirs, None, second).await;
    assert_no_group_forms(&nodes, "one member is down").await;
    shutdown.cancel();
    let addrs = [nodes[0].addr, nodes[1].addr];
    for node in &nodes {
        node.stop().await;
    }
    drop(nodes);

    for dir in &dirs {
        waive_the_second(dir.path())
            .await
            .expect("a store without state takes the waiver");
    }
    let (nodes, _, shutdown) = first_start_without_the_second(&dirs, Some(addrs), second).await;
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
            "https://localhost:{}{}",
            nodes[0].2,
            felix_controlplane_service::raft::peer_route("standing")
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

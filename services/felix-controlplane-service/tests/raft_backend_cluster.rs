//! The Raft store backend as a group: transparent forwarding, single
//! leadership for background work, and honest failure when quorum is gone.
//!
//! Serialized, because each case runs a real multi-member group in this
//! process and `losing_quorum_fails_writes_loudly_not_silently` waits on a
//! *timing* signal — a leader noticing it has not heard a quorum in five
//! seconds. Three groups at once delay that past the bound it waits within,
//! and the wait is the thing under test, so widening it would only make the
//! test slower at noticing nothing. Costs no wall clock either way: that one
//! case is nearly all of the file's ten seconds.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_controlplane_service::model::{
    Node, NodeCapacity, NodeLifecycle, NodeSpec, NodeStatus, Tenant,
};
use felix_controlplane_service::raft::{
    AppStateMachine, LeadershipGate, NodeId, RaftHandle, RaftSettings,
};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::raft::RaftStore;
use felix_controlplane_service::store::raft::state_machine::MetadataStateMachine;
use felix_controlplane_service::store::{ControlPlaneStore, StoreConfig};

struct TestNode {
    handle: RaftHandle,
    store: Arc<RaftStore>,
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

/// Stamps a heartbeat with the node's own id instead of a clock.
///
/// Every member here runs in one process and reads the same wall clock, so a
/// real timestamp could not say *which* of them produced it. This substitutes
/// something that can: the recorded `at_millis` names the instance that
/// stamped the command.
struct StampsWithNodeId {
    inner: Arc<MetadataStateMachine>,
    id: NodeId,
}

#[async_trait::async_trait]
impl AppStateMachine for StampsWithNodeId {
    async fn apply(&self, command: &[u8]) -> Vec<u8> {
        self.inner.apply(command).await
    }
    async fn snapshot(&self) -> Vec<u8> {
        self.inner.snapshot().await
    }
    async fn restore(&self, snapshot: &[u8]) {
        self.inner.restore(snapshot).await
    }
    fn restamp(&self, command: &[u8], _now_millis: u64) -> Option<Vec<u8>> {
        self.inner.restamp(command, self.id)
    }
    fn version(&self) -> u16 {
        self.inner.version()
    }
}

async fn start_node(id: NodeId, dir: &std::path::Path) -> TestNode {
    let inner = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    }));
    let machine = Arc::new(MetadataStateMachine::new(inner));
    let mut settings = RaftSettings::new(id, dir.into(), peer_security());
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(200), Duration::from_millis(400));
    // The seam sees `StampsWithNodeId`, the store sees the real machine
    // underneath it; both drive the same state. Only the wrapper's `restamp`
    // differs, and that is what `a_heartbeat_carries_the_leaders_clock` reads.
    let seam = Arc::new(StampsWithNodeId {
        inner: Arc::clone(&machine),
        id,
    });
    let handle = RaftHandle::start(settings, seam as Arc<dyn AppStateMachine>)
        .await
        .expect("start node");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = handle.rpc_router();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    TestNode {
        store: Arc::new(RaftStore::new(handle.clone(), machine)),
        handle,
        addr,
        server,
    }
}

async fn wait_for_leader(nodes: &[TestNode]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !nodes.iter().any(|n| n.handle.status().leader.is_some()) {
        assert!(Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[serial_test::serial(raft_cluster)]
async fn a_follower_serves_writes_by_forwarding_and_reads_locally() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(start_node((i + 1) as NodeId, dir.path()).await);
    }
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|n| (n.handle.status().id, n.addr.to_string()))
        .collect();
    nodes[0]
        .handle
        .initialize(members)
        .await
        .expect("initialize");
    wait_for_leader(&nodes).await;

    let leader_id = nodes
        .iter()
        .find_map(|n| n.handle.status().leader)
        .expect("leader");
    let follower = nodes
        .iter()
        .find(|n| n.handle.status().id != leader_id)
        .expect("follower");

    // The write lands on a follower's *store trait* — exactly what an API
    // handler behind a load balancer would do — and must succeed without the
    // caller knowing forwarding happened.
    let created = follower
        .store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("write through a follower");
    assert_eq!(created.tenant_id, "t1");

    // Every member serves the read locally once applied.
    let deadline = Instant::now() + Duration::from_secs(5);
    for node in &nodes {
        loop {
            let tenants = node.store.list_tenants().await.expect("list");
            if tenants.len() == 1 {
                break;
            }
            assert!(Instant::now() < deadline, "replica never applied the write");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Exactly one member holds the leadership gate, and it is the leader: the
    // sweep and placement run once per cluster, not once per instance.
    //
    // Retried within a bound, and the two halves are not equally urgent. The
    // gate is a *linearizable* check — openraft's read-index — so a leader that
    // has not heard from a quorum in the last instant fails it. That is
    // ordinary and passes, and asserting once caught the group mid-blink. What
    // must never happen is **two** holders, so that is checked on every attempt
    // rather than only at the end.
    //
    // The leader is re-read here rather than compared against the one sampled
    // before the writes: an election in between moves it, and the gate is right
    // to follow it.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut holders = Vec::new();
        for node in &nodes {
            if LeadershipGate::Leader(node.handle.clone()).holds().await {
                holders.push(node.handle.status().id);
            }
        }
        assert!(
            holders.len() <= 1,
            "two members held the leadership gate at once, so the sweep would \
             run twice: {holders:?}",
        );
        if let [holder] = holders[..] {
            assert_eq!(
                Some(holder),
                nodes.iter().find_map(|node| node.handle.status().leader),
                "the gate is held by a member nobody believes is the leader",
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no member ever held the leadership gate, so nothing would sweep",
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for node in &nodes {
        let _ = node.handle.shutdown().await;
        node.server.abort();
    }
}

fn node_named(node_id: &str) -> Node {
    Node {
        node_id: node_id.to_string(),
        spec: NodeSpec {
            advertise_addr: "10.0.0.4:7000".to_string(),
            client_addr: None,
            kafka_addr: None,
            region: "r1".to_string(),
            zone: None,
            labels: Default::default(),
            capacity: NodeCapacity::default(),
        },
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: 0,
            registered_at_millis: 1,
            incarnation: 0,
        },
    }
}

/// A heartbeat carries the leader's clock, whoever received it.
///
/// Liveness is a comparison: this stamp against a cutoff the leader-gated
/// sweep computes from its own clock. If the instance that happens to receive
/// a broker's heartbeat stamps it, the comparison spans two machines' wall
/// clocks, and a broker is expired early or kept alive late by whatever they
/// disagree by — which is why the receiving instance must not be the one to
/// answer "what time is it".
#[tokio::test]
#[serial_test::serial(raft_cluster)]
async fn a_heartbeat_through_a_follower_reaches_the_leader_not_the_log() {
    // Two members: a leader and a follower that is not it, which is the whole
    // shape this needs.
    let dirs: Vec<tempfile::TempDir> = (0..2)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(start_node((i + 1) as NodeId, dir.path()).await);
    }
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|n| (n.handle.status().id, n.addr.to_string()))
        .collect();
    nodes[0]
        .handle
        .initialize(members)
        .await
        .expect("initialize");
    wait_for_leader(&nodes).await;

    let leader_id = nodes
        .iter()
        .find_map(|n| n.handle.status().leader)
        .expect("leader");
    let follower = nodes
        .iter()
        .find(|n| n.handle.status().id != leader_id)
        .expect("follower");

    follower
        .store
        .register_node(node_named("broker-1"))
        .await
        .expect("register through a follower");

    // Straight at a follower's store trait, the way a heartbeat arrives when
    // the load balancer picks that instance. It reaches the leader's soft
    // state, not the log.
    let leader = nodes
        .iter()
        .find(|n| n.handle.status().id == leader_id)
        .expect("leader");
    let applied_before = leader.handle.status().last_applied_index;
    let recorded = follower
        .store
        .record_node_heartbeat("broker-1", 1, 999)
        .await
        .expect("heartbeat through a follower");

    assert_ne!(
        recorded.status.last_heartbeat_at_millis, 999,
        "the value the caller passed survived"
    );
    assert_eq!(
        leader
            .store
            .get_node("broker-1")
            .await
            .expect("leader view")
            .status
            .last_heartbeat_at_millis,
        recorded.status.last_heartbeat_at_millis,
        "the leader holds the heartbeat the follower forwarded"
    );
    assert_eq!(
        leader.handle.status().last_applied_index,
        applied_before,
        "a heartbeat became a log entry"
    );

    for node in &nodes {
        let _ = node.handle.shutdown().await;
        node.server.abort();
    }
}

/// With quorum gone, a write fails within its bounded retry budget instead
/// of hanging — the caller gets an error it can surface, and brokers keep
/// serving on their catalogs exactly as during any control-plane outage.
#[tokio::test]
#[serial_test::serial(raft_cluster)]
async fn losing_quorum_fails_writes_loudly_not_silently() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(start_node((i + 1) as NodeId, dir.path()).await);
    }
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|n| (n.handle.status().id, n.addr.to_string()))
        .collect();
    nodes[0]
        .handle
        .initialize(members)
        .await
        .expect("initialize");
    wait_for_leader(&nodes).await;

    let leader_id = nodes
        .iter()
        .find_map(|n| n.handle.status().leader)
        .expect("leader");
    let survivor = nodes
        .iter()
        .position(|n| n.handle.status().id == leader_id)
        .expect("leader position");

    for (i, node) in nodes.iter().enumerate() {
        if i != survivor {
            let _ = node.handle.shutdown().await;
            node.server.abort();
        }
    }

    let refused = nodes[survivor]
        .store
        .create_tenant(Tenant {
            tenant_id: "t-after".to_string(),
            display_name: "After".to_string(),
        })
        .await;
    // And fails as "no quorum", which the API answers with a retryable 503,
    // not as an internal error.
    match refused {
        Err(felix_controlplane_service::store::StoreError::Unexpected(err)) => assert!(
            err.chain()
                .any(|cause| cause.is::<felix_controlplane_service::raft::NoQuorum>()),
            "a write without quorum failed, but not as no-quorum: {err:#}"
        ),
        other => panic!("a write without quorum must fail, not hang or pretend: {other:?}"),
    }

    // And readiness follows: the survivor still calls itself leader, but a
    // leader no quorum has acknowledged in the bound is a leader in name
    // only, and the probe must take it out of rotation.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if nodes[survivor].store.health_check().await.is_err() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "readiness kept passing on a quorumless leader"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let _ = nodes[survivor].handle.shutdown().await;
    nodes[survivor].server.abort();
}

/// Peer credentials every member of a test group shares.
fn peer_security() -> felix_controlplane_service::raft::PeerSecurity {
    felix_controlplane_service::raft::PeerSecurity {
        cluster_id: "test-cluster".to_string(),
        token: Some("0123456789abcdef0123456789abcdef".to_string()),
        tls: None,
    }
}

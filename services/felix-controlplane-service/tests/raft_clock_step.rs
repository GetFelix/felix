//! Raft leadership moving after the control plane's wall clock stepped back,
//! taken through the `felix_common::clock` seam.
//!
//! Its own test binary because the fault is process-wide.
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use felix_controlplane_service::model::{Node, NodeCapacity, NodeLifecycle, NodeSpec, NodeStatus};
use felix_controlplane_service::raft::{AppStateMachine, NodeId, RaftHandle, RaftSettings};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::raft::RaftStore;
use felix_controlplane_service::store::raft::state_machine::MetadataStateMachine;
use felix_controlplane_service::store::{ControlPlaneStore, StoreConfig};
use tokio_util::sync::CancellationToken;

const STEP_MS: i64 = 60_000;
const WINDOW: Duration = Duration::from_millis(1_000);

struct Member {
    handle: RaftHandle,
    store: Arc<RaftStore>,
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

impl Member {
    async fn stop(&self) {
        let _ = self.handle.shutdown().await;
        self.server.abort();
    }
}

async fn start_member(id: NodeId, dir: &Path) -> Member {
    let inner = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    }));
    let machine = Arc::new(MetadataStateMachine::new(inner));
    let mut settings = RaftSettings::new(
        id,
        dir.into(),
        felix_controlplane_service::raft::PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some("0123456789abcdef0123456789abcdef".to_string()),
            tls: None,
        },
    );
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(200), Duration::from_millis(400));
    let handle = RaftHandle::start(settings, Arc::clone(&machine) as Arc<dyn AppStateMachine>)
        .await
        .expect("start member");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = handle.rpc_router();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Member {
        store: Arc::new(RaftStore::new(handle.clone(), machine)),
        handle,
        addr,
        server,
    }
}

async fn start_group(dirs: &[tempfile::TempDir]) -> Vec<Member> {
    let mut members = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        members.push(start_member((i + 1) as NodeId, dir.path()).await);
    }
    let addrs: BTreeMap<NodeId, String> = members
        .iter()
        .map(|m| (m.handle.status().id, m.addr.to_string()))
        .collect();
    members[0]
        .handle
        .initialize(addrs)
        .await
        .expect("initialize");
    members
}

/// The index of the member that leads, once one does and every other agrees.
async fn leader_of(members: &[Member], among: &[usize]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let leaders: Vec<usize> = among
            .iter()
            .copied()
            .filter(|&i| members[i].handle.is_leader())
            .collect();
        if let [leader] = leaders[..] {
            return leader;
        }
        assert!(Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn broker(node_id: &str, port: u16, now: u64) -> Node {
    Node {
        node_id: node_id.to_string(),
        spec: NodeSpec {
            advertise_addr: format!("127.0.0.1:{port}"),
            client_addr: None,
            kafka_addr: None,
            region: "local".to_string(),
            zone: None,
            labels: BTreeMap::new(),
            capacity: NodeCapacity::default(),
        },
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: now,
            registered_at_millis: now,
            incarnation: 0,
            features: Default::default(),
        },
    }
}

/// Heartbeats for `alive` always, and for `dies` until `dead` is set, through
/// whichever of `stores` answers: a broker retries the next instance when
/// one has no leader.
fn heartbeat(
    stores: Vec<Arc<RaftStore>>,
    dead: Arc<AtomicBool>,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut turn = 0;
        while !stop.is_cancelled() {
            let mut ids = vec!["alive"];
            if !dead.load(Ordering::Acquire) {
                ids.push("dies");
            }
            for id in ids {
                for _ in 0..stores.len() {
                    turn += 1;
                    let store = &stores[turn % stores.len()];
                    // Bounded: a stopped member must not hold up the others.
                    let beat = tokio::time::timeout(
                        Duration::from_millis(300),
                        store.record_node_heartbeat(id, 0, 0),
                    );
                    if matches!(beat.await, Ok(Ok(_))) {
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

/// The expiry sweep, every 50 ms, through whichever store answers.
fn sweep(stores: Vec<Arc<RaftStore>>, stop: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while !stop.is_cancelled() {
            for store in &stores {
                let Ok(now) = store.now_millis().await else {
                    continue;
                };
                let cutoff = now.saturating_sub(WINDOW.as_millis() as u64);
                let swept = tokio::time::timeout(
                    Duration::from_millis(300),
                    store.expire_stale_nodes(cutoff),
                );
                if matches!(swept.await, Ok(Ok(_))) {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
}

async fn lifecycle(store: &RaftStore, node_id: &str) -> NodeLifecycle {
    store.get_node(node_id).await.expect("get").status.lifecycle
}

/// Kill the leader, then watch until `dies` goes down: never before a full
/// window after the old leader stopped, and `alive` never at all.
async fn move_leadership_and_watch(members: &[Member], dead: &AtomicBool) {
    let old = leader_of(members, &[0, 1, 2]).await;
    let survivors: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    let reader = &members[survivors[0]].store;
    dead.store(true, Ordering::Release);
    members[old].stop().await;
    let stopped = Instant::now();
    leader_of(members, &survivors).await;

    let deadline = stopped + Duration::from_secs(5);
    loop {
        let down = lifecycle(reader, "dies").await == NodeLifecycle::Down;
        assert_eq!(
            lifecycle(reader, "alive").await,
            NodeLifecycle::Live,
            "a heartbeating broker was expired by the leadership change",
        );
        if down {
            assert!(
                stopped.elapsed() >= WINDOW,
                "a silent broker was expired {:?} after the leader stopped, before one window",
                stopped.elapsed(),
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a silent broker was still live 5s after leadership moved",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // A few more windows under the new leader: the live broker stays up.
    let until = Instant::now() + 2 * WINDOW;
    while Instant::now() < until {
        assert_eq!(lifecycle(reader, "alive").await, NodeLifecycle::Live);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Stamps taken on a clock a minute fast are a minute in the future once it
/// steps back. A leader elected after that, which never hears from a dead
/// broker, must expire it one window after the election, not wait the
/// minute out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(raft_clock_step)]
async fn a_leader_elected_after_a_clock_step_back_expires_a_dead_broker_within_a_window() {
    let fault_dir = tempfile::tempdir().expect("tempdir");
    let fault = fault_dir.path().join("clock");
    felix_common::clock::fault::follow(&fault);
    std::fs::write(&fault, format!("offset_ms={STEP_MS}\n")).expect("step forward");
    // Past the fault module's reread.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let members = start_group(&dirs).await;
    let leader = leader_of(&members, &[0, 1, 2]).await;
    let now = members[leader].store.now_millis().await.expect("clock");
    for (id, port) in [("alive", 7001), ("dies", 7002)] {
        members[leader]
            .store
            .register_node(broker(id, port, now))
            .await
            .expect("register");
    }

    let stop = CancellationToken::new();
    let dead = Arc::new(AtomicBool::new(false));
    let stores: Vec<Arc<RaftStore>> = members.iter().map(|m| Arc::clone(&m.store)).collect();
    let beats = heartbeat(stores.clone(), Arc::clone(&dead), stop.clone());
    let sweeper = sweep(stores, stop.clone());
    tokio::time::sleep(Duration::from_millis(500)).await;

    let true_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis() as u64;
    let stamp = members[leader]
        .store
        .get_node("dies")
        .await
        .expect("get")
        .status
        .last_heartbeat_at_millis;
    assert!(
        stamp > true_now + 30_000,
        "no stamp was taken on the stepped clock ({stamp} vs {true_now}), so this tests nothing",
    );
    std::fs::remove_file(&fault).expect("step back");
    tokio::time::sleep(Duration::from_millis(200)).await;

    move_leadership_and_watch(&members, &dead).await;

    stop.cancel();
    beats.await.expect("heartbeats");
    sweeper.await.expect("sweep");
    for member in &members {
        member.stop().await;
    }
    felix_common::clock::fault::stop_following(&fault);
}

/// Without any clock fault: a leadership change gives every broker a fresh
/// window, so the one still heartbeating is never expired by it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(raft_clock_step)]
async fn a_live_broker_survives_a_leadership_change() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let members = start_group(&dirs).await;
    let leader = leader_of(&members, &[0, 1, 2]).await;
    let now = members[leader].store.now_millis().await.expect("clock");
    for (id, port) in [("alive", 7001), ("dies", 7002)] {
        members[leader]
            .store
            .register_node(broker(id, port, now))
            .await
            .expect("register");
    }

    let stop = CancellationToken::new();
    let dead = Arc::new(AtomicBool::new(false));
    let stores: Vec<Arc<RaftStore>> = members.iter().map(|m| Arc::clone(&m.store)).collect();
    let beats = heartbeat(stores.clone(), Arc::clone(&dead), stop.clone());
    let sweeper = sweep(stores, stop.clone());
    tokio::time::sleep(Duration::from_millis(500)).await;

    move_leadership_and_watch(&members, &dead).await;

    stop.cancel();
    beats.await.expect("heartbeats");
    sweeper.await.expect("sweep");
    for member in &members {
        member.stop().await;
    }
}

//! The expiry sweep across a wall-clock step back, taken through the
//! `felix_common::clock` seam.
//!
//! Its own test binary because the fault is process-wide: every other test
//! sharing the process would see the stepped clock too.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use felix_controlplane_service::cluster::membership::spawn_expiry_sweep;
use felix_controlplane_service::config::NodeLivenessConfig;
use felix_controlplane_service::model::{Node, NodeCapacity, NodeLifecycle, NodeSpec, NodeStatus};
use felix_controlplane_service::raft::LeadershipGate;
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{ControlPlaneStore, StoreConfig};
use tokio_util::sync::CancellationToken;

const STEP_MS: i64 = 60_000;

fn liveness() -> NodeLivenessConfig {
    NodeLivenessConfig {
        heartbeat_interval_ms: 100,
        expiry_timeout_ms: 400,
        regrant_margin_ms: None,
        sweep_interval_ms: 50,
        shard_reconcile_interval_ms: 1_000,
    }
}

fn node(node_id: &str, port: u16, now: u64) -> Node {
    Node {
        node_id: node_id.to_string(),
        spec: NodeSpec {
            advertise_addr: format!("127.0.0.1:{port}"),
            client_addr: None,
            kafka_addr: None,
            region: "local".to_string(),
            zone: None,
            labels: BTreeMap::new(),
            capacity: NodeCapacity {
                max_shards: None,
                weight: 1,
            },
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

async fn lifecycle(store: &InMemoryStore, node_id: &str) -> NodeLifecycle {
    store.get_node(node_id).await.expect("get").status.lifecycle
}

/// Heartbeat stamps never move backwards, so a step back leaves them in the
/// future. A broker that dies then must still go down after one window of
/// silence, not after the step, and the one still heartbeating must stay up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clock_stepped_back_still_expires_a_broker_that_goes_silent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fault = dir.path().join("clock");
    felix_common::clock::fault::follow(&fault);

    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    }));
    let now = store.now_millis().await.expect("clock");
    for (id, port) in [("alive", 7001), ("dies", 7002)] {
        store
            .register_node(node(id, port, now))
            .await
            .expect("register");
    }

    let shutdown = CancellationToken::new();
    let dead = Arc::new(AtomicBool::new(false));
    let beats = {
        let store = Arc::clone(&store);
        let dead = Arc::clone(&dead);
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            while !shutdown.is_cancelled() {
                let at = store.now_millis().await.expect("clock");
                store
                    .record_node_heartbeat("alive", 0, at)
                    .await
                    .expect("beat");
                if !dead.load(Ordering::Acquire) {
                    store
                        .record_node_heartbeat("dies", 0, at)
                        .await
                        .expect("beat");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    let sweep = spawn_expiry_sweep(
        Arc::clone(&store) as Arc<dyn ControlPlaneStore + Send + Sync>,
        liveness(),
        LeadershipGate::Always,
        shutdown.clone(),
    );

    // Forward a minute, long enough for both stamps to be taken on it, then
    // back to the true clock: every stamp is now a minute ahead.
    std::fs::write(&fault, format!("offset_ms={STEP_MS}\n")).expect("step forward");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let true_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis() as u64;
    let stamp = store
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
    // Past the fault module's reread, so the sweep is on the stepped-back clock.
    tokio::time::sleep(Duration::from_millis(200)).await;
    dead.store(true, Ordering::Release);

    let deadline = Instant::now() + Duration::from_secs(5);
    while lifecycle(&store, "dies").await != NodeLifecycle::Down {
        assert!(
            Instant::now() < deadline,
            "a silent broker was still live 5s after it stopped, a minute-long step back ago",
        );
        assert_eq!(lifecycle(&store, "alive").await, NodeLifecycle::Live);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // A few more windows: the broker still heartbeating is never expired.
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until {
        assert_eq!(lifecycle(&store, "alive").await, NodeLifecycle::Live);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown.cancel();
    beats.await.expect("heartbeats");
    sweep.await.expect("sweep");
    felix_common::clock::fault::stop_following(&fault);
}

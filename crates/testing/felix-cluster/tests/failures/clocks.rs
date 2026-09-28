//! Clock faults: a broker's lease clock and the control plane's expiry clock
//! skewed, stepped and sped up through `felix_common::clock`.
//!
//! Run with `cargo test -p felix-cluster --test failures clocks::`.
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use felix_cluster::{ClockFault, Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use felix_controlplane_service::store::ControlPlaneStore;
use serial_test::serial;

const STREAM: &str = "orders";

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, 1)],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    }
}

async fn lease_expiries(cluster: &Cluster, node: &str) -> f64 {
    cluster
        .metric(node, "felix_broker_lease_expiries_total")
        .await
        .expect("read metrics")
        .unwrap_or(0.0)
}

/// Every broker's last heartbeat stamp, as the control plane stored it.
async fn heartbeat_stamps(cluster: &Cluster) -> Vec<u64> {
    let control_plane = cluster.control_plane.as_ref().expect("control plane");
    control_plane
        .store
        .list_nodes()
        .await
        .expect("list nodes")
        .iter()
        .map(|node| node.status.last_heartbeat_at_millis)
        .collect()
}

/// This test process's true wall clock, which the harness's own skew of the
/// in-process control plane does not touch.
fn true_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis() as u64
}

/// **A broker's clock cannot be stepped back.** Its lease runs on boottime,
/// which no real machine runs backwards, so `inject` refuses the fault
/// before it touches the broker rather than report unsafety that cannot
/// happen.
#[serial]
#[tokio::test]
async fn a_broker_clock_cannot_be_stepped_back() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        ..config()
    })
    .await
    .expect("start");
    let node = cluster.node_ids()[0].clone();
    let back = Fault::Clock {
        process: Endpoint::node(&node),
        fault: ClockFault::back(Duration::from_secs(5)),
    };
    assert!(cluster.inject(&back).await.is_err());
    assert!(cluster.active_faults().is_empty());
    cluster.shutdown().await;
}

/// **A broker whose clock runs fifty times fast cannot hold its lease.** A
/// renewal is good for three quarters of a second of lease time, which at
/// 50x is 15ms of real time against a 200ms heartbeat, so the lease lapses
/// between every pair of heartbeats. Healing puts it back on 1x, keeping the
/// drift so the clock does not run back, and the lapses stop.
#[serial]
#[tokio::test]
async fn a_fast_broker_clock_lapses_its_lease_until_healed() {
    let cluster = Cluster::start(config()).await.expect("start");
    let node = cluster.node_ids()[0].clone();
    let before = lease_expiries(&cluster, &node).await;

    let fault = Fault::Clock {
        process: Endpoint::node(&node),
        fault: ClockFault::Rate(50.0),
    };
    cluster.inject(&fault).await.expect("inject");
    let deadline = Instant::now() + Duration::from_secs(10);
    while lease_expiries(&cluster, &node).await < before + 3.0 {
        assert!(
            Instant::now() < deadline,
            "{node}'s lease never lapsed on a clock running 50x fast",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    cluster.heal(&fault).await.expect("heal");
    // A lapse can still be counted for the renewal in flight at the heal; any
    // lapse after that settles is the fault not healing.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let settled = lease_expiries(&cluster, &node).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        lease_expiries(&cluster, &node).await,
        settled,
        "{node}'s lease went on lapsing on the true clock",
    );
    assert_eq!(
        cluster
            .metric(&node, "felix_broker_lease_held")
            .await
            .expect("metrics"),
        Some(1.0),
    );
    cluster.shutdown().await;
}

/// **A control-plane clock stepped forward does not expire a live broker.**
/// The step is real: every heartbeat from then on is stamped a minute ahead.
/// And it is harmless, because expiry also needs a node to have been silent
/// on the control plane's monotonic clock, which a wall-clock step does not
/// move. That second check is the defence this pins.
#[serial]
#[tokio::test]
async fn a_control_plane_clock_stepped_forward_expires_no_live_broker() {
    const STEP: Duration = Duration::from_secs(60);
    let cluster = Cluster::start(config()).await.expect("start");
    let fault = Fault::Clock {
        process: Endpoint::ControlPlane,
        fault: ClockFault::forward(STEP),
    };
    cluster.inject(&fault).await.expect("inject");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let stepped = true_now_millis() + STEP.as_millis() as u64 - 5_000;
        if heartbeat_stamps(&cluster)
            .await
            .iter()
            .all(|&at| at >= stepped)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "heartbeats were never stamped on the stepped clock",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Several expiry windows (1s plus the margin) on the stepped clock.
    let expected = cluster.node_ids();
    let watch_until = Instant::now() + Duration::from_secs(4);
    while Instant::now() < watch_until {
        let live = cluster.placeable_nodes().await.expect("nodes");
        for node in &expected {
            assert!(
                live.contains(node),
                "{node} was expired by a wall-clock step while heartbeating",
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    cluster.heal(&fault).await.expect("heal");
    cluster.shutdown().await;
}

/// **A control-plane clock stepped back still expires a broker that dies.**
///
/// Heartbeat stamps never move backwards, so after a step back of a minute
/// every stamp is a minute in the future, and a broker that dies in that
/// minute has a last heartbeat newer than any expiry threshold the clock
/// can produce until real time catches up. The silence watch agrees it has
/// gone quiet, but expiry is capped by the stamp, so the node stays `live`
/// and placeable for the length of the step instead of the one-second
/// window.
#[serial]
#[tokio::test]
#[ignore = "control-plane bug: a backward wall-clock step delays expiry of a dead broker by the \
            size of the step, because heartbeat stamps are max-merged and expiry compares them \
            against the stepped-back clock"]
async fn a_control_plane_clock_stepped_back_still_expires_a_dead_broker() {
    const STEP: Duration = Duration::from_secs(60);
    let mut cluster = Cluster::start(config()).await.expect("start");
    let forward = Fault::Clock {
        process: Endpoint::ControlPlane,
        fault: ClockFault::forward(STEP),
    };
    cluster.inject(&forward).await.expect("inject");
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Healing the forward step is the backward step.
    cluster.heal(&forward).await.expect("heal");

    let node = cluster.node_ids()[0].clone();
    cluster.kill_node(&node).expect("kill");
    let deadline = Instant::now() + Duration::from_secs(15);
    while cluster
        .placeable_nodes()
        .await
        .expect("nodes")
        .contains(&node)
    {
        assert!(
            Instant::now() < deadline,
            "{node} was still placeable 15s after it died, a minute-long step back ago",
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    cluster.shutdown().await;
}

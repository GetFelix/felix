//! Membership across outages: a broker marked down finds its way back, and a
//! control-plane restart does not mark the fleet down in the first place.
//!
//! Both failures leave every process running and healthy while the cluster
//! believes otherwise, so each test asserts on the control plane's view.
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_controlplane_service::model::NodeLifecycle;
use felix_controlplane_service::store::ControlPlaneStore;
use serial_test::serial;

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new("orders", 1)],
        ..Default::default()
    }
}

async fn wait_until_placeable(cluster: &Cluster, expected: &[String], budget: Duration) {
    let started = Instant::now();
    loop {
        if let Ok(live) = cluster.placeable_nodes().await
            && expected.iter().all(|id| live.contains(id))
        {
            return;
        }
        assert!(
            started.elapsed() < budget,
            "{expected:?} were not all placeable after {budget:?}",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn incarnation(cluster: &Cluster, node_id: &str) -> (u64, NodeLifecycle) {
    let node = cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .get_node(node_id)
        .await
        .expect("node");
    (node.status.incarnation, node.status.lifecycle)
}

/// **A broker marked down registers again.** A heartbeat never revives a down
/// node, so a broker that only kept heartbeating would stay out of the
/// cluster, leaseless, for the rest of its life.
#[serial]
#[tokio::test]
async fn a_broker_marked_down_rejoins_the_cluster() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let node_id = cluster.nodes[0].node_id.clone();
    let (before, _) = incarnation(&cluster, &node_id).await;

    // Silent past the 1 s expiry window, so the sweep marks it down.
    cluster.pause_node(&node_id).expect("pause");
    let started = Instant::now();
    while incarnation(&cluster, &node_id).await.1 != NodeLifecycle::Down {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the paused broker was never marked down",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.resume_node(&node_id).expect("resume");

    wait_until_placeable(
        &cluster,
        std::slice::from_ref(&node_id),
        Duration::from_secs(20),
    )
    .await;
    let (after, lifecycle) = incarnation(&cluster, &node_id).await;
    assert_eq!(lifecycle, NodeLifecycle::Live);
    assert!(
        after > before,
        "rejoining takes a new registration: {before} -> {after}"
    );
    cluster.shutdown().await;
}

/// **A control-plane restart does not expire the fleet.** The restarted
/// instance sees heartbeat stamps as old as its outage. Sweeping on them at
/// once would mark every broker down; the grace window lets them report first,
/// so none has to register again.
#[serial]
#[tokio::test]
async fn a_control_plane_restart_does_not_expire_the_fleet() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let ids: Vec<String> = cluster.nodes.iter().map(|n| n.node_id.clone()).collect();
    let mut before = Vec::new();
    for id in &ids {
        before.push(incarnation(&cluster, id).await.0);
    }

    // Three expiry windows: every stamp is stale when it comes back.
    cluster
        .restart_control_plane(Duration::from_secs(3))
        .await
        .expect("restart control plane");

    wait_until_placeable(&cluster, &ids, Duration::from_secs(20)).await;
    // Past the grace window, so a sweep that was going to fire has fired.
    tokio::time::sleep(Duration::from_secs(2)).await;
    for (id, before) in ids.iter().zip(before) {
        let (after, lifecycle) = incarnation(&cluster, id).await;
        assert_eq!(lifecycle, NodeLifecycle::Live, "{id}");
        assert_eq!(
            after, before,
            "{id} was marked down by the restart and had to register again",
        );
    }
    cluster.shutdown().await;
}

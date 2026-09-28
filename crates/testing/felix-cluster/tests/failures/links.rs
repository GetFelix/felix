//! Link faults through the harness's proxies: one direction of one link
//! dropped or slowed while every process keeps running.
//!
//! Each test checks the fault took effect before it checks anything else,
//! and heals it, so a fault that silently did nothing fails here rather than
//! passing a scenario built on it.
//!
//! Run with `cargo test -p felix-cluster --test failures links::`.
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

fn proxied_quorum_cluster() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        proxy_links: true,
        // A Quorum publish nobody acks fails after this, rather than the
        // five-second default.
        broker_env: vec![(
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            "1500".to_string(),
        )],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    }
}

/// `felix_broker_lease_held` on `node`, if it answers.
async fn lease_held(cluster: &Cluster, node: &str) -> Option<f64> {
    cluster
        .metric(node, "felix_broker_lease_held")
        .await
        .ok()
        .flatten()
}

/// The fastest of three acknowledged publishes through `node`, on one
/// connection so the time is the publish and not the handshake.
async fn publish_latency(cluster: &Cluster, node: &str) -> Duration {
    let client = cluster.client_on(node).await.expect("connect");
    let publisher = client.publisher().await.expect("publisher");
    let mut fastest = Duration::MAX;
    for _ in 0..3 {
        let started = Instant::now();
        publisher
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                b"timed".to_vec(),
                felix_wire::AckMode::PerMessage,
            )
            .await
            .expect("a timed publish should succeed");
        fastest = fastest.min(started.elapsed());
    }
    fastest
}

/// Keep publishing through `node` until one succeeds.
async fn publish_eventually(cluster: &Cluster, node: &str, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match cluster
            .publish_via(node, STREAM, what.as_bytes().to_vec())
            .await
        {
            Ok(()) => return,
            Err(err) => assert!(
                Instant::now() < deadline,
                "{what}: publishes through {node} never succeeded again; last: {err:#}",
            ),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// **A leader cut off one way from its followers cannot acknowledge a
/// `Quorum` publish, and can once the link heals.** Only the leader's own
/// packets are lost; the followers can still reach it. It stays live to the
/// control plane throughout, which is what makes this different from a kill.
#[serial]
#[tokio::test]
async fn a_one_way_partition_from_the_leader_stops_quorum_acks_until_healed() {
    let cluster = Cluster::start(proxied_quorum_cluster())
        .await
        .expect("start");
    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&leader, STREAM, b"before".to_vec())
        .await
        .expect("publish while the cluster is whole");

    let faults: Vec<Fault> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .map(|follower| Fault::Drop {
            from: Endpoint::node(&leader),
            to: Endpoint::node(follower),
        })
        .collect();
    for fault in &faults {
        cluster.inject(fault).await.expect("inject");
    }

    let outcome = cluster
        .publish_via(&leader, STREAM, b"while-cut-off".to_vec())
        .await;
    assert!(
        outcome.is_err(),
        "a leader whose packets reach no follower acknowledged a Quorum publish",
    );
    assert!(
        cluster.node(&leader).expect("leader").is_running(),
        "the leader should still be running: only its outbound peer packets were cut",
    );
    assert_eq!(
        cluster.unattributed_datagrams(),
        0,
        "datagrams the proxy could not attribute bypassed the partition",
    );

    cluster.heal_all().await.expect("heal");
    assert!(cluster.active_faults().is_empty());
    publish_eventually(&cluster, &leader, "after-healing").await;
    cluster.shutdown().await;
}

/// **A slow link slows the acknowledgement by at least its delay.** Each
/// `Quorum` publish waits on a follower's round trip, so holding the
/// leader's packets to both followers for 400ms makes every one of them take
/// that long, and healing takes it away again.
#[serial]
#[tokio::test]
async fn a_delayed_link_slows_quorum_acks_by_the_delay() {
    const DELAY: Duration = Duration::from_millis(400);
    let cluster = Cluster::start(proxied_quorum_cluster())
        .await
        .expect("start");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let baseline = publish_latency(&cluster, &leader).await;

    for follower in cluster.node_ids().into_iter().filter(|id| *id != leader) {
        cluster
            .inject(&Fault::Delay {
                from: Endpoint::node(&leader),
                to: Endpoint::node(follower),
                by: DELAY,
            })
            .await
            .expect("inject");
    }
    // The first publish after the fault may ride a round trip already in
    // flight, so time the second.
    publish_eventually(&cluster, &leader, "warm").await;
    let slow = publish_latency(&cluster, &leader).await;
    assert!(
        slow >= DELAY,
        "a Quorum publish over a link delayed {DELAY:?} took {slow:?}, against {baseline:?} before",
    );

    cluster.heal_all().await.expect("heal");
    publish_eventually(&cluster, &leader, "after-healing").await;
    let fast = publish_latency(&cluster, &leader).await;
    assert!(
        fast < DELAY / 2,
        "healing the delay left publishes slow: {fast:?}, against {baseline:?} before",
    );
    cluster.shutdown().await;
}

/// **The asymmetric control-plane partition.** The control plane's replies
/// to one broker are lost while its heartbeats still arrive. The control
/// plane keeps it live, since it hears from it, but the broker never sees an
/// answer, so its own lease runs out. Two parties disagreeing about one
/// broker, with neither wrong by its own lights: the fault no kill, pause or
/// symmetric partition produces.
#[serial]
#[tokio::test]
async fn lost_heartbeat_replies_lapse_the_lease_while_the_control_plane_keeps_the_broker() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, 1)],
        proxy_links: true,
        ..Default::default()
    })
    .await
    .expect("start");
    let node = cluster.node_ids()[0].clone();
    // Start returns once the broker is ready, which can be a beat before its
    // first heartbeat reply grants the lease.
    let deadline = Instant::now() + Duration::from_secs(10);
    while lease_held(&cluster, &node).await != Some(1.0) {
        assert!(Instant::now() < deadline, "{node} never took its lease");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let fault = Fault::Drop {
        from: Endpoint::ControlPlane,
        to: Endpoint::node(&node),
    };
    cluster.inject(&fault).await.expect("inject");

    let deadline = Instant::now() + Duration::from_secs(15);
    while lease_held(&cluster, &node).await != Some(0.0) {
        assert!(
            Instant::now() < deadline,
            "{node} kept its lease with every heartbeat reply lost",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        cluster
            .placeable_nodes()
            .await
            .expect("nodes")
            .contains(&node),
        "the control plane should still count {node} live: its heartbeats arrive",
    );

    cluster.heal(&fault).await.expect("heal");
    let deadline = Instant::now() + Duration::from_secs(15);
    while lease_held(&cluster, &node).await != Some(1.0) {
        assert!(
            Instant::now() < deadline,
            "{node} never renewed its lease after the link healed",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.shutdown().await;
}

/// A link fault on a cluster that has no proxies is refused, not quietly
/// ignored: the test would go on to prove nothing.
#[serial]
#[tokio::test]
async fn a_link_fault_without_proxies_is_an_error() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start");
    let [a, b] = [cluster.node_ids()[0].clone(), cluster.node_ids()[1].clone()];
    let fault = Fault::Drop {
        from: Endpoint::node(a),
        to: Endpoint::node(b),
    };
    assert!(cluster.inject(&fault).await.is_err());
    assert!(
        cluster
            .inject(&Fault::Drop {
                from: Endpoint::node("broker-does-not-exist"),
                to: Endpoint::ControlPlane,
            })
            .await
            .is_err()
    );
    cluster.shutdown().await;
}

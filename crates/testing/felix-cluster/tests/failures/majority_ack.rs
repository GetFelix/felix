//! Once the fleet finalizes `majority_ack`, a `Quorum` stream acknowledges a
//! write when a majority of its replicas has answered that it holds it at the
//! leader's generation. The control plane's report and the leader's lease are
//! off the write's path, so a leader cut off from the control plane goes on
//! acknowledging what its followers hold, and nothing else.
//!
//! Run with `cargo test -p felix-cluster --test failures majority_ack::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use felix_controlplane_service::store::ControlPlaneStore;
use serial_test::serial;

const STREAM: &str = "orders";
const LEASE_HELD: &str = "felix_broker_lease_held";

/// Three brokers, a `Quorum` stream on all three, and both features the
/// follower acks need finalized.
async fn start(quorum_timeout: Duration) -> Cluster {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        proxy_links: true,
        broker_env: vec![(
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            quorum_timeout.as_millis().to_string(),
        )],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let store = &cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store;
    for feature in ["generation_start", "majority_ack"] {
        store
            .finalize_fleet_feature(feature)
            .await
            .unwrap_or_else(|err| panic!("finalize {feature}: {err}"));
    }
    for id in cluster.node_ids() {
        felix_cluster::wait::until(Duration::from_secs(20), "both features to turn on", || {
            let id = id.clone();
            let cluster = &cluster;
            async move {
                cluster
                    .metric(&id, "felix_broker_fleet_feature_enabled")
                    .await
                    .ok()
                    .flatten()
                    == Some(2.0)
            }
        })
        .await
        .expect("every broker enables generation_start and majority_ack");
    }
    cluster
}

/// Cut every broker off from the control plane, and wait until `leader` no
/// longer holds its lease.
async fn partition_control_plane(cluster: &Cluster, leader: &str) {
    for id in cluster.node_ids() {
        for fault in Fault::partition(Endpoint::node(&id), Endpoint::ControlPlane) {
            cluster.inject(&fault).await.expect("partition");
        }
    }
    felix_cluster::wait::until(Duration::from_secs(20), "the lease to lapse", || async {
        cluster.metric(leader, LEASE_HELD).await.ok().flatten() == Some(0.0)
    })
    .await
    .expect("the leader's lease lapses with the control plane gone");
}

/// Everything `node` replays. Reads still need the lease, so a replay is
/// retried until the node has renewed it.
async fn held_by(cluster: &Cluster, node: &str) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let (_client, mut replay) = loop {
        match cluster.replay_on(node, STREAM).await {
            Ok(opened) => break opened,
            Err(err) if tokio::time::Instant::now() > deadline => {
                panic!("replay on {node}: {err:#}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    };
    let mut held = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(2), replay.next_event()).await
    {
        held.push(String::from_utf8_lossy(&event.payload).to_string());
    }
    held
}

/// **With the control plane unreachable, a leader keeps acknowledging what a
/// majority of its replicas holds.** Its lease has lapsed and no report can
/// land, and neither is in the condition any more. Once the control plane is
/// back, whoever leads holds every write acknowledged meanwhile.
#[serial]
#[tokio::test]
async fn acknowledgements_continue_while_the_control_plane_is_partitioned() {
    let cluster = start(Duration::from_secs(10)).await;
    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&leader, STREAM, b"before".to_vec())
        .await
        .expect("publish with the control plane reachable");

    partition_control_plane(&cluster, &leader).await;

    let mut acknowledged = vec!["before".to_string()];
    for i in 0..5 {
        let payload = format!("partitioned-{i}");
        cluster
            .publish_via(&leader, STREAM, payload.clone().into_bytes())
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "{payload} is on a majority at the leader's generation, so it is \
                     acknowledged whatever the lease and the report say: {err:#}"
                )
            });
        acknowledged.push(payload);
    }
    assert_eq!(
        cluster.metric(&leader, LEASE_HELD).await.ok().flatten(),
        Some(0.0),
        "the lease has to be lapsed while the writes are acknowledged",
    );

    cluster.heal_all().await.expect("heal");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "a leader to take writes again",
        || async {
            cluster
                .publish_via_any(STREAM, b"after".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("the shard serves once the control plane is back");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let held = held_by(&cluster, &owner).await;
    for record in &acknowledged {
        assert!(
            held.contains(record),
            "{owner} lost {record}, which was acknowledged: it holds {held:?}"
        );
    }
    cluster.shutdown().await;
}

/// **No majority, no acknowledgement**, with the lease out of the picture:
/// the control plane is gone, so the lease is lapsed either way, and what
/// holds the write back is the followers not having it.
#[serial]
#[tokio::test]
async fn a_write_no_majority_holds_is_not_acknowledged() {
    let cluster = start(Duration::from_secs(3)).await;
    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&leader, STREAM, b"before".to_vec())
        .await
        .expect("publish while whole");
    partition_control_plane(&cluster, &leader).await;

    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    let refuse = Fault::Refuse {
        node: leader.clone(),
        peers: followers,
    };
    cluster
        .inject(&refuse)
        .await
        .expect("cut the followers off");
    let refused = cluster
        .publish_via(&leader, STREAM, b"alone".to_vec())
        .await
        .expect_err("a write only the leader holds was acknowledged");
    let why = format!("{refused:#}");
    assert!(
        !why.contains("lease"),
        "the write was refused for the lease, which proves nothing here: {why}"
    );

    cluster.heal(&refuse).await.expect("heal the peers");
    cluster
        .publish_via(&leader, STREAM, b"together".to_vec())
        .await
        .expect("acknowledged again once a follower answers");
    cluster.shutdown().await;
}

/// **A leader cut off from the control plane keeps writing past its lease,
/// and loses nothing it acknowledged.** The control plane promotes a follower
/// from a report that predates those writes; the new leader fences a majority
/// and takes the furthest log before it serves, so every write acknowledged
/// on a majority is in it. From the fence on, the old leader's followers
/// refuse it and nothing more is acknowledged there.
#[serial]
#[tokio::test]
async fn a_leader_cut_off_from_the_control_plane_loses_nothing_it_acknowledged() {
    let cluster = start(Duration::from_secs(2)).await;
    let old = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&old, STREAM, b"before".to_vec())
        .await
        .expect("publish while whole");
    for fault in Fault::partition(Endpoint::node(&old), Endpoint::ControlPlane) {
        cluster.inject(&fault).await.expect("partition");
    }

    // The harness's control plane places only when asked, so nothing is
    // promoted until the loop below starts: this write is acknowledged with
    // the old leader's own lease lapsed and nobody else leading.
    felix_cluster::wait::until(Duration::from_secs(10), "the lease to lapse", || async {
        cluster.metric(&old, LEASE_HELD).await.ok().flatten() == Some(0.0)
    })
    .await
    .expect("the old leader's lease lapses");
    cluster
        .publish_via(&old, STREAM, b"past-the-lease".to_vec())
        .await
        .expect("a majority holds it, so it is acknowledged without the lease");

    // Then writing and promotion at once. The control plane promotes only
    // from a report fresher than about two expiries, and the old leader can
    // send none from here, so placement is stepped straight away.
    let promoted = std::sync::atomic::AtomicBool::new(false);
    let writes = async {
        let mut acknowledged = vec!["before".to_string(), "past-the-lease".to_string()];
        let mut after_promotion = 0;
        for i in 0.. {
            if after_promotion >= 10 {
                break;
            }
            if promoted.load(std::sync::atomic::Ordering::SeqCst) {
                after_promotion += 1;
            }
            let payload = format!("cut-off-{i}");
            if cluster
                .publish_via(&old, STREAM, payload.clone().into_bytes())
                .await
                .is_ok()
            {
                acknowledged.push(payload);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        acknowledged
    };
    let promotion = async {
        let promoted_in =
            felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
                cluster.place_shards().await;
                cluster.owner(STREAM).await.is_ok_and(|owner| owner != old)
            })
            .await;
        promoted.store(true, std::sync::atomic::Ordering::SeqCst);
        promoted_in
    };
    let (acknowledged, promoted_in) = tokio::join!(writes, promotion);
    promoted_in.expect("the control plane promotes a follower");

    cluster.heal_all().await.expect("heal");
    let new = cluster.owner(STREAM).await.expect("owner");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the new leader to serve",
        || async {
            cluster
                .publish_via(&new, STREAM, b"after".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("the new leader serves");
    let held = held_by(&cluster, &new).await;
    for record in &acknowledged {
        assert!(
            held.contains(record),
            "{new} lost {record}, which {old} acknowledged: it holds {held:?}"
        );
    }
    cluster.shutdown().await;
}

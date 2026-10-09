//! Once the fleet finalizes `majority_ack` and `lease_free_reads`, a durable
//! `Quorum` stream's leader is replaced as soon as a majority of its set says
//! it cannot reach it, without waiting for the control plane to mark it down.
//! The leaders here keep heartbeating throughout, so the lease alone would
//! never replace them.
//!
//! Run with `cargo test -p felix-cluster --test failures followers_word::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Fault, StreamSpec};
use felix_controlplane_service::model::NodeLifecycle;
use felix_controlplane_service::store::ControlPlaneStore;
use serial_test::serial;

const STREAM: &str = "orders";

/// Three brokers, an RF 3 `Quorum` stream, a one-second suspect window, and
/// `features` finalized.
async fn start(features: &[&str]) -> Cluster {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        broker_env: vec![
            (
                "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
                "2000".to_string(),
            ),
            (
                "FELIX_LEADER_SUSPECT_AFTER_MS".to_string(),
                "1000".to_string(),
            ),
        ],
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
    for feature in features {
        store
            .finalize_fleet_feature(feature)
            .await
            .unwrap_or_else(|err| panic!("finalize {feature}: {err}"));
    }
    let enabled = features.len() as f64;
    for id in cluster.node_ids() {
        felix_cluster::wait::until(Duration::from_secs(20), "the features to turn on", || {
            let id = id.clone();
            let cluster = &cluster;
            async move {
                cluster
                    .metric(&id, "felix_broker_fleet_feature_enabled")
                    .await
                    .ok()
                    .flatten()
                    == Some(enabled)
            }
        })
        .await
        .expect("every broker enables the finalized features");
    }
    cluster
}

const ALL: &[&str] = &["generation_start", "majority_ack", "lease_free_reads"];

/// Publish through `leader` and wait for a report naming a follower caught
/// up, so placement has someone to promote.
async fn publish_and_report(cluster: &Cluster, leader: &str) {
    cluster
        .publish_via(leader, STREAM, b"before".to_vec())
        .await
        .expect("publish while whole");
    felix_cluster::wait::until(
        Duration::from_secs(10),
        "a report naming a follower",
        || async {
            cluster
                .replica_report(STREAM, 0)
                .await
                .ok()
                .flatten()
                .is_some_and(|report| report.caught_up.iter().any(|id| id.as_str() != leader))
        },
    )
    .await
    .expect("the leader reports a caught-up follower");
}

/// Cut each of `followers` off from `leader`, both ways, through the peer
/// transport alone. Every broker still reaches the control plane.
async fn cut_off(cluster: &Cluster, leader: &str, followers: &[String]) {
    cluster
        .inject(&Fault::Refuse {
            node: leader.to_string(),
            peers: followers.to_vec(),
        })
        .await
        .expect("cut the leader off");
    for follower in followers {
        cluster
            .inject(&Fault::Refuse {
                node: follower.clone(),
                peers: vec![leader.to_string()],
            })
            .await
            .expect("cut a follower off");
    }
}

fn others(cluster: &Cluster, leader: &str) -> Vec<String> {
    cluster
        .node_ids()
        .into_iter()
        .filter(|id| id != leader)
        .collect()
}

async fn lifecycle(cluster: &Cluster, node: &str) -> NodeLifecycle {
    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .get_node(node)
        .await
        .expect("node")
        .status
        .lifecycle
}

/// Step placement for `window`, and say whether `old` still leads at the end.
async fn still_leads_after(cluster: &Cluster, old: &str, window: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        cluster.place_shards().await;
        if cluster.owner(STREAM).await.is_ok_and(|owner| owner != old) {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    true
}

/// **A leader its followers cannot reach is replaced while it still
/// heartbeats, and the new leader holds what the old one acknowledged.** The
/// lease never lapses here, so this is the followers' word or nothing.
#[serial]
#[tokio::test]
async fn a_leader_its_followers_cannot_reach_is_replaced_while_it_heartbeats() {
    let cluster = start(ALL).await;
    let old = cluster.owner(STREAM).await.expect("owner");
    publish_and_report(&cluster, &old).await;
    cut_off(&cluster, &old, &others(&cluster, &old)).await;

    assert!(
        !still_leads_after(&cluster, &old, Duration::from_secs(20)).await,
        "{old} still leads 20 s after its followers lost it"
    );
    assert_eq!(
        lifecycle(&cluster, &old).await,
        NodeLifecycle::Live,
        "{old} was marked down, so the lease moved the shard, not its followers"
    );

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
    .expect("the new leader takes writes");
    let (_client, mut replay) = cluster.replay_on(&new, STREAM).await.expect("replay");
    let mut held = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(2), replay.next_event()).await
    {
        held.push(String::from_utf8_lossy(&event.payload).to_string());
    }
    assert!(
        held.iter().any(|record| record == "before"),
        "{new} lost a record {old} acknowledged: it holds {held:?}"
    );
    cluster.shutdown().await;
}

/// **One follower is not enough.** The leader and the other follower are
/// still a majority that can acknowledge, so nothing moves.
#[serial]
#[tokio::test]
async fn one_follower_that_cannot_reach_the_leader_moves_nothing() {
    let cluster = start(ALL).await;
    let old = cluster.owner(STREAM).await.expect("owner");
    publish_and_report(&cluster, &old).await;
    let follower = others(&cluster, &old).remove(0);
    cluster
        .inject(&Fault::Refuse {
            node: follower,
            peers: vec![old.clone()],
        })
        .await
        .expect("cut one follower off");

    assert!(still_leads_after(&cluster, &old, Duration::from_secs(5)).await);
    cluster
        .publish_via(&old, STREAM, b"still".to_vec())
        .await
        .expect("the leader and the other follower still acknowledge");
    cluster.shutdown().await;
}

/// **Without `lease_free_reads` the followers' word moves nothing.** The old
/// leader would still serve reads on its lease, so it waits for the lease,
/// which here never lapses.
#[serial]
#[tokio::test]
async fn without_lease_free_reads_the_leader_is_not_moved_on_its_followers_word() {
    let cluster = start(&["generation_start", "majority_ack"]).await;
    let old = cluster.owner(STREAM).await.expect("owner");
    publish_and_report(&cluster, &old).await;
    cut_off(&cluster, &old, &others(&cluster, &old)).await;

    assert!(still_leads_after(&cluster, &old, Duration::from_secs(5)).await);
    cluster.shutdown().await;
}

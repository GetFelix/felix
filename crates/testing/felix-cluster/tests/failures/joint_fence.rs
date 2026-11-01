//! With joint fences on (`FELIX_INTERNAL_ELECTION`, and `joint_fence`
//! reported through `FELIX_TEST_FLEET_FEATURES` and finalized), a set change
//! that swaps two members does not serve until a majority of the set it
//! leaves has taken its fence.
//!
//! Run with `cargo test -p felix-cluster --test failures joint_fence::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_controlplane_service::store::ControlPlaneStore;
use serial_test::serial;

const STREAM: &str = "orders";

async fn start() -> Cluster {
    let features = felix_common::fleet::IMPLEMENTED
        .iter()
        .map(|feature| feature.name())
        .chain([felix_common::fleet::JOINT_FENCE.name()])
        .collect::<Vec<_>>()
        .join(",");
    let cluster = Cluster::start(ClusterConfig {
        nodes: 5,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        broker_env: vec![
            ("FELIX_INTERNAL_ELECTION".to_string(), "true".to_string()),
            ("FELIX_TEST_FLEET_FEATURES".to_string(), features),
            (
                "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
                "2000".to_string(),
            ),
        ],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(
            &["generation_start", "majority_ack", "joint_fence"],
            Duration::from_secs(20),
        )
        .await
        .expect("every broker enables joint_fence");
    cluster
}

async fn assignment(cluster: &Cluster) -> felix_cluster::Assignment {
    let key = format!("{}/{}/{STREAM}/0", cluster.tenant_id, cluster.namespace);
    cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .remove(&key)
        .expect("the stream is placed")
}

/// Publish through `leader` until a report at `generation` names every one of
/// `followers` level with it.
async fn level_at(cluster: &Cluster, leader: &str, followers: &[String], generation: u64) {
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "a level report at the generation",
        || async {
            if cluster
                .publish_via(leader, STREAM, b"level".to_vec())
                .await
                .is_err()
            {
                return false;
            }
            cluster
                .replica_report(STREAM, 0)
                .await
                .ok()
                .flatten()
                .is_some_and(|report| {
                    report.generation == generation
                        && followers.iter().all(|id| {
                            report.caught_up.contains(id)
                                && report.offsets.get(id).copied() == report.leader_offset
                        })
                })
        },
    )
    .await
    .expect("the set holds the leader's writes");
}

/// **A two-member swap waits for a majority of the set it leaves.** Five
/// brokers, RF 3: the leader keeps the shard while both followers are
/// replaced at once by the two spares, as `replicas_after_cut_over` can do,
/// with both old followers down. The new set alone is a majority with the
/// leader, but the old set is not, so the shard does not serve. Once one old
/// follower is back it does, and the new followers hold its writes.
#[serial]
#[tokio::test]
async fn a_two_member_swap_waits_for_the_old_majority() {
    let mut cluster = start().await;
    let before = assignment(&cluster).await;
    let leader = before.leader.clone();
    assert_eq!(before.replicas.len(), 2, "RF 3: {before:?}");
    let spares: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader && !before.replicas.contains(id))
        .collect();
    assert_eq!(spares.len(), 2, "two spares: {spares:?}");
    level_at(&cluster, &leader, &before.replicas, before.generation).await;

    for follower in &before.replicas {
        cluster.kill_node(follower).expect("kill an old follower");
    }
    let store = &cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store;
    let mut swapped = store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .find(|assignment| assignment.key.stream == STREAM)
        .expect("the stream's assignment");
    swapped.replicas = spares.clone();
    store
        .put_shard_assignment(swapped)
        .await
        .expect("swap both followers");
    let after = assignment(&cluster).await;
    assert!(after.generation > before.generation, "{after:?}");
    assert_eq!(after.leader, leader);

    // Long enough for the leader to see the new set and fence it several
    // times over.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        assert!(
            cluster
                .publish_via(&leader, STREAM, b"too-early".to_vec())
                .await
                .is_err(),
            "the leader served the new set with no majority of the old one"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    cluster
        .restart_node(&before.replicas[0])
        .await
        .expect("restart one old follower");
    level_at(&cluster, &leader, &spares, after.generation).await;
    cluster.shutdown().await;
}

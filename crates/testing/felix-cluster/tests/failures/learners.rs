//! With joint fences on, a follower that lost its storage is a learner until
//! it holds the start record of a generation it was shipped: it is never
//! named the leader and takes no part in a fence or a majority. The failover
//! names it under its new storage id, and it counts again once the leader's
//! report shows it holding that generation's start.
//!
//! Run with `cargo test -p felix-cluster --test failures learners::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Fault, StreamSpec};
use felix_controlplane_service::model::ShardAssignment;
use serial_test::serial;

const STREAM: &str = "orders";
const RECORDS: usize = 20;

async fn start() -> Cluster {
    Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        broker_env: vec![(
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            "2000".to_string(),
        )],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        joint_fence: true,
        ..Default::default()
    })
    .await
    .expect("start cluster with joint_fence")
}

async fn assignment(cluster: &Cluster) -> ShardAssignment {
    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .find(|assignment| assignment.key.stream == STREAM)
        .expect("the stream's assignment")
}

/// Whether a publish through `node` is acknowledged within the quorum timeout.
async fn acknowledged(cluster: &Cluster, node: &str, payload: &str) -> bool {
    cluster
        .publish_via(node, STREAM, payload.as_bytes().to_vec())
        .await
        .is_ok()
}

/// Wipe a follower the leader cannot reach, fail the leader over, and follow
/// the wiped copy from learner to voter. Before it is re-admitted the new
/// leader cannot serve with the old leader down; after, it can.
#[serial]
#[tokio::test]
async fn a_wiped_follower_counts_only_once_it_holds_the_start() {
    let mut cluster = start().await;
    let leader = cluster.owner(STREAM).await.expect("owner");
    for i in 0..RECORDS {
        cluster
            .publish_via(&leader, STREAM, format!("record-{i}").into_bytes())
            .await
            .expect("publish");
    }
    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    let (stays, wiped) = (followers[0].clone(), followers[1].clone());
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "a report naming both followers at the tail",
        || async {
            cluster
                .replica_report(STREAM, 0)
                .await
                .ok()
                .flatten()
                .is_some_and(|report| {
                    report.leader_offset.is_some()
                        && [&stays, &wiped]
                            .iter()
                            .all(|id| report.offsets.get(*id) == report.leader_offset.as_ref())
                })
        },
    )
    .await
    .expect("both followers level");

    // The leader must not ship to the wiped copy, or it would be re-admitted
    // before the failover this test is about.
    let refused = Fault::Refuse {
        node: leader.clone(),
        peers: vec![wiped.clone()],
    };
    cluster.inject(&refused).await.expect("inject");
    cluster.stop_node(&wiped).await.expect("kill the follower");
    let storage = cluster.node(&wiped).expect("wiped").storage_dir();
    std::fs::remove_dir_all(&storage).expect("wipe its storage");
    cluster.restart_node(&wiped).await.expect("restart it");
    // The harness steps placement only when asked; the failover and the
    // re-admission below need it running.
    cluster.run_placement(Duration::from_millis(200));

    // With the leader gone, placement names the follower that kept its data
    // and holds the wiped one a learner.
    let before = assignment(&cluster).await;
    cluster.stop_node(&leader).await.expect("kill the leader");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "a failover to the follower that kept its data",
        || async {
            let now = assignment(&cluster).await;
            now.generation > before.generation && now.leader == stays
        },
    )
    .await
    .expect("failover");
    let failed_over = assignment(&cluster).await;
    assert!(
        failed_over.learners.contains(&wiped),
        "the wiped copy is a learner: {failed_over:?}"
    );

    // The old leader is down and the learner does not vote, so no majority
    // of the set can take the new leader's fence. Long enough for the new
    // leader to try its fence several times over.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        assert!(
            !acknowledged(&cluster, &stays, "no-majority").await,
            "acknowledged with one voter of three"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(assignment(&cluster).await.learners.contains(&wiped));

    // The old leader comes back with its data: the new leader serves, ships
    // to the learner, and the report names it holding the start.
    cluster
        .restart_node(&leader)
        .await
        .expect("restart the old leader");
    // The refusal is kept in its data directory; lift it so the old leader
    // can ship to the copy if it leads again.
    cluster.heal(&refused).await.expect("heal");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the new leader to serve once the old one is back",
        || async {
            let owner = cluster.owner(STREAM).await.unwrap_or_default();
            acknowledged(&cluster, &owner, "after-restart").await
        },
    )
    .await
    .expect("serving with two voters");
    // The failover named it under its new storage id, so it counts once the
    // leader's report at the current generation shows it holding the start.
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "a report naming the copy holding the start",
        || async {
            let now = assignment(&cluster).await;
            cluster
                .replica_report(STREAM, 0)
                .await
                .ok()
                .flatten()
                .is_some_and(|report| {
                    report.generation == now.generation
                        && report.leader.as_deref() == Some(now.leader.as_str())
                        && report.holds_start.contains(&wiped)
                })
        },
    )
    .await
    .expect("holds the start");

    // Now it counts: with the old leader down again, the leader and the
    // re-admitted copy are a majority.
    let owner = cluster.owner(STREAM).await.expect("owner");
    assert_ne!(owner, wiped, "a copy just re-admitted is not made leader");
    let down = if owner == leader { &stays } else { &leader };
    cluster.stop_node(down).await.expect("kill the other voter");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "an acknowledgement counting the re-admitted copy",
        || async {
            let owner = cluster.owner(STREAM).await.unwrap_or_default();
            acknowledged(&cluster, &owner, "counted").await
        },
    )
    .await
    .expect("the re-admitted copy counts");
}

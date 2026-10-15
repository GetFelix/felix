//! A follower that lost its data directory is never promoted on a report
//! about the copy it lost, and one that only restarted still is.
//!
//! The leader here cannot reach the follower after it comes back (its peer
//! requests to it are refused), so its reports keep naming the follower
//! caught up under the identity it had before. That is the idle-shard case
//! where nothing tells the leader otherwise. Placement compares that
//! identity with the one the follower registered with.
//!
//! Run with `cargo test -p felix-cluster --test failures wiped::`.
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, Fault, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";
const RECORDS: usize = 20;

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    }
}

/// The leader, the follower that stays (`b`), and the one that restarts
/// (`c`), after [`RECORDS`] acknowledged records, with the leader's report
/// naming `c` caught up under the identity it registered with.
async fn written(cluster: &Cluster) -> (String, String, String) {
    let leader = cluster.owner(STREAM).await.expect("owner");
    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    for i in 0..RECORDS {
        cluster
            .publish_via(&leader, STREAM, format!("record-{i}").into_bytes())
            .await
            .expect("publish");
    }
    let (b, c) = (followers[0].clone(), followers[1].clone());
    let c_id = registered_storage(cluster, &c).await;
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "a report naming both followers at the tail, under their identities",
        || async {
            let Ok(Some(report)) = cluster.replica_report(STREAM, 0).await else {
                return false;
            };
            report.leader_offset.is_some()
                && [&b, &c]
                    .iter()
                    .all(|node| report.offsets.get(*node) == report.leader_offset.as_ref())
                && report.caught_up.contains(&c)
                && report.follower_storage.get(&c) == Some(&c_id)
        },
    )
    .await
    .expect("the leader reports its followers");
    (leader, b, c)
}

async fn registered_storage(cluster: &Cluster, node: &str) -> String {
    cluster
        .registered_node(node)
        .await
        .expect("node")
        .status
        .storage_id
        .unwrap_or_else(|| panic!("{node} registered without a storage identity"))
}

/// Stop `c`, wipe its storage if asked, and start it again while the leader
/// cannot reach it. Waits for a report the leader made after `c`
/// registered again, so placement has the same evidence it would have had
/// after a long idle stretch.
async fn restart_unreached(cluster: &mut Cluster, leader: &str, c: &str, wipe: bool) {
    cluster
        .inject(&Fault::Refuse {
            node: leader.to_string(),
            peers: vec![c.to_string()],
        })
        .await
        .expect("inject");
    let before = registered_storage(cluster, c).await;
    cluster.stop_node(c).await.expect("stop c");
    if wipe {
        let storage = cluster.node(c).expect("c").storage_dir();
        std::fs::remove_dir_all(&storage).expect("wipe c's storage");
    }
    cluster.restart_node(c).await.expect("restart c");
    let after = registered_storage(cluster, c).await;
    assert_eq!(
        before != after,
        wipe,
        "a wipe changes the storage identity and a restart keeps it"
    );
    let since = cluster.control_plane_now_millis().await.expect("clock");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "a report from the leader made after c registered again",
        || async {
            matches!(
                cluster.replica_report(STREAM, 0).await,
                Ok(Some(report)) if report.reported_at_millis > since
            )
        },
    )
    .await
    .expect("the leader keeps reporting");
    let report = cluster
        .replica_report(STREAM, 0)
        .await
        .expect("report")
        .expect("held");
    assert!(
        report.caught_up.contains(c),
        "the leader learned of the restart, so this does not test placement: {report:?}"
    );
    assert_eq!(report.follower_storage.get(c), Some(&before));
}

/// Kill the leader, stop `b`, and wait until the control plane has marked
/// the leader down.
async fn lose_leader_and_b(cluster: &mut Cluster, leader: &str, b: &str) {
    cluster.kill_node(leader).expect("kill the leader");
    cluster.stop_node(b).await.expect("stop b");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the control plane to mark the leader down",
        || async {
            cluster
                .placeable_nodes()
                .await
                .is_ok_and(|live| !live.iter().any(|node| node == leader))
        },
    )
    .await
    .expect("the leader is marked down");
}

/// Step placement until the shard is led by someone other than `gone`.
async fn promoted_away_from(cluster: &Cluster, gone: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        cluster.place_shards().await;
        if let Ok(owner) = cluster.owner(STREAM).await
            && owner != gone
        {
            return owner;
        }
        assert!(Instant::now() < deadline, "the shard never left {gone}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Everything `node` replays from the start, retried while it opens.
async fn replay(cluster: &Cluster, node: &str) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut payloads = Vec::new();
        if let Ok((_client, mut subscription)) = cluster.replay_on(node, STREAM).await {
            while let Ok(Ok(Some(event))) =
                tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await
            {
                payloads.push(String::from_utf8_lossy(&event.payload).into_owned());
                if payloads.len() == RECORDS {
                    return payloads;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "{node} never served every acknowledged record: {payloads:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn all_records() -> Vec<String> {
    (0..RECORDS).map(|i| format!("record-{i}")).collect()
}

/// **A wiped follower is not promoted (#1266).** The leader's last report
/// still names it caught up under its old identity, and it registered under
/// a new one. Promoting it would let it fence with its empty log counted as
/// a vote. The shard waits instead, and fails over to the follower that kept
/// its log once that one is back.
#[tokio::test]
#[serial]
async fn a_wiped_follower_is_not_promoted() {
    let mut cluster = Cluster::start(config()).await.expect("start");
    let (leader, b, c) = written(&cluster).await;
    restart_unreached(&mut cluster, &leader, &c, true).await;
    lose_leader_and_b(&mut cluster, &leader, &b).await;

    // Unplaceable is placement having decided the failover: the lease has
    // lapsed, and no live replica holds the log.
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "placement to find no replica to promote",
        || async {
            let outcome = cluster.place_shards().await;
            let owner = cluster.owner(STREAM).await.expect("owner");
            assert_ne!(owner, c, "the wiped follower was promoted");
            outcome.unplaceable > 0
        },
    )
    .await
    .expect("placement refuses the wiped follower");
    assert_eq!(cluster.owner(STREAM).await.expect("owner"), leader);

    cluster.restart_node(&b).await.expect("restart b");
    let promoted = promoted_away_from(&cluster, &leader).await;
    assert_eq!(promoted, b);
    assert_eq!(replay(&cluster, &b).await, all_records());
    cluster.shutdown().await;
}

/// **A follower that only restarted is promoted past the report's TTL
/// (#1250, #1266).** Its identity survived the restart, so the dead
/// leader's last report still describes its log, and it is trusted for as
/// long as the leader was heard from, not only for the TTL after it was
/// made.
#[tokio::test]
#[serial]
async fn a_restarted_follower_is_promoted_past_the_ttl() {
    let mut cluster = Cluster::start(config()).await.expect("start");
    let (leader, b, c) = written(&cluster).await;
    restart_unreached(&mut cluster, &leader, &c, false).await;
    lose_leader_and_b(&mut cluster, &leader, &b).await;

    // No placement pass until the report is older than its TTL, so only the
    // freeze can promote.
    let reported_at = cluster
        .replica_report(STREAM, 0)
        .await
        .expect("report")
        .expect("held")
        .reported_at_millis;
    let ttl = cluster.report_ttl_millis();
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the leader's last report to pass its TTL",
        || async {
            cluster
                .control_plane_now_millis()
                .await
                .is_ok_and(|now| now > reported_at + ttl)
        },
    )
    .await
    .expect("the clock moves");

    let promoted = promoted_away_from(&cluster, &leader).await;
    assert_eq!(promoted, c);
    // The fence needs a majority: c and b.
    cluster.restart_node(&b).await.expect("restart b");
    assert_eq!(replay(&cluster, &c).await, all_records());
    cluster.shutdown().await;
}

//! A follower its leader has stopped shipping to shows in the control plane,
//! and placement replaces its copy rather than waiting on it.
//!
//! Run with `cargo test -p felix-cluster --test failures halted::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_controlplane_service::cluster::placement::MovePolicy;
use felix_controlplane_service::store::ControlPlaneStore;
use serial_test::serial;

const STREAM: &str = "orders";

/// Four brokers and an RF 3 `Quorum` stream, with small segments and a tight
/// size bound so retention trims a leader's log within a few hundred records,
/// and no rebuilds, so a follower that needs one stays halted.
async fn start() -> Cluster {
    let env = |key: &str, value: &str| (key.to_string(), value.to_string());
    Cluster::start(ClusterConfig {
        nodes: 4,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        broker_env: vec![
            env("FELIX_DURABLE_SEGMENT_BYTES", "4096"),
            env("FELIX_DURABLE_RETENTION_BYTES", "8192"),
            env("FELIX_DURABLE_RETENTION_INTERVAL_SECONDS", "1"),
            env("FELIX_REPLICATION_REBUILD_MAX_CONCURRENT", "0"),
        ],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster")
}

/// `GET /v1/placement/replication` for the stream's shard.
async fn replication(cluster: &Cluster) -> serde_json::Value {
    let control_plane = cluster.control_plane.as_ref().expect("control plane");
    let response: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "{}/v1/placement/replication",
            control_plane.base_url
        ))
        .bearer_auth(&cluster.admin_token)
        .send()
        .await
        .expect("read replication")
        .error_for_status()
        .expect("replication answered")
        .json()
        .await
        .expect("decode replication");
    response["items"]
        .as_array()
        .and_then(|items| items.iter().find(|item| item["stream"] == STREAM))
        .cloned()
        .expect("the stream's shard is listed")
}

/// **A halted follower is named, and replaced.** broker-F misses enough of
/// the log that retention trims what it needs from the leader. When it comes
/// back it holds records of its own, so it refuses the log placed at the
/// surviving base and the leader stops shipping to it. The control plane
/// shows the halt and its reason, counts the shard a copy short, and once
/// the restore delay has passed copies the shard onto the spare and drops
/// the halted copy.
#[serial]
#[tokio::test]
async fn a_halted_follower_is_shown_and_replaced() {
    let mut cluster = start().await;
    let key = format!("{}/{}/{STREAM}/0", cluster.tenant_id, cluster.namespace);
    let before = cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .remove(&key)
        .expect("the stream is placed");
    let leader = before.leader.clone();
    let (halting, kept) = (before.replicas[0].clone(), before.replicas[1].clone());
    let spare = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader && !before.replicas.contains(id))
        .expect("a spare broker");

    for i in 0..5 {
        cluster
            .publish_via(&leader, STREAM, format!("early-{i}").into_bytes())
            .await
            .expect("publish while whole");
    }
    felix_cluster::wait::until(Duration::from_secs(20), "every copy level", || async {
        cluster
            .replica_report(STREAM, 0)
            .await
            .ok()
            .flatten()
            .is_some_and(|report| {
                report.caught_up.contains(&halting) && report.caught_up.contains(&kept)
            })
    })
    .await
    .expect("both followers hold the early records");

    cluster.stop_node(&halting).await.expect("stop a follower");
    let filler = "x".repeat(400);
    for i in 0..200 {
        cluster
            .publish_via(&leader, STREAM, format!("{i}-{filler}").into_bytes())
            .await
            .unwrap_or_else(|err| panic!("publish {i} with a follower down: {err:#}"));
    }
    // Two retention ticks, so the leader's log starts past what it missed.
    tokio::time::sleep(Duration::from_secs(3)).await;
    cluster.restart_node(&halting).await.expect("restart");

    felix_cluster::wait::until(Duration::from_secs(30), "the halt reported", || async {
        cluster
            .replica_report(STREAM, 0)
            .await
            .ok()
            .flatten()
            .is_some_and(|report| {
                report
                    .halted
                    .get(&halting)
                    .is_some_and(|halt| halt.reason == "needs_bootstrap")
            })
    })
    .await
    .expect("the leader reports the follower halted");

    let shown = replication(&cluster).await;
    assert_eq!(shown["halted"][0]["node_id"], halting.as_str(), "{shown}");
    assert_eq!(shown["halted"][0]["reason"], "needs_bootstrap", "{shown}");
    assert_eq!(shown["current_replicas"], 2, "{shown}");
    assert_eq!(shown["under_replicated"], true, "{shown}");

    let control_plane = cluster.control_plane.as_ref().expect("control plane");
    let restoring = MovePolicy {
        restore_after_millis: Some(1_000),
        ..MovePolicy::default()
    };
    let store = &control_plane.store;
    let shard = felix_controlplane_service::model::ShardKey {
        tenant_id: cluster.tenant_id.clone(),
        namespace: cluster.namespace.clone(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: felix_controlplane_service::model::ShardKind::Stream,
    };
    felix_cluster::wait::until(Duration::from_secs(60), "the copy replaced", || {
        let restoring = restoring.clone();
        let (halting, spare, shard) = (halting.clone(), spare.clone(), shard.clone());
        async move {
            control_plane.place_shards_with(restoring).await;
            store.get_shard_assignment(&shard).await.is_ok_and(|now| {
                now.joining.is_none()
                    && !now.replicas.contains(&halting)
                    && now.replicas.contains(&spare)
            })
        }
    })
    .await
    .expect("placement replaces the halted copy on the spare");

    let after = store
        .get_shard_assignment(&shard)
        .await
        .expect("assignment");
    assert_eq!(after.leader, leader);
    assert!(after.replicas.contains(&kept), "{after:?}");
    let shown = replication(&cluster).await;
    assert_eq!(shown["current_replicas"], 3, "{shown}");
    assert!(shown.get("halted").is_none(), "{shown}");
}

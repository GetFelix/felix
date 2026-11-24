//! A follower its leader has stopped shipping to shows in the control plane,
//! and placement replaces its copy rather than waiting on it.
//!
//! On Raft no follower is halted: one behind the leader's purged log is sent
//! the leader's purge snapshot and counts again once it holds the log.
//!
//! Run with `cargo test -p felix-cluster --test failures halted::`.
use std::time::Duration;

use felix_client::{CursorErrorReason, SubscribeCursorError};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_controlplane_service::cluster::placement::MovePolicy;
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
        .bearer_auth(cluster.admin_token())
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

/// Bytes of segment files under `node`'s storage.
fn segment_bytes(cluster: &Cluster, node: &str) -> u64 {
    fn walk(dir: &std::path::Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| {
                let path = entry.path();
                match entry.file_type() {
                    Ok(kind) if kind.is_dir() => walk(&path),
                    Ok(_) if path.extension().is_some_and(|ext| ext == "log") => {
                        entry.metadata().map_or(0, |meta| meta.len())
                    }
                    _ => 0,
                }
            })
            .sum()
    }
    walk(&cluster.node(node).expect("node").storage_dir())
}

/// The oldest offset `node` serves a read from, by the refusal of a read
/// at offset 0. `None` while it still serves offset 0 or cannot be asked.
async fn floor_on(cluster: &Cluster, node: &str) -> Option<u64> {
    let connection = cluster.client_on(node).await.ok()?;
    let read = connection
        .read(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            0,
            0,
            None,
            1,
        )
        .await;
    let refused = read.err()?;
    let cursor = refused.downcast_ref::<SubscribeCursorError>()?;
    (cursor.reason == CursorErrorReason::TooOld).then_some(cursor.available)
}

/// The Raft form. broker-F stops while the leader writes far past the
/// retention bound, and the leader trims and purges its log past what F
/// holds, so F cannot be caught up from entries. When F is back it installs
/// the leader's purge snapshot instead of sitting a copy short: it holds the
/// leader's log, and with the other follower down the leader and F are the
/// majority that acknowledges.
async fn raft_a_follower_behind_the_purge_catches_up(mut cluster: Cluster) {
    let leader = cluster
        .serving_leader(STREAM, Duration::from_secs(30))
        .await
        .expect("a serving leader");
    let mut last_early = 0;
    for i in 0..5 {
        last_early = cluster
            .publish_via_at(&leader, STREAM, format!("early-{i}").into_bytes())
            .await
            .expect("publish while whole")
            .expect("a durable publish reports its offset");
    }
    let leader = cluster
        .wait_for_every_member_level(STREAM, Duration::from_secs(30))
        .await
        .expect("every member holds the early records");
    // Four brokers: the spare runs no member of the group.
    let mut followers = Vec::new();
    for id in cluster.node_ids() {
        if id != leader && cluster.raft_member(&id, STREAM).await.is_some() {
            followers.push(id);
        }
    }
    assert_eq!(followers.len(), 2, "two followers: {followers:?}");
    let (behind, kept) = (followers[0].clone(), followers[1].clone());
    let held = cluster
        .raft_member(&behind, STREAM)
        .await
        .and_then(|raft| raft.last_index)
        .expect("the follower's last index");

    cluster.stop_node(&behind).await.expect("stop a follower");
    let filler = "x".repeat(400);
    for i in 0..200 {
        cluster
            .publish_via(&leader, STREAM, format!("{i}-{filler}").into_bytes())
            .await
            .unwrap_or_else(|err| panic!("publish {i} with a follower down: {err:#}"));
    }
    // The hazard: the leader serves nothing at or below what the stopped
    // follower holds, and its purge freed the segments that held it, so the
    // entries the follower needs next are gone from the leader's log.
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the leader to trim past the stopped follower",
        || async {
            floor_on(&cluster, &leader)
                .await
                .is_some_and(|floor| floor > last_early)
        },
    )
    .await
    .expect("the leader's floor passes the follower's last record");
    // 200 publishes of over 400 bytes; the 8 KiB bound, the segment the purge
    // point falls in, the open one and a margin for the other logs are kept.
    let kept_bytes = 8192 + 4 * 4096 + 16 * 1024;
    felix_cluster::wait::until(Duration::from_secs(60), "the leader's purge", || {
        let bytes = segment_bytes(&cluster, &leader);
        async move { bytes <= kept_bytes }
    })
    .await
    .unwrap_or_else(|err| {
        panic!(
            "{leader} holds {} bytes of segments: {err:#}",
            segment_bytes(&cluster, &leader)
        )
    });

    cluster.restart_node(&behind).await.expect("restart");
    let tail = cluster
        .raft_member(&leader, STREAM)
        .await
        .and_then(|raft| raft.last_index)
        .expect("the leader's last index");
    assert!(tail > held, "the leader wrote past {held}: {tail}");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the restarted follower to hold the leader's log",
        || async {
            cluster
                .raft_member(&behind, STREAM)
                .await
                .and_then(|raft| raft.last_index)
                .is_some_and(|index| index >= tail)
        },
    )
    .await
    .expect("the follower catches up past the purge");

    cluster
        .stop_node(&kept)
        .await
        .expect("stop the other follower");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "an acknowledgement counting the caught-up follower",
        || async {
            let Ok(serving) = cluster.serving_leader(STREAM, Duration::from_secs(5)).await else {
                return false;
            };
            cluster
                .publish_via(&serving, STREAM, b"counted".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("the caught-up follower counts in a majority");
    cluster.shutdown().await;
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
    if felix_cluster::suite_runs_on_raft() {
        raft_a_follower_behind_the_purge_catches_up(cluster).await;
        return;
    }
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

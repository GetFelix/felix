//! A durable `Quorum` stream deleted and created again on a new cluster
//! starts empty on every replica, and replicates (#1447).
//!
//! Run with `cargo test -p felix-cluster --test failures recreate::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig};
use serial_test::serial;

const STREAM: &str = "recreated";

fn streams_url(cluster: &Cluster) -> String {
    format!(
        "{}/v1/tenants/{}/namespaces/{}/streams",
        cluster.control_plane_url(),
        cluster.tenant_id,
        cluster.namespace
    )
}

/// Create the stream: one shard, three copies, `Quorum`, durable. Answers its
/// incarnation.
async fn create(cluster: &Cluster) -> u64 {
    let response = reqwest::Client::new()
        .post(streams_url(cluster))
        .bearer_auth(cluster.admin_token())
        .json(&serde_json::json!({
            "tenant_id": cluster.tenant_id,
            "namespace": cluster.namespace,
            "stream": STREAM,
            "kind": "Stream",
            "shards": 1,
            "replication_factor": 3,
            "retention": { "max_age_seconds": null, "max_size_bytes": null },
            "consistency": "Quorum",
            "delivery": "AtLeastOnce",
            "durable": true,
        }))
        .send()
        .await
        .expect("create");
    let status = response.status();
    let body: serde_json::Value = response.json().await.expect("create answer");
    assert!(status.is_success(), "create: {status} {body}");
    body["incarnation"].as_u64().unwrap_or(0)
}

async fn delete(cluster: &Cluster) {
    let response = reqwest::Client::new()
        .delete(format!("{}/{STREAM}", streams_url(cluster)))
        .bearer_auth(cluster.admin_token())
        .send()
        .await
        .expect("delete");
    assert!(
        response.status().is_success(),
        "delete: {}",
        response.status()
    );
}

/// One acked publish: the value, the offset the broker answered, and the
/// broker the client published through.
type Acks = Vec<(String, Option<u64>, String)>;

/// Publish `values` once the stream serves, stepping placement meanwhile.
/// Each ack is kept in `acks`.
async fn publish(cluster: &Cluster, values: &[&str], acks: &mut Acks) {
    for value in values {
        let acked = felix_cluster::wait::until_some(
            Duration::from_secs(30),
            &format!("{value} accepted"),
            || async {
                cluster.place_shards().await;
                for node in cluster.node_ids() {
                    if let Ok(offset) = cluster
                        .publish_acked_via(&node, STREAM, value.as_bytes().to_vec())
                        .await
                    {
                        return Some((offset, node));
                    }
                }
                None
            },
        )
        .await
        .expect("publish");
        acks.push((value.to_string(), acked.0, acked.1));
    }
}

/// A failure message's evidence for #3378: what every member says about the
/// new shard, and what each member's own replay holds. A record missing from
/// the serving leader's replay but held by another member is a replay race; one
/// held by none was lost after its ack.
async fn diagnose(cluster: &Cluster, acks: &Acks) -> String {
    let mut out = format!("acks (value, offset, entry broker): {acks:?}\n");
    for node in cluster.node_ids() {
        let addr = cluster.node(&node).expect("node").client_addr;
        let view = match felix_cluster::client::connect(
            addr,
            &cluster.tenant_id,
            &cluster.inspector_token(),
        )
        .await
        {
            Ok(client) => client
                .inspect_shard(
                    felix_client::ShardKind::Stream,
                    &cluster.tenant_id,
                    &cluster.namespace,
                    STREAM,
                    0,
                )
                .await
                .map(|v| {
                    format!(
                        "role={} phase={} serving={} generation={:?} tail={:?} committed={:?} accepted_generation={:?} raft={:?}",
                        v.role, v.phase, v.serving, v.generation, v.tail, v.committed,
                        v.accepted_generation, v.raft
                    )
                })
                .unwrap_or_else(|err| format!("inspect failed: {err:#}")),
            Err(err) => format!("connect failed: {err:#}"),
        };
        let replay = match cluster
            .whole_replay_on(
                &node,
                STREAM,
                Duration::from_secs(2),
                Duration::from_secs(10),
            )
            .await
        {
            Ok(events) => events
                .iter()
                .map(|e| {
                    format!(
                        "{:?}:{}(skip {})",
                        e.offset,
                        String::from_utf8_lossy(&e.payload),
                        e.skipped_before
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
            Err(err) => format!("replay failed: {err:#}"),
        };
        out.push_str(&format!("{node}: {view}\n    log: [{replay}]\n"));
    }
    out
}

/// Every follower holds the leader's tail, and none halted.
async fn every_copy_level(cluster: &Cluster) {
    if felix_cluster::suite_runs_on_raft() {
        // A Raft group reports nothing to the control plane; a leader with a
        // caught-up majority is the same state.
        cluster
            .wait_for_replication(STREAM, Duration::from_secs(30))
            .await
            .unwrap_or_else(|err| panic!("{err:#}"));
        return;
    }
    felix_cluster::wait::until(Duration::from_secs(30), "every copy level", || async {
        cluster.place_shards().await;
        cluster
            .replica_report(STREAM, 0)
            .await
            .ok()
            .flatten()
            .is_some_and(|report| {
                let Some(tail) = report.leader_offset else {
                    return false;
                };
                report.halted.is_empty()
                    && report.caught_up.len() == 2
                    && report
                        .caught_up
                        .iter()
                        .all(|follower| report.offsets.get(follower).is_some_and(|at| *at >= tail))
            })
    })
    .await
    .unwrap_or_else(|err| panic!("{err:#}"));
}

/// **A stream created again on a new cluster replicates from empty.** The
/// brokers register before any stream exists, so the cluster finalizes its
/// features, incarnations among them. A durable `Quorum` stream is written, deleted and created
/// again under its name. The new stream has an incarnation of its own, so
/// each broker opens new logs: every copy takes the new records, and none
/// halts on the deleted stream's.
#[serial]
#[tokio::test]
async fn a_recreated_quorum_stream_replicates_on_a_new_cluster() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: Vec::new(),
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let store = &cluster.control_plane.as_ref().expect("control plane").store;
    // A new cluster starts with every feature but the opt-ins
    // `publisher_principal` and `record_generations`; `majority_ack` keeps the
    // control plane's replica report off a `Quorum` write's path.
    let enabled = store.enabled_fleet_features().await.expect("features");
    for feature in felix_common::fleet::IMPLEMENTED {
        let name = feature.name();
        assert_eq!(
            enabled.contains(name),
            *feature != felix_common::fleet::PUBLISHER_PRINCIPAL
                && *feature != felix_common::fleet::RECORD_GENERATIONS,
            "{name}: {enabled:?}"
        );
    }

    let first = create(&cluster).await;
    assert_ne!(first, 0, "a new cluster's stream has an incarnation");
    let mut acks = Acks::new();
    publish(&cluster, &["old-a", "old-b", "old-c", "old-d"], &mut acks).await;
    every_copy_level(&cluster).await;

    delete(&cluster).await;
    let second = create(&cluster).await;
    assert!(second > first, "{second} after {first}");
    acks.clear();
    publish(&cluster, &["new-a", "new-b"], &mut acks).await;
    every_copy_level(&cluster).await;

    if felix_cluster::suite_runs_on_raft() {
        // The leader holds the new stream's two records and none of the
        // deleted stream's four.
        let leader = cluster
            .serving_leader(STREAM, Duration::from_secs(30))
            .await
            .expect("leader");
        let served_by = leader.clone();
        let held = felix_cluster::wait::until_some(
            Duration::from_secs(30),
            "the new records to replay",
            || async {
                let (_client, mut replay) = cluster.replay_on(&leader, STREAM).await.ok()?;
                let mut held = Vec::new();
                while let Ok(Ok(Some(event))) =
                    tokio::time::timeout(Duration::from_secs(2), replay.next_event()).await
                {
                    held.push((
                        event.offset,
                        String::from_utf8_lossy(&event.payload).to_string(),
                    ));
                }
                held.iter().any(|(_, v)| v == "new-b").then_some(held)
            },
        )
        .await;
        let held = match held {
            Ok(held) => held,
            Err(err) => panic!(
                "no replay held new-b ({err:#}); serving leader {served_by}\n{}",
                diagnose(&cluster, &acks).await
            ),
        };
        let missing_new_a = !held.iter().any(|(_, v)| v == "new-a");
        let has_old = held.iter().any(|(_, v)| v.starts_with("old-"));
        if missing_new_a || has_old {
            let problem = match (missing_new_a, has_old) {
                (true, true) => {
                    "new-a is missing from the replay and the deleted stream's records are present"
                }
                (true, false) => "new-a is missing from the replay",
                _ => "the deleted stream's records are present in the replay",
            };
            panic!(
                "{problem}: {served_by} replayed (offset, value) {held:?}\n{}",
                diagnose(&cluster, &acks).await
            );
        }
        cluster.shutdown().await;
        return;
    }
    let report = cluster
        .replica_report(STREAM, 0)
        .await
        .expect("report")
        .expect("a report");
    // The deleted stream's four records and the new two would be six.
    assert!(
        report.leader_offset.is_some_and(|tail| tail < 6),
        "the new stream holds the deleted one's records: {report:?}"
    );
    cluster.shutdown().await;
}

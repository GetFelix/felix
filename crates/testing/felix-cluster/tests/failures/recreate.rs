//! A durable `Quorum` stream deleted and created again on a new cluster
//! starts empty on every replica, and replicates (#1447).
//!
//! Run with `cargo test -p felix-cluster --test failures recreate::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig};
use felix_controlplane_service::store::ControlPlaneStore;
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

/// Publish `values` once the stream serves, stepping placement meanwhile.
async fn publish(cluster: &Cluster, values: &[&str]) {
    for value in values {
        felix_cluster::wait::until(
            Duration::from_secs(30),
            &format!("{value} accepted"),
            || async {
                cluster.place_shards().await;
                cluster
                    .publish_via_any(STREAM, value.as_bytes().to_vec())
                    .await
                    .is_ok()
            },
        )
        .await
        .expect("publish");
    }
}

/// Every follower holds the leader's tail, and none halted.
async fn every_copy_level(cluster: &Cluster) {
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
    // A new cluster starts with every feature but the opt-in
    // `publisher_principal`; `majority_ack` keeps the control plane's replica
    // report off a `Quorum` write's path.
    let enabled = store.enabled_fleet_features().await.expect("features");
    for feature in felix_common::fleet::IMPLEMENTED {
        let name = feature.name();
        assert_eq!(
            enabled.contains(name),
            *feature != felix_common::fleet::PUBLISHER_PRINCIPAL,
            "{name}: {enabled:?}"
        );
    }

    let first = create(&cluster).await;
    assert_ne!(first, 0, "a new cluster's stream has an incarnation");
    publish(&cluster, &["old-a", "old-b", "old-c", "old-d"]).await;
    every_copy_level(&cluster).await;

    delete(&cluster).await;
    let second = create(&cluster).await;
    assert!(second > first, "{second} after {first}");
    publish(&cluster, &["new-a", "new-b"]).await;
    every_copy_level(&cluster).await;

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

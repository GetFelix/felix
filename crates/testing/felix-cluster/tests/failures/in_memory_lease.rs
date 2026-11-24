//! An in-memory `Quorum` stream keeps nothing on its followers, so its lease
//! is what stops an owner cut off from the control plane. Lease-free serving
//! applies only to shards with a log, even once the fleet finalized it
//! (#3069).
//!
//! Run with `cargo test -p felix-cluster --test failures in_memory_lease::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use serial_test::serial;

const STREAM: &str = "presence";
const LEASE_HELD: &str = "felix_broker_lease_held";
const ALL: &[&str] = &["generation_start", "majority_ack", "lease_free_reads"];

/// **A cut-off owner of an in-memory `Quorum` stream refuses once its lease
/// lapses.** The control plane places the shard again after the lapse, so an
/// owner that kept accepting would be a second one.
#[serial]
#[tokio::test]
async fn an_in_memory_quorum_owner_refuses_after_its_lease_lapses() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3).in_memory()],
        proxy_links: true,
        broker_env: vec![(
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            "2000".to_string(),
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
    for feature in ALL {
        store
            .finalize_fleet_feature(feature)
            .await
            .unwrap_or_else(|err| panic!("finalize {feature}: {err}"));
    }
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
                    == Some(ALL.len() as f64)
            }
        })
        .await
        .expect("every broker enables the finalized features");
    }

    let owner = cluster
        .shard_owner_of("stream", STREAM, 0)
        .await
        .expect("stream shard owner");
    felix_cluster::wait::until(Duration::from_secs(20), "a publish to land", || async {
        cluster
            .publish_via(&owner, STREAM, b"before".to_vec())
            .await
            .is_ok()
    })
    .await
    .expect("the owner takes a publish while whole");

    for fault in Fault::partition(Endpoint::node(&owner), Endpoint::ControlPlane) {
        cluster.inject(&fault).await.expect("partition");
    }
    felix_cluster::wait::until(Duration::from_secs(20), "the lease to lapse", || async {
        cluster.metric(&owner, LEASE_HELD).await.ok().flatten() == Some(0.0)
    })
    .await
    .expect("the lease lapses with the control plane gone");

    // Every publish from here on must be refused, not just the first.
    for attempt in 0..5 {
        let result = cluster
            .publish_via(&owner, STREAM, format!("after-{attempt}").into_bytes())
            .await;
        assert!(
            result.is_err(),
            "a cut-off owner acknowledged publish {attempt} on an in-memory Quorum stream \
             after its lease lapsed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

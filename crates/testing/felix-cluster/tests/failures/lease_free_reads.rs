//! Once the fleet finalizes `lease_free_reads`, a `Quorum` read confirms that
//! its broker still leads with one round of fences at the broker's own
//! generation, answered by a majority after the read took its value, instead
//! of trusting the lease. A leader cut off from its replicas cannot confirm,
//! whatever it believes; one cut off only from the control plane still can.
//!
//! Run with `cargo test -p felix-cluster --test failures lease_free_reads::`.
use std::time::Duration;

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use felix_controlplane_service::store::ControlPlaneStore;
use serial_test::serial;

const CACHE: &str = "profiles";
const LEASE_HELD: &str = "felix_broker_lease_held";

/// Three brokers, a `Quorum` cache on all three, and `features` finalized.
async fn start(features: &[&str]) -> Cluster {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new("orders", 1)],
        caches: vec![CacheSpec::quorum(CACHE, 1, 3)],
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

async fn owner(cluster: &Cluster) -> String {
    cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("cache shard owner")
}

/// Cut every broker off from the control plane, and wait until `leader` no
/// longer holds its lease.
async fn partition_control_plane(cluster: &Cluster, leader: &str) {
    for id in cluster.node_ids() {
        for fault in Fault::partition(Endpoint::node(&id), Endpoint::ControlPlane) {
            cluster.inject(&fault).await.expect("partition");
        }
    }
    wait_for_lapse(cluster, leader).await;
}

async fn wait_for_lapse(cluster: &Cluster, leader: &str) {
    felix_cluster::wait::until(Duration::from_secs(20), "the lease to lapse", || async {
        cluster.metric(leader, LEASE_HELD).await.ok().flatten() == Some(0.0)
    })
    .await
    .expect("the leader's lease lapses with the control plane gone");
}

/// **A leader cut off from its replicas cannot serve a `Quorum` read once a
/// new leader has taken writes.** It still believes it leads the cache shard
/// and still holds the old value; what stops it answering with it is that no
/// replica answers its round.
#[serial]
#[tokio::test]
async fn a_cut_off_leader_cannot_serve_a_quorum_read_once_a_new_leader_took_writes() {
    let cluster = start(ALL).await;
    let old = owner(&cluster).await;
    cluster
        .cache_put_via(&old, CACHE, "k", b"old")
        .await
        .expect("put while whole");
    assert_eq!(
        cluster
            .cache_get_via(&old, CACHE, "k")
            .await
            .expect("a read confirmed by its replicas"),
        Some(b"old".to_vec())
    );

    let others: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != old)
        .collect();
    cluster
        .inject(&Fault::Refuse {
            node: old.clone(),
            peers: others,
        })
        .await
        .expect("cut the old leader off from its replicas");
    for fault in Fault::partition(Endpoint::node(&old), Endpoint::ControlPlane) {
        cluster.inject(&fault).await.expect("partition");
    }
    wait_for_lapse(&cluster, &old).await;

    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        cluster
            .shard_owner_of("cache", CACHE, 0)
            .await
            .is_ok_and(|owner| owner != old)
    })
    .await
    .expect("the control plane promotes a replica");
    let new = owner(&cluster).await;
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the new leader to take a write",
        || async {
            cluster
                .cache_put_via(&new, CACHE, "k", b"new")
                .await
                .is_ok()
        },
    )
    .await
    .expect("the new leader takes writes");

    let read = cluster.cache_get_via(&old, CACHE, "k").await;
    assert!(
        !matches!(&read, Ok(Some(value)) if value == b"old"),
        "{old} answered a Quorum read with the value {new} has overwritten"
    );
    let refused = read.expect_err("the cut-off leader answered a Quorum read");
    let why = format!("{refused:#}");
    assert!(
        !why.contains("lease"),
        "the read was refused for the lease, which proves nothing here: {why}"
    );
    cluster.shutdown().await;
}

/// **With the control plane unreachable, `Quorum` reads keep working.** The
/// lease has lapsed everywhere; the replicas still answer the leader's round.
#[serial]
#[tokio::test]
async fn quorum_reads_continue_while_the_control_plane_is_partitioned() {
    let cluster = start(ALL).await;
    let leader = owner(&cluster).await;
    cluster
        .cache_put_via(&leader, CACHE, "k", b"v")
        .await
        .expect("put while whole");
    partition_control_plane(&cluster, &leader).await;

    for _ in 0..3 {
        assert_eq!(
            cluster
                .cache_get_via(&leader, CACHE, "k")
                .await
                .expect("a read its replicas confirm, lease or not"),
            Some(b"v".to_vec())
        );
    }
    cluster.shutdown().await;
}

/// **Until `lease_free_reads` is finalized, reads follow the lease**, as
/// before: the same partition refuses them.
#[serial]
#[tokio::test]
async fn without_the_fleet_feature_a_quorum_read_follows_the_lease() {
    let cluster = start(&["generation_start", "majority_ack"]).await;
    let leader = owner(&cluster).await;
    cluster
        .cache_put_via(&leader, CACHE, "k", b"v")
        .await
        .expect("put while whole");
    partition_control_plane(&cluster, &leader).await;

    let refused = cluster
        .cache_get_via(&leader, CACHE, "k")
        .await
        .expect_err("a read was served on a lapsed lease before the fleet finalized reads");
    let why = format!("{refused:#}");
    assert!(
        why.contains("lease"),
        "refused, but not for the lease: {why}"
    );
    cluster.shutdown().await;
}

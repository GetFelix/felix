//! A Raft member on a `periodic` broker counts an entry once it is written
//! and leaves the fsync to the broker's tick, as in a new production
//! cluster (`docs/per-shard-raft.md`, "Proposal: commit on a written
//! majority on relaxed brokers").
//!
//! Run with `cargo test -p felix-cluster --test failures written_commit::`.
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";
const PUBLISHES: usize = 20;

/// Summed over every broker. Unrecorded reads as zero.
async fn summed(cluster: &Cluster, name: &str) -> f64 {
    let mut total = 0.0;
    for id in cluster.node_ids() {
        total += cluster
            .metric(&id, name)
            .await
            .expect("scrape")
            .unwrap_or(0.0);
    }
    total
}

/// **A Raft member of a fresh periodic cluster does not fsync per commit.**
/// A member reads `raft_written_commit` when it opens, so one opened before
/// the fleet finalized it runs strict: one fsync per append on every member
/// (#3238). The tick here is an hour, so any fsync during the publishes is
/// one the append path started. `felix_storage_sync_batch_appends` is
/// recorded only for a log that fsyncs on commit.
#[serial]
#[tokio::test]
async fn a_periodic_raft_member_leaves_the_fsync_to_the_tick() {
    if !felix_cluster::suite_runs_on_raft() {
        eprintln!("skipped: a Raft-only check");
        return;
    }
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        broker_env: vec![
            (
                "FELIX_DURABLE_FSYNC_MODE".to_string(),
                "periodic".to_string(),
            ),
            (
                "FELIX_DURABLE_FSYNC_INTERVAL_MS".to_string(),
                "3600000".to_string(),
            ),
        ],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let strict_before = summed(&cluster, "felix_storage_sync_batch_appends_count").await;
    let syncs_before = summed(&cluster, "felix_storage_sync_total").await;
    for i in 0..PUBLISHES {
        cluster
            .publish_via(&owner, STREAM, format!("p{i}").into_bytes())
            .await
            .expect("publish");
    }
    let strict = summed(&cluster, "felix_storage_sync_batch_appends_count").await - strict_before;
    let syncs = summed(&cluster, "felix_storage_sync_total").await - syncs_before;
    assert_eq!(
        strict, 0.0,
        "{strict} fsyncs on commit for {PUBLISHES} publishes: a member opened strict"
    );
    // A strict trio takes about three per publish.
    assert!(
        syncs < PUBLISHES as f64,
        "{syncs} fsyncs for {PUBLISHES} publishes on an hour's tick"
    );
}

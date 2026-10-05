//! One consumer group across every shard of a stream, through one handle.
//!
//! A group is bound to one shard and served only by that shard's leader, so
//! consuming a multi-shard stream through a group used to mean enumerating the
//! shards, finding each owner, and routing every ack by hand (#610).
//! `ClusterClient::group_sharded` does that by following each shard's
//! `NotLeader`.
//!
//! Run with `cargo test -p felix-cluster --test queues sharded_group::`.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "jobs";
const GROUP: &str = "workers";
const SHARDS: u32 = 4;
const RECORDS: usize = 24;

fn sharded() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        ..Default::default()
    }
}

async fn cluster_client(cluster: &Cluster) -> Arc<felix_client::ClusterClient> {
    Arc::new(
        felix_cluster::client::connect_cluster(
            &cluster.broker_addrs(),
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("connect a cluster client"),
    )
}

/// Publish `RECORDS` keyed records, which the key hash spreads across shards.
async fn publish_spread(cluster: &Cluster) {
    let via = cluster.node_ids()[0].clone();
    for i in 0..RECORDS {
        let key = format!("key-{i:03}");
        cluster
            .publish_keyed_via_settled(
                &via,
                STREAM,
                key.as_bytes(),
                format!("job-{i}").into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish");
    }
}

/// **Every record, whichever broker leads its shard, and each finished once.**
/// Every first delivery is handed back, so each record is also redelivered —
/// and the redeliveries must come from more than one shard, which is the case
/// a single-shard group could never show.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn a_sharded_group_delivers_and_redelivers_across_shards() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");
    let owners = cluster.shard_owners().await.expect("shard owners");
    publish_spread(&cluster).await;

    let client = cluster_client(&cluster).await;
    let group = client
        .group_sharded(&cluster.tenant_id, &cluster.namespace, STREAM, GROUP)
        .await
        .expect("sharded group");
    assert_eq!(group.shards(), SHARDS);

    // payload -> shard it was finished on
    let mut finished: HashMap<String, u32> = HashMap::new();
    let mut handed_back: HashSet<(u32, u64)> = HashSet::new();
    let mut redelivered_from: HashSet<u32> = HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while finished.len() < RECORDS {
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {} of {RECORDS} records finished: {finished:?}",
            finished.len(),
        );
        let batch = group.poll(8).await.expect("poll");
        if batch.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        for claimed in batch {
            let payload = String::from_utf8_lossy(&claimed.record.payload).to_string();
            if !payload.starts_with("job-") {
                // The harness's own readiness probes share the stream.
                group.ack(&claimed).await.expect("ack a probe");
                continue;
            }
            let id = (claimed.shard, claimed.record.offset);
            if handed_back.insert(id) {
                group.nack(&claimed).await.expect("nack");
                continue;
            }
            assert!(
                claimed.record.attempts > 1,
                "{payload} came back without its attempt count rising",
            );
            redelivered_from.insert(claimed.shard);
            group.ack(&claimed).await.expect("ack");
            let first = finished.insert(payload.clone(), claimed.shard);
            assert!(first.is_none(), "{payload} was finished twice");
        }
    }

    assert!(
        redelivered_from.len() > 1,
        "redelivery was only exercised on shards {redelivered_from:?}",
    );
    let leaders: HashSet<&String> = owners.values().collect();
    assert!(
        leaders.len() > 1,
        "every shard landed on one broker, so no redirect was followed: {owners:?}",
    );
    // Finished is finished: nothing comes back.
    let leftover: Vec<_> = group
        .poll(RECORDS as u32)
        .await
        .expect("poll")
        .into_iter()
        .filter(|claimed| claimed.record.payload.starts_with(b"job-"))
        .collect();
    assert!(
        leftover.is_empty(),
        "finished records came back: {leftover:?}"
    );
    cluster.shutdown().await;
}

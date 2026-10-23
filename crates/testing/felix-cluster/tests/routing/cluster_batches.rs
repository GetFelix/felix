//! Batches through a `ClusterClient` go to each shard's leader: from the first
//! one, with or without an ack, and across a leader change.
//!
//! Run with `cargo test -p felix-cluster --test routing cluster_batches::`.
//! The brokers are the prebuilt `target/<profile>/felix-broker`; build it
//! first with `cargo build -p felix-broker-service --bin felix-broker`.
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "batched";
const SHARDS: u32 = 6;

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        ..Default::default()
    }
}

fn keys() -> Vec<String> {
    (0..24).map(|i| format!("customer-{i}")).collect()
}

/// `count` records for every key, tagged with a round so order is checkable.
fn records(round: usize, count: usize) -> Vec<(Bytes, Vec<u8>)> {
    keys()
        .iter()
        .flat_map(|key| {
            (0..count).map(move |n| {
                (
                    Bytes::from(key.clone().into_bytes()),
                    format!("{key}/{round:04}/{n}").into_bytes(),
                )
            })
        })
        .collect()
}

async fn connect_via_non_owner(cluster: &Cluster) -> felix_client::ClusterClient {
    let (_owner, non_owner) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("owner and non-owner");
    let node = cluster.node(&non_owner).expect("node");
    felix_cluster::client::connect_cluster(
        &[node.client_addr],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect a cluster client to a non-owner")
}

/// Forwards the brokers have done, summed.
async fn broker_forwards(cluster: &Cluster) -> f64 {
    let mut total = 0.0;
    for node in cluster.placeable_nodes().await.expect("nodes") {
        total += cluster
            .metric(&node, "felix_broker_forwards_total")
            .await
            .expect("metric")
            .unwrap_or(0.0);
    }
    total
}

/// Everything in the stream, per shard, as `(offset, payload)` in log order,
/// read from each shard's current owner.
async fn read_all(cluster: &Cluster) -> BTreeMap<u32, Vec<(u64, String)>> {
    let mut out = BTreeMap::new();
    for shard in 0..SHARDS {
        let owner = cluster
            .shard_owner_of("stream", STREAM, shard)
            .await
            .expect("owner");
        let (_client, mut sub) = cluster
            .replay_shard(&owner, STREAM, shard)
            .await
            .expect("replay");
        let mut rows = Vec::new();
        while let Ok(Ok(Some(event))) =
            tokio::time::timeout(Duration::from_secs(3), sub.next_event()).await
        {
            rows.push((
                event.offset.expect("a durable stream carries offsets"),
                String::from_utf8_lossy(&event.payload).to_string(),
            ));
        }
        out.insert(shard, rows);
    }
    out
}

fn payloads(all: &BTreeMap<u32, Vec<(u64, String)>>) -> Vec<&str> {
    all.values()
        .flat_map(|rows| rows.iter().map(|(_, payload)| payload.as_str()))
        .collect()
}

/// Offsets rise within each shard, and each key's records are in the order they
/// were sent.
fn assert_ordered(all: &BTreeMap<u32, Vec<(u64, String)>>) {
    for (shard, rows) in all {
        assert!(
            rows.windows(2).all(|w| w[0].0 < w[1].0),
            "shard {shard}: offsets do not rise: {rows:?}"
        );
        let mut last: BTreeMap<&str, &str> = BTreeMap::new();
        for (_, payload) in rows {
            let (key, rest) = payload.split_once('/').expect("tagged payload");
            if let Some(before) = last.insert(key, rest) {
                assert!(
                    before <= rest,
                    "shard {shard}, {key}: {before} after {rest}"
                );
            }
        }
    }
}

/// **The first batch is owner-direct, acked or not.** The client was pointed at
/// a broker that does not lead every shard; asking the cluster who does means no
/// batch is forwarded, which for fire-and-forget (no ack to learn from) is the
/// only way it can happen.
#[serial]
#[tokio::test]
async fn the_first_batches_go_to_the_leaders_with_or_without_an_ack() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let client = connect_via_non_owner(&cluster).await;
    let client_before = felix_client::publishes_forwarded();
    let brokers_before = broker_forwards(&cluster).await;

    let acked = client
        .publish_records(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            records(0, 2),
            AckMode::PerBatch,
        )
        .await;
    assert!(acked.is_ok(), "{acked:?}");
    let silent = client
        .publish_records(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            records(1, 2),
            AckMode::None,
        )
        .await;
    assert!(silent.is_ok(), "{silent:?}");
    client.finish().await.expect("flush");

    // Give the brokers' counters time to settle past the last write.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        felix_client::publishes_forwarded(),
        client_before,
        "an acked batch was forwarded"
    );
    assert_eq!(
        broker_forwards(&cluster).await,
        brokers_before,
        "a batch was forwarded by a broker that does not lead its shard"
    );
    let all = read_all(&cluster).await;
    assert_eq!(payloads(&all).len(), 2 * 2 * keys().len());
    assert_ordered(&all);

    cluster.shutdown().await;
}

/// Publish rounds of records until `rounds` are done, recording which records
/// the broker accepted. `publish` is one of the two `ClusterClient` calls.
async fn publish_rounds<F, Fut>(rounds: usize, publish: F) -> (BTreeSet<String>, usize)
where
    F: Fn(Vec<(Bytes, Vec<u8>)>) -> Fut,
    Fut: std::future::Future<Output = felix_client::BatchReport>,
{
    let mut accepted = BTreeSet::new();
    let mut failed = 0;
    for round in 0..rounds {
        let batch = records(round, 2);
        let report = publish(batch.clone()).await;
        let bad: BTreeSet<usize> = report.failed_records().into_iter().collect();
        for (i, (_, payload)) in batch.iter().enumerate() {
            if bad.contains(&i) {
                failed += 1;
            } else {
                accepted.insert(String::from_utf8(payload.clone()).unwrap());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (accepted, failed)
}

/// **A leader change mid-run loses nothing it acknowledged and writes nothing
/// twice.** At most once: a batch whose outcome was unknown is reported, not
/// re-sent, so the log has no duplicates and each shard's offsets and each
/// key's order hold.
#[serial]
#[tokio::test]
async fn a_leader_change_mid_run_duplicates_nothing() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let client = connect_via_non_owner(&cluster).await;

    let publishing = publish_rounds(40, |batch| {
        client.publish_records(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            batch,
            AckMode::PerBatch,
        )
    });
    let moving = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        cluster.move_shard(STREAM).await.expect("move shard 0")
    };
    let ((accepted, _failed), _new_owner) = tokio::join!(publishing, moving);

    let all = read_all(&cluster).await;
    let seen = payloads(&all);
    let unique: BTreeSet<&str> = seen.iter().copied().collect();
    assert_eq!(seen.len(), unique.len(), "a record was written twice");
    for payload in &accepted {
        assert!(
            unique.contains(payload.as_str()),
            "{payload} was acked but is missing"
        );
    }
    assert_ordered(&all);

    cluster.shutdown().await;
}

/// **At least once loses nothing it accepted across a leader change**, and
/// may duplicate.
#[serial]
#[tokio::test]
async fn at_least_once_loses_nothing_across_a_leader_change() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let client = connect_via_non_owner(&cluster).await;

    let publishing = publish_rounds(40, |batch| {
        client.publish_records_at_least_once(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            batch,
            AckMode::PerBatch,
        )
    });
    let moving = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        cluster.move_shard(STREAM).await.expect("move shard 0")
    };
    let ((accepted, _failed), _new_owner) = tokio::join!(publishing, moving);

    // Records the client gave up on are allowed: the move can outlast the
    // reconnect policy's attempts on a loaded runner, and what a client
    // reports as failed it never claimed to have stored. What it did accept
    // must be there.
    let all = read_all(&cluster).await;
    let unique: BTreeSet<&str> = payloads(&all).into_iter().collect();
    for payload in &accepted {
        assert!(unique.contains(payload.as_str()), "{payload} is missing");
    }

    cluster.shutdown().await;
}

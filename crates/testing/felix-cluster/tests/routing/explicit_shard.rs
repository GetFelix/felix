//! A publish that names its shard, across real brokers.
//!
//! Run with `cargo test -p felix-cluster --test routing explicit_shard::`.
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "districts";
const SHARDS: u32 = 4;

fn sharded() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        ..Default::default()
    }
}

/// Everything on one shard, read from its owner, without the harness probes.
async fn read_shard(cluster: &Cluster, owner: &str, shard: u32) -> Vec<String> {
    let Ok((_client, mut subscription)) = cluster.replay_shard(owner, STREAM, shard).await else {
        return Vec::new();
    };
    let mut seen = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await
    {
        let payload = String::from_utf8_lossy(&event.payload).to_string();
        if payload != "harness-probe" {
            seen.push(payload);
        }
    }
    seen
}

/// A shard past 0 and the broker that does not own it.
async fn shard_and_non_owner(cluster: &Cluster) -> (u32, String, String) {
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    for shard in 1..SHARDS {
        let owner = owners.get(&shard).expect("an owner per shard").clone();
        if let Some(node) = cluster.nodes.iter().find(|node| node.node_id != owner) {
            return (shard, owner, node.node_id.clone());
        }
    }
    panic!("no shard past 0 with a non-owner: {owners:?}");
}

async fn forwards(cluster: &Cluster, node: &str) -> f64 {
    cluster
        .metric(node, "felix_broker_forwards_total")
        .await
        .expect("scrape")
        .unwrap_or(0.0)
}

/// Retries while the entry broker's routing catches up: the publish is refused
/// with a retryable `shard_unavailable` until then, and writes nothing.
async fn until_written<F, Fut>(mut publish: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Option<u64>>>,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match publish().await {
            Ok(_) => return,
            Err(err) if Instant::now() < deadline => {
                tracing::debug!(error = %format!("{err:#}"), "named-shard publish not taken yet");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!("named-shard publish never taken: {err:#}"),
        }
    }
}

/// **A named-shard publish entering a broker that does not own the shard is
/// forwarded to the owner** and lands on the named shard, not on the shard
/// any key would pick.
#[serial]
#[tokio::test]
async fn a_named_shard_publish_is_forwarded_to_its_owner() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");
    let (shard, owner, entry) = shard_and_non_owner(&cluster).await;
    let node = cluster.node(&entry).expect("node");
    let client = felix_cluster::client::connect(
        node.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect to the non-owner");
    let publisher = client.publisher().await.expect("publisher");

    let before = forwards(&cluster, &entry).await;
    until_written(|| {
        publisher.publish_on_shard(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            shard,
            b"named".to_vec(),
            AckMode::PerMessage,
        )
    })
    .await;

    assert!(
        forwards(&cluster, &entry).await > before,
        "the non-owner did not forward the named-shard publish"
    );
    assert_eq!(read_shard(&cluster, &owner, shard).await, ["named"]);
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    for (other, other_owner) in &owners {
        if *other != shard {
            assert!(
                read_shard(&cluster, other_owner, *other).await.is_empty(),
                "shard {other} got a record named to shard {shard}"
            );
        }
    }
    cluster.shutdown().await;
}

/// **A `ClusterClient` sends a named-shard publish straight to the owner**,
/// learned the way a keyed publish learns it, so the broker it entered
/// through forwards nothing.
#[serial]
#[tokio::test]
async fn a_cluster_client_sends_a_named_shard_to_its_owner() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");
    let (shard, owner, entry) = shard_and_non_owner(&cluster).await;
    let node = cluster.node(&entry).expect("node");
    let client = felix_cluster::client::connect_cluster(
        &[node.client_addr],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect a cluster client to the non-owner");

    let before = forwards(&cluster, &entry).await;
    for i in 0..5 {
        until_written(|| {
            client.publish_on_shard(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                shard,
                format!("m{i}").into_bytes(),
                AckMode::PerMessage,
            )
        })
        .await;
    }

    assert_eq!(
        forwards(&cluster, &entry).await,
        before,
        "the entry broker forwarded a publish the client could send to the owner"
    );
    assert_eq!(
        read_shard(&cluster, &owner, shard).await,
        ["m0", "m1", "m2", "m3", "m4"]
    );
    cluster.shutdown().await;
}

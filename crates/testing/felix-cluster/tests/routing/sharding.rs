//! A stream whose shards live on different brokers.
//!
//! Until routing keys landed (#240), the wire carried no key and every record
//! of every stream went to shard 0 — so a stream configured with four shards
//! had three that never received anything, and every shard-related test in
//! the project was a single-shard test.
//!
//! These are the tests that could not previously be written.
//!
//! Run with `cargo test -p felix-cluster --test routing sharding::`.
use std::collections::HashSet;
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";
const SHARDS: u32 = 4;

fn sharded() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        ..Default::default()
    }
}

/// Everything a test published on one shard, read from the broker that owns it.
///
/// The harness publishes a `harness-probe` per broker at startup to prove the
/// cluster can serve before a test begins. Those go to shard 0 like any unkeyed
/// publish, and counting them would make shard 0 look occupied in every test
/// here.
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

/// **A stream's shards are placed across brokers.** The precondition for
/// everything else here, and something no existing test could observe: with one
/// reachable shard, a stream had one owner however many shards it declared.
#[serial]
#[tokio::test]
async fn a_sharded_stream_is_placed_across_brokers() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");

    let owners = cluster
        .shard_owners_for(STREAM)
        .await
        .expect("shard owners");
    assert_eq!(
        owners.len(),
        SHARDS as usize,
        "every shard should have been placed: {owners:?}",
    );
    let distinct: HashSet<&String> = owners.values().collect();
    assert!(
        distinct.len() > 1,
        "all {SHARDS} shards landed on one broker, so nothing here tests sharding: {owners:?}",
    );

    cluster.shutdown().await;
}

/// **A keyed publish takes the binary encoding.**
///
/// The key used to force the JSON framing — the binary layouts had nowhere to
/// put one — and a perf session measured that fallback at roughly 30% of
/// throughput (#549). Without this assertion the routing tests below would go on
/// passing over JSON and the regression would be invisible.
#[serial]
#[tokio::test]
async fn a_keyed_publish_negotiates_the_binary_frame() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");
    let client = felix_cluster::client::connect(
        cluster.nodes[0].client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect");
    let publisher = client.publisher().await.expect("publisher");

    let flags = publisher.negotiated_server_flags();
    assert!(
        felix_wire::supports(flags, felix_wire::FLAG_BINARY_PUBLISH_KEYED),
        "the broker did not advertise the keyed binary frame (flags {flags:#06x}), so every \
         keyed publish here silently falls back to JSON",
    );

    cluster.shutdown().await;
}

/// **Different keys reach different shards.** The whole point of a routing key:
/// without it every record went to shard 0 and a stream could not scale past
/// one broker.
#[serial]
#[tokio::test]
async fn keys_spread_records_across_shards() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    let ingress = cluster.nodes[0].node_id.clone();

    // Enough distinct keys that landing on one shard would be a routing bug
    // rather than luck.
    for index in 0..40u32 {
        // `_settled`: at startup a broker can hold the placement before the
        // owner has applied it, and a forwarded publish then fails with
        // "redirected to generation 0". Only the first publish pays anything.
        cluster
            .publish_keyed_via_settled(
                &ingress,
                STREAM,
                format!("customer-{index}").as_bytes(),
                format!("record-{index}").into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("keyed publish");
    }

    let mut occupied = 0;
    let mut total = 0;
    for (shard, owner) in &owners {
        let records = read_shard(&cluster, owner, *shard).await;
        if !records.is_empty() {
            occupied += 1;
        }
        total += records.len();
    }

    assert_eq!(total, 40, "records were lost between the key and the log");
    assert!(
        occupied > 1,
        "all 40 records landed on one shard; the routing key is not reaching shard_for",
    );

    cluster.shutdown().await;
}

/// **An unacked keyed publish reaches its shard too.**
///
/// `AckMode::None` takes the plain `FLAG_BINARY_PUBLISH_BATCH` frame rather than
/// the acked one, so it is a second decode path with its own key handling — and
/// it is the path `felix-loadgen` drives, which makes it the one the perf rig
/// measures. One node, so nothing here waits on a forward settling: every shard
/// is local and the only question is whether the key survived the frame.
#[serial]
#[tokio::test]
async fn an_unacked_keyed_publish_reaches_its_shard() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.node_ids().first().expect("a node").clone();

    let client = felix_cluster::client::connect(
        cluster.nodes[0].client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect");
    let publisher = client.publisher().await.expect("publisher");

    for index in 0..40u32 {
        publisher
            .publish_keyed(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                bytes::Bytes::from(format!("customer-{index}")),
                format!("record-{index}").into_bytes(),
                felix_wire::AckMode::None,
            )
            .await
            .expect("unacked keyed publish");
    }
    // Nothing acknowledged these, so the only way to know they landed is to
    // wait for the flush behind them.
    publisher.finish().await.expect("flush publisher");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut occupied = 0;
    let mut total = 0;
    for shard in 0..SHARDS {
        let records = read_shard(&cluster, &owner, shard).await;
        if !records.is_empty() {
            occupied += 1;
        }
        total += records.len();
    }

    assert_eq!(total, 40, "records were lost between the key and the log");
    assert!(
        occupied > 1,
        "all 40 unacked records landed on one shard; the key is not reaching \
         shard_for on the plain binary publish frame",
    );

    cluster.shutdown().await;
}

/// **The same key always lands on the same shard.** Ordering is per key, and a
/// key that moved between shards would break it — the records would be on
/// different brokers with no order between them.
#[serial]
#[tokio::test]
async fn one_key_always_lands_on_one_shard() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    let ingress = cluster.nodes[0].node_id.clone();

    for index in 0..8u32 {
        cluster
            .publish_keyed_via_settled(
                &ingress,
                STREAM,
                b"customer-stable",
                format!("record-{index}").into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("keyed publish");
    }

    let mut holding = Vec::new();
    for (shard, owner) in &owners {
        let records = read_shard(&cluster, owner, *shard).await;
        if !records.is_empty() {
            holding.push((*shard, records));
        }
    }

    assert_eq!(
        holding.len(),
        1,
        "one key spread across {} shards: {holding:?}",
        holding.len(),
    );
    let (_shard, records) = &holding[0];
    assert_eq!(records.len(), 8, "records for one key were lost");
    // Per-key ordering is the guarantee sharding preserves.
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record, &format!("record-{index}"), "records reordered");
    }

    cluster.shutdown().await;
}

/// **A keyed publish reaches the shard's owner from any broker.** The record is
/// forwarded to whichever broker holds the shard the key resolved to, which is
/// usually not the one the client is talking to.
#[serial]
#[tokio::test]
async fn a_keyed_publish_is_forwarded_to_the_shards_owner() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");

    // Publish every key through one broker, then check each shard's records
    // arrived at the broker that owns that shard rather than at the ingress.
    let ingress = cluster.nodes[0].node_id.clone();
    for index in 0..20u32 {
        cluster
            .publish_keyed_via_settled(
                &ingress,
                STREAM,
                format!("k{index}").as_bytes(),
                format!("v{index}").into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("keyed publish");
    }

    let mut found = 0;
    for (shard, owner) in &owners {
        found += read_shard(&cluster, owner, *shard).await.len();
    }
    assert_eq!(
        found, 20,
        "records did not reach the owners of the shards their keys resolved to",
    );

    cluster.shutdown().await;
}

/// A publish with no key still lands on shard 0, so a client that predates
/// routing keys behaves exactly as it did.
#[serial]
#[tokio::test]
async fn an_unkeyed_publish_still_lands_on_shard_zero() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    let shard_zero_owner = owners.get(&0).expect("shard 0 has an owner").clone();

    cluster
        .publish_via(&shard_zero_owner, STREAM, b"unkeyed".to_vec())
        .await
        .expect("publish without a key");

    assert_eq!(
        read_shard(&cluster, &shard_zero_owner, 0).await,
        vec!["unkeyed".to_string()],
    );

    cluster.shutdown().await;
}

//! A publish should stop being forwarded once the client learns the owner.
//!
//! Felix routes subscribes on the client and publishes on the server: a publish
//! for a shard the entry broker does not own is forwarded to the owner and
//! acknowledged once written. Correct, but it costs a decrypt at the entry
//! broker, a re-encrypt to the owner and a decrypt there -- measured at roughly
//! half the throughput per core (#536).
//!
//! Run with `cargo test -p felix-cluster --test routing publish_routing::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "orders";

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    }
}

/// **A client that publishes through a non-owner stops forwarding.**
///
/// The first publish is forwarded and its ack names the owner; every publish
/// after it goes straight there. Asserted on `publishes_forwarded()`, which the
/// client increments for each ack that came back marked as forwarded -- so this
/// measures the thing the issue is about rather than a proxy for it.
#[serial]
#[tokio::test]
async fn a_forwarded_publish_teaches_the_client_where_to_send_the_next_one() {
    let cluster = Cluster::start(config()).await.expect("start cluster");

    // Deliberately the wrong broker: one that does not own the shard, so the
    // first publish has to be forwarded.
    let (_owner, non_owner) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("owner and non-owner");
    let node = cluster.node(&non_owner).expect("node");

    let client = felix_cluster::client::connect_cluster(
        &[node.client_addr],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect a cluster client to the non-owner");

    let before = felix_client::publishes_forwarded();
    client
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"first".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect("first publish");
    let after_first = felix_client::publishes_forwarded();
    assert!(
        after_first > before,
        "publishing through a non-owner should have been forwarded ({before} -> {after_first})",
    );

    // The owner is learned from that ack, so the rest should go direct.
    for i in 0..5 {
        client
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("m{i}").into_bytes(),
                AckMode::PerMessage,
            )
            .await
            .expect("publish after learning the owner");
    }
    let after_rest = felix_client::publishes_forwarded();
    assert_eq!(
        after_rest, after_first,
        "publishes after the owner was learned were still forwarded \
         ({after_first} -> {after_rest})",
    );

    // And every record is readable, so routing did not lose any.
    let owner = cluster.owner(STREAM).await.expect("owner");
    let (_c, mut sub) = cluster.replay_on(&owner, STREAM).await.expect("replay");
    let mut seen = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), sub.next_event()).await
    {
        seen.push(String::from_utf8_lossy(&event.payload).to_string());
    }
    assert!(seen.contains(&"first".to_string()), "{seen:?}");
    for i in 0..5 {
        assert!(
            seen.contains(&format!("m{i}")),
            "m{i} missing from {seen:?}"
        );
    }

    cluster.shutdown().await;
}

/// **Keyed publishes across a multi-shard stream stop forwarding too.**
///
/// A key's shard is a hash the client now computes with the same function the
/// broker routes with, so each shard's owner gets its own cache entry. Twelve
/// shards over three brokers means most keys start off forwarded; once every
/// shard in play has been learned, none are.
#[serial]
#[tokio::test]
async fn keyed_publishes_learn_each_shard_owner() {
    const KEYED: &str = "keyed";
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(KEYED, 12)],
        ..Default::default()
    })
    .await
    .expect("start cluster");

    let (_owner, non_owner) = cluster
        .owner_and_non_owner(KEYED)
        .await
        .expect("owner and non-owner");
    let node = cluster.node(&non_owner).expect("node");
    let client = felix_cluster::client::connect_cluster(
        &[node.client_addr],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect");

    let keys: Vec<String> = (0..12).map(|i| format!("customer-{i}")).collect();
    let publish = async |key: &str, body: String| {
        client
            .publish_keyed(
                &cluster.tenant_id,
                &cluster.namespace,
                KEYED,
                body.into_bytes(),
                bytes::Bytes::from(key.to_string().into_bytes()),
                AckMode::PerMessage,
            )
            .await
    };

    // First pass: every shard is cold, so these are the ones that teach.
    for key in &keys {
        publish(key, format!("warm-{key}")).await.expect("warm");
    }
    let after_warm = felix_client::publishes_forwarded();

    // Second pass: the same keys, so the same shards, all now known.
    for key in &keys {
        publish(key, format!("hot-{key}")).await.expect("hot");
    }
    let after_hot = felix_client::publishes_forwarded();

    assert_eq!(
        after_hot, after_warm,
        "keyed publishes were still forwarded after their shard owners were \
         learned ({after_warm} -> {after_hot})",
    );

    cluster.shutdown().await;
}

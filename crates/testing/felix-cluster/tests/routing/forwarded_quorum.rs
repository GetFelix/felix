//! Every broker forwarding durable `Quorum` publishes to every other at once.
//!
//! An owner answers a forward only after its quorum wait, and that wait needs
//! its replication to the broker that forwarded. Replication queued behind
//! those forwards on a shared peer stream deadlocked the cluster until every
//! quorum timed out (#1458).
//!
//! Run with `cargo test -p felix-cluster --test routing forwarded_quorum::`.
//! The brokers are the prebuilt `target/<profile>/felix-broker`; build it
//! first with `cargo build -p felix-broker-service --bin felix-broker`.
use std::time::Duration;

use bytes::Bytes;
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "orders";
const SHARDS: u32 = 6;
/// Publishers per broker, each its own client. Several times the peer
/// streams a connection has, so forwards occupy all of them.
const PUBLISHERS: usize = 16;
const PER_PUBLISHER: usize = 25;

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_quorum_publishes_from_every_broker_do_not_time_out() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, SHARDS, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");

    // Every broker routes over every shard before the load starts, so a
    // failure below is the load's and not routing still converging.
    for node in cluster.node_ids() {
        for shard_key in 0..SHARDS * 4 {
            cluster
                .publish_keyed_via_settled(
                    &node,
                    STREAM,
                    format!("warm-{shard_key}").as_bytes(),
                    b"warm".to_vec(),
                    Duration::from_secs(30),
                )
                .await
                .expect("routing settles");
        }
    }

    let mut publishers = Vec::new();
    for node in cluster.node_ids() {
        for publisher in 0..PUBLISHERS {
            let client = cluster.client_on(&node).await.expect("connect");
            let tenant = cluster.tenant_id.clone();
            let namespace = cluster.namespace.clone();
            let node = node.clone();
            publishers.push(tokio::spawn(async move {
                let handle = client.publisher().await.expect("open publisher");
                for n in 0..PER_PUBLISHER {
                    // A fresh key each time spreads the publishes over every
                    // shard, and most of them land on another broker's.
                    let key = format!("{node}/{publisher}/{n}");
                    handle
                        .publish_keyed(
                            &tenant,
                            &namespace,
                            STREAM,
                            Bytes::from(key.clone().into_bytes()),
                            key.clone().into_bytes(),
                            AckMode::PerMessage,
                        )
                        .await
                        .unwrap_or_else(|err| panic!("publish {key} via {node}: {err:#}"));
                }
            }));
        }
    }
    tokio::time::timeout(Duration::from_secs(120), async {
        for publisher in publishers {
            publisher.await.expect("publisher");
        }
    })
    .await
    .expect("forwarded quorum publishes stalled");

    for node in cluster.node_ids() {
        let forwarded = cluster
            .metric(&node, "felix_broker_forwards_total")
            .await
            .expect("metrics");
        assert!(
            forwarded.is_some_and(|count| count > 0.0),
            "{node} forwarded nothing, so this did not test forwarding",
        );
        let failed = cluster
            .metric(&node, "felix_broker_publish_quorum_failed_total")
            .await
            .expect("metrics");
        assert_eq!(
            failed.unwrap_or(0.0),
            0.0,
            "{node} timed out quorum waits under forwarded load",
        );
    }

    cluster.shutdown().await;
}

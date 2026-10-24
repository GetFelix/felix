//! A durable `Quorum` shard's quorum mark moves when a follower answers, not
//! when the replication tick next fires (#1479).
//!
//! Run with `cargo test -p felix-cluster --test failures quorum_mark_wake::`.
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_common::fleet;
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "orders";
const SHARDS: u32 = 4;
const CONNECTIONS: usize = 8;
const PUBLISHERS_PER_CONNECTION: usize = 8;
const PAYLOAD: usize = 256;
const LOAD: Duration = Duration::from_secs(25);
/// The replication tick. A wake the mark misses costs about this long.
const TICK_MS: u64 = 30_000;
/// A healthy ack takes milliseconds; this leaves room for a slow runner.
const LIMIT: Duration = Duration::from_secs(10);

/// **A busy follower's late answer moves the quorum mark at once.** The tick is
/// thirty seconds, so a publish whose mark waits for it is acknowledged far
/// past the limit. Every publisher keeps a window of publishes open, so no new
/// append arrives to start a pass in place of the missed wake.
#[serial]
#[tokio::test]
async fn a_busy_followers_answer_moves_the_mark_without_the_tick() {
    let env = |key: &str, value: &str| (key.to_string(), value.to_string());
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, SHARDS, 3)],
        sync_interval_ms: TICK_MS,
        broker_env: vec![
            env("FELIX_DURABLE_FSYNC_MODE", "on_commit"),
            env("FELIX_ACK_ON_COMMIT", "1"),
        ],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");

    // A new cluster finalizes every feature but the opt-in one itself.
    let expected = (fleet::IMPLEMENTED.len() - 1) as f64;
    for id in cluster.node_ids() {
        felix_cluster::wait::until(
            Duration::from_secs(90),
            "the new cluster's fleet features to turn on",
            || {
                let id = id.clone();
                let cluster = &cluster;
                async move {
                    cluster
                        .metric(&id, "felix_broker_fleet_feature_enabled")
                        .await
                        .ok()
                        .flatten()
                        == Some(expected)
                }
            },
        )
        .await
        .expect("every broker enables the features a new cluster finalizes");
    }

    let mut clients = Vec::new();
    for _ in 0..CONNECTIONS {
        clients.push(Arc::new(
            felix_cluster::client::connect_cluster(
                &cluster.broker_addrs(),
                &cluster.tenant_id,
                &cluster.client_token(),
            )
            .await
            .expect("connect"),
        ));
    }

    let stop = Instant::now() + LOAD;
    let mut tasks = Vec::new();
    for (c, client) in clients.iter().enumerate() {
        for p in 0..PUBLISHERS_PER_CONNECTION {
            let client = Arc::clone(client);
            let tenant = cluster.tenant_id.clone();
            let namespace = cluster.namespace.clone();
            let shard = ((c * PUBLISHERS_PER_CONNECTION + p) as u32) % SHARDS;
            tasks.push(tokio::spawn(async move {
                let mut latencies = Vec::new();
                while Instant::now() < stop {
                    let sent = Instant::now();
                    // Bounded so a stalled ack is reported rather than hanging.
                    let acked = tokio::time::timeout(
                        Duration::from_secs(60),
                        client.publish_on_shard(
                            &tenant,
                            &namespace,
                            STREAM,
                            shard,
                            vec![b'x'; PAYLOAD],
                            AckMode::PerMessage,
                        ),
                    )
                    .await;
                    let latency = sent.elapsed();
                    match acked {
                        Ok(Ok(_)) => latencies.push(latency),
                        Ok(Err(err)) => panic!("publish failed after {latency:?}: {err:#}"),
                        Err(_) => panic!("publish unacknowledged after {latency:?}"),
                    }
                }
                latencies
            }));
        }
    }
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.expect("publisher"));
    }
    cluster.shutdown().await;

    assert!(!latencies.is_empty(), "no publish was acknowledged");
    latencies.sort();
    let max = *latencies.last().unwrap();
    let p99 = latencies[(latencies.len() * 99 / 100).min(latencies.len() - 1)];
    assert!(
        max < LIMIT,
        "an ack took {max:?} (p99 {p99:?}, {} acks) with the replication tick at {TICK_MS} ms: \
         the quorum mark waited for the tick (#1479)",
        latencies.len()
    );
}

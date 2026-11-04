//! Record headers across a forward and a failover.
//!
//! Run with `cargo test -p felix-cluster --test failures record_headers::`. The
//! harness runs a prebuilt `target/<profile>/felix-broker`; build it first with
//! `cargo build -p felix-broker-service --bin felix-broker`.
use std::time::Duration;

use felix_client::RecordHeaders;
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

fn headers(trace: &str) -> RecordHeaders {
    RecordHeaders {
        key: Some(bytes::Bytes::from(format!("key-{trace}"))),
        headers: vec![("trace".to_string(), bytes::Bytes::from(trace.to_string()))],
    }
}

/// Publish one headered record through `node`, retried while the broker has not
/// yet told clients it accepts headers: the finalize reaches each broker on its
/// own schedule.
async fn publish_headered(cluster: &Cluster, node: &str, trace: &str) -> u64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let attempt = async {
            let client = cluster.client_on(node).await?;
            let publisher = client.publisher().await?;
            publisher
                .publish_with_headers(
                    &cluster.tenant_id,
                    &cluster.namespace,
                    STREAM,
                    headers(trace),
                    trace.as_bytes().to_vec(),
                    felix_wire::AckMode::PerMessage,
                )
                .await
        }
        .await;
        match attempt {
            Ok(Some(offset)) => return offset,
            Ok(None) => panic!("{trace} was acked without an offset"),
            Err(err) if std::time::Instant::now() >= deadline => {
                panic!("{node} never took {trace}: {err:#}")
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

/// What `node` replays from the start, with each record's key and headers,
/// once it holds `wanted` records. The harness's startup `harness-probe`
/// records are left out: they are not part of what this test published.
async fn replay_headers(
    cluster: &Cluster,
    node: &str,
    wanted: usize,
) -> Vec<(u64, String, Option<RecordHeaders>)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut held = Vec::new();
        if let Ok((_client, mut subscription)) = cluster.replay_on(node, STREAM).await {
            while let Ok(Ok(Some(event))) =
                tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await
            {
                if event.payload.as_ref() == b"harness-probe" {
                    continue;
                }
                held.push((
                    event
                        .offset
                        .expect("a durable stream's events carry offsets"),
                    String::from_utf8_lossy(&event.payload).into_owned(),
                    event.headers,
                ));
            }
        }
        if held.len() >= wanted || std::time::Instant::now() >= deadline {
            return held;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// **A record's key and headers outlive the leader that stored them.** With
/// `record_headers` finalized, headered records published through a broker
/// that does not own the shard are forwarded to the owner and replicated. After
/// the owner is killed, the promoted replica delivers the same headers.
#[serial]
#[tokio::test]
async fn headers_published_through_a_forward_survive_the_leader() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(&["record_headers"], Duration::from_secs(30))
        .await
        .expect("finalize record_headers");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let ingress = cluster
        .node_ids()
        .into_iter()
        .find(|node| *node != leader)
        .expect("a broker that does not own the shard");

    let mut published = Vec::new();
    for trace in ["one", "two", "three"] {
        let offset = publish_headered(&cluster, &ingress, trace).await;
        published.push((offset, trace.to_string(), Some(headers(trace))));
    }
    let forwarded = cluster
        .metric(&ingress, "felix_broker_forwards_total")
        .await
        .expect("metrics");
    assert!(
        forwarded.is_some_and(|count| count > 0.0),
        "{ingress} forwarded nothing, so this did not test forwarding",
    );

    // Same blocks on the leader before the kill.
    let on_leader = replay_headers(&cluster, &leader, published.len()).await;
    assert_eq!(on_leader, published, "the leader delivers what was sent");

    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the records to reach the followers",
        || async {
            cluster
                .metric(&leader, "felix_broker_replication_lag_records")
                .await
                .ok()
                .flatten()
                == Some(0.0)
        },
    )
    .await
    .expect("replication should reach the followers");

    cluster.kill_node(&leader).expect("kill the leader");
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        cluster
            .owner(STREAM)
            .await
            .is_ok_and(|owner| owner != leader)
    })
    .await
    .expect("a replica should have been promoted");
    let promoted = cluster.owner(STREAM).await.expect("owner");

    let on_promoted = replay_headers(&cluster, &promoted, published.len()).await;
    assert_eq!(
        on_promoted, published,
        "the promoted replica delivers the same keys and headers"
    );
    cluster.shutdown().await;
}

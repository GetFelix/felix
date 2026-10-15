//! A subscription through a `ClusterClient` follows its shard when the shard
//! moves, rather than ending.
//!
//! Run with `cargo test -p felix-cluster --test routing subscriptions_follow::`.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;
use tokio::sync::Mutex;

const STREAM: &str = "orders";

/// **Every record once, in order, across the switch-over.** A subscriber reads
/// a durable stream from its start while a publisher keeps writing and the
/// shard moves to another broker. It sees each offset exactly once and in
/// order, and every acknowledged record, on either side of the move.
#[serial]
#[tokio::test]
async fn a_subscription_follows_its_shard_to_the_new_owner() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");
    let addr = |node: &str| cluster.node(node).expect("node").client_addr;

    for i in 0..20 {
        cluster
            .publish_keyed_via_settled(
                &owner,
                STREAM,
                b"k",
                format!("before-{i}").into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish before the move");
    }

    // Subscribed on the owner, so the move is what the subscription has to
    // survive.
    let reader = Arc::new(
        felix_cluster::client::connect_cluster(
            &[addr(&owner)],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("reader"),
    );
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe");

    // Keeps publishing through the other broker, which forwards to whichever
    // broker leads, for the whole move.
    let writer = felix_cluster::client::connect_cluster(
        &[addr(&other)],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("writer");
    let acknowledged: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let publisher = {
        let (acknowledged, stop) = (Arc::clone(&acknowledged), Arc::clone(&stop));
        let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
        tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let payload = format!("during-{i}").into_bytes();
                i += 1;
                // A publish refused while the shard is moving is simply not
                // acknowledged; only acknowledged records are owed.
                if writer
                    .publish(
                        &tenant,
                        &namespace,
                        STREAM,
                        payload.clone(),
                        felix_wire::AckMode::PerMessage,
                    )
                    .await
                    .is_ok()
                {
                    acknowledged.lock().await.push(payload);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(300)).await;
    cluster.drain_node(&owner).await.expect("drain");
    felix_cluster::wait::until(Duration::from_secs(60), "the shard to move", || async {
        cluster.place_shards().await;
        cluster.owner(STREAM).await.is_ok_and(|now| now == other)
    })
    .await
    .expect("move");
    // Some traffic after the cut-over too, then stop.
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    publisher.await.expect("publisher");
    let mut owed: Vec<Vec<u8>> = (0..20)
        .map(|i| format!("before-{i}").into_bytes())
        .collect();
    owed.extend(acknowledged.lock().await.iter().cloned());
    assert!(
        owed.len() > 20,
        "nothing was acknowledged during the move, so it proves nothing"
    );

    let mut offsets = Vec::new();
    let mut payloads = Vec::new();
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    while !owed.iter().all(|payload| payloads.contains(payload)) {
        match tokio::time::timeout_at(deadline, subscription.next_event()).await {
            Ok(Ok(Some(event))) => {
                offsets.push(event.offset.expect("a durable stream carries offsets"));
                payloads.push(event.payload.to_vec());
            }
            Ok(Ok(None)) => panic!(
                "the subscription ended after {} records instead of following the shard",
                payloads.len()
            ),
            Ok(Err(err)) => panic!("the subscription failed: {err:#}"),
            Err(_) => panic!(
                "timed out with {} of {} acknowledged records",
                owed.iter().filter(|p| payloads.contains(p)).count(),
                owed.len()
            ),
        }
    }

    let expected: Vec<u64> = (0..offsets.len() as u64).collect();
    assert_eq!(
        offsets, expected,
        "every offset once and in order across the move"
    );
    let mut unique = payloads.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), payloads.len(), "a record arrived twice");
    cluster.shutdown().await;
}

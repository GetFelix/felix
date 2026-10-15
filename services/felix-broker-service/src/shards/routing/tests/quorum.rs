//! The quorum wait, answered by the ingress router this broker serves from.
use std::time::Duration;

use felix_replication::quorum::{QuorumMarks, await_quorum};

use crate::shards::{ShardKey, ShardKind};

const QUICK: Duration = Duration::from_millis(200);

fn key(stream: &str) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

/// A broker leading `orders` at `generation` with `replicas`, and a
/// publish's outcome on it.
struct Leading {
    _dir: tempfile::TempDir,
    handle: felix_broker::StreamHandle,
    outcome: felix_broker::PublishOutcome,
    ingress: crate::shards::routing::IngressRouter,
}

async fn leading(replicas: &[&str], successor: Option<&str>, generation: u64) -> Leading {
    use std::collections::HashMap;
    use std::sync::Arc;

    use felix_router::{NodeRef, RegionRouter, ShardRouter};

    use crate::shards::routing::{IngressRouter, routing_table_from};
    use crate::shards::watch::ShardAssignment;

    let dir = tempfile::tempdir().expect("tempdir");
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..Default::default()
        },
    )
    .expect("storage");
    let broker = felix_broker::Broker::new(felix_storage::EphemeralCache::new().into())
        .with_durable_storage(storage);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            felix_broker::StreamMetadata {
                durable: true,
                consistency: felix_broker::ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("stream");

    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let assignments: HashMap<ShardKey, ShardAssignment> = [(
        key("orders"),
        ShardAssignment {
            key: key("orders"),
            leader: "broker-a".to_string(),
            replicas: replicas.iter().map(|r| r.to_string()).collect(),
            generation,
            state: "active".to_string(),
            successor: successor.map(str::to_string),
            routing: Default::default(),
        },
    )]
    .into_iter()
    .collect();
    let nodes: HashMap<String, NodeRef> = std::iter::once("broker-a")
        .chain(replicas.iter().copied())
        .enumerate()
        .map(|(i, node)| {
            (
                node.to_string(),
                NodeRef {
                    node_id: node.to_string(),
                    advertise_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 7001 + i as u16)),
                    region: "us-west-2".to_string(),
                    live: true,
                },
            )
        })
        .collect();
    let ingress = IngressRouter::new(Arc::clone(&router), Arc::default());
    ingress.publish(
        routing_table_from(&assignments, &nodes),
        &nodes,
        [(key("orders"), generation)].into_iter().collect(),
    );

    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let outcome = broker
        .publish_batch_with_outcome(&handle, &[bytes::Bytes::from_static(b"one")], None)
        .await
        .expect("publish");
    Leading {
        _dir: dir,
        handle,
        outcome,
        ingress,
    }
}

/// A `Quorum` stream placed with one replica: the leader alone is the
/// majority. Nothing ships for it, so no mark is ever published, and the wait
/// must not read that silence as a lost leadership.
#[tokio::test]
async fn an_unreplicated_quorum_shard_is_acknowledged_by_its_leader() {
    let shard = leading(&[], None, 4).await;
    let marks = QuorumMarks::new();

    await_quorum(
        &shard.handle,
        Some(&key("orders")),
        &shard.outcome,
        Some(&marks),
        Some(&shard.ingress),
        QUICK,
    )
    .await
    .expect("the leader is the whole replica set, and it has the record");
}

/// **A publish just after a move stages its destination is not refused.**
/// Staging starts a generation under the same leader, and until the first
/// replication pass at it reports there is no mark to wait on. That is not a
/// leadership change; the publish waits for the mark.
#[tokio::test]
async fn a_quorum_publish_before_a_new_generations_first_mark_waits_for_it() {
    let shard = leading(&["broker-b"], Some("broker-b"), 5).await;
    let marks = std::sync::Arc::new(QuorumMarks::new());
    let (_, last) = shard.outcome.offsets.expect("a durable stream has offsets");

    let publisher = {
        let marks = std::sync::Arc::clone(&marks);
        tokio::spawn(async move {
            await_quorum(
                &shard.handle,
                Some(&key("orders")),
                &shard.outcome,
                Some(&marks),
                Some(&shard.ingress),
                Duration::from_secs(5),
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    marks.publish(&key("orders"), 5, last + 1);

    publisher
        .await
        .expect("join")
        .expect("the leader never stopped leading; the first mark acknowledges the publish");
}

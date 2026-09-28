//! The quorum wait, answered by the ingress router this broker serves from.
use std::time::Duration;

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

/// A `Quorum` stream placed with one replica: the leader alone is the
/// majority. Nothing ships for it, so no mark is ever published, and the wait
/// must not read that silence as a lost leadership.
#[tokio::test]
async fn an_unreplicated_quorum_shard_is_acknowledged_by_its_leader() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use felix_router::{NodeRef, RegionRouter, ShardRouter};

    use crate::shards::routing::{IngressRouter, routing_table_from};
    use crate::shards::watch::ShardAssignment;
    use felix_replication::quorum::{QuorumMarks, await_quorum};

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
            replicas: Vec::new(),
            generation: 4,
            state: "active".to_string(),
            successor: None,
        },
    )]
    .into_iter()
    .collect();
    let nodes: HashMap<String, NodeRef> = [(
        "broker-a".to_string(),
        NodeRef {
            node_id: "broker-a".to_string(),
            advertise_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 7001)),
            region: "us-west-2".to_string(),
            live: true,
        },
    )]
    .into_iter()
    .collect();
    let ingress = IngressRouter::new(Arc::clone(&router), Arc::default());
    ingress.publish(
        routing_table_from(&assignments, &nodes),
        &nodes,
        [(key("orders"), 4)].into_iter().collect(),
    );

    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let outcome = broker
        .publish_batch_with_outcome(&handle, &[bytes::Bytes::from_static(b"one")])
        .await
        .expect("publish");
    let marks = QuorumMarks::new();

    await_quorum(
        &handle,
        Some(&key("orders")),
        &outcome,
        Some(&marks),
        Some(&ingress),
        QUICK,
    )
    .await
    .expect("the leader is the whole replica set, and it has the record");
}

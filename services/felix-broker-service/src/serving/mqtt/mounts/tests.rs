use felix_common::random::HashMap;
use std::net::SocketAddr;

use felix_broker::StreamMetadata;
use felix_router::{NodeRef, RegionRouter, ShardRouter};
use felix_storage::EphemeralCache;

use super::*;
use crate::shards::routing::routing_table_from;
use crate::shards::watch::ShardAssignment;
use crate::shards::{ShardKey, ShardKind};

async fn broker_with_mount(prefix: &str) -> Arc<Broker> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "plant",
            StreamMetadata {
                mount: Some(prefix.to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    broker
}

fn four_shard_ingress(routing: felix_wire::routing::ShardRouting) -> Arc<IngressRouter> {
    let node = NodeRef {
        node_id: "broker-a".to_string(),
        advertise_addr: SocketAddr::from(([10, 0, 0, 4], 7001)),
        region: "us-west-2".to_string(),
        live: true,
    };
    let nodes = HashMap::from_iter([(node.node_id.clone(), node)]);
    let owned: HashMap<ShardKey, ShardAssignment> = (0..4)
        .map(|shard| {
            let key = ShardKey {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "plant".to_string(),
                shard,
                kind: ShardKind::Stream,
            };
            let assignment = ShardAssignment {
                key: key.clone(),
                leader: "broker-a".to_string(),
                replicas: Vec::new(),
                generation: 1,
                state: "active".to_string(),
                successor: None,
                routing,
                incarnation: 0,
                fence_storage_ids: Default::default(),
                member_storage_ids: Default::default(),
                pending_set: None,
            };
            (key, assignment)
        })
        .collect();
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    router.publish(routing_table_from(&owned, &nodes), &nodes);
    Arc::new(IngressRouter::new(router, Arc::default()))
}

#[tokio::test]
async fn a_topic_resolves_to_its_stream_and_a_stranger_does_not() {
    let mounts = BrokerMounts::new(broker_with_mount("plant/").await, None);
    let found = mounts.resolve("t1", "plant/line1/temp").expect("mounted");
    assert_eq!((&*found.namespace, &*found.stream), ("ns", "plant"));
    assert_eq!(found.shard, 0, "a single node has one shard");
    assert!(mounts.resolve("t1", "yard/gate").is_none());
    assert!(mounts.resolve("t1", "plant").is_none());
    assert!(
        mounts.resolve("t2", "plant/line1").is_none(),
        "another tenant"
    );
}

/// The key is the whole topic name: one topic always lands on one shard, and
/// the stream's own mapping decides which.
#[tokio::test]
async fn the_shard_is_chosen_by_the_full_topic_under_the_streams_routing() {
    use felix_wire::routing::{ShardRouting, shard_for_routing};

    let broker = broker_with_mount("plant/").await;
    for routing in [ShardRouting::Modulo, ShardRouting::JumpHash] {
        let mounts = BrokerMounts::new(Arc::clone(&broker), Some(four_shard_ingress(routing)));
        let mut seen = felix_common::random::HashSet::default();
        for line in 0..64 {
            let topic = format!("plant/line{line}/temp");
            let found = mounts.resolve("t1", &topic).expect("mounted");
            assert_eq!(
                found.shard,
                shard_for_routing(routing, 4, Some(topic.as_bytes())),
                "{routing:?} {topic}"
            );
            assert_eq!(
                mounts.resolve("t1", &topic).expect("again").shard,
                found.shard,
                "a topic always lands on one shard"
            );
            seen.insert(found.shard);
        }
        assert!(
            seen.len() > 1,
            "topics spread over shards under {routing:?}"
        );
    }
}

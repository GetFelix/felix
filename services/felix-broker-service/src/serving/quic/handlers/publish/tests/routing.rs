//! Resolving a publish's stream and the shard it is routed for.

use super::*;

/// A connection's context must keep the cluster view.
///
/// This exact field was overridden to `None` when the ownership gate shipped,
/// which left every broker writing shards it did not own — the gate's own unit
/// tests passed throughout, because they call it directly. The invariant only
/// shows up at the seam.
#[tokio::test]
async fn a_connections_context_keeps_the_cluster_view() {
    use crate::shards::routing::IngressRouter;
    use felix_router::{RegionRouter, ShardRouter};

    let (mut context, _rx, _tx) = make_publish_context(1);
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    context.ingress = Some(Arc::new(IngressRouter::new(router, Arc::default())));

    let derived = context.for_connection(&crate::config::BrokerConfig::default());
    assert!(
        derived.ingress.is_some(),
        "a connection that loses the router serves every shard locally",
    );
}

#[tokio::test]
async fn resolve_stream_cached_uses_cached_entry_until_cleared() {
    let broker = Broker::new(EphemeralCache::new().into());
    let mut cache = HashMap::new();
    let mut key = String::new();

    let handle = resolve_route(
        &broker,
        Authority {
            ingress: None,
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    assert!(
        matches!(handle, PublishRoute::Refused(_)),
        "no tenant/namespace yet"
    );

    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");

    let cached = resolve_route(
        &broker,
        Authority {
            ingress: None,
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    assert!(
        matches!(cached, PublishRoute::Refused(_)),
        "cached miss should be returned until cache expires or clears"
    );

    cache.clear();
    let refreshed = resolve_route(
        &broker,
        Authority {
            ingress: None,
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    assert!(
        matches!(refreshed, PublishRoute::Local { .. }),
        "cache refresh should see stream"
    );
}

/// **A forwarded publish carries the shard it was routed for.**
///
/// The batch and the route have to name the same shard: the owner appends to
/// the shard the `ForwardKey` names, so a key routed to shard 3 and forwarded
/// with shard 0 is written to the wrong log entirely. That was the defect —
/// both the local and the forward branch stamped a hardcoded `shard_for(1,
/// None)`, which is always 0 — and its signature was every key of a multi-shard
/// stream arriving on shard 0 while the router had dispatched them correctly.
///
/// Revert the `shard` argument to `shard_for(1, None)` and this fails.
#[tokio::test]
async fn a_forwarded_publish_is_stamped_with_the_shard_it_was_routed_for() {
    let shutdown = tokio_util::sync::CancellationToken::new();
    let peers = felix_replication::peer::PeerPool::new(
        "broker-a".to_string(),
        felix_replication::peer::PeerTransportConfig::default(),
        shutdown.clone(),
    )
    .expect("bind a peer pool");
    let (mut ctx, _rx, _tx) = make_publish_context(1);
    ctx.peers = Some(peers);

    let target = ForwardTarget {
        node_id: "broker-b".to_string(),
        advertise_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 7001)),
        generation: 4,
    };
    let routed = publish_target(
        PublishRoute::Forward(target),
        &ctx,
        "t1",
        "ns",
        "stream",
        3,
        felix_wire::internal::AckMode::OnCommit,
        "test-token",
    )
    .expect("a forwardable route with a peer pool must produce a target");

    match routed {
        PublishTarget::Forward { key, .. } => assert_eq!(
            key.shard, 3,
            "the batch must name the shard the route was resolved for",
        ),
        _ => panic!("expected a forward target"),
    }
}

fn ingress_with_routing(
    shards: u32,
    routing: felix_wire::routing::ShardRouting,
) -> crate::shards::routing::IngressRouter {
    let router = Arc::new(felix_router::ShardRouter::new(
        "broker-a",
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    let assignments: HashMap<_, _> = (0..shards)
        .map(|shard| {
            let key = crate::shards::ShardKey {
                shard,
                ..watch_key()
            };
            let assignment = crate::shards::watch::ShardAssignment {
                key: key.clone(),
                leader: "broker-a".to_string(),
                replicas: Vec::new(),
                generation: 1,
                state: "active".to_string(),
                successor: None,
                routing,
            };
            (key, assignment)
        })
        .collect();
    let nodes = HashMap::new();
    router.publish(
        crate::shards::routing::routing_table_from(&assignments, &nodes),
        &nodes,
    );
    crate::shards::routing::IngressRouter::new(router, Arc::default())
}

/// A publish lands on the shard the stream's own mapping picks, which is the
/// shard a client computes from `stream_shards_view`. A legacy stream keeps
/// modulo.
#[tokio::test]
async fn a_publish_resolves_its_shard_with_the_streams_routing() {
    use crate::serving::quic::handlers::publish::route::resolve_shard;
    use felix_wire::routing::{ShardRouting, shard_for_routing};

    for routing in [ShardRouting::Modulo, ShardRouting::JumpHash] {
        let (mut context, _rx, _tx) = make_publish_context(1);
        context.ingress = Some(Arc::new(ingress_with_routing(8, routing)));
        for i in 0..64 {
            let key = format!("customer-{i}");
            assert_eq!(
                resolve_shard(&context, "t1", "ns", "stream", Some(key.as_bytes())),
                shard_for_routing(routing, 8, Some(key.as_bytes())),
                "{routing:?} stream, key {key}"
            );
        }
    }
}

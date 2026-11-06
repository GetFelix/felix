//! Route resolution: every outcome, and the concurrency the hot path needs.
use std::collections::HashMap;
use std::net::SocketAddr;

use super::*;
use crate::RegionRouter;
use felix_wire::routing::ShardRouting;

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

fn node(id: &str, port: u16, region: &str, live: bool) -> NodeRef {
    NodeRef {
        node_id: id.to_string(),
        advertise_addr: SocketAddr::from(([10, 0, 0, 4], port)),
        region: region.to_string(),
        live,
    }
}

fn catalog(nodes: &[NodeRef]) -> HashMap<String, NodeRef> {
    nodes
        .iter()
        .map(|n| (n.node_id.clone(), n.clone()))
        .collect()
}

fn router(nodes: &HashMap<String, NodeRef>, assignments: &[(u32, &str, u64)]) -> ShardRouter {
    let router = ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    );
    let table = RoutingTable::build(
        assignments.iter().map(|(shard, leader, generation)| {
            (key(*shard), leader.to_string(), Vec::new(), *generation)
        }),
        nodes,
    );
    router.publish(table, nodes);
    router
}

#[test]
fn a_shard_led_here_resolves_local() {
    let nodes = catalog(&[node("broker-a", 7001, "us-west-2", true)]);
    let router = router(&nodes, &[(0, "broker-a", 4)]);

    assert_eq!(router.resolve(&key(0)), Resolution::Local { generation: 4 });
    assert!(router.resolve(&key(0)).is_local());
}

#[test]
fn a_shard_led_elsewhere_resolves_remote_with_an_address() {
    let nodes = catalog(&[
        node("broker-a", 7001, "us-west-2", true),
        node("broker-b", 7002, "us-west-2", true),
    ]);
    let router = router(&nodes, &[(0, "broker-b", 7)]);

    assert_eq!(
        router.resolve(&key(0)),
        Resolution::Remote {
            node_id: "broker-b".to_string(),
            advertise_addr: SocketAddr::from(([10, 0, 0, 4], 7002)),
            generation: 7,
            incarnation: 0,
        },
    );
}

/// The acceptance criterion that matters most: an unrouted shard must never
/// look like a local one, or a broker writes a shard it does not own.
#[test]
fn an_unassigned_shard_is_never_local() {
    let nodes = catalog(&[node("broker-a", 7001, "us-west-2", true)]);
    let router = router(&nodes, &[]);

    assert_eq!(
        router.resolve(&key(9)),
        Resolution::Unavailable(Unavailable::NoAssignment),
    );
    assert!(!router.resolve(&key(9)).is_local());
}

/// Three different unavailable reasons, because they need three different
/// responses: wait for placement, wait for failover, or change policy.
#[test]
fn unavailable_reasons_are_distinguishable() {
    let nodes = catalog(&[
        node("broker-a", 7001, "us-west-2", true),
        node("broker-dead", 7003, "us-west-2", false),
        node("broker-far", 7004, "eu-central-1", true),
    ]);
    let router = router(
        &nodes,
        &[
            (0, "broker-dead", 1),
            (1, "broker-far", 1),
            (2, "broker-ghost", 1),
        ],
    );

    assert_eq!(
        router.resolve(&key(0)),
        Resolution::Unavailable(Unavailable::LeaderNotLive {
            node_id: "broker-dead".to_string()
        }),
    );
    assert_eq!(
        router.resolve(&key(1)),
        Resolution::Unavailable(Unavailable::RegionNotRoutable {
            region: "eu-central-1".to_string()
        }),
    );
    assert_eq!(
        router.resolve(&key(2)),
        Resolution::Unavailable(Unavailable::LeaderUnknown {
            node_id: "broker-ghost".to_string()
        }),
    );
}

/// A bridge is what turns a blocked region into a reachable one, and it is the
/// existing allowlist doing the work.
#[test]
fn a_bridge_makes_another_region_routable() {
    let nodes = catalog(&[
        node("broker-a", 7001, "us-west-2", true),
        node("broker-far", 7004, "eu-central-1", true),
    ]);
    let mut regions = RegionRouter::new("us-west-2".to_string());
    regions.allow_bridge("us-west-2".to_string(), "eu-central-1".to_string());

    let router = ShardRouter::new("broker-a", "us-west-2", regions);
    router.publish(
        RoutingTable::build([(key(0), "broker-far".to_string(), Vec::new(), 2)], &nodes),
        &nodes,
    );

    assert_eq!(
        router.resolve(&key(0)),
        Resolution::Remote {
            node_id: "broker-far".to_string(),
            advertise_addr: SocketAddr::from(([10, 0, 0, 4], 7004)),
            generation: 2,
            incarnation: 0,
        },
    );
}

/// Policy must not override this node's own ownership: a shard led here is
/// served here, and a region rule about reaching elsewhere does not apply.
#[test]
fn local_ownership_is_not_subject_to_region_policy() {
    let nodes = catalog(&[node("broker-a", 7001, "somewhere-else", true)]);
    let router = ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    );
    router.publish(
        RoutingTable::build([(key(0), "broker-a".to_string(), Vec::new(), 1)], &nodes),
        &nodes,
    );

    assert!(router.resolve(&key(0)).is_local());
}

/// A node that has not been heard from is still ours to serve. A broker that
/// stopped serving its own shards waiting to see its own heartbeat land would
/// take itself out of the cluster for no reason.
#[test]
fn this_node_serves_its_own_shards_even_when_marked_not_live() {
    let nodes = catalog(&[node("broker-a", 7001, "us-west-2", false)]);
    let router = router(&nodes, &[(0, "broker-a", 1)]);
    assert!(router.resolve(&key(0)).is_local());
}

/// Being behind is not the same as being wrong: the caller waits for the watch
/// rather than failing the stream.
#[test]
fn a_caller_ahead_of_the_table_gets_stale_not_unavailable() {
    let nodes = catalog(&[node("broker-a", 7001, "us-west-2", true)]);
    let router = router(&nodes, &[(0, "broker-a", 3)]);

    assert_eq!(
        router.resolve_at(&key(0), 5),
        Resolution::Stale { have: 3, wanted: 5 },
    );
    // At or below what the table holds, the route stands.
    assert!(router.resolve_at(&key(0), 3).is_local());
    assert!(router.resolve_at(&key(0), 2).is_local());
}

/// A generation for a shard the table has never seen means this router is
/// behind, not that the shard is unassigned.
#[test]
fn an_expected_generation_for_an_unknown_shard_reads_as_stale() {
    let nodes = catalog(&[node("broker-a", 7001, "us-west-2", true)]);
    let router = router(&nodes, &[]);

    assert_eq!(
        router.resolve_at(&key(0), 2),
        Resolution::Stale { have: 0, wanted: 2 },
    );
}

#[test]
fn publishing_replaces_the_whole_table() {
    let nodes = catalog(&[
        node("broker-a", 7001, "us-west-2", true),
        node("broker-b", 7002, "us-west-2", true),
    ]);
    let router = router(&nodes, &[(0, "broker-a", 1), (1, "broker-a", 1)]);
    assert!(router.resolve(&key(1)).is_local());

    // A rebalance that moved shard 0 away and dropped shard 1 entirely.
    router.publish(
        RoutingTable::build([(key(0), "broker-b".to_string(), Vec::new(), 2)], &nodes),
        &nodes,
    );

    assert!(matches!(router.resolve(&key(0)), Resolution::Remote { .. }));
    assert_eq!(
        router.resolve(&key(1)),
        Resolution::Unavailable(Unavailable::NoAssignment),
        "a shard dropped from the table must not keep its old route",
    );
}

#[test]
fn a_snapshot_is_stable_while_the_table_is_replaced() {
    let nodes = catalog(&[
        node("broker-a", 7001, "us-west-2", true),
        node("broker-b", 7002, "us-west-2", true),
    ]);
    let router = router(&nodes, &[(0, "broker-a", 1)]);

    let snapshot = router.snapshot();
    router.publish(
        RoutingTable::build([(key(0), "broker-b".to_string(), Vec::new(), 2)], &nodes),
        &nodes,
    );

    assert_eq!(
        snapshot.get(&key(0)).expect("route").leader.node_id,
        "broker-a",
        "a held snapshot must not change under the reader",
    );
    assert_eq!(router.snapshot().get(&key(0)).expect("route").generation, 2);
}

/// Readers must never see a torn or missing route while a writer swaps tables,
/// and must never block on one.
#[test]
fn readers_and_writers_do_not_interfere() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let nodes = Arc::new(catalog(&[
        node("broker-a", 7001, "us-west-2", true),
        node("broker-b", 7002, "us-west-2", true),
    ]));
    let router = Arc::new(router(&nodes, &[(0, "broker-a", 1)]));
    let stop = Arc::new(AtomicBool::new(false));

    let writer = {
        let router = Arc::clone(&router);
        let nodes = Arc::clone(&nodes);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut generation = 1u64;
            while !stop.load(Ordering::Acquire) {
                generation += 1;
                let leader = if generation.is_multiple_of(2) {
                    "broker-a"
                } else {
                    "broker-b"
                };
                router.publish(
                    RoutingTable::build(
                        [(key(0), leader.to_string(), Vec::new(), generation)],
                        &nodes,
                    ),
                    &nodes,
                );
            }
        })
    };

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let router = Arc::clone(&router);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut reads = 0u64;
                while !stop.load(Ordering::Acquire) {
                    // Whatever a reader sees, it is always one of the two valid
                    // routes -- never absent, never a mixture.
                    match router.resolve(&key(0)) {
                        Resolution::Local { .. } | Resolution::Remote { .. } => reads += 1,
                        other => panic!("unexpected resolution during swap: {other:?}"),
                    }
                }
                reads
            })
        })
        .collect();

    std::thread::sleep(std::time::Duration::from_millis(150));
    stop.store(true, Ordering::Release);

    writer.join().expect("writer");
    let total: u64 = readers.into_iter().map(|r| r.join().expect("reader")).sum();
    assert!(total > 1000, "readers should not be blocked: {total} reads");
}

#[test]
fn a_replica_missing_from_the_catalog_keeps_its_place_in_the_route() {
    let nodes = catalog(&[
        node("broker-a", 7001, "us-west-2", true),
        node("broker-b", 7002, "us-west-2", true),
    ]);
    let table = RoutingTable::build(
        [(
            key(0),
            "broker-a".to_string(),
            vec!["broker-b".to_string(), "broker-ghost".to_string()],
            1,
        )],
        &nodes,
    );

    // Quorums are sized from the route's replicas, so a replica the catalog
    // does not name must still be counted, just with nowhere to send to.
    let replicas = &table.get(&key(0)).expect("route").replicas;
    let named: Vec<&str> = replicas.iter().map(|r| r.node_id.as_str()).collect();
    assert_eq!(named, ["broker-b", "broker-ghost"]);
    assert_eq!(replicas[0].advertise_addr.port(), 7002);
    assert_eq!(replicas[1].advertise_addr.port(), 0, "no address to dial");
    assert!(!replicas[1].live);
}

#[test]
fn an_empty_catalog_keeps_every_replica() {
    // The feed publishes before its first catalog read succeeds.
    let table = RoutingTable::build(
        [(
            key(0),
            "broker-a".to_string(),
            vec!["broker-b".to_string(), "broker-c".to_string()],
            1,
        )],
        &HashMap::new(),
    );

    let route = table.get(&key(0)).expect("route");
    assert_eq!(route.replicas.len(), 2);
    assert!(route.replicas.iter().all(|r| r.advertise_addr.port() == 0));
}

#[test]
fn an_empty_table_resolves_nothing() {
    let router = ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    );
    assert!(router.snapshot().is_empty());
    assert_eq!(
        router.resolve(&key(0)),
        Resolution::Unavailable(Unavailable::NoAssignment),
    );
}

/// "I do not know this stream" and "exactly one shard" must be different
/// answers.
///
/// They are the same value out of `shards_for`, which is correct for routing —
/// a publish to a stream the snapshot has not caught up on hashes to shard 0
/// rather than failing — and wrong for anything a client acts on. A client told
/// "one shard" for a stream that does not exist opens one subscription, reports
/// success, and reads nothing, which surfaces later as missing data.
#[test]
fn an_unplaced_stream_is_distinguishable_from_a_single_shard_one() {
    let nodes = catalog(&[node("broker-a", 7001, "us-west-2", true)]);
    let router = router(&nodes, &[(0, "broker-a", 1)]);
    let table = router.snapshot();

    assert_eq!(
        table.placed_shards_for(ShardKind::Stream, "t1", "ns", "orders"),
        Some(1),
        "a stream placed with one shard reports one",
    );
    assert_eq!(
        table.placed_shards_for(ShardKind::Stream, "t1", "ns", "nothing-here"),
        None,
        "a stream this table has never heard of reported a shard count, which a \
         client cannot tell from a real single-shard stream",
    );

    // Routing keeps its fallback: the two callers want different answers, which
    // is why they are different methods.
    assert_eq!(
        table.shards_for(ShardKind::Stream, "t1", "ns", "nothing-here"),
        1,
        "routing lost its fallback, so a publish to a stream the snapshot has \
         not caught up on now has nowhere to go",
    );
}

fn placed(stream: &str, shard: u32, routing: ShardRouting) -> Placed {
    Placed {
        key: ShardKey {
            stream: stream.to_string(),
            ..key(shard)
        },
        leader: "broker-a".to_string(),
        replicas: Vec::new(),
        generation: 1,
        draining: false,
        successor: None,
        routing,
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending: None,
    }
}

/// Each stream resolves keys with the mapping its assignments carry, so a
/// jump-hash stream and a legacy one side by side in one table each keep
/// their own.
#[test]
fn keys_resolve_with_each_streams_own_mapping() {
    let nodes = catalog(&[node("broker-a", 7001, "us-west-2", true)]);
    let table = RoutingTable::build_with(
        (0..8).flat_map(|shard| {
            [
                placed("legacy", shard, ShardRouting::Modulo),
                placed("fresh", shard, ShardRouting::JumpHash),
            ]
        }),
        &nodes,
    );
    assert_eq!(
        table.placement_for(ShardKind::Stream, "t1", "ns", "fresh"),
        Some(StreamPlacement {
            shards: 8,
            routing: ShardRouting::JumpHash
        })
    );

    let mut differ = 0;
    for i in 0..200 {
        let key = format!("key-{i}");
        let key = Some(key.as_bytes());
        let legacy = table.shard_for_key(ShardKind::Stream, "t1", "ns", "legacy", key);
        let fresh = table.shard_for_key(ShardKind::Stream, "t1", "ns", "fresh", key);
        assert_eq!(legacy, felix_wire::routing::shard_for(8, key));
        assert_eq!(
            fresh,
            felix_wire::routing::shard_for_routing(ShardRouting::JumpHash, 8, key)
        );
        differ += usize::from(legacy != fresh);
    }
    assert!(
        differ > 100,
        "the two mappings agreed on {} of 200 keys",
        200 - differ
    );
    assert_eq!(
        table.shard_for_key(ShardKind::Stream, "t1", "ns", "unknown", Some(b"k")),
        0,
        "an unplaced stream routes to shard 0"
    );
}

/// **A prior set describes the route it was recorded for only.** One recorded
/// before generation 5 says nothing once the table routes generation 6.
#[test]
fn a_prior_set_answers_only_for_its_route_generation() {
    let nodes = HashMap::from([("a".to_string(), node("a", 7001, "r", true))]);
    let prior = PriorSet {
        generation: 5,
        before: None,
    };
    for (generation, known) in [(5, true), (6, false)] {
        let table =
            RoutingTable::build([(key(0), "a".to_string(), Vec::new(), generation)], &nodes)
                .with_priors(HashMap::from([(key(0), prior.clone())]));
        assert_eq!(table.prior_set(&key(0)).is_some(), known, "{generation}");
    }
}

/// A walk of the table is in key order, not the map's, so work done shard by
/// shard repeats in a replay of a simulated seed.
#[test]
fn a_walk_of_the_table_is_in_key_order() {
    let nodes = catalog(&[node("broker-a", 7001, "us-west-2", true)]);
    let table = RoutingTable::build(
        (0..64u32)
            .rev()
            .map(|shard| (key(shard), "broker-a".to_string(), Vec::new(), 1)),
        &nodes,
    );
    let walked: Vec<u32> = table.iter().map(|(key, _)| key.shard).collect();
    assert_eq!(walked, (0..64).collect::<Vec<_>>());
}

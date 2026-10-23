//! Caches are placed by the same pass as streams, and are not streams.
use super::*;

fn cache(name: &str, shards: u32) -> Cache {
    replicated_cache(name, shards, 1)
}

fn replicated_cache(name: &str, shards: u32, replication_factor: u32) -> Cache {
    Cache {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: name.to_string(),
        display_name: name.to_string(),
        shards,
        replication_factor,
        consistency: crate::model::ConsistencyLevel::Leader,
        incarnation: 0,
    }
}

/// Placements keyed by the whole identity, so a cache and a stream sharing a
/// name are two entries rather than one overwriting the other.
fn placements_by_kind(plan: &Plan) -> BTreeMap<(ShardKind, String, u32), String> {
    plan.shards
        .iter()
        .filter_map(|p| match &p.decision {
            Decision::Place(leader, _) => Some((
                (p.key.kind, p.key.stream.clone(), p.key.shard),
                leader.clone(),
            )),
            _ => None,
        })
        .collect()
}

#[test]
fn a_cache_gets_one_assignment_per_shard() {
    let plan = plan(
        &[],
        &[cache("sessions", 4)],
        &live(&["broker-a", "broker-b"]),
        &[],
        &NothingCaughtUp,
    );

    assert_eq!(plan.shards.len(), 4);
    assert!(
        plan.shards
            .iter()
            .all(|p| p.key.kind == ShardKind::Cache && p.key.stream == "sessions"),
        "every planned shard belongs to the cache",
    );
    assert_eq!(
        plan.shards
            .iter()
            .map(|p| p.key.shard)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1, 2, 3]),
    );
}

/// The collision this whole change exists to prevent. A cache and a stream may
/// share a name in one namespace; if the key did not carry the kind, one would
/// take the other's ownership row and a client would be routed to a broker
/// holding entirely the wrong log.
#[test]
fn a_cache_and_a_stream_of_the_same_name_are_placed_separately() {
    let plan = plan(
        &[stream("orders", 2)],
        &[cache("orders", 2)],
        &live(&["broker-a", "broker-b", "broker-c"]),
        &[],
        &NothingCaughtUp,
    );

    let placed = placements_by_kind(&plan);
    assert_eq!(placed.len(), 4, "two stream shards and two cache shards");
    for shard in 0..2 {
        assert!(placed.contains_key(&(ShardKind::Stream, "orders".to_string(), shard)));
        assert!(placed.contains_key(&(ShardKind::Cache, "orders".to_string(), shard)));
    }
}

/// Rendezvous scoring must actually separate them. Two shards that score
/// identically would track each other onto the same node forever, which turns
/// one node's loss into the loss of both the stream and the cache.
#[test]
fn a_cache_does_not_score_identically_to_the_stream_it_shares_a_name_with() {
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d", "broker-e"]);
    let plan = plan(
        &[stream("orders", 16)],
        &[cache("orders", 16)],
        &nodes,
        &[],
        &NothingCaughtUp,
    );

    let placed = placements_by_kind(&plan);
    let differing = (0..16)
        .filter(|shard| {
            placed.get(&(ShardKind::Stream, "orders".to_string(), *shard))
                != placed.get(&(ShardKind::Cache, "orders".to_string(), *shard))
        })
        .count();

    // Five nodes, so a shard pair lands together about a fifth of the time by
    // chance. Anything near zero means the two are correlated, not independent.
    assert!(
        differing >= 8,
        "cache and stream shards landed together {} times out of 16",
        16 - differing,
    );
}

/// Stream placement is pinned to what it was before caches were placed at all.
///
/// These leaders were produced by the algorithm with each stream's leaders
/// spread (3/2/3 and 1/2/2) and are asserted verbatim: the whole reason `score` leaves a stream's hash
/// input alone is that changing it silently reshuffles every stream in every
/// fresh cluster. A comparison against a freshly computed plan cannot catch
/// that -- both sides would move together -- so the expected values are
/// written down. Hash the kind unconditionally in `score` and this fails.
#[test]
fn stream_placement_is_unchanged_by_cache_placement() {
    const GOLDEN: &[(&str, u32, &str)] = &[
        ("orders", 0, "broker-a"),
        ("orders", 1, "broker-b"),
        ("orders", 2, "broker-c"),
        ("orders", 3, "broker-a"),
        ("orders", 4, "broker-b"),
        ("orders", 5, "broker-c"),
        ("orders", 6, "broker-c"),
        ("orders", 7, "broker-a"),
        ("payments", 0, "broker-c"),
        ("payments", 1, "broker-b"),
        ("payments", 2, "broker-a"),
        ("payments", 3, "broker-c"),
        ("payments", 4, "broker-b"),
    ];

    let streams = vec![stream("orders", 8), stream("payments", 5)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let expected: BTreeMap<(String, u32), String> = GOLDEN
        .iter()
        .map(|(name, shard, leader)| ((name.to_string(), *shard), leader.to_string()))
        .collect();

    assert_eq!(
        placements(&plan(&streams, &[], &nodes, &[], &NothingCaughtUp)),
        expected,
        "stream placement moved with no caches in play",
    );

    // And placing caches alongside them moves nothing either.
    let alongside = placements_by_kind(&plan(
        &streams,
        &[cache("sessions", 6), cache("orders", 3)],
        &nodes,
        &[],
        &NothingCaughtUp,
    ));
    let stream_placements: BTreeMap<(String, u32), String> = alongside
        .into_iter()
        .filter(|((kind, _, _), _)| *kind == ShardKind::Stream)
        .map(|((_, name, shard), leader)| ((name, shard), leader))
        .collect();

    assert_eq!(stream_placements, expected);
}

/// One pass over both, so a node's `max_shards` is a budget for everything it
/// holds. Two passes would each believe it had the whole cap to itself and
/// together overshoot it.
#[test]
fn a_nodes_capacity_is_shared_between_its_streams_and_its_caches() {
    let nodes = vec![node("broker-a", NodeLifecycle::Live, Some(3))];
    let plan = plan(
        &[stream("orders", 2)],
        &[cache("sessions", 4)],
        &nodes,
        &[],
        &NothingCaughtUp,
    );

    let placed = placements_by_kind(&plan);
    assert_eq!(placed.len(), 3, "the cap is three shards, of either kind");
    assert_eq!(
        plan.shards
            .iter()
            .filter(|p| matches!(
                p.decision,
                Decision::Unplaceable(Unplaceable::AllNodesAtCapacity)
            ))
            .count(),
        3,
    );
}

#[test]
fn a_replicated_cache_gets_followers() {
    let plan = plan(
        &[],
        &[replicated_cache("sessions", 2, 3)],
        &live(&["broker-a", "broker-b", "broker-c", "broker-d"]),
        &[],
        &NothingCaughtUp,
    );

    for shard in &plan.shards {
        let Decision::Place(leader, replicas) = &shard.decision else {
            panic!("expected a placement, got {:?}", shard.decision);
        };
        assert_eq!(replicas.len(), 2, "leader plus two followers");
        assert!(!replicas.contains(leader));
    }
}

/// A cache created before it had a shard count reads back as one shard, and one
/// shard is a single owner for the whole keyspace — exactly the behaviour it
/// had when nothing placed it at all.
#[test]
fn a_cache_defaults_to_a_single_shard() {
    let json = r#"{
        "tenant_id": "t1",
        "namespace": "ns",
        "cache": "sessions",
        "display_name": "Sessions"
    }"#;
    let cache: Cache = serde_json::from_str(json).expect("cache without a shard count");

    assert_eq!(cache.shards, 1);
    assert_eq!(cache.replication_factor, 1);
}

//! A shard's copies are spread across the zones brokers register: at first
//! placement, through moves and drains, and by replacing a follower that
//! crowds a zone. Where that cannot be done the copies are placed anyway and
//! the shard is reported in `Plan::unspread`.
use super::*;

fn zoned(id: &str, zone: Option<&str>) -> Node {
    let mut node = node(id, NodeLifecycle::Live, None);
    node.spec.zone = zone.map(str::to_string);
    node
}

/// Nodes named `<zone><n>`, each in the zone its name starts with.
fn in_zones(ids: &[&str]) -> Vec<Node> {
    ids.iter().map(|id| zoned(id, Some(&id[..1]))).collect()
}

fn key_of(stream: &str, shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

/// A stream name whose shard 0 scores so that `wanted` holds, so a test can
/// make the zone-blind choice the wrong one.
fn named_so_that(wanted: impl Fn(&ShardKey) -> bool) -> String {
    (0..10_000)
        .map(|i| format!("s{i}"))
        .find(|name| wanted(&key_of(name, 0)))
        .expect("some name scores that way")
}

fn highest<'a>(key: &ShardKey, ids: &[&'a str]) -> &'a str {
    ids.iter()
        .copied()
        .max_by_key(|id| (score(key, id), id.to_string()))
        .expect("ids")
}

fn zone_of<'a>(nodes: &'a [Node], id: &str) -> Option<&'a str> {
    nodes
        .iter()
        .find(|node| node.node_id == id)
        .and_then(|node| node.spec.zone.as_deref())
}

fn placed_sets(plan: &Plan) -> Vec<Vec<String>> {
    plan.to_place()
        .map(|(_, leader, replicas)| {
            std::iter::once(leader.to_string())
                .chain(replicas.iter().cloned())
                .collect()
        })
        .collect()
}

fn only_decision(plan: &Plan) -> &Decision {
    assert_eq!(plan.shards.len(), 1);
    &plan.shards[0].decision
}

struct Reported {
    caught_up: BTreeSet<String>,
    drained: bool,
}

impl Reported {
    fn caught_up(nodes: &[&str]) -> Self {
        Self {
            caught_up: nodes.iter().map(|n| n.to_string()).collect(),
            drained: false,
        }
    }

    fn drained(mut self) -> Self {
        self.drained = true;
        self
    }
}

impl CaughtUp for Reported {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.caught_up.contains(node_id)
    }

    fn is_drained(&self, _key: &ShardKey, generation: u64) -> bool {
        self.drained && generation == 3
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }
}

#[test]
fn every_copy_of_a_shard_is_in_a_different_zone() {
    let nodes = in_zones(&["a1", "a2", "b1", "b2", "c1", "c2"]);
    let streams = vec![replicated_stream("orders", 24, 3)];

    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);

    let sets = placed_sets(&plan);
    assert_eq!(sets.len(), 24);
    for set in sets {
        let zones: BTreeSet<_> = set.iter().map(|id| zone_of(&nodes, id)).collect();
        assert_eq!(zones.len(), 3, "{set:?} shares a zone");
    }
    assert!(plan.unspread.is_empty(), "{:?}", plan.unspread);
}

/// One broker in `b` with room for one copy: the other shards still get both
/// copies, in `a`, and are reported as the fallback they are.
#[test]
fn copies_that_cannot_be_spread_are_placed_anyway_and_reported() {
    let mut nodes = in_zones(&["a1", "a2", "b1"]);
    nodes[2].spec.capacity.max_shards = Some(1);
    let streams = vec![replicated_stream("orders", 4, 2)];

    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);

    let mut crowded = Vec::new();
    for (key, leader, replicas) in plan.to_place() {
        assert_eq!(
            replicas.len(),
            1,
            "{key:?}: the second copy is still placed"
        );
        if leader != "b1" && replicas[0] != "b1" {
            crowded.push(key.clone());
        }
    }
    assert_eq!(crowded.len(), 3);
    assert_eq!(plan.unspread, crowded);
}

/// All in one zone is as spread as that cluster gets, so there is nothing
/// to report.
#[test]
fn a_single_zone_is_not_reported() {
    let nodes = in_zones(&["a1", "a2", "a3"]);
    let plan = plan(
        &[replicated_stream("orders", 6, 3)],
        &[],
        &nodes,
        &[],
        &NothingCaughtUp,
    );

    assert_eq!(plan.to_place().count(), 6);
    assert!(plan.unspread.is_empty(), "{:?}", plan.unspread);
}

/// A broker without a zone shares one with nobody, so the two in `a` are
/// never both given a copy of one shard while a zoneless broker has room.
#[test]
fn a_broker_without_a_zone_counts_as_its_own() {
    let nodes = vec![
        zoned("a1", Some("a")),
        zoned("a2", Some("a")),
        zoned("n1", None),
        zoned("n2", None),
    ];
    let plan = plan(
        &[replicated_stream("orders", 12, 3)],
        &[],
        &nodes,
        &[],
        &NothingCaughtUp,
    );

    let sets = placed_sets(&plan);
    assert_eq!(sets.len(), 12);
    for set in sets {
        assert_eq!(set.len(), 3);
        assert!(
            !(set.contains(&"a1".to_string()) && set.contains(&"a2".to_string())),
            "{set:?} has both copies in a"
        );
    }
    assert!(plan.unspread.is_empty(), "{:?}", plan.unspread);
}

/// What a cluster of brokers that predate zones gets: the same placement as
/// one where every broker is alone in its zone.
#[test]
fn no_zones_places_as_if_every_broker_had_its_own() {
    let ids = ["n1", "n2", "n3", "n4", "n5"];
    let without: Vec<Node> = ids.iter().map(|id| zoned(id, None)).collect();
    let alone: Vec<Node> = ids.iter().map(|id| zoned(id, Some(id))).collect();
    let streams = vec![replicated_stream("orders", 20, 3)];

    assert_eq!(
        plan(&streams, &[], &without, &[], &NothingCaughtUp),
        plan(&streams, &[], &alone, &[], &NothingCaughtUp),
    );
}

/// At the cut-over the old leader stays as a copy only if that keeps the
/// zones: here it would put both copies in `a`, so the one in `b` is kept.
#[test]
fn a_cut_over_keeps_the_copy_in_the_zone_the_shard_would_lose() {
    let nodes = in_zones(&["a1", "a2", "b1"]);
    let mut fenced = assigned("orders", "a1", &["b1", "a2"]);
    fenced.successor = Some("a2".to_string());
    fenced.state = ShardState::Draining;

    let plan = plan(
        &[replicated_stream("orders", 1, 2)],
        &[],
        &nodes,
        &[fenced],
        &Reported::caught_up(&["a2", "b1"]).drained(),
    );

    match only_decision(&plan) {
        Decision::Move(MoveStep::CutOver { to, .. }, next) => {
            assert_eq!(to, "a2");
            assert_eq!(next.replicas, vec!["b1".to_string()]);
        }
        other => panic!("expected a cut-over, got {other:?}"),
    }
}

/// A draining leader whose follower holds nothing yet: moving to `b2` would
/// leave both copies in `b`, so the move goes to a node that keeps `a`, even
/// though `b2` scores highest.
#[test]
fn a_drain_goes_where_the_shard_keeps_its_zones() {
    let name = named_so_that(|key| highest(key, &["a2", "b1", "b2"]) == "b2");
    let mut nodes = in_zones(&["a1", "a2", "b1", "b2"]);
    nodes[0].status.lifecycle = NodeLifecycle::Draining;

    let plan = plan(
        &[replicated_stream(&name, 1, 2)],
        &[],
        &nodes,
        &[assigned(&name, "a1", &["b1"])],
        &NothingCaughtUp,
    );

    match only_decision(&plan) {
        Decision::Move(MoveStep::Stage { successor }, _) => {
            assert_ne!(successor, "b2", "the shard would lose zone a");
        }
        other => panic!("expected a stage, got {other:?}"),
    }
}

/// Rebalancing is optional, so it never costs a zone. `b1` is down; the
/// shard's live copies are in `a` and `c`, and `b2` could take the missing
/// one. Moving leadership to `a2` would leave `a1` and `c1` as the copies
/// and no room for `b`, however well `a2` scores.
#[test]
fn a_rebalance_never_narrows_the_zones() {
    let name = named_so_that(|key| highest(key, &["a2", "b2", "c1"]) == "a2");
    let mut nodes = in_zones(&["a1", "a2", "b1", "b2", "c1"]);
    nodes[2].status.lifecycle = NodeLifecycle::Down;
    // Every shard led by a1, which puts it over its share.
    let existing: Vec<ShardAssignment> = (0..4)
        .map(|shard| ShardAssignment {
            key: key_of(&name, shard),
            ..assigned(&name, "a1", &["b1", "c1"])
        })
        .collect();

    let plan = plan(
        &[replicated_stream(&name, 4, 3)],
        &[],
        &nodes,
        &existing,
        &NothingCaughtUp,
    );

    let staged: Vec<_> = plan
        .moves()
        .filter_map(|(key, step, _)| match step {
            MoveStep::Stage { successor } => Some((key.shard, successor.clone())),
            _ => None,
        })
        .collect();
    assert!(
        staged.iter().any(|(shard, _)| *shard == 0),
        "the rebalance still happens: {staged:?}"
    );
    for (shard, successor) in staged {
        assert_ne!(successor, "a2", "shard {shard} would lose zone b");
    }
}

/// A follower leaving a draining broker is replaced in the zone the shard
/// would otherwise lose, not by the best-scoring broker.
#[test]
fn a_draining_follower_is_replaced_in_its_own_zone() {
    let name = named_so_that(|key| score(key, "a2") > score(key, "b2"));
    let mut nodes = in_zones(&["a1", "a2", "b1", "b2"]);
    nodes[2].status.lifecycle = NodeLifecycle::Draining;

    let plan = plan(
        &[replicated_stream(&name, 1, 2)],
        &[],
        &nodes,
        &[assigned(&name, "a1", &["b1"])],
        &NothingCaughtUp,
    );

    assert!(
        matches!(
            only_decision(&plan),
            Decision::Move(MoveStep::Reseat { from, to }, _) if from == "b1" && to == "b2"
        ),
        "{:?}",
        only_decision(&plan)
    );
}

/// Copies placed before a zone was available (or before zones were
/// reported) are spread once a broker in the missing zone can take one: the
/// crowded follower is replaced beside itself, then leaves.
#[test]
fn a_follower_crowding_a_zone_is_moved_to_one_the_shard_lacks() {
    let nodes = in_zones(&["a1", "a2", "b1"]);
    let streams = vec![replicated_stream("orders", 1, 2)];
    let crowded = assigned("orders", "a1", &["a2"]);

    let plan_a = plan(
        &streams,
        &[],
        &nodes,
        std::slice::from_ref(&crowded),
        &NothingCaughtUp,
    );
    let joining = match only_decision(&plan_a) {
        Decision::Move(MoveStep::Reseat { from, to }, next) => {
            assert_eq!((from.as_str(), to.as_str()), ("a2", "b1"));
            next.clone()
        }
        other => panic!("expected a reseat, got {other:?}"),
    };
    assert_eq!(plan_a.unspread, Vec::<ShardKey>::new(), "b1 is on its way");

    let joining = ShardAssignment {
        generation: 3,
        ..joining
    };
    let plan_b = plan(
        &streams,
        &[],
        &nodes,
        &[joining],
        &Reported::caught_up(&["a2", "b1"]),
    );
    match only_decision(&plan_b) {
        Decision::Move(MoveStep::Seat { from, to }, next) => {
            assert_eq!((from.as_str(), to.as_str()), ("a2", "b1"));
            assert_eq!(next.replicas, vec!["b1".to_string()]);
        }
        other => panic!("expected a seat, got {other:?}"),
    }
}

/// The same copies with no zones reported are left alone: nothing crowds.
#[test]
fn without_zones_nothing_is_reseated() {
    let nodes: Vec<Node> = ["a1", "a2", "b1"]
        .iter()
        .map(|id| zoned(id, None))
        .collect();

    let plan = plan(
        &[replicated_stream("orders", 1, 2)],
        &[],
        &nodes,
        &[assigned("orders", "a1", &["a2"])],
        &NothingCaughtUp,
    );

    assert_eq!(only_decision(&plan), &Decision::Kept);
}

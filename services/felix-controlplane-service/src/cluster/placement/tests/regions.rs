//! A stream with a home region has copies only in that region, or in one the
//! allowlist bridges it to: placed there, moved there, promoted there, and
//! never moved elsewhere by an operator.
use std::sync::Arc;

use felix_router::RegionRouter;

use super::*;

fn in_region(id: &str, region: &str) -> Node {
    let mut node = node(id, NodeLifecycle::Live, None);
    node.spec.region = region.to_string();
    node
}

/// Two brokers in `eu`, two in `us`.
fn two_regions() -> Vec<Node> {
    vec![
        in_region("eu-1", "eu"),
        in_region("eu-2", "eu"),
        in_region("us-1", "us"),
        in_region("us-2", "us"),
    ]
}

fn homed(name: &str, shards: u32, replication_factor: u32, region: &str) -> Stream {
    Stream {
        region: Some(region.to_string()),
        ..replicated_stream(name, shards, replication_factor)
    }
}

fn bridged(pairs: &[(&str, &str)]) -> MovePolicy {
    MovePolicy {
        regions: Arc::new(RegionRouter::with_bridges(
            String::new(),
            pairs.iter().map(|(a, b)| (a.to_string(), b.to_string())),
        )),
        ..MovePolicy::default()
    }
}

fn no_bridges() -> MovePolicy {
    bridged(&[])
}

/// Every node a `Place` decision names, leader and followers.
fn placed_nodes(plan: &Plan) -> BTreeSet<String> {
    plan.to_place()
        .flat_map(|(_, leader, replicas)| {
            std::iter::once(leader.to_string()).chain(replicas.iter().cloned())
        })
        .collect()
}

pub(super) struct Level(BTreeSet<String>);

impl CaughtUp for Level {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.0.contains(node_id)
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }
}

pub(super) fn level(nodes: &[&str]) -> Level {
    Level(nodes.iter().map(|n| n.to_string()).collect())
}

#[test]
fn a_homed_stream_is_placed_only_in_its_region() {
    let streams = vec![homed("orders", 8, 2, "eu")];

    let plan = plan_with(
        &streams,
        &[],
        &two_regions(),
        &[],
        &NothingCaughtUp,
        no_bridges(),
    );

    assert_eq!(plan.to_place().count(), 8);
    let expected: BTreeSet<String> = ["eu-1", "eu-2"].iter().map(|n| n.to_string()).collect();
    assert_eq!(placed_nodes(&plan), expected);
}

/// The allowlist does nothing to a stream without a home region.
#[test]
fn a_stream_without_a_region_is_placed_anywhere() {
    let streams = vec![replicated_stream("orders", 8, 2)];

    let plan = plan_with(
        &streams,
        &[],
        &two_regions(),
        &[],
        &NothingCaughtUp,
        no_bridges(),
    );

    assert!(placed_nodes(&plan).iter().any(|n| n.starts_with("us-")));
    assert!(placed_nodes(&plan).iter().any(|n| n.starts_with("eu-")));
}

#[test]
fn with_no_node_in_its_region_a_homed_stream_is_unplaceable() {
    let streams = vec![homed("orders", 1, 1, "eu")];
    let nodes = vec![in_region("us-1", "us"), in_region("us-2", "us")];

    let plan = plan_with(&streams, &[], &nodes, &[], &NothingCaughtUp, no_bridges());

    let unplaceable: Vec<_> = plan.unplaceable().map(|(_, why)| why.clone()).collect();
    assert_eq!(
        unplaceable,
        vec![Unplaceable::NoNodeInRegion {
            region: "eu".to_string()
        }]
    );
}

/// A bridge is directional: `eu>us` lets an `eu` stream into `us`, and not
/// the other way round.
#[test]
fn a_bridge_admits_the_region_it_leads_to() {
    let nodes = vec![in_region("us-1", "us")];

    let plan = plan_with(
        &[homed("orders", 1, 1, "eu")],
        &[],
        &nodes,
        &[],
        &NothingCaughtUp,
        bridged(&[("eu", "us")]),
    );
    assert_eq!(plan.to_place().count(), 1);

    let plan = plan_with(
        &[homed("orders", 1, 1, "eu")],
        &[],
        &nodes,
        &[],
        &NothingCaughtUp,
        bridged(&[("us", "eu")]),
    );
    assert_eq!(plan.to_place().count(), 0);
}

/// A leader in a region the stream may not be in -- its node moved region,
/// or a bridge was removed -- is moved out like a draining node's.
#[test]
fn a_leader_outside_the_region_is_moved_into_it() {
    let streams = vec![homed("orders", 1, 1, "eu")];
    let existing = vec![assigned("orders", "us-1", &[])];

    let plan = plan_with(
        &streams,
        &[],
        &two_regions(),
        &existing,
        &NothingCaughtUp,
        no_bridges(),
    );

    match &plan.shards[0].decision {
        Decision::Move(MoveStep::Stage { successor }, _) => {
            assert!(successor.starts_with("eu-"), "staged on {successor}");
        }
        other => panic!("expected a move into eu, got {other:?}"),
    }
}

#[test]
fn a_follower_outside_the_region_is_replaced_by_one_inside_it() {
    let streams = vec![homed("orders", 1, 2, "eu")];
    let existing = vec![assigned("orders", "eu-1", &["us-1"])];

    let plan = plan_with(
        &streams,
        &[],
        &two_regions(),
        &existing,
        &NothingCaughtUp,
        no_bridges(),
    );

    match &plan.shards[0].decision {
        Decision::Move(MoveStep::Reseat { from, to }, _) => {
            assert_eq!(from, "us-1");
            assert_eq!(to, "eu-2");
        }
        other => panic!("expected the us follower to be replaced, got {other:?}"),
    }
}

/// Failover promotes only a replica the stream may be in, even when one
/// outside its region also holds the log.
#[test]
fn failover_promotes_only_inside_the_region() {
    let streams = vec![homed("orders", 1, 3, "eu")];
    // eu-1 led and is gone.
    let nodes = vec![in_region("eu-2", "eu"), in_region("us-1", "us")];
    let existing = vec![assigned("orders", "eu-1", &["eu-2", "us-1"])];

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &level(&["us-1"]),
        no_bridges(),
    );
    assert_eq!(plan.to_place().count(), 0, "promoted outside the region");

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &level(&["eu-2", "us-1"]),
        no_bridges(),
    );
    let (_, leader, replicas) = plan.to_place().next().expect("promoted");
    assert_eq!(leader, "eu-2");
    assert!(replicas.is_empty(), "{replicas:?}");
}

#[test]
fn an_operator_cannot_move_a_shard_out_of_its_region() {
    let streams = vec![homed("orders", 1, 1, "eu")];
    let nodes = two_regions();
    let existing = vec![assigned("orders", "eu-1", &[])];
    let catalog = Catalog {
        streams: &streams,
        caches: &[],
        nodes: &nodes,
        existing: &existing,
        caught_up: &NothingCaughtUp,
        policy: no_bridges(),
    };

    assert_eq!(
        start_move(&catalog, &existing[0].key, "us-1"),
        Err(Refused::RegionNotAllowed {
            node: "us-1".to_string(),
            region: "us".to_string(),
            home: "eu".to_string(),
        })
    );
    assert!(start_move(&catalog, &existing[0].key, "eu-2").is_ok());
}

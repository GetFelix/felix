//! A node marked internal holds no tenant shard: not placed there, not
//! promoted there after other nodes fail, not moved there by an operator,
//! and moved off when one is already there.
use felix_common::membership::{NODE_ROLE_INTERNAL, NODE_ROLE_LABEL};

use super::caches::cache;
use super::restore::{MINUTE, Reports, decide, gone};
use super::*;

fn internal(id: &str) -> Node {
    let mut node = node(id, NodeLifecycle::Live, None);
    node.spec
        .labels
        .insert(NODE_ROLE_LABEL.to_string(), NODE_ROLE_INTERNAL.to_string());
    node
}

fn placed(plan: &Plan) -> BTreeSet<String> {
    plan.to_place()
        .flat_map(|(_, leader, replicas)| {
            std::iter::once(leader.to_string()).chain(replicas.iter().cloned())
        })
        .collect()
}

#[test]
fn an_internal_node_is_given_no_stream_or_cache_shard() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(internal("core"));
    let streams = vec![replicated_stream("orders", 12, 3)];
    let caches = vec![cache("sessions", 6)];

    let plan = plan(&streams, &caches, &nodes, &[], &NothingCaughtUp);

    assert_eq!(plan.to_place().count(), 18);
    assert!(!placed(&plan).contains("core"), "{:?}", placed(&plan));
}

#[test]
fn a_cluster_of_only_internal_nodes_places_nothing() {
    let nodes = vec![internal("core-1"), internal("core-2")];
    let streams = vec![replicated_stream("orders", 2, 1)];

    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);

    assert_eq!(plan.to_place().count(), 0);
}

/// With every ordinary node down, the internal one holds a copy of the log and
/// is the only broker left, and failover still does not promote it.
#[test]
fn failover_after_other_nodes_fail_never_picks_an_internal_node() {
    let nodes = vec![
        node("a", NodeLifecycle::Down, None),
        node("b", NodeLifecycle::Down, None),
        internal("core"),
    ];
    let streams = vec![replicated_stream("orders", 1, 2)];
    let existing = vec![assigned("orders", "a", &["b", "core"])];
    let caught_up = super::regions::level(&["core"]);

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);

    assert!(!placed(&plan).contains("core"), "{:?}", placed(&plan));
}

/// The only spare broker is internal, so the lost follower is not replaced.
#[test]
fn a_lost_copy_is_not_restored_onto_an_internal_node() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 10 * MINUTE),
        internal("core"),
    ];
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);

    let (decision, _) = decide(
        replicated_stream("orders", 1, 3),
        &nodes,
        existing,
        &Reports::default(),
    );

    assert!(
        !matches!(decision, Decision::Move(MoveStep::Restore { .. }, _)),
        "{decision:?}"
    );
}

#[test]
fn a_shard_already_on_an_internal_node_is_moved_off() {
    let nodes = vec![internal("core"), node("a", NodeLifecycle::Live, None)];
    let streams = vec![stream("orders", 1)];
    let existing = vec![assigned("orders", "core", &[])];
    let caught_up = super::regions::level(&["a"]);

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);

    let decision = &plan.shards[0].decision;
    assert!(
        matches!(decision, Decision::Move(MoveStep::Stage { successor }, _) if successor == "a"),
        "{decision:?}"
    );
}

#[test]
fn an_operator_cannot_move_a_shard_onto_an_internal_node() {
    let nodes = vec![
        node("a", NodeLifecycle::Live, None),
        node("b", NodeLifecycle::Live, None),
        internal("core"),
    ];
    let streams = vec![stream("orders", 1)];
    let existing = vec![assigned("orders", "a", &[])];
    let catalog = Catalog {
        streams: &streams,
        caches: &[],
        nodes: &nodes,
        existing: &existing,
        caught_up: &NothingCaughtUp,
        policy: MovePolicy::default(),
    };

    assert_eq!(
        start_move(&catalog, &existing[0].key, "core"),
        Err(Refused::InternalNode("core".to_string()))
    );
    assert!(start_move(&catalog, &existing[0].key, "b").is_ok());
}

//! A node marked internal holds no tenant shard: not placed there, not
//! promoted there after other nodes fail, not moved there by an operator,
//! and moved off when one is already there.
use felix_common::membership::{NODE_ROLE_INTERNAL, NODE_ROLE_LABEL};

use super::caches::{cache, replicated_cache};
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

/// Brokers `a` and `b` and `core` hold a stream's three copies; `core` was
/// marked internal after the group formed.
fn raft_member_of_internal() -> Vec<ShardAssignment> {
    let mut assignment = assigned("orders", "a", &["b", "core"]);
    assignment.raft_target = Some(crate::model::RaftTarget {
        counter: 2,
        members: ["a", "b", "core"]
            .iter()
            .map(|node| (node.to_string(), 7))
            .collect(),
    });
    assignment.raft_formed = Some(crate::model::RaftFormed {
        incarnation: assignment.incarnation,
        epoch: 0,
    });
    vec![assignment]
}

fn raft() -> MovePolicy {
    MovePolicy {
        raft_streams: true,
        ..MovePolicy::default()
    }
}

#[test]
fn a_raft_member_set_with_an_internal_node_gets_a_replacement_in_one_pass() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(internal("core"));
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = raft_member_of_internal();

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    let (_, step, written) = plan.moves().next().expect("a set change");
    assert!(
        matches!(step, MoveStep::RaftReplaceInternal { from, to } if from == "core" && to == "c"),
        "{step:?}"
    );
    // The group stays at its factor, and the leader and the other member stay.
    assert_eq!(written.leader, "a");
    assert_eq!(written.replicas, vec!["b".to_string(), "c".to_string()]);

    // Once the target is settled for the new set nothing more is written.
    let mut after = written.clone();
    after.raft_target = Some(crate::model::RaftTarget {
        counter: 3,
        members: ["a", "b", "c"]
            .iter()
            .map(|node| (node.to_string(), 7))
            .collect(),
    });
    let again = plan_with(&streams, &[], &nodes, &[after], &NothingCaughtUp, raft());
    assert_eq!(again.kept(), 1, "{again:?}");
}

#[test]
fn a_raft_member_set_keeps_an_internal_node_when_no_replacement_exists() {
    let mut nodes = live(&["a", "b"]);
    nodes.push(internal("core"));
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = raft_member_of_internal();

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert_eq!(plan.to_place().count(), 0, "{plan:?}");
    assert!(
        matches!(
            plan.waiting().next(),
            Some((_, Blocked::NoReplacement { node })) if node == "core"
        ),
        "{plan:?}"
    );
}

/// A change the group has not been told yet, or whose target is not stored
/// for the current set, is waited on: one set change at a time.
#[test]
fn a_raft_set_change_waits_for_the_target_of_the_current_set() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(internal("core"));
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut existing = raft_member_of_internal();
    existing[0].raft_target = None;

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert_eq!(plan.waiting().count(), 1, "{plan:?}");
}

#[test]
fn a_hybrid_stream_follower_on_an_internal_node_is_replaced() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(internal("core"));
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![assigned("orders", "a", &["b", "core"])];
    let caught_up = super::regions::level(&["a", "b", "c"]);

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);

    assert_eq!(plan.moves().count(), 1, "{plan:?}");
    let (_, _, written) = plan.moves().next().expect("a move");
    assert!(
        written.successor.as_deref() == Some("c") || written.joining.as_deref() == Some("c"),
        "{written:?}"
    );
}

#[test]
fn a_cache_follower_on_an_internal_node_is_replaced() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(internal("core"));
    let caches = vec![replicated_cache("sessions", 1, 3)];
    let mut existing = assigned("sessions", "a", &["b", "core"]);
    existing.key.kind = ShardKind::Cache;
    let caught_up = super::regions::level(&["a", "b", "c"]);

    let plan = plan(&[], &caches, &nodes, &[existing], &caught_up);

    assert_eq!(plan.moves().count(), 1, "{plan:?}");
    let (_, _, written) = plan.moves().next().expect("a move");
    assert!(
        written.successor.as_deref() == Some("c") || written.joining.as_deref() == Some("c"),
        "{written:?}"
    );
}

fn zoned_live(id: &str, zone: &str) -> Node {
    let mut node = node(id, NodeLifecycle::Live, None);
    node.spec.zone = Some(zone.to_string());
    node
}

#[test]
fn the_replacement_keeps_the_sets_zone_spread() {
    // a and b hold z1 and z2; the internal node held z3. c is in z1, d in z3.
    let mut core = internal("core");
    core.spec.zone = Some("z3".to_string());
    let nodes = vec![
        zoned_live("a", "z1"),
        zoned_live("b", "z2"),
        zoned_live("c", "z1"),
        zoned_live("d", "z3"),
        core,
    ];
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = raft_member_of_internal();

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    let (_, step, _) = plan.moves().next().expect("a set change");
    assert!(
        matches!(step, MoveStep::RaftReplaceInternal { to, .. } if to == "d"),
        "{step:?}"
    );
}

#[test]
fn a_down_member_holds_the_swap_visibly_until_it_returns() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(internal("core"));
    nodes[1].status.lifecycle = NodeLifecycle::Down;
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = raft_member_of_internal();

    let held = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());
    assert_eq!(held.moves().count(), 0, "{held:?}");
    assert!(
        matches!(held.waiting().next(), Some((_, Blocked::MemberDown { node })) if node == "b"),
        "{held:?}"
    );

    nodes[1].status.lifecycle = NodeLifecycle::Live;
    let back = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());
    assert_eq!(back.moves().count(), 1, "{back:?}");
}

#[test]
fn a_paused_placement_holds_the_swap() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(internal("core"));
    let streams = vec![replicated_stream("orders", 1, 3)];
    let policy = MovePolicy {
        paused: true,
        ..raft()
    };

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &raft_member_of_internal(),
        &NothingCaughtUp,
        policy,
    );

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert!(matches!(plan.waiting().next(), Some((_, Blocked::Paused))));
}

#[test]
fn swaps_past_the_per_pass_limit_wait() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(internal("core"));
    let shards = (super::super::raft_leaders::RAFT_REPLACEMENTS_PER_PASS + 2) as u32;
    let streams = vec![replicated_stream("orders", shards, 3)];
    let existing: Vec<ShardAssignment> = (0..shards)
        .map(|shard| {
            let mut one = raft_member_of_internal().remove(0);
            one.key.shard = shard;
            one
        })
        .collect();

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    assert_eq!(
        plan.moves().count(),
        super::super::raft_leaders::RAFT_REPLACEMENTS_PER_PASS,
        "{plan:?}"
    );
    assert_eq!(plan.waiting().count(), 2);
}

/// The leader leaves the internal node first; the follower is swapped on the
/// next pass, once the stored target covers the new set.
#[test]
fn an_internal_leader_moves_before_its_internal_follower_is_swapped() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(internal("core"));
    nodes.push(internal("core2"));
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut existing = assigned("orders", "core", &["a", "core2"]);
    existing.raft_target = Some(crate::model::RaftTarget {
        counter: 2,
        members: ["core", "a", "core2"]
            .iter()
            .map(|node| (node.to_string(), 7))
            .collect(),
    });
    existing.raft_formed = Some(crate::model::RaftFormed {
        incarnation: existing.incarnation,
        epoch: 0,
    });
    // The leader leads from a member that is live; only core2 is internal
    // among the followers, but the leader is internal too.
    let first = plan_with(
        &streams,
        &[],
        &nodes,
        &[existing.clone()],
        &NothingCaughtUp,
        raft(),
    );
    let (_, step, written) = first.moves().next().expect("a move");
    assert!(
        matches!(step, MoveStep::RaftLeader { to, .. } if to == "a"),
        "{step:?}"
    );

    // Next pass: the leader is a, its follower core is still a member.
    let mut next = written.clone();
    next.raft_target = existing.raft_target.clone();
    let second = plan_with(&streams, &[], &nodes, &[next], &NothingCaughtUp, raft());
    let (_, step, _) = second.moves().next().expect("a swap");
    assert!(
        matches!(step, MoveStep::RaftReplaceInternal { .. }),
        "{step:?}"
    );
}

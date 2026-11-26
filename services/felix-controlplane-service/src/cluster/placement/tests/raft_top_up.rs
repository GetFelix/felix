//! A Raft set short of its replication factor grows back to it, one
//! member a write, through the group's membership target.
use super::super::raft_leaders::RAFT_REPLACEMENTS_PER_PASS;
use super::raft_leaders::{ABC, member_of, raft, set_of, settle};
use super::*;

/// `orders` shard 0 led by `leader` on `followers`, settled.
fn short(leader: &str, followers: &[&str]) -> ShardAssignment {
    let mut assignment = member_of(0, leader);
    assignment.replicas = followers.iter().map(|node| node.to_string()).collect();
    settle(&mut assignment);
    assignment
}

/// The one top-up the plan makes, as (broker added, the write).
fn the_top_up(plan: &Plan) -> (String, ShardAssignment) {
    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    match moves[0] {
        (_, MoveStep::RaftTopUp { to }, written) => (to.clone(), written.clone()),
        other => panic!("expected a top-up, got {other:?}"),
    }
}

/// **A shard left on one copy at RF 3 gets its copies back.** An abandon
/// places the leader alone, and nothing else adds a member, so `Quorum`
/// writes were acknowledged on that one copy for good. Each pass adds one
/// broker, only once the group has taken the last change, and the set
/// stops at the factor.
#[test]
fn a_raft_shard_on_one_copy_at_rf_3_grows_back_to_three_voters() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    let mut current = short("broker-a", &[]);

    for size in 2..=3 {
        let plan = plan_with(
            &streams,
            &[],
            &nodes,
            std::slice::from_ref(&current),
            &NothingCaughtUp,
            raft(),
        );
        let (to, written) = the_top_up(&plan);
        let mut expected = set_of(&current);
        assert!(expected.insert(to.clone()), "{to} was already a member");
        assert_eq!(set_of(&written), expected, "one member a write");
        assert_eq!(written.nodes().count(), size);
        assert_eq!(written.leader, "broker-a", "the leader keeps its seat");

        // Not settled yet: the next pass must not stack a second change.
        let held = plan_with(
            &streams,
            &[],
            &nodes,
            std::slice::from_ref(&written),
            &NothingCaughtUp,
            raft(),
        );
        assert_eq!(held.moves().count(), 0, "{held:?}");

        current = written;
        settle(&mut current);
    }

    let voters = &current.raft_voters.as_ref().unwrap().members;
    assert_eq!(voters.len(), 3, "{current:?}");
    let full = plan_with(
        &streams,
        &[],
        &nodes,
        std::slice::from_ref(&current),
        &NothingCaughtUp,
        raft(),
    );
    assert!(
        full.moves()
            .all(|(_, step, _)| !matches!(step, MoveStep::RaftTopUp { .. })),
        "a full set grows no further: {full:?}"
    );
}

/// The joint change needs a majority of both sets, so a set with a member
/// down is not grown.
#[test]
fn a_short_raft_set_with_a_member_down_is_not_grown() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut nodes = live(&ABC);
    nodes[1].status.lifecycle = NodeLifecycle::Down;
    let existing = vec![short("broker-a", &["broker-b"])];

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
}

/// Paused, placement adds no copy, as for a hybrid restore.
#[test]
fn a_paused_placement_does_not_grow_a_short_raft_set() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let policy = MovePolicy {
        paused: true,
        ..raft()
    };
    let existing = vec![short("broker-a", &[])];

    let plan = plan_with(
        &streams,
        &[],
        &live(&ABC),
        &existing,
        &NothingCaughtUp,
        policy,
    );

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert!(
        plan.waiting().any(|(_, why)| why == &Blocked::Paused),
        "{plan:?}"
    );
}

/// The new copy goes to a zone the set lacks when there is one.
#[test]
fn a_short_raft_set_grows_into_a_zone_it_lacks() {
    let zoned = |id: &str, zone: &str| {
        let mut n = node(id, NodeLifecycle::Live, None);
        n.spec.zone = Some(zone.to_string());
        n
    };
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        zoned("broker-a", "z1"),
        zoned("broker-b", "z2"),
        zoned("broker-c", "z1"),
        zoned("broker-d", "z3"),
    ];
    let existing = vec![short("broker-a", &["broker-b"])];

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    assert_eq!(the_top_up(&plan).0, "broker-d");
}

/// `orders` shard 0 on `broker-a` and `broker-d`, with the target stored
/// and formed for that set but only `broker-a` reported voting: `broker-d`
/// is a learner that has not caught up, as one stays across a re-form's
/// gap (#3005).
fn learner_stuck() -> ShardAssignment {
    let mut assignment = short("broker-a", &["broker-d"]);
    assignment
        .raft_voters
        .as_mut()
        .unwrap()
        .members
        .retain(|node, _| node == "broker-a");
    assignment
}

/// **2 to 3 waits for the second copy to vote.** Adding a third member
/// while the voters are only `broker-a` would leave them a third of the
/// next set, so the write is held and nothing is written.
#[test]
fn a_short_raft_set_whose_newest_member_is_not_voting_is_held() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[learner_stuck()],
        &NothingCaughtUp,
        raft(),
    );

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    let waits: Vec<_> = plan.waiting().map(|(_, why)| why.clone()).collect();
    assert_eq!(waits, vec![Blocked::Unapplied], "{plan:?}");
}

/// A learner that has not caught up is no copy for the replication count,
/// so a shard stuck with one shows as under-replicated.
#[test]
fn a_raft_learner_is_not_counted_as_a_copy() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = live(&["broker-a", "broker-d"]);

    let stuck = plan_with(
        &streams,
        &[],
        &nodes,
        &[learner_stuck()],
        &NothingCaughtUp,
        raft(),
    );
    assert_eq!(
        stuck.under_replicated,
        vec![learner_stuck().key],
        "{stuck:?}"
    );
    assert_eq!(stuck.missing_copies, 1);

    let voting = plan_with(
        &streams,
        &[],
        &nodes,
        &[short("broker-a", &["broker-d"])],
        &NothingCaughtUp,
        raft(),
    );
    assert!(voting.under_replicated.is_empty(), "{voting:?}");
}

/// A down leader is not grown around: the group cannot change its
/// membership without it.
#[test]
fn a_short_raft_set_with_its_leader_down_is_not_grown() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut nodes = live(&ABC);
    nodes[0].status.lifecycle = NodeLifecycle::Down;

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[short("broker-a", &[])],
        &NothingCaughtUp,
        raft(),
    );

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
}

/// A member that must leave is swapped out first, one change at a time;
/// the set grows on a later pass.
#[test]
fn a_short_raft_set_replaces_a_leaving_member_before_growing() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    nodes[1].status.lifecycle = NodeLifecycle::Draining;

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[short("broker-a", &["broker-b"])],
        &NothingCaughtUp,
        raft(),
    );

    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    assert!(
        matches!(moves[0].1, MoveStep::RaftReplace { from, .. } if from == "broker-b"),
        "{plan:?}"
    );
    assert_eq!(moves[0].2.nodes().count(), 2, "a swap keeps the size");
}

/// Top-ups share the per-pass budget of set changes.
#[test]
fn growing_short_raft_sets_takes_the_replacement_budget() {
    let shards = RAFT_REPLACEMENTS_PER_PASS as u32 + 1;
    let streams = vec![replicated_stream("orders", shards, 3)];
    let existing: Vec<ShardAssignment> = (0..shards)
        .map(|shard| {
            let mut assignment = short("broker-a", &[]);
            assignment.key.shard = shard;
            assignment
        })
        .collect();

    let plan = plan_with(
        &streams,
        &[],
        &live(&ABC),
        &existing,
        &NothingCaughtUp,
        raft(),
    );

    let grown = plan
        .moves()
        .filter(|(_, step, _)| matches!(step, MoveStep::RaftTopUp { .. }))
        .count();
    assert_eq!(grown, RAFT_REPLACEMENTS_PER_PASS, "{plan:?}");
    assert!(
        plan.waiting().any(|(_, why)| why == &Blocked::MoveLimit),
        "{plan:?}"
    );
}

//! A drain of a Raft shard's leader with no live member to hand to: the
//! draining broker keeps serving, so it stays leader and voter and moves
//! the shard off itself through the group's membership target.
use super::super::raft_leaders::RAFT_REPLACEMENTS_PER_PASS;
use super::*;
use crate::model::{RaftFormed, RaftTarget, RaftVoters};

fn raft() -> MovePolicy {
    MovePolicy {
        raft_streams: true,
        ..MovePolicy::default()
    }
}

/// Shard `shard` of `orders` led by `leader` on `followers`, formed, with
/// the target stored for that set and `voters` the reported voters.
fn shard_on(shard: u32, leader: &str, followers: &[&str], voters: &[&str]) -> ShardAssignment {
    let mut assignment = pinned("orders", shard, leader);
    assignment.replicas = followers.iter().map(|node| node.to_string()).collect();
    assignment.state = ShardState::Active;
    let members: BTreeMap<String, u64> = assignment.nodes().map(|n| (n.clone(), 7)).collect();
    assignment.raft_target = Some(RaftTarget {
        counter: 2,
        members: members.clone(),
    });
    assignment.raft_formed = Some(RaftFormed {
        incarnation: assignment.incarnation,
        epoch: 0,
    });
    assignment.raft_voters = Some(RaftVoters {
        incarnation: assignment.incarnation,
        epoch: 0,
        index: 1,
        members: members
            .iter()
            .filter(|(node, _)| voters.contains(&node.as_str()))
            .map(|(node, copy)| (node.clone(), *copy))
            .collect(),
    });
    assignment.raft_copy_ids = members;
    assignment
}

/// An RF 1 shard on `broker-a` alone, its one voter reported.
fn single(shard: u32) -> ShardAssignment {
    shard_on(shard, "broker-a", &[], &["broker-a"])
}

/// `written` as the store and the group leave it: the target stored for
/// its set and `voters` the reported voters.
fn applied(written: &ShardAssignment, voters: &[&str]) -> ShardAssignment {
    let followers: Vec<&str> = written.replicas.iter().map(String::as_str).collect();
    let mut next = shard_on(written.key.shard, &written.leader, &followers, voters);
    next.incarnation = written.incarnation;
    next
}

/// `ids` live, then `broker-a` set to `lifecycle`.
fn a_is(lifecycle: NodeLifecycle, ids: &[&str]) -> Vec<Node> {
    let mut nodes = live(ids);
    nodes[0].status.lifecycle = lifecycle;
    nodes
}

fn only_waits(plan: &Plan) -> Vec<Blocked> {
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    plan.waiting().map(|(_, blocked)| blocked.clone()).collect()
}

fn the_move(plan: &Plan) -> (MoveStep, ShardAssignment) {
    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    (moves[0].1.clone(), moves[0].2.clone())
}

/// **A drain moves an RF 1 shard to a live broker.** The draining broker
/// still serves, so it stays leader and voter: the set grows to it and the
/// destination, waits while the destination's copy learns, and shrinks to
/// the destination, leading, only once the group reports it voting.
#[test]
fn a_drain_moves_a_one_member_raft_shard_to_a_live_broker() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let nodes = a_is(NodeLifecycle::Draining, &["broker-a", "broker-b"]);

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[single(0)],
        &NothingCaughtUp,
        raft(),
    );
    let (step, grown) = the_move(&plan);
    assert_eq!(
        step,
        MoveStep::RaftReplace {
            from: "broker-a".to_string(),
            to: "broker-b".to_string()
        }
    );
    assert_eq!(grown.leader, "broker-a", "the old copy keeps leading");
    assert_eq!(grown.replicas, vec!["broker-b".to_string()]);

    // The destination is a learner: nothing drops the old copy yet.
    let learning = applied(&grown, &["broker-a"]);
    let plan = plan_with(&streams, &[], &nodes, &[learning], &NothingCaughtUp, raft());
    assert_eq!(only_waits(&plan), vec![Blocked::Unapplied]);

    let voting = applied(&grown, &["broker-a", "broker-b"]);
    let plan = plan_with(&streams, &[], &nodes, &[voting], &NothingCaughtUp, raft());
    let (_, shrunk) = the_move(&plan);
    assert_eq!(shrunk.leader, "broker-b");
    assert!(shrunk.replicas.is_empty());

    let done = applied(&shrunk, &["broker-b"]);
    let plan = plan_with(&streams, &[], &nodes, &[done], &NothingCaughtUp, raft());
    assert_eq!(only_waits(&plan), vec![]);
}

/// **A destination that dies before it votes is dropped and another is
/// picked.** The set goes back to the draining leader alone and the next
/// pass grows it to the next live broker. One that dies after it votes
/// strands the shard: either set of one needs its vote.
#[test]
fn a_drain_move_picks_another_destination_when_the_first_dies() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let mut nodes = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c"],
    );
    nodes[1].status.lifecycle = NodeLifecycle::Down;

    let learning = shard_on(0, "broker-a", &["broker-b"], &["broker-a"]);
    let plan = plan_with(&streams, &[], &nodes, &[learning], &NothingCaughtUp, raft());
    let (step, undone) = the_move(&plan);
    assert!(matches!(step, MoveStep::RaftMoveUndone { .. }), "{plan:?}");
    assert_eq!(undone.leader, "broker-a");
    assert!(undone.replicas.is_empty());

    let back = applied(&undone, &["broker-a"]);
    let plan = plan_with(&streams, &[], &nodes, &[back], &NothingCaughtUp, raft());
    let (_, regrown) = the_move(&plan);
    assert_eq!(regrown.leader, "broker-a");
    assert_eq!(regrown.replicas, vec!["broker-c".to_string()]);

    let voting = shard_on(0, "broker-a", &["broker-b"], &["broker-a", "broker-b"]);
    let plan = plan_with(&streams, &[], &nodes, &[voting], &NothingCaughtUp, raft());
    assert_eq!(
        only_waits(&plan),
        vec![Blocked::MoveStranded {
            node: "broker-b".to_string()
        }]
    );
}

/// **A cancelled drain lets a started move finish.** The move is as safe
/// to finish as an operator's, so the shard ends on the destination; a
/// balance pass may move it back later.
#[test]
fn a_cancelled_drain_finishes_the_move_it_started() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let nodes = live(&["broker-a", "broker-b"]);

    let learning = shard_on(0, "broker-a", &["broker-b"], &["broker-a"]);
    let plan = plan_with(&streams, &[], &nodes, &[learning], &NothingCaughtUp, raft());
    assert_eq!(only_waits(&plan), vec![Blocked::Unapplied]);

    let voting = shard_on(0, "broker-a", &["broker-b"], &["broker-a", "broker-b"]);
    let plan = plan_with(&streams, &[], &nodes, &[voting], &NothingCaughtUp, raft());
    let (_, shrunk) = the_move(&plan);
    assert_eq!(shrunk.leader, "broker-b");
    assert!(shrunk.replicas.is_empty());
}

/// **With no live broker to move to, the shard waits.** A down broker is
/// no destination, and nor is another draining one.
#[test]
fn a_drain_of_a_one_member_raft_shard_waits_for_a_live_broker() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    for other in [NodeLifecycle::Down, NodeLifecycle::Draining] {
        let mut nodes = a_is(NodeLifecycle::Draining, &["broker-a", "broker-b"]);
        nodes[1].status.lifecycle = other;
        let plan = plan_with(
            &streams,
            &[],
            &nodes,
            &[single(0)],
            &NothingCaughtUp,
            raft(),
        );
        assert_eq!(only_waits(&plan), vec![Blocked::NoDestination], "{other:?}");
    }
}

/// **The move is a set change: a pause holds it and the budget caps it.**
#[test]
fn a_drain_of_one_member_raft_shards_is_paused_and_paced() {
    let count = RAFT_REPLACEMENTS_PER_PASS as u32 + 3;
    let streams = vec![replicated_stream("orders", count, 1)];
    let existing: Vec<_> = (0..count).map(single).collect();
    let nodes = a_is(NodeLifecycle::Draining, &["broker-a", "broker-b"]);

    let paused = MovePolicy {
        paused: true,
        ..raft()
    };
    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, paused);
    assert!(
        only_waits(&plan).iter().all(|b| *b == Blocked::Paused),
        "{plan:?}"
    );

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());
    assert_eq!(plan.moves().count(), RAFT_REPLACEMENTS_PER_PASS, "{plan:?}");
    assert_eq!(
        plan.waiting()
            .filter(|(_, b)| **b == Blocked::MoveLimit)
            .count(),
        3,
        "{plan:?}"
    );
}

/// **A larger set whose every member drains still gets off them.** No
/// member is live to lead, so the draining leader keeps the lead and swaps
/// a draining follower for a live broker; the next pass names that broker
/// leader, and the leader's own copy is swapped out after.
#[test]
fn a_drain_of_every_member_swaps_a_follower_for_a_live_broker() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let mut nodes = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c"],
    );
    nodes[1].status.lifecycle = NodeLifecycle::Draining;

    let pair = shard_on(0, "broker-a", &["broker-b"], &["broker-a", "broker-b"]);
    let plan = plan_with(&streams, &[], &nodes, &[pair], &NothingCaughtUp, raft());
    let (step, swapped) = the_move(&plan);
    assert_eq!(
        step,
        MoveStep::RaftReplace {
            from: "broker-b".to_string(),
            to: "broker-c".to_string()
        }
    );
    assert_eq!(swapped.leader, "broker-a");
    assert_eq!(swapped.replicas, vec!["broker-c".to_string()]);

    let joined = applied(&swapped, &["broker-a", "broker-c"]);
    let plan = plan_with(&streams, &[], &nodes, &[joined], &NothingCaughtUp, raft());
    let (step, renamed) = the_move(&plan);
    assert!(matches!(step, MoveStep::RaftLeader { .. }), "{plan:?}");
    assert_eq!(renamed.leader, "broker-c");
}

/// **An RF 3 drain moves the lead to a member, then swaps the old copy
/// out.** The draining broker is never left in the set at the end.
#[test]
fn an_rf3_drain_moves_the_lead_then_the_copy() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c", "broker-d"],
    );
    let all = ["broker-a", "broker-b", "broker-c"];
    let three = shard_on(0, "broker-a", &["broker-b", "broker-c"], &all);

    let plan = plan_with(&streams, &[], &nodes, &[three], &NothingCaughtUp, raft());
    let (step, renamed) = the_move(&plan);
    assert!(matches!(step, MoveStep::RaftLeader { .. }), "{plan:?}");
    assert_ne!(renamed.leader, "broker-a");

    let led = applied(&renamed, &all);
    let plan = plan_with(&streams, &[], &nodes, &[led], &NothingCaughtUp, raft());
    let (step, swapped) = the_move(&plan);
    assert_eq!(
        step,
        MoveStep::RaftReplace {
            from: "broker-a".to_string(),
            to: "broker-d".to_string()
        }
    );
    assert!(!swapped.nodes().any(|n| n == "broker-a"), "{swapped:?}");
}

/// **A draining broker that dies mid-move is not dropped before the new
/// copy votes.** Until then it holds the only full copy, so the shard waits
/// for it rather than shrinking to the learner.
#[test]
fn a_drain_move_waits_for_a_dead_old_copy_until_the_destination_votes() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let nodes = a_is(NodeLifecycle::Down, &["broker-a", "broker-b"]);

    let learning = shard_on(0, "broker-a", &["broker-b"], &["broker-a"]);
    let plan = plan_with(&streams, &[], &nodes, &[learning], &NothingCaughtUp, raft());
    assert_eq!(only_waits(&plan), vec![Blocked::Unapplied]);
}

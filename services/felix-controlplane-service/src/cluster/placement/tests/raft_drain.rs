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

/// **A destination that dies before it votes is undone whether or not its
/// copy opened.** With no copy id the target is never stored, which is the
/// shape a dead seat is given away in. The RF 1 move's undo runs first for
/// either shape, so the draining broker stays leader and voter, the set goes
/// back to it alone, and the next pass grows it to another live broker.
#[test]
fn a_drain_move_whose_destination_dies_unopened_is_undone_first() {
    use super::restore::{MINUTE, Reports, gone};
    let streams = vec![replicated_stream("orders", 1, 1)];
    let opened = shard_on(0, "broker-a", &["broker-b"], &["broker-a"]);
    let mut unopened = opened.clone();
    unopened.raft_target = None;
    unopened.raft_copy_ids.remove("broker-b");
    let a_only: BTreeSet<String> = ["broker-a".to_string()].into();

    let mut down = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c"],
    );
    down[1].status.lifecycle = NodeLifecycle::Down;
    let mut lost = a_is(NodeLifecycle::Draining, &["broker-a", "broker-c"]);
    lost.push(gone("broker-b", 10 * MINUTE));

    for nodes in [&down, &lost] {
        for existing in [&opened, &unopened] {
            let plan = plan_with(
                &streams,
                &[],
                nodes,
                std::slice::from_ref(existing),
                &Reports::default(),
                raft(),
            );
            let (step, undone) = the_move(&plan);
            assert_eq!(
                step,
                MoveStep::RaftMoveUndone {
                    node: "broker-b".to_string()
                },
                "{plan:?}"
            );
            assert_eq!(undone.leader, "broker-a");
            assert_eq!(undone.nodes().cloned().collect::<BTreeSet<_>>(), a_only);
            // The store's checks take the undo: the reported voter keeps a
            // majority of the set of it alone.
            assert!(!existing.holds_formed_set(&a_only));
            assert!(!existing.holds_set_from(&undone.leader, &undone.replicas));

            let back = applied(&undone, &["broker-a"]);
            let plan = plan_with(&streams, &[], nodes, &[back], &Reports::default(), raft());
            let (_, regrown) = the_move(&plan);
            assert_eq!(regrown.leader, "broker-a");
            assert_eq!(regrown.replicas, vec!["broker-c".to_string()]);
        }
    }
}

/// Walks a draining short set from `existing` until placement writes no
/// more, applying each write first with only the voters it kept (the new
/// member learning) and then with every member voting. Checks on the way
/// that the store takes every write and that `broker-a` leaves only once
/// a copy that votes takes the lead. Returns the last record.
fn walk_drain(streams: &[Stream], nodes: &[Node], existing: ShardAssignment) -> ShardAssignment {
    let mut record = existing;
    for _ in 0..8 {
        let plan = plan_with(
            streams,
            &[],
            nodes,
            std::slice::from_ref(&record),
            &NothingCaughtUp,
            raft(),
        );
        if plan.moves().count() == 0 {
            assert_eq!(only_waits(&plan), vec![], "{record:?}");
            return record;
        }
        let (step, next) = the_move(&plan);
        let set = next.raft_set();
        assert!(!record.holds_formed_set(&set), "store refuses {step:?}");
        if !set.contains("broker-a") {
            assert_ne!(record.leader, "broker-a", "dropped while leading: {step:?}");
            assert!(record.reports_voting(&record.leader), "{record:?}");
        }
        let kept: Vec<&str> = set
            .iter()
            .map(String::as_str)
            .filter(|node| record.reports_voting(node))
            .collect();
        // While the newcomer learns, nothing drops a copy and no non-voter
        // is named leader.
        if kept.len() < set.len() {
            let learning = applied(&next, &kept);
            let plan = plan_with(
                streams,
                &[],
                nodes,
                std::slice::from_ref(&learning),
                &NothingCaughtUp,
                raft(),
            );
            for (_, step, after) in plan.moves() {
                assert!(kept.contains(&after.leader.as_str()), "{step:?}");
                assert!(after.raft_set().is_superset(&set), "{step:?}");
            }
        }
        let all: Vec<&str> = set.iter().map(String::as_str).collect();
        record = applied(&next, &all);
    }
    panic!("the drain did not settle: {record:?}");
}

/// **An RF 3 set down to its draining leader grows, hands over and leaves
/// (#3303).** No follower exists to swap, so the set grows by one live
/// broker while the old copy leads and votes; the lead moves once the new
/// copy votes, the old copy is swapped out, and a top-up brings the set
/// back to three voters without it.
#[test]
fn a_drain_of_an_rf3_shard_down_to_its_leader_reaches_three_voters_without_it() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c", "broker-d"],
    );
    let alone = shard_on(0, "broker-a", &[], &["broker-a"]);

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        std::slice::from_ref(&alone),
        &NothingCaughtUp,
        raft(),
    );
    let (step, grown) = the_move(&plan);
    assert!(matches!(step, MoveStep::RaftTopUp { .. }), "{step:?}");
    assert_eq!(grown.leader, "broker-a", "the old copy keeps leading");
    assert_eq!(grown.replicas.len(), 1);

    let done = walk_drain(&streams, &nodes, alone);
    assert_eq!(done.raft_set().len(), 3, "{done:?}");
    assert!(!done.raft_set().contains("broker-a"), "{done:?}");
}

/// **An RF 2 set down to its draining leader ends on two live brokers.**
#[test]
fn a_drain_of_an_rf2_shard_down_to_its_leader_moves_off_it() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c"],
    );
    let done = walk_drain(&streams, &nodes, single(0));
    assert_eq!(done.raft_set().len(), 2, "{done:?}");
    assert!(!done.raft_set().contains("broker-a"), "{done:?}");
}

/// **The old copy stays until the new one votes.** While the grown set's
/// newcomer learns, the draining leader keeps the lead and its seat; the
/// store would refuse any set without it, as the reported voters (it
/// alone) hold no majority of one.
#[test]
fn a_short_drain_keeps_the_old_copy_until_the_new_one_votes() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c"],
    );
    let learning = shard_on(0, "broker-a", &["broker-b"], &["broker-a"]);
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        std::slice::from_ref(&learning),
        &NothingCaughtUp,
        raft(),
    );
    assert_eq!(only_waits(&plan), vec![Blocked::Unapplied]);
    for without_a in [&["broker-b"][..], &["broker-b", "broker-c"]] {
        let set = without_a.iter().map(|n| n.to_string()).collect();
        assert!(learning.holds_formed_set(&set), "{without_a:?}");
    }

    let voting = shard_on(0, "broker-a", &["broker-b"], &["broker-a", "broker-b"]);
    let plan = plan_with(&streams, &[], &nodes, &[voting], &NothingCaughtUp, raft());
    let (step, renamed) = the_move(&plan);
    assert!(matches!(step, MoveStep::RaftLeader { .. }), "{step:?}");
    assert_eq!(renamed.leader, "broker-b");
    assert_eq!(renamed.replicas, vec!["broker-a".to_string()]);
}

/// **A newcomer that dies before it votes holds the shard until it is
/// lost, then its seat is given away.** The draining broker stays leader
/// throughout, so the shard is never named onto a copy that never led.
#[test]
fn a_short_drain_whose_newcomer_dies_gives_its_seat_away_once_lost() {
    use super::restore::{MINUTE, Reports, gone};
    let streams = vec![replicated_stream("orders", 1, 3)];
    let learning = shard_on(0, "broker-a", &["broker-b"], &["broker-a"]);

    let mut down = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c"],
    );
    down[1].status.lifecycle = NodeLifecycle::Down;
    let plan = plan_with(
        &streams,
        &[],
        &down,
        std::slice::from_ref(&learning),
        &NothingCaughtUp,
        raft(),
    );
    assert_eq!(
        only_waits(&plan),
        vec![Blocked::MemberDown {
            node: "broker-b".to_string(),
            why: Leaving::Draining
        }]
    );

    let mut lost = a_is(NodeLifecycle::Draining, &["broker-a", "broker-c"]);
    lost.push(gone("broker-b", 10 * MINUTE));
    let plan = plan_with(
        &streams,
        &[],
        &lost,
        std::slice::from_ref(&learning),
        &Reports::default(),
        raft(),
    );
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
    assert!(!learning.holds_formed_set(&swapped.raft_set()));
}

/// **A draining leader that dies mid-way is never dropped by a write the
/// store takes.** Its copy is the only one that votes, so every write
/// without it needs the newcomer's vote first.
#[test]
fn a_short_drain_never_drops_a_dead_old_copy_before_the_new_one_votes() {
    use super::restore::{MINUTE, Reports, gone};
    let streams = vec![replicated_stream("orders", 1, 3)];
    let learning = shard_on(0, "broker-a", &["broker-b"], &["broker-a"]);
    let down = a_is(NodeLifecycle::Down, &["broker-a", "broker-b", "broker-c"]);
    let mut lost = live(&["broker-b", "broker-c"]);
    lost.push(gone("broker-a", 10 * MINUTE));
    for nodes in [&down, &lost] {
        let plan = plan_with(
            &streams,
            &[],
            nodes,
            std::slice::from_ref(&learning),
            &Reports::default(),
            raft(),
        );
        for (_, step, next) in plan.moves() {
            let set = next.raft_set();
            assert!(
                set.contains("broker-a") || learning.holds_formed_set(&set),
                "{step:?}"
            );
        }
    }
}

/// **A cancelled drain keeps topping up with the old leader in place.**
#[test]
fn a_cancelled_short_drain_tops_up_around_the_leader() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let learning = shard_on(0, "broker-a", &["broker-b"], &["broker-a"]);
    let plan = plan_with(&streams, &[], &nodes, &[learning], &NothingCaughtUp, raft());
    assert_eq!(only_waits(&plan), vec![Blocked::Unapplied]);

    let voting = shard_on(0, "broker-a", &["broker-b"], &["broker-a", "broker-b"]);
    let plan = plan_with(&streams, &[], &nodes, &[voting], &NothingCaughtUp, raft());
    let (step, grown) = the_move(&plan);
    assert_eq!(
        step,
        MoveStep::RaftTopUp {
            to: "broker-c".to_string()
        }
    );
    assert_eq!(grown.leader, "broker-a");
}

/// **With no live broker the short set waits, and moves once one is.**
#[test]
fn a_short_drain_waits_for_a_live_broker_then_proceeds() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut nodes = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c"],
    );
    nodes[1].status.lifecycle = NodeLifecycle::Down;
    nodes[2].status.lifecycle = NodeLifecycle::Draining;
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[single(0)],
        &NothingCaughtUp,
        raft(),
    );
    assert_eq!(only_waits(&plan), vec![Blocked::NoDestination]);

    nodes[1].status.lifecycle = NodeLifecycle::Live;
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
        MoveStep::RaftTopUp {
            to: "broker-b".to_string()
        }
    );
    assert_eq!(grown.leader, "broker-a");
}

/// **The grow is a set change: a pause holds it and the budget caps it.**
#[test]
fn a_short_drain_is_paused_and_paced() {
    let count = RAFT_REPLACEMENTS_PER_PASS as u32 + 3;
    let streams = vec![replicated_stream("orders", count, 3)];
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

/// **A draining leader that crashes while its newcomer learns keeps its
/// name.** A learner cannot be elected, so naming it would leave the shard
/// on it if it then died too, with nothing to move it on. The shard waits
/// for the old leader; once it is back and the newcomer votes, the drain
/// goes on.
#[test]
fn a_short_drain_keeps_a_crashed_leader_named_over_a_learner() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let learning = shard_on(0, "broker-a", &["broker-b"], &["broker-a"]);
    let crashed = a_is(NodeLifecycle::Down, &["broker-a", "broker-b", "broker-c"]);
    let plan = plan_with(
        &streams,
        &[],
        &crashed,
        std::slice::from_ref(&learning),
        &NothingCaughtUp,
        raft(),
    );
    assert_eq!(plan.moves().count(), 0, "{plan:?}");

    // Back and still draining, with the learner down: no rename onto it.
    let mut back = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c"],
    );
    back[1].status.lifecycle = NodeLifecycle::Down;
    let plan = plan_with(
        &streams,
        &[],
        &back,
        std::slice::from_ref(&learning),
        &NothingCaughtUp,
        raft(),
    );
    assert!(matches!(
        only_waits(&plan).as_slice(),
        [Blocked::MemberDown { .. }]
    ));

    back[1].status.lifecycle = NodeLifecycle::Live;
    let voting = shard_on(0, "broker-a", &["broker-b"], &["broker-a", "broker-b"]);
    let plan = plan_with(&streams, &[], &back, &[voting], &NothingCaughtUp, raft());
    let (step, renamed) = the_move(&plan);
    assert!(matches!(step, MoveStep::RaftLeader { .. }), "{step:?}");
    assert_eq!(renamed.leader, "broker-b");
}

/// **A voter report from before the grow still holds the old copy.** The
/// grow write carries the report of the set of one forward. Until a report
/// at the grown set names the newcomer voting, no write drops the draining
/// broker or names the newcomer, and the store refuses a set without it.
#[test]
fn a_short_drain_holds_on_a_voter_report_from_before_the_grow() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = a_is(
        NodeLifecycle::Draining,
        &["broker-a", "broker-b", "broker-c"],
    );
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[single(0)],
        &NothingCaughtUp,
        raft(),
    );
    let (_, grown) = the_move(&plan);
    assert_eq!(grown.replicas, vec!["broker-b".to_string()]);

    // As the store leaves the write: copy ids and target dropped, then b's
    // copy reported and the target stored, the old report kept throughout.
    let mut unreported = grown.clone();
    unreported.raft_target = None;
    unreported
        .raft_copy_ids
        .retain(|node, _| node == "broker-a");
    let mut stored = grown.clone();
    stored.raft_copy_ids.insert("broker-b".to_string(), 7);
    stored.raft_target = Some(RaftTarget {
        counter: 3,
        members: stored.raft_copy_ids.clone(),
    });
    for stale in [&unreported, &stored] {
        assert_eq!(
            stale.raft_voters.as_ref().map(|v| v.members.len()),
            Some(1),
            "the report from before the grow"
        );
        let plan = plan_with(
            &streams,
            &[],
            &nodes,
            std::slice::from_ref(stale),
            &NothingCaughtUp,
            raft(),
        );
        for (_, step, next) in plan.moves() {
            assert_eq!(next.leader, "broker-a", "{step:?}");
            assert!(next.raft_set().contains("broker-a"), "{step:?}");
        }
        for without_a in [&["broker-b"][..], &["broker-b", "broker-c"]] {
            let set = without_a.iter().map(|n| n.to_string()).collect();
            assert!(stale.holds_formed_set(&set), "{without_a:?}");
        }
    }

    let mut reported = stored.clone();
    let voters = reported.raft_voters.as_mut().unwrap();
    voters.index += 1;
    voters.members = reported.raft_copy_ids.clone();
    let plan = plan_with(&streams, &[], &nodes, &[reported], &NothingCaughtUp, raft());
    let (step, renamed) = the_move(&plan);
    assert!(matches!(step, MoveStep::RaftLeader { .. }), "{step:?}");
    assert_eq!(renamed.leader, "broker-b");
}

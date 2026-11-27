//! Shards a Raft group runs: placement names the member it wants leading
//! with one write that changes only `leader`, and leaves failover to the
//! group. The set changes one member at a time, through the group's
//! membership target.
use super::super::raft_leaders::RAFT_REPLACEMENTS_PER_PASS;
use super::*;
use crate::model::RaftTarget;
use crate::store::ControlPlaneStore;

pub(super) const ABC: [&str; 3] = ["broker-a", "broker-b", "broker-c"];

pub(super) fn raft() -> MovePolicy {
    MovePolicy {
        raft_streams: true,
        ..MovePolicy::default()
    }
}

/// Shard `shard` of `orders`, led by `leader` with every other broker of
/// `ABC` a member, a target the group was told, and the group formed.
pub(super) fn member_of(shard: u32, leader: &str) -> ShardAssignment {
    let mut assignment = pinned("orders", shard, leader);
    assignment.replicas = ABC
        .iter()
        .filter(|node| **node != leader)
        .map(|node| node.to_string())
        .collect();
    assignment.state = ShardState::Active;
    assignment.raft_target = Some(RaftTarget {
        counter: 2,
        members: ABC.iter().map(|node| (node.to_string(), 7)).collect(),
    });
    assignment.raft_formed = Some(crate::model::RaftFormed {
        incarnation: assignment.incarnation,
        epoch: 0,
    });
    // The group applied its target, so one-member swaps go ahead.
    assignment.raft_voters = Some(crate::model::RaftVoters {
        incarnation: assignment.incarnation,
        epoch: 0,
        index: 1,
        members: assignment.raft_target.as_ref().unwrap().members.clone(),
    });
    assignment.raft_copy_ids = assignment.raft_target.as_ref().unwrap().members.clone();
    assignment
}

pub(super) fn set_of(assignment: &ShardAssignment) -> BTreeSet<String> {
    assignment.nodes().cloned().collect()
}

/// Each decision's write, checked to change only the leader, to a member.
fn leader_writes(plan: &Plan, existing: &[ShardAssignment]) -> Vec<ShardAssignment> {
    plan.moves()
        .map(|(key, step, written)| {
            let before = existing.iter().find(|a| &a.key == key).expect("read");
            match step {
                MoveStep::RaftLeader { from, to } => {
                    assert_eq!(from, &before.leader);
                    assert_eq!(to, &written.leader);
                }
                other => panic!("expected a leader-only write, got {other:?}"),
            }
            assert!(before.replicas.contains(&written.leader));
            assert_eq!(set_of(written), set_of(before), "the set stays");
            assert_eq!(written.raft_target, before.raft_target);
            assert_eq!(written.successor, None);
            assert_eq!(written.joining, None);
            assert_eq!(written.state, before.state);
            written.clone()
        })
        .collect()
}

fn leaders_on(existing: &[ShardAssignment], node: &str) -> usize {
    existing.iter().filter(|a| a.leader == node).count()
}

#[test]
fn a_balance_pass_moves_a_raft_leader_in_one_write() {
    let streams = vec![replicated_stream("orders", 3, 3)];
    let existing: Vec<ShardAssignment> = (0..3).map(|shard| member_of(shard, "broker-a")).collect();

    let plan = plan_with(
        &streams,
        &[],
        &live(&ABC),
        &existing,
        &NothingCaughtUp,
        raft(),
    );

    let written = leader_writes(&plan, &existing);
    assert_eq!(written.len(), 2, "{plan:?}");
    let after: Vec<ShardAssignment> = existing
        .iter()
        .map(|a| {
            written
                .iter()
                .find(|w| w.key == a.key)
                .cloned()
                .unwrap_or_else(|| a.clone())
        })
        .collect();
    for node in ABC {
        assert_eq!(leaders_on(&after, node), 1, "{node} leads one");
    }
    assert_eq!(plan.to_place().count(), 0);
    assert_eq!(plan.waiting().count(), 0);

    // Balanced: the next pass writes nothing.
    let again = plan_with(&streams, &[], &live(&ABC), &after, &NothingCaughtUp, raft());
    assert_eq!(again.kept(), 3, "{again:?}");
}

#[test]
fn a_drain_moves_every_raft_leader_off_the_broker_in_one_pass() {
    let streams = vec![replicated_stream("orders", 4, 3)];
    let existing: Vec<ShardAssignment> = (0..4).map(|shard| member_of(shard, "broker-a")).collect();
    let mut nodes = live(&ABC);
    nodes[0].status.lifecycle = NodeLifecycle::Draining;

    // One move slot does not hold a leader-only write back.
    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    let written = leader_writes(&plan, &existing);
    assert_eq!(written.len(), 4, "{plan:?}");
    assert_eq!(leaders_on(&written, "broker-a"), 0);
    assert_eq!(
        leaders_on(&written, "broker-b"),
        2,
        "spread over the members"
    );
    assert_eq!(leaders_on(&written, "broker-c"), 2);

    // The leaders are off; the follower copies have no broker outside the
    // set to go to, so they wait and say why.
    let again = plan_with(&streams, &[], &nodes, &written, &NothingCaughtUp, raft());
    assert_eq!(again.moves().count(), 0, "{again:?}");
    assert!(
        again.waiting().all(|(_, why)| matches!(
            why,
            Blocked::NoReplacement { node, why: Leaving::Draining } if node == "broker-a"
        )),
        "{again:?}"
    );
}

#[test]
fn a_dead_raft_leader_is_renamed_to_a_live_member() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![member_of(0, "broker-a")];
    let mut nodes = live(&ABC);
    nodes[0].status.lifecycle = NodeLifecycle::Down;

    // Even paused: brokers with no member forward to the named leader.
    let paused = MovePolicy {
        paused: true,
        ..raft()
    };
    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, paused);

    let written = leader_writes(&plan, &existing);
    assert_eq!(written.len(), 1, "{plan:?}");
    assert_ne!(written[0].leader, "broker-a");
    assert_eq!(plan.unplaceable().count(), 0, "no alert: the group elects");

    // With no live member left there is nobody to name, and no alert.
    nodes[1].status.lifecycle = NodeLifecycle::Down;
    nodes[2].status.lifecycle = NodeLifecycle::Down;
    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());
    assert_eq!(plan.shards[0].decision, Decision::Kept);
    assert_eq!(plan.unplaceable().count(), 0);

    // The same shard on the hybrid waits for a caught-up follower.
    nodes[1].status.lifecycle = NodeLifecycle::Live;
    let hybrid = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &NothingCaughtUp,
        MovePolicy::default(),
    );
    assert_eq!(
        hybrid.shards[0].decision,
        Decision::Unplaceable(Unplaceable::NoCaughtUpReplica)
    );
}

#[test]
fn a_draining_hybrid_leader_still_stages_a_successor() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![member_of(0, "broker-a")];
    let mut nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    nodes[0].status.lifecycle = NodeLifecycle::Draining;

    // A stream in memory has no group: it takes the in-memory step.
    let in_memory = vec![Stream {
        durable: false,
        ..streams[0].clone()
    }];
    for (streams, policy) in [(&streams, MovePolicy::default()), (&in_memory, raft())] {
        let plan = plan_with(streams, &[], &nodes, &existing, &NothingCaughtUp, policy);
        assert!(
            !matches!(
                plan.shards[0].decision,
                Decision::Move(MoveStep::RaftLeader { .. }, _)
            ),
            "{plan:?}"
        );
    }
}

#[test]
fn a_raft_leader_only_moves_to_a_live_member() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let mut existing = member_of(0, "broker-a");
    existing.replicas = vec!["broker-b".to_string()];
    let mut nodes = live(&["broker-a", "broker-b", "broker-c"]);
    nodes[0].status.lifecycle = NodeLifecycle::Draining;
    nodes[1].status.lifecycle = NodeLifecycle::Down;

    let plan = plan_with(&streams, &[], &nodes, &[existing], &NothingCaughtUp, raft());

    // broker-c is live but holds no copy.
    assert_eq!(
        plan.shards[0].decision,
        Decision::Waiting(Blocked::NoDestination)
    );
}

#[test]
fn a_hybrid_move_left_on_a_raft_shard_is_dropped() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut staged = member_of(0, "broker-a");
    staged.replicas.push("broker-d".to_string());
    staged.successor = Some("broker-d".to_string());
    staged.move_reason = Some(crate::model::MoveReason::Drain);
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        std::slice::from_ref(&staged),
        &NothingCaughtUp,
        raft(),
    );

    match &plan.shards[0].decision {
        Decision::Move(MoveStep::RaftMoveDropped, written) => {
            assert_eq!(written.leader, "broker-a");
            assert_eq!(written.successor, None);
            assert_eq!(written.move_reason, None);
            assert_eq!(set_of(written), set_of(&member_of(0, "broker-a")));
        }
        other => panic!("expected the move dropped, got {other:?}"),
    }

    // Before its group forms the drop changes a held set, which the store
    // refuses, so placement waits rather than propose it every pass (#2794).
    staged.raft_formed = None;
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        std::slice::from_ref(&staged),
        &NothingCaughtUp,
        raft(),
    );
    assert_eq!(
        plan.shards[0].decision,
        Decision::Waiting(Blocked::Unformed)
    );
    assert_eq!(plan.moves().count(), 0, "no refused write is proposed");
}

#[test]
fn an_operator_moves_a_raft_leader_to_a_member_in_one_write() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![member_of(0, "broker-a")];
    let nodes = live(&ABC);
    let catalog = Catalog {
        streams: &streams,
        caches: &[],
        nodes: &nodes,
        existing: &existing,
        caught_up: &NothingCaughtUp,
        policy: MovePolicy {
            max_concurrent: 0,
            ..raft()
        },
    };

    // No move slot is needed: nothing is copied.
    let step = start_move(&catalog, &existing[0].key, "broker-b").expect("moves");
    assert_eq!(
        step.step,
        MoveStep::RaftLeader {
            from: "broker-a".to_string(),
            to: "broker-b".to_string()
        }
    );
    assert_eq!(step.assignment.leader, "broker-b");
    assert_eq!(set_of(&step.assignment), set_of(&existing[0]));
    assert_eq!(step.assignment.raft_target, existing[0].raft_target);
    assert_eq!(step.expected_generation, existing[0].generation);
}

/// An operator's leader move within the set is marked as an operator's,
/// so the group's leader hands over with one member down (#3234). With no
/// majority serving it could never land, so it is refused.
#[test]
fn an_operator_raft_leader_move_is_marked_and_refused_without_a_majority() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![member_of(0, "broker-a")];
    let moved = |down: &[&str]| {
        let mut nodes = live(&ABC);
        for node in &mut nodes {
            if down.contains(&node.node_id.as_str()) {
                node.status.lifecycle = NodeLifecycle::Down;
            }
        }
        let catalog = Catalog {
            streams: &streams,
            caches: &[],
            nodes: &nodes,
            existing: &existing,
            caught_up: &NothingCaughtUp,
            policy: raft(),
        };
        start_move(&catalog, &existing[0].key, "broker-b")
    };
    let step = moved(&["broker-c"]).expect("moves");
    assert_eq!(step.assignment.leader, "broker-b");
    assert_eq!(
        step.assignment.move_reason,
        Some(crate::model::MoveReason::Operator)
    );
    assert!(matches!(
        moved(&["broker-a", "broker-c"]).unwrap_err(),
        Refused::MemberDown(_)
    ));
}

/// **An operator move outside the set gives the destination the leader's
/// seat.** One write names it leader in place of the old leader and keeps
/// the followers, so the group's leader adds it as a learner and swaps the
/// voters; the group never runs below its factor.
#[test]
fn an_operator_moves_a_raft_leader_outside_its_set_by_swapping_the_seat() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![member_of(0, "broker-a")];
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    let catalog = Catalog {
        streams: &streams,
        caches: &[],
        nodes: &nodes,
        existing: &existing,
        caught_up: &NothingCaughtUp,
        policy: raft(),
    };

    let step = start_move(&catalog, &existing[0].key, "broker-d").expect("moves");
    assert_eq!(
        step.step,
        MoveStep::RaftReplace {
            from: "broker-a".to_string(),
            to: "broker-d".to_string()
        }
    );
    assert_eq!(step.assignment.leader, "broker-d");
    assert_eq!(
        step.assignment.replicas,
        vec!["broker-b".to_string(), "broker-c".to_string()]
    );
    assert_eq!(step.expected_generation, existing[0].generation);
}

/// An RF 1 shard of `orders` on `broker-a` alone, formed, its one voter
/// reported.
fn single_on_a() -> ShardAssignment {
    let mut single = member_of(0, "broker-a");
    single.replicas.clear();
    let members: BTreeMap<String, u64> = [("broker-a".to_string(), 7)].into();
    single.raft_target.as_mut().unwrap().members = members.clone();
    single.raft_voters.as_mut().unwrap().members = members.clone();
    single.raft_copy_ids = members;
    single
}

/// `single_on_a` grown by `broker-b` with its copy reported and the
/// target stored, voters as `voters` names them.
fn single_grown(voters: &[&str]) -> ShardAssignment {
    let mut grown = single_on_a();
    grown.replicas = vec!["broker-b".to_string()];
    let members: BTreeMap<String, u64> =
        [("broker-a".to_string(), 7), ("broker-b".to_string(), 9)].into();
    grown.raft_target = Some(RaftTarget {
        counter: 3,
        members: members.clone(),
    });
    grown.raft_copy_ids = members.clone();
    grown.raft_voters.as_mut().unwrap().members = members
        .into_iter()
        .filter(|(node, _)| voters.contains(&node.as_str()))
        .collect();
    grown
}

/// **An operator move of an RF 1 shard grows its set first (#3012).** A
/// set of the destination alone would make its empty copy the whole group,
/// so the leader stays and the destination joins beside it.
#[test]
fn an_operator_move_of_a_one_member_raft_shard_grows_its_set() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let existing = vec![single_on_a()];
    let nodes = live(&["broker-a", "broker-b"]);
    let catalog = Catalog {
        streams: &streams,
        caches: &[],
        nodes: &nodes,
        existing: &existing,
        caught_up: &NothingCaughtUp,
        policy: raft(),
    };

    let step = start_move(&catalog, &existing[0].key, "broker-b").expect("a move");
    assert_eq!(
        step.step,
        MoveStep::RaftReplace {
            from: "broker-a".to_string(),
            to: "broker-b".to_string()
        }
    );
    assert_eq!(step.assignment.leader, "broker-a");
    assert_eq!(step.assignment.replicas, vec!["broker-b".to_string()]);
    // A set over the factor is the move in progress: no second one.
    let grown = vec![single_grown(&["broker-a"])];
    let catalog = Catalog {
        existing: &grown,
        ..catalog
    };
    assert_eq!(
        start_move(&catalog, &grown[0].key, "broker-b").unwrap_err(),
        Refused::SetUnsettled
    );
}

/// **The grown set shrinks to the destination only once its copy votes.**
/// Before the leader reports it, the shard waits: a set of the destination
/// alone would be its empty copy. After, placement writes the destination
/// alone and names it leader.
#[test]
fn a_grown_one_member_raft_shard_shrinks_once_the_destination_votes() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let nodes = live(&["broker-a", "broker-b"]);

    let learning = vec![single_grown(&["broker-a"])];
    let plan = plan_with(&streams, &[], &nodes, &learning, &NothingCaughtUp, raft());
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert!(
        matches!(plan.waiting().next(), Some((_, Blocked::Unapplied))),
        "{plan:?}"
    );

    let voting = vec![single_grown(&["broker-a", "broker-b"])];
    let plan = plan_with(&streams, &[], &nodes, &voting, &NothingCaughtUp, raft());
    let (_, step, written) = plan.moves().next().expect("the shrink");
    assert_eq!(
        step,
        &MoveStep::RaftReplace {
            from: "broker-a".to_string(),
            to: "broker-b".to_string()
        }
    );
    assert_eq!(written.leader, "broker-b");
    assert!(written.replicas.is_empty());
}

/// `broker-a` live and `broker-b` in `lifecycle`.
fn a_live_b(lifecycle: NodeLifecycle) -> Vec<Node> {
    let mut nodes = live(&["broker-a", "broker-b"]);
    nodes[1].status.lifecycle = lifecycle;
    nodes
}

/// **An RF 1 move whose destination dies before it votes is undone.** The
/// set goes back to the leader alone, which the group's one reported voter
/// keeps a majority of, so the shard does not wait on a learner that will
/// never catch up. A draining or departed destination is undone the same
/// way.
#[test]
fn a_one_member_raft_move_is_undone_when_its_learner_is_lost() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let learning = vec![single_grown(&["broker-a"])];
    for lifecycle in [
        NodeLifecycle::Down,
        NodeLifecycle::Draining,
        NodeLifecycle::Left,
    ] {
        let nodes = a_live_b(lifecycle);
        let plan = plan_with(&streams, &[], &nodes, &learning, &NothingCaughtUp, raft());
        let (_, step, written) = plan.moves().next().expect("the undo");
        assert_eq!(
            step,
            &MoveStep::RaftMoveUndone {
                node: "broker-b".to_string()
            },
            "{lifecycle:?}"
        );
        assert_eq!(written.leader, "broker-a");
        assert!(written.replicas.is_empty());
    }
}

/// **Once the destination votes, a down one is not dropped.** Either set of
/// one needs its vote in a group of two, so placement names the shard
/// stranded and leaves it for the broker to come back or a re-form. A
/// draining voter still serves and votes for its own removal, so the move
/// is undone.
#[test]
fn a_one_member_raft_move_is_stranded_once_a_down_destination_votes() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let voting = vec![single_grown(&["broker-a", "broker-b"])];

    let nodes = a_live_b(NodeLifecycle::Down);
    let plan = plan_with(&streams, &[], &nodes, &voting, &NothingCaughtUp, raft());
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert!(
        matches!(
            plan.waiting().next(),
            Some((_, Blocked::MoveStranded { node })) if node == "broker-b"
        ),
        "{plan:?}"
    );

    let nodes = a_live_b(NodeLifecycle::Draining);
    let plan = plan_with(&streams, &[], &nodes, &voting, &NothingCaughtUp, raft());
    let (_, step, written) = plan.moves().next().expect("the undo");
    assert!(matches!(step, MoveStep::RaftMoveUndone { .. }), "{plan:?}");
    assert_eq!(written.leader, "broker-a");
}

/// **A drain of the old broker finishes the move, not blocks it.** With a
/// live destination the shard waits for it to vote and then shrinks to
/// it, as for any move.
#[test]
fn a_draining_leader_of_a_one_member_raft_move_still_shrinks_to_the_destination() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let mut nodes = live(&["broker-a", "broker-b"]);
    nodes[0].status.lifecycle = NodeLifecycle::Draining;
    let voting = vec![single_grown(&["broker-a", "broker-b"])];
    let plan = plan_with(&streams, &[], &nodes, &voting, &NothingCaughtUp, raft());
    let (_, _, written) = plan.moves().next().expect("the shrink");
    assert_eq!(written.leader, "broker-b");
    assert!(written.replicas.is_empty());
}

/// **An operator moves an RF 1 shard back while its destination learns.**
/// Naming the old leader again undoes the move; once the destination votes
/// and is down, that needs its vote and is refused.
#[test]
fn an_operator_moves_a_one_member_raft_shard_back() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let learning = vec![single_grown(&["broker-a"])];
    let nodes = live(&["broker-a", "broker-b"]);
    let catalog = Catalog {
        streams: &streams,
        caches: &[],
        nodes: &nodes,
        existing: &learning,
        caught_up: &NothingCaughtUp,
        policy: raft(),
    };
    let step = start_move(&catalog, &learning[0].key, "broker-a").expect("an undo");
    assert_eq!(
        step.step,
        MoveStep::RaftMoveUndone {
            node: "broker-b".to_string()
        }
    );
    assert!(step.assignment.replicas.is_empty());

    let voting = vec![single_grown(&["broker-a", "broker-b"])];
    let nodes = a_live_b(NodeLifecycle::Down);
    let catalog = Catalog {
        nodes: &nodes,
        existing: &voting,
        ..catalog
    };
    assert_eq!(
        start_move(&catalog, &voting[0].key, "broker-a").unwrap_err(),
        Refused::MemberDown("broker-b".to_string())
    );
}

/// One set change at a time, and only with every member serving: the joint
/// change needs a majority of both sets.
#[test]
fn an_operator_move_outside_a_raft_set_waits_for_a_settled_serving_set() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut unsettled = member_of(0, "broker-a");
    unsettled.raft_target = None;
    let mut learner = member_of(0, "broker-a");
    learner.learners.insert("broker-d".to_string());
    let mut nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    let refused = |existing: &ShardAssignment, nodes: &[Node]| {
        let existing = vec![existing.clone()];
        let catalog = Catalog {
            streams: &streams,
            caches: &[],
            nodes,
            existing: &existing,
            caught_up: &NothingCaughtUp,
            policy: raft(),
        };
        start_move(&catalog, &existing[0].key, "broker-d").unwrap_err()
    };

    assert_eq!(refused(&unsettled, &nodes), Refused::SetUnsettled);
    assert_eq!(
        refused(&learner, &nodes),
        Refused::NotAMember("broker-d".to_string())
    );
    nodes[2].status.lifecycle = NodeLifecycle::Down;
    assert_eq!(
        refused(&member_of(0, "broker-a"), &nodes),
        Refused::MemberDown("broker-c".to_string())
    );
}

/// **A drained broker's follower copy is replaced.** Its leader moves off on
/// one pass; the follower is swapped for a broker outside the set on the
/// next, through the group's membership target.
#[test]
fn a_drain_replaces_a_raft_follower_with_a_broker_outside_the_set() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![member_of(0, "broker-b")];
    let mut nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    nodes[0].status.lifecycle = NodeLifecycle::Draining;

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    let (_, step, written) = plan.moves().next().expect("a set change");
    assert_eq!(
        step,
        &MoveStep::RaftReplace {
            from: "broker-a".to_string(),
            to: "broker-d".to_string()
        }
    );
    assert_eq!(written.leader, "broker-b");
    assert_eq!(
        set_of(written),
        ["broker-b", "broker-c", "broker-d"]
            .iter()
            .map(|n| n.to_string())
            .collect()
    );
}

/// Shard `shard` of `stream` on `leader` and `followers`, with its target
/// stored for that set and the group formed on it.
fn seated(stream: &str, shard: u32, leader: &str, followers: &[&str]) -> ShardAssignment {
    let mut assignment = member_of(shard, leader);
    assignment.key.stream = stream.to_string();
    assignment.replicas = followers.iter().map(|node| node.to_string()).collect();
    settle(&mut assignment);
    assignment
}

/// `assignment` as the group leaves it once its set change is done: the
/// target stored for the set it names and every copy a reported voter.
pub(super) fn settle(assignment: &mut ShardAssignment) {
    let members: BTreeMap<String, u64> = assignment.nodes().map(|n| (n.clone(), 7)).collect();
    assignment.raft_target.as_mut().unwrap().members = members.clone();
    assignment.raft_voters.as_mut().unwrap().members = members.clone();
    assignment.raft_copy_ids = members;
}

fn pair_of(shard: u32, leader: &str, follower: &str) -> ShardAssignment {
    seated("orders", shard, leader, &[follower])
}

/// Two shards of `orders` led by `broker-b` and one by `broker-c`, each on
/// those two alone: `broker-a` holds no seat and leads two fewer.
fn drained_a() -> Vec<ShardAssignment> {
    vec![
        pair_of(0, "broker-c", "broker-b"),
        pair_of(1, "broker-b", "broker-c"),
        pair_of(2, "broker-b", "broker-c"),
    ]
}

/// **A broker back from a drain leads again (#3113).** The drain swapped it
/// out of every set, so no member can take a leader off a broker that leads
/// two more than it. Balance gives it a follower's seat and names it leader
/// in one write; the group's leader stays a voter and hands over once the
/// new copy votes.
#[test]
fn a_raft_leader_balances_onto_a_broker_that_holds_no_seat() {
    let streams = vec![replicated_stream("orders", 3, 2)];
    let existing = drained_a();

    let plan = plan_with(
        &streams,
        &[],
        &live(&ABC),
        &existing,
        &NothingCaughtUp,
        raft(),
    );

    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    let (key, step, written) = moves[0];
    let before = existing.iter().find(|a| &a.key == key).unwrap();
    assert_eq!(before.leader, "broker-b");
    assert_eq!(
        step,
        &MoveStep::RaftReplace {
            from: "broker-c".to_string(),
            to: "broker-a".to_string()
        }
    );
    assert_eq!(written.leader, "broker-a");
    assert_eq!(written.replicas, vec!["broker-b".to_string()]);
    assert_eq!(written.raft_target, before.raft_target);

    // Balanced once the write lands: the next pass moves nothing.
    // Settled, so only balance can hold it back, not a stale target.
    let after: Vec<ShardAssignment> = existing
        .iter()
        .map(|a| {
            let mut a = if &a.key == key {
                written.clone()
            } else {
                a.clone()
            };
            settle(&mut a);
            a
        })
        .collect();
    let again = plan_with(&streams, &[], &live(&ABC), &after, &NothingCaughtUp, raft());
    assert_eq!(again.moves().count(), 0, "{again:?}");
}

/// The seat moves only on a settled set: one change at a time.
#[test]
fn a_raft_leader_balances_outside_its_set_only_once_the_set_settles() {
    let streams = vec![replicated_stream("orders", 3, 2)];
    let mut existing = drained_a();
    for assignment in &mut existing {
        assignment.raft_target = None;
    }

    let plan = plan_with(
        &streams,
        &[],
        &live(&ABC),
        &existing,
        &NothingCaughtUp,
        raft(),
    );

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    let waiting: Vec<_> = plan.waiting().collect();
    assert!(
        !waiting.is_empty()
            && waiting
                .iter()
                .all(|(_, why)| matches!(why, Blocked::TargetUnsettled)),
        "{plan:?}"
    );
}

/// The reasons every shard of `plan` waits, with no move made.
fn only_waits(plan: &Plan) -> Vec<Blocked> {
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    plan.waiting().map(|(_, why)| why.clone()).collect()
}

/// Balance outside the set waits while a broker went down a moment ago,
/// as every balance move does.
#[test]
fn a_raft_leader_balances_outside_its_set_only_while_nothing_recovers() {
    const NOW: u64 = 10_000_000_000;
    struct Clocked;
    impl CaughtUp for Clocked {
        fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
            false
        }
        fn as_of_millis(&self) -> Option<u64> {
            Some(NOW)
        }
    }
    let streams = vec![replicated_stream("orders", 3, 2)];
    let mut nodes = live(&ABC);
    let mut e = node("broker-e", NodeLifecycle::Down, None);
    e.status.last_heartbeat_at_millis = NOW - 10_000;
    nodes.push(e);

    let plan = plan_with(&streams, &[], &nodes, &drained_a(), &Clocked, raft());

    assert!(only_waits(&plan).contains(&Blocked::Recovering), "{plan:?}");
}

/// A swap is a joint change, which needs every member serving.
#[test]
fn a_raft_leader_balances_outside_its_set_only_with_every_member_serving() {
    let streams = vec![replicated_stream("orders", 2, 2)];
    let existing = vec![
        pair_of(0, "broker-b", "broker-c"),
        pair_of(1, "broker-b", "broker-c"),
    ];
    let mut nodes = live(&ABC);
    nodes[2].status.lifecycle = NodeLifecycle::Down;

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    let waits = only_waits(&plan);
    assert!(
        !waits.is_empty() && waits.iter().all(|why| why == &Blocked::Recovering),
        "{plan:?}"
    );
}

#[test]
fn a_paused_placement_balances_no_raft_leader_outside_its_set() {
    let streams = vec![replicated_stream("orders", 3, 2)];
    let policy = MovePolicy {
        paused: true,
        ..raft()
    };

    let plan = plan_with(
        &streams,
        &[],
        &live(&ABC),
        &drained_a(),
        &NothingCaughtUp,
        policy,
    );

    assert!(only_waits(&plan).contains(&Blocked::Paused), "{plan:?}");
}

/// The swaps share the pass's budget of set changes: seventeen streams that
/// each want one start sixteen.
#[test]
fn balancing_outside_raft_sets_takes_the_replacement_budget() {
    let names: Vec<String> = (0..17).map(|n| format!("s{n:02}")).collect();
    let streams: Vec<Stream> = names
        .iter()
        .map(|name| replicated_stream(name, 3, 2))
        .collect();
    let existing: Vec<ShardAssignment> = names
        .iter()
        .flat_map(|name| {
            [
                seated(name, 0, "broker-c", &["broker-b"]),
                seated(name, 1, "broker-b", &["broker-c"]),
                seated(name, 2, "broker-b", &["broker-c"]),
            ]
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

    let swaps = plan
        .moves()
        .filter(|(_, step, _)| matches!(step, MoveStep::RaftReplace { .. }))
        .count();
    assert_eq!(swaps, RAFT_REPLACEMENTS_PER_PASS, "{plan:?}");
    assert!(
        plan.waiting().any(|(_, why)| why == &Blocked::MoveLimit),
        "{plan:?}"
    );
}

/// The store would refuse a set the reported voters hold no majority of,
/// so placement waits instead of proposing it every pass.
#[test]
fn a_raft_leader_balances_outside_its_set_only_onto_a_set_the_voters_hold() {
    let streams = vec![replicated_stream("orders", 3, 2)];
    let mut existing = drained_a();
    for assignment in &mut existing {
        // `broker-b`'s copy is not the one the voters reported.
        assignment.raft_copy_ids.insert("broker-b".to_string(), 8);
    }

    let plan = plan_with(
        &streams,
        &[],
        &live(&ABC),
        &existing,
        &NothingCaughtUp,
        raft(),
    );

    assert!(only_waits(&plan).contains(&Blocked::Unapplied), "{plan:?}");
}

/// A broker at its shard limit takes no seat.
#[test]
fn a_raft_leader_balances_only_onto_a_broker_with_room() {
    let streams = vec![replicated_stream("orders", 3, 2)];
    let mut nodes = live(&ABC);
    nodes[0].spec.capacity.max_shards = Some(0);

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &drained_a(),
        &NothingCaughtUp,
        raft(),
    );

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
}

/// RF 1 shard `shard` of `stream` on `leader` alone, formed and settled.
fn single_of(stream: &str, shard: u32, leader: &str) -> ShardAssignment {
    seated(stream, shard, leader, &[])
}

/// `written`, an RF 1 shard's grown set, as the store and group leave it:
/// the target stored for both brokers, and `voters` the reported voters.
fn grown_as(written: &ShardAssignment, voters: &[&str]) -> ShardAssignment {
    let mut grown = written.clone();
    let members: BTreeMap<String, u64> = grown.nodes().map(|n| (n.clone(), 7)).collect();
    grown.raft_target.as_mut().unwrap().members = members.clone();
    grown.raft_copy_ids = members.clone();
    grown.raft_voters.as_mut().unwrap().members = members
        .into_iter()
        .filter(|(node, _)| voters.contains(&node.as_str()))
        .collect();
    grown
}

/// **An RF 1 shard balances onto a broker that leads none (#3118).** A set
/// of one has no member to rename to and no follower to swap, so balance
/// starts the two-write move an operator would: grow to the leader and the
/// broker, then shrink once its copy votes. The move counts for the broker
/// while it runs, so the gap of two starts one move, not one per pass.
#[test]
fn a_one_member_raft_shard_balances_onto_a_broker_that_leads_none() {
    let streams = vec![replicated_stream("orders", 2, 1)];
    let nodes = live(&["broker-a", "broker-b"]);
    let existing = vec![
        single_of("orders", 0, "broker-a"),
        single_of("orders", 1, "broker-a"),
    ];

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());
    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    let (key, step, written) = moves[0];
    assert_eq!(
        step,
        &MoveStep::RaftReplace {
            from: "broker-a".to_string(),
            to: "broker-b".to_string()
        }
    );
    // The old copy keeps leading until the new one votes.
    assert_eq!(written.leader, "broker-a");
    assert_eq!(written.replicas, vec!["broker-b".to_string()]);
    let other = existing.iter().find(|a| &a.key != key).unwrap().clone();

    // The copy is still a learner: the move waits, and the other shard
    // stays, since `broker-b` already counts as leading this one.
    let learning = vec![grown_as(written, &["broker-a"]), other.clone()];
    let plan = plan_with(&streams, &[], &nodes, &learning, &NothingCaughtUp, raft());
    assert_eq!(only_waits(&plan), vec![Blocked::Unapplied], "{plan:?}");

    let voting = vec![grown_as(written, &["broker-a", "broker-b"]), other.clone()];
    let plan = plan_with(&streams, &[], &nodes, &voting, &NothingCaughtUp, raft());
    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    let (_, _, shrunk) = moves[0];
    assert_eq!(shrunk.leader, "broker-b");
    assert!(shrunk.replicas.is_empty());

    // One each: balanced, and nothing moves back.
    let mut done = shrunk.clone();
    settle(&mut done);
    let after = vec![done, other];
    let plan = plan_with(&streams, &[], &nodes, &after, &NothingCaughtUp, raft());
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert_eq!(plan.waiting().count(), 0, "{plan:?}");
}

/// A gap of one is balanced: an RF 1 move copies the whole log, so it is
/// not made to trade one leader back and forth.
#[test]
fn a_one_member_raft_shard_stays_on_a_gap_of_one() {
    let streams = vec![replicated_stream("orders", 3, 1)];
    let existing = vec![
        single_of("orders", 0, "broker-a"),
        single_of("orders", 1, "broker-a"),
        single_of("orders", 2, "broker-b"),
    ];
    let nodes = live(&["broker-a", "broker-b"]);

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
}

/// An RF 1 balance move is held by what holds any set change: a pause, a
/// target not stored for the set, and a broker with no room.
#[test]
fn a_one_member_raft_shard_balances_only_when_its_set_may_change() {
    let streams = vec![replicated_stream("orders", 2, 1)];
    let nodes = live(&["broker-a", "broker-b"]);
    let existing = vec![
        single_of("orders", 0, "broker-a"),
        single_of("orders", 1, "broker-a"),
    ];
    let plan_of = |nodes: &[Node], existing: &[ShardAssignment], policy| {
        plan_with(&streams, &[], nodes, existing, &NothingCaughtUp, policy)
    };

    let paused = MovePolicy {
        paused: true,
        ..raft()
    };
    let plan = plan_of(&nodes, &existing, paused);
    assert!(only_waits(&plan).contains(&Blocked::Paused), "{plan:?}");

    let mut unsettled = existing.clone();
    for assignment in &mut unsettled {
        assignment.raft_target = None;
    }
    let plan = plan_of(&nodes, &unsettled, raft());
    assert!(
        only_waits(&plan).contains(&Blocked::TargetUnsettled),
        "{plan:?}"
    );

    let mut full = nodes.clone();
    full[1].spec.capacity.max_shards = Some(0);
    let plan = plan_of(&full, &existing, raft());
    assert_eq!(only_waits(&plan), vec![], "{plan:?}");
}

/// RF 1 moves take the pass's budget of set changes: seventeen streams
/// that each want one start sixteen.
#[test]
fn balancing_one_member_raft_shards_takes_the_replacement_budget() {
    let names: Vec<String> = (0..17).map(|n| format!("s{n:02}")).collect();
    let streams: Vec<Stream> = names
        .iter()
        .map(|name| replicated_stream(name, 2, 1))
        .collect();
    let existing: Vec<ShardAssignment> = names
        .iter()
        .flat_map(|name| {
            [
                single_of(name, 0, "broker-a"),
                single_of(name, 1, "broker-a"),
            ]
        })
        .collect();
    let nodes = live(&["broker-a", "broker-b"]);

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    assert_eq!(plan.moves().count(), RAFT_REPLACEMENTS_PER_PASS, "{plan:?}");
    assert!(
        plan.moves()
            .all(|(_, _, written)| written.leader == "broker-a"
                && written.replicas == ["broker-b".to_string()]),
        "{plan:?}"
    );
    assert!(
        plan.waiting().any(|(_, why)| why == &Blocked::MoveLimit),
        "{plan:?}"
    );
}

/// **A move in flight counts for its destination, whoever started it.**
/// Three RF 1 shards on `broker-a`, one of them growing onto `broker-b`:
/// that is two against one once it lands, so nothing else starts.
#[test]
fn a_one_member_raft_move_in_flight_counts_for_its_destination() {
    let streams = vec![replicated_stream("orders", 3, 1)];
    let mut moving = single_of("orders", 0, "broker-a");
    moving.replicas = vec!["broker-b".to_string()];
    let existing = vec![
        grown_as(&moving, &["broker-a"]),
        single_of("orders", 1, "broker-a"),
        single_of("orders", 2, "broker-a"),
    ];
    let nodes = live(&["broker-a", "broker-b"]);

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    assert_eq!(only_waits(&plan), vec![Blocked::Unapplied], "{plan:?}");
}

/// Four shards of `orders` on `broker-a`, `-b` and `-c`, `broker-a`
/// leading two, and one shard of `extra` on `broker-c` alone, so `broker-c`
/// holds the most copies. `broker-d` leads none and holds no seat.
fn three_of_four() -> (Vec<Stream>, Vec<ShardAssignment>) {
    let streams = vec![
        replicated_stream("orders", 4, 3),
        replicated_stream("extra", 1, 1),
    ];
    let existing = vec![
        seated("orders", 0, "broker-a", &["broker-b", "broker-c"]),
        seated("orders", 1, "broker-a", &["broker-b", "broker-c"]),
        seated("orders", 2, "broker-b", &["broker-a", "broker-c"]),
        seated("orders", 3, "broker-c", &["broker-a", "broker-b"]),
        seated("extra", 0, "broker-c", &[]),
    ];
    (streams, existing)
}

fn zoned(id: &str, zone: &str) -> Node {
    let mut n = node(id, NodeLifecycle::Live, None);
    n.spec.zone = Some(zone.to_string());
    n
}

/// The one swap the plan makes, as (from, to, new leader).
fn the_swap(plan: &Plan) -> (String, String, String) {
    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    match moves[0] {
        (_, MoveStep::RaftReplace { from, to }, written) => {
            (from.clone(), to.clone(), written.leader.clone())
        }
        other => panic!("expected a swap, got {other:?}"),
    }
}

/// At RF 3 the busiest follower gives its seat up, never the leader.
#[test]
fn a_raft_leader_balances_outside_its_set_by_swapping_the_busiest_follower() {
    let (streams, existing) = three_of_four();
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    let (from, to, leader) = the_swap(&plan);
    assert_eq!((from.as_str(), to.as_str()), ("broker-c", "broker-d"));
    assert_eq!(leader, "broker-d");
}

/// A swap that would put two of a set's three copies in one zone is not
/// made, even for balance: nothing would spread the set again.
#[test]
fn a_raft_leader_balances_outside_its_set_only_where_the_set_keeps_its_zones() {
    // `orders` alone: no broker leads two more in total than `broker-d`.
    let (mut streams, mut existing) = three_of_four();
    streams.retain(|stream| stream.stream == "orders");
    existing.retain(|assignment| assignment.key.stream == "orders");
    let nodes = vec![
        zoned("broker-a", "z1"),
        zoned("broker-b", "z2"),
        zoned("broker-c", "z3"),
        zoned("broker-d", "z1"),
    ];

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
}

/// The follower in the new broker's zone gives its seat up, though another
/// holds more copies.
#[test]
fn a_raft_leader_balances_outside_its_set_by_swapping_within_a_zone() {
    let (streams, existing) = three_of_four();
    let nodes = vec![
        zoned("broker-a", "z1"),
        zoned("broker-b", "z2"),
        zoned("broker-c", "z3"),
        zoned("broker-d", "z2"),
    ];

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    let (from, to, _) = the_swap(&plan);
    assert_eq!((from.as_str(), to.as_str()), ("broker-b", "broker-d"));
}

/// With no broker outside the set, a drained follower stays, and the shard
/// says why.
#[test]
fn a_drained_raft_follower_with_nowhere_to_go_waits_visibly() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![member_of(0, "broker-b")];
    let mut nodes = live(&ABC);
    nodes[0].status.lifecycle = NodeLifecycle::Draining;

    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft());

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert!(
        matches!(
            plan.waiting().next(),
            Some((_, Blocked::NoReplacement { node, why: Leaving::Draining })) if node == "broker-a"
        ),
        "{plan:?}"
    );
}

/// **A lost broker is replaced.** A follower whose broker has been gone past
/// `restore_after_millis` is swapped for a live one. Before the delay it
/// stays: a restart is cheaper than a copy.
#[test]
fn a_raft_follower_lost_past_the_delay_is_replaced() {
    use super::restore::{MINUTE, Reports, gone};
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![member_of(0, "broker-a")];
    let nodes = |silent_for| {
        vec![
            node("broker-a", NodeLifecycle::Live, None),
            node("broker-b", NodeLifecycle::Live, None),
            gone("broker-c", silent_for),
            node("broker-d", NodeLifecycle::Live, None),
        ]
    };
    let reports = Reports::default();

    let early = plan_with(&streams, &[], &nodes(MINUTE), &existing, &reports, raft());
    assert_eq!(early.moves().count(), 0, "{early:?}");

    let late = plan_with(
        &streams,
        &[],
        &nodes(10 * MINUTE),
        &existing,
        &reports,
        raft(),
    );
    let (_, step, written) = late.moves().next().expect("a set change");
    assert_eq!(
        step,
        &MoveStep::RaftReplace {
            from: "broker-c".to_string(),
            to: "broker-d".to_string()
        }
    );
    assert_eq!(written.leader, "broker-a");
    assert!(!set_of(written).contains("broker-c"));
}

/// Brokers `a` (leader), `b` and `core` hold a group; `b` is draining.
fn drained_beside(core: Node) -> (Vec<Stream>, Vec<ShardAssignment>, Vec<Node>) {
    let mut existing = member_of(0, "a");
    existing.replicas = vec!["b".to_string(), "core".to_string()];
    let members = ["a", "b", "core"]
        .iter()
        .map(|node| (node.to_string(), 7))
        .collect::<BTreeMap<_, _>>();
    existing.raft_target = Some(RaftTarget {
        counter: 2,
        members: members.clone(),
    });
    existing.raft_voters = Some(crate::model::RaftVoters {
        incarnation: existing.incarnation,
        epoch: 0,
        index: 1,
        members: members.clone(),
    });
    existing.raft_copy_ids = members;
    let mut nodes = live(&["a", "b", "c", "d"]);
    nodes[1].status.lifecycle = NodeLifecycle::Draining;
    nodes.push(core);
    (
        vec![replicated_stream("orders", 1, 3)],
        vec![existing],
        nodes,
    )
}

fn internal_down(silent_for: u64) -> Node {
    use felix_common::membership::{NODE_ROLE_INTERNAL, NODE_ROLE_LABEL};
    let mut node = super::restore::gone("core", silent_for);
    node.spec
        .labels
        .insert(NODE_ROLE_LABEL.to_string(), NODE_ROLE_INTERNAL.to_string());
    node
}

/// **A member that is down is replaced before one that is only leaving.**
/// Every other swap waits on it, so picking the draining member first would
/// hold the shard on the down one, and the down one would never be picked.
#[test]
fn a_down_internal_member_is_replaced_before_a_draining_one() {
    use super::restore::{MINUTE, Reports};
    let (streams, existing, nodes) = drained_beside(internal_down(10 * MINUTE));
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &Reports::default(),
        raft(),
    );

    let (_, step, _) = plan.moves().next().expect("a set change");
    assert!(
        matches!(step, MoveStep::RaftReplace { from, .. } if from == "core"),
        "{plan:?}"
    );
}

/// Internal is not a reason to wait on the delay: the down member is taken
/// first even before it counts as lost.
#[test]
fn a_down_internal_member_inside_the_delay_does_not_hold_a_draining_one() {
    use super::restore::{MINUTE, Reports};
    let (streams, existing, nodes) = drained_beside(internal_down(MINUTE));
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &Reports::default(),
        raft(),
    );

    let (_, step, _) = plan.moves().next().expect("a set change");
    assert!(
        matches!(step, MoveStep::RaftReplace { from, .. } if from == "core"),
        "{plan:?}"
    );
}

/// A lost member goes before a draining one that is still serving.
#[test]
fn a_lost_raft_member_is_replaced_before_a_draining_one() {
    use super::restore::{MINUTE, Reports, gone};
    let (streams, existing, mut nodes) = drained_beside(gone("core", 10 * MINUTE));
    nodes.retain(|node| node.node_id != "core");
    nodes.push(gone("core", 10 * MINUTE));
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &Reports::default(),
        raft(),
    );

    let (_, step, _) = plan.moves().next().expect("a set change");
    assert!(
        matches!(step, MoveStep::RaftReplace { from, .. } if from == "core"),
        "{plan:?}"
    );
}

/// Through the store: the cluster's identity says Raft, and a drained
/// broker ends up leading nothing, with every set as placed.
#[tokio::test]
async fn a_drain_on_a_raft_cluster_finishes_through_the_store() {
    let store = super::reconciler::cluster(&ABC)
        .await
        .with_shard_engine("raft");
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the one-copy stream");
    store
        .create_stream(replicated_stream("events", 3, 3))
        .await
        .expect("stream");
    let policy = MovePolicy {
        settle_millis: None,
        ..MovePolicy::default()
    };
    let liveness = Default::default();
    reconcile_once(&store, &liveness, policy.clone()).await;
    let placed = store.list_shard_assignments().await.expect("list");
    assert_eq!(placed.len(), 3);
    let led = placed.iter().find(|a| a.leader == "broker-a");
    assert!(led.is_some(), "rendezvous gives broker-a one: {placed:?}");

    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Draining)
        .await
        .expect("drain");
    let outcome = reconcile_once(&store, &liveness, policy.clone()).await;
    assert_eq!(outcome.failed, 0);
    // The followers on broker-a have nowhere to go on three brokers, and no
    // stored target either; they wait, the set unchanged.
    assert_eq!(outcome.moved, 1, "{outcome:?}");

    let after = store.list_shard_assignments().await.expect("list");
    assert_eq!(leaders_on(&after, "broker-a"), 0, "{after:?}");
    for assignment in &after {
        let before = placed.iter().find(|a| a.key == assignment.key).unwrap();
        assert_eq!(set_of(assignment), set_of(before));
        assert_eq!(assignment.successor, None);
    }
}

/// Leaders a stream cannot move off a broker keep its total high. Evening
/// the totals out by unbalancing another stream would only be moved back by
/// that stream's own rule on the next pass, forever.
#[test]
fn the_totals_never_unbalance_a_raft_stream() {
    let ab = ["broker-a", "broker-b"];
    let streams = vec![
        replicated_stream("orders", 2, 2),
        replicated_stream("pinned", 2, 1),
    ];
    let mut existing = Vec::new();
    for (shard, leader, replica) in [(0, "broker-a", "broker-b"), (1, "broker-b", "broker-a")] {
        let mut assignment = member_of(shard, leader);
        assignment.replicas = vec![replica.to_string()];
        existing.push(assignment);
    }
    for shard in 0..2 {
        existing.push(pinned("pinned", shard, "broker-a"));
    }

    let plan = plan_with(
        &streams,
        &[],
        &live(&ab),
        &existing,
        &NothingCaughtUp,
        raft(),
    );

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
}

/// [`member_of`] for shard `shard` of the cache `sessions`.
fn cache_member_of(shard: u32, leader: &str) -> ShardAssignment {
    let mut assignment = member_of(shard, leader);
    assignment.key.stream = "sessions".to_string();
    assignment.key.kind = ShardKind::Cache;
    assignment
}

#[test]
fn a_drain_moves_a_raft_cache_leader_in_one_write() {
    let caches = vec![super::caches::replicated_cache("sessions", 2, 3)];
    let existing: Vec<ShardAssignment> = (0..2)
        .map(|shard| cache_member_of(shard, "broker-a"))
        .collect();
    let mut nodes = live(&ABC);
    nodes[0].status.lifecycle = NodeLifecycle::Draining;

    let plan = plan_with(&[], &caches, &nodes, &existing, &NothingCaughtUp, raft());

    // Not a staged successor: the group keeps its members.
    let written = leader_writes(&plan, &existing);
    assert_eq!(written.len(), 2, "{plan:?}");
    assert_eq!(leaders_on(&written, "broker-a"), 0);
}

#[test]
fn a_hybrid_move_left_on_a_raft_cache_shard_is_dropped() {
    let caches = vec![super::caches::replicated_cache("sessions", 1, 3)];
    let mut staged = cache_member_of(0, "broker-a");
    staged.replicas.push("broker-d".to_string());
    staged.successor = Some("broker-d".to_string());
    staged.move_reason = Some(crate::model::MoveReason::Drain);
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);

    let plan = plan_with(
        &[],
        &caches,
        &nodes,
        std::slice::from_ref(&staged),
        &NothingCaughtUp,
        raft(),
    );

    match &plan.shards[0].decision {
        Decision::Move(MoveStep::RaftMoveDropped, written) => {
            assert_eq!(written.successor, None);
            assert_eq!(set_of(written), set_of(&cache_member_of(0, "broker-a")));
        }
        other => panic!("expected the move dropped, got {other:?}"),
    }
}

#[test]
fn an_operator_moves_a_raft_cache_leader_to_a_member_or_its_seat_outside() {
    let caches = vec![super::caches::replicated_cache("sessions", 1, 3)];
    let existing = vec![cache_member_of(0, "broker-a")];
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    let catalog = Catalog {
        streams: &[],
        caches: &caches,
        nodes: &nodes,
        existing: &existing,
        caught_up: &NothingCaughtUp,
        policy: MovePolicy {
            max_concurrent: 0,
            ..raft()
        },
    };

    let outside = start_move(&catalog, &existing[0].key, "broker-d").expect("seat move");
    assert_eq!(
        outside.step,
        MoveStep::RaftReplace {
            from: "broker-a".to_string(),
            to: "broker-d".to_string()
        }
    );
    let step = start_move(&catalog, &existing[0].key, "broker-b").expect("moves");
    assert_eq!(
        step.step,
        MoveStep::RaftLeader {
            from: "broker-a".to_string(),
            to: "broker-b".to_string()
        }
    );
    assert_eq!(step.assignment.raft_target, existing[0].raft_target);
}

/// [`a_drain_on_a_raft_cluster_finishes_through_the_store`] for a cache.
#[tokio::test]
async fn a_cache_drain_on_a_raft_cluster_finishes_through_the_store() {
    let store = super::reconciler::cluster(&ABC)
        .await
        .with_shard_engine("raft");
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the one-copy stream");
    store
        .create_cache(super::caches::replicated_cache("sessions", 3, 3))
        .await
        .expect("cache");
    let policy = MovePolicy {
        settle_millis: None,
        ..MovePolicy::default()
    };
    let liveness = Default::default();
    reconcile_once(&store, &liveness, policy.clone()).await;
    let placed = store.list_shard_assignments().await.expect("list");
    assert_eq!(placed.len(), 3);
    assert!(placed.iter().all(|a| a.key.kind == ShardKind::Cache));
    assert!(placed.iter().any(|a| a.leader == "broker-a"), "{placed:?}");

    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Draining)
        .await
        .expect("drain");
    let outcome = reconcile_once(&store, &liveness, policy.clone()).await;
    assert_eq!(outcome.failed, 0);
    // The followers on broker-a have nowhere to go on three brokers, and no
    // stored target either; they wait, the set unchanged.
    assert_eq!(outcome.moved, 1, "{outcome:?}");

    let after = store.list_shard_assignments().await.expect("list");
    assert_eq!(leaders_on(&after, "broker-a"), 0, "{after:?}");
    for assignment in &after {
        let before = placed.iter().find(|a| a.key == assignment.key).unwrap();
        assert_eq!(set_of(assignment), set_of(before));
        assert_eq!(assignment.successor, None);
    }
}

/// **A fresh cluster's RF 1 shards stay where they were placed.** The first
/// pass places leaders even on the cluster's totals, two each here, which
/// leaves `wide` led twice by one broker. Balancing that spread would trade a
/// shard each way between two brokers, each a whole log copy, and publishes
/// during the trade reach a broker that does not lead yet.
#[test]
fn a_fresh_cluster_starts_no_one_member_raft_moves() {
    let streams = vec![
        replicated_stream("orders", 1, 1),
        replicated_stream("wide", 3, 1),
    ];
    let caches = vec![super::caches::cache("sessions", 2)];
    let nodes = live(&["broker-0", "broker-1", "broker-2"]);
    let first = plan_with(&streams, &caches, &nodes, &[], &NothingCaughtUp, raft());
    let existing: Vec<ShardAssignment> = first
        .shards
        .iter()
        .filter_map(|p| match &p.decision {
            Decision::Place(leader, _) => {
                let mut placed = single_of(&p.key.stream, p.key.shard, leader);
                placed.key.kind = p.key.kind;
                Some(placed)
            }
            _ => None,
        })
        .collect();
    assert_eq!(existing.len(), 6, "{first:?}");

    let plan = plan_with(
        &streams,
        &caches,
        &nodes,
        &existing,
        &NothingCaughtUp,
        raft(),
    );

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
}

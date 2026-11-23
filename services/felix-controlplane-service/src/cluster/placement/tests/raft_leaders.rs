//! Shards a Raft group runs: placement names the member it wants leading
//! with one write that changes only `leader`, and leaves failover to the
//! group. The set changes one member at a time, through the group's
//! membership target.
use super::*;
use crate::model::RaftTarget;
use crate::store::ControlPlaneStore;

const ABC: [&str; 3] = ["broker-a", "broker-b", "broker-c"];

fn raft() -> MovePolicy {
    MovePolicy {
        raft_streams: true,
        ..MovePolicy::default()
    }
}

/// Shard `shard` of `orders`, led by `leader` with every other broker of
/// `ABC` a member, a target the group was told, and the group formed.
fn member_of(shard: u32, leader: &str) -> ShardAssignment {
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

fn set_of(assignment: &ShardAssignment) -> BTreeSet<String> {
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

    // A stream in memory has no group, so it stays on the hybrid's path.
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

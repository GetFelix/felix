//! Shards a Raft group runs: placement names the member it wants leading
//! with one write that changes only `leader`, and leaves failover and the set
//! to the group.
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
/// `ABC` a member, and a target the group was told.
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

    let again = plan_with(&streams, &[], &nodes, &written, &NothingCaughtUp, raft());
    assert_eq!(again.kept(), 4, "the drain is done: {again:?}");
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
}

#[test]
fn an_operator_moves_a_raft_leader_only_to_a_member() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let mut existing = member_of(0, "broker-a");
    existing.replicas = vec!["broker-b".to_string()];
    let existing = vec![existing];
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

    assert_eq!(
        start_move(&catalog, &existing[0].key, "broker-c").unwrap_err(),
        Refused::NotAMember("broker-c".to_string())
    );
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
    assert_eq!(step.assignment.replicas, vec!["broker-a".to_string()]);
    assert_eq!(step.assignment.raft_target, existing[0].raft_target);
    assert_eq!(step.expected_generation, existing[0].generation);
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
    assert_eq!(outcome.waiting, 0, "{outcome:?}");

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

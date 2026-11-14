//! While a broker is down or a copy is being restored, placement fails over
//! and restores but does not move shards for balance, and a fenced move whose
//! destination died gives the shard back to its leader at once (#1808).
use super::*;
use crate::model::MoveReason;

const NOW: u64 = 10_000_000_000;

/// Every copy level at generation 3, as of `NOW`; no leader drained.
struct Level;

impl CaughtUp for Level {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        true
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }
}

/// `broker-a` and `broker-b` live, and `broker-c` down since `down_for` ago.
fn with_c_down(down_for: u64) -> Vec<Node> {
    let mut nodes = live(&["broker-a", "broker-b"]);
    let mut c = node("broker-c", NodeLifecycle::Down, None);
    c.status.last_heartbeat_at_millis = NOW - down_for;
    nodes.push(c);
    nodes
}

/// Four shards of `orders`, all led by `broker-a`: two too many.
fn lopsided() -> Vec<ShardAssignment> {
    (0..4)
        .map(|shard| pinned("orders", shard, "broker-a"))
        .collect()
}

fn decisions(nodes: &[Node], existing: &[ShardAssignment], streams: &[Stream]) -> Vec<Decision> {
    decisions_with(nodes, existing, streams, MovePolicy::default())
}

fn decisions_with(
    nodes: &[Node],
    existing: &[ShardAssignment],
    streams: &[Stream],
    policy: MovePolicy,
) -> Vec<Decision> {
    plan_with(streams, &[], nodes, existing, &Level, policy)
        .shards
        .into_iter()
        .map(|shard| shard.decision)
        .collect()
}

fn started(decisions: &[Decision]) -> usize {
    decisions
        .iter()
        .filter(|decision| {
            matches!(
                decision,
                Decision::Move(MoveStep::Stage { .. } | MoveStep::Fence, _)
            )
        })
        .count()
}

/// A broker that went down a moment ago holds every move for balance, and
/// says why.
#[test]
fn no_balance_move_starts_while_a_broker_is_down() {
    let decided = decisions(&with_c_down(10_000), &lopsided(), &[stream("orders", 4)]);
    assert_eq!(started(&decided), 0, "{decided:?}");
    assert!(
        decided.contains(&Decision::Waiting(Blocked::Recovering)),
        "{decided:?}"
    );

    // The same cluster with every broker up balances.
    let decided = decisions(
        &live(&["broker-a", "broker-b"]),
        &lopsided(),
        &[stream("orders", 4)],
    );
    assert_eq!(started(&decided), 1, "{decided:?}");
}

/// A broker down past the restore window no longer holds balancing: one that
/// never comes back must not stop it for good.
#[test]
fn a_broker_down_past_the_restore_window_does_not_hold_balancing() {
    let decided = decisions(
        &with_c_down(DEFAULT_RESTORE_AFTER_MILLIS),
        &lopsided(),
        &[stream("orders", 4)],
    );
    assert_eq!(started(&decided), 1, "{decided:?}");
}

/// A restore copying a shard back holds balancing too, with every broker up.
#[test]
fn a_restore_in_flight_holds_balancing() {
    let mut existing = lopsided();
    existing.push(ShardAssignment {
        joining: Some("broker-b".to_string()),
        move_reason: Some(MoveReason::Restore),
        move_started_at_millis: Some(NOW),
        ..assigned("logs", "broker-a", &["broker-b"])
    });
    let decided = decisions(
        &live(&["broker-a", "broker-b"]),
        &existing,
        &[stream("orders", 4), replicated_stream("logs", 1, 2)],
    );
    assert_eq!(started(&decided), 0, "{decided:?}");
    assert!(
        decided.contains(&Decision::Waiting(Blocked::Recovering)),
        "{decided:?}"
    );
}

/// Failover is not held: the dead broker's shard goes to the follower that
/// holds its log in the same pass.
#[test]
fn failover_is_placed_while_a_broker_is_down() {
    let existing = [assigned("orders", "broker-c", &["broker-a"])];
    let decided = decisions(
        &with_c_down(10_000),
        &existing,
        &[replicated_stream("orders", 1, 2)],
    );
    assert!(
        matches!(&decided[..], [Decision::Place(leader, _)] if leader == "broker-a"),
        "{decided:?}"
    );
}

/// A staged move for balance is dropped, giving its slot back, and it never
/// fences; a drain's goes on to the fence.
#[test]
fn a_staged_balance_move_is_dropped_while_a_broker_is_down() {
    let staged = |reason| ShardAssignment {
        successor: Some("broker-b".to_string()),
        move_reason: Some(reason),
        move_started_at_millis: Some(NOW),
        ..assigned("orders", "broker-a", &["broker-b"])
    };
    let streams = [stream("orders", 1)];
    let decided = decisions(
        &with_c_down(10_000),
        &[staged(MoveReason::Balance)],
        &streams,
    );
    let [Decision::Move(MoveStep::Abandon { successor }, next)] = &decided[..] else {
        panic!("expected the move to be dropped, got {decided:?}");
    };
    assert_eq!(successor, "broker-b");
    assert_eq!(next.state, ShardState::Active);
    assert_eq!(next.successor, None);
    assert!(next.replicas.is_empty(), "{:?}", next.replicas);
    assert_eq!(next.move_started_at_millis, None);

    let mut nodes = with_c_down(10_000);
    nodes[0].status.lifecycle = NodeLifecycle::Draining;
    let decided = decisions(&nodes, &[staged(MoveReason::Drain)], &streams);
    assert!(
        matches!(&decided[..], [Decision::Move(MoveStep::Fence, _)]),
        "{decided:?}"
    );
}

/// A follower crowding a zone is balance, so its reseat waits while a broker
/// is down; once the broker is long gone, or when the follower is draining,
/// the reseat goes ahead.
#[test]
fn a_crowded_zone_reseat_is_held_while_a_broker_is_down() {
    let zoned_nodes = |down_for| {
        let mut nodes = live(&["a1", "a2", "b1"]);
        for node in &mut nodes {
            node.spec.zone = Some(node.node_id[..1].to_string());
        }
        let mut c1 = node("c1", NodeLifecycle::Down, None);
        c1.status.last_heartbeat_at_millis = NOW - down_for;
        nodes.push(c1);
        nodes
    };
    let existing = [assigned("orders", "a1", &["a2"])];
    let streams = [replicated_stream("orders", 1, 2)];

    let decided = decisions(&zoned_nodes(10_000), &existing, &streams);
    assert_eq!(
        decided,
        vec![Decision::Waiting(Blocked::Recovering)],
        "a reseat for a crowded zone must wait"
    );

    let decided = decisions(
        &zoned_nodes(DEFAULT_RESTORE_AFTER_MILLIS),
        &existing,
        &streams,
    );
    assert!(
        matches!(&decided[..], [Decision::Move(MoveStep::Reseat { from, to }, _)] if from == "a2" && to == "b1"),
        "{decided:?}"
    );

    let mut nodes = zoned_nodes(10_000);
    nodes[1].status.lifecycle = NodeLifecycle::Draining;
    let decided = decisions(&nodes, &existing, &streams);
    assert!(
        matches!(&decided[..], [Decision::Move(MoveStep::Reseat { from, to }, _)] if from == "a2" && to == "b1"),
        "a draining follower is not held: {decided:?}"
    );
}

/// A fenced move whose destination is down gives the shard back to its
/// leader in the next pass, not at the fence timeout: the leader serves again
/// at a new generation and the destination's staged copy is dropped.
#[test]
fn a_fenced_move_to_a_dead_destination_unfences_at_once() {
    let fenced = ShardAssignment {
        state: ShardState::Draining,
        successor: Some("broker-b".to_string()),
        move_reason: Some(MoveReason::Balance),
        move_started_at_millis: Some(NOW),
        ..assigned("orders", "broker-a", &["broker-b"])
    };
    let mut nodes = live(&["broker-a"]);
    nodes.push(node("broker-b", NodeLifecycle::Down, None));

    let decided = decisions(&nodes, &[fenced], &[stream("orders", 1)]);
    let [Decision::Move(step, next)] = &decided[..] else {
        panic!("expected the move to be given up, got {decided:?}");
    };
    assert_eq!(
        step,
        &MoveStep::Abandon {
            successor: "broker-b".to_string()
        }
    );
    assert_eq!(next.leader, "broker-a");
    assert_eq!(next.state, ShardState::Assigning);
    assert_eq!(next.successor, None);
    assert!(next.replicas.is_empty(), "{:?}", next.replicas);
    assert_eq!(next.move_reason, None);
    assert_eq!(next.move_started_at_millis, None);
}

/// Through the store: the pass after the destination is marked down writes
/// the leader serving again, with the fence timeout nowhere near.
#[tokio::test]
async fn a_dead_destination_unfences_through_the_store() {
    use super::reconciler::{cluster, report, shard_zero};
    use crate::store::ControlPlaneStore;
    let store = cluster(&["broker-x", "broker-y"]).await;
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the three-shard stream");
    store.forget_deleted_names().await;
    store
        .create_stream(replicated_stream("orders", 1, 1))
        .await
        .expect("stream");
    let fenced = store
        .put_shard_assignment(ShardAssignment {
            key: shard_zero(),
            leader: "broker-x".to_string(),
            replicas: vec!["broker-y".to_string()],
            generation: 0,
            state: ShardState::Draining,
            successor: Some("broker-y".to_string()),
            joining: None,
            move_started_at_millis: None,
            move_reason: Some(MoveReason::Balance),
            incarnation: 0,
            fence_storage_ids: Default::default(),
            member_storage_ids: Default::default(),
            learners: Default::default(),
            pending_set: None,
            previous_generation: None,
            readmitted: Default::default(),
            placed: None,
        })
        .await
        .expect("fenced");
    report(&store, fenced.generation, &[], false).await;
    store
        .set_node_lifecycle("broker-y", NodeLifecycle::Down)
        .await
        .expect("down");

    let planned = super::super::reconciler::plan_pass(
        &store,
        &Default::default(),
        MovePolicy::default(),
        &mut Default::default(),
    )
    .await
    .expect("plan");
    let outcome = super::super::reconciler::apply_pass(
        &store,
        &planned,
        &mut Default::default(),
        &Default::default(),
    )
    .await;
    assert_eq!(outcome.moved, 1, "{outcome:?}");

    let after = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(after.leader, "broker-x");
    assert_eq!(after.state, ShardState::Assigning);
    assert_eq!(after.successor, None);
    assert!(after.replicas.is_empty(), "{:?}", after.replicas);
    assert!(after.generation > fenced.generation);
}

/// A broker that left gracefully holds balancing as a down one does, and so
/// does one down with restores turned off, for the default restore window.
#[test]
fn a_left_broker_or_restores_off_still_hold_balancing() {
    let mut nodes = with_c_down(10_000);
    nodes[2].status.lifecycle = NodeLifecycle::Left;
    let decided = decisions(&nodes, &lopsided(), &[stream("orders", 4)]);
    assert_eq!(started(&decided), 0, "{decided:?}");

    let off = || MovePolicy {
        restore_after_millis: None,
        ..MovePolicy::default()
    };
    let streams = [stream("orders", 4)];
    let decided = decisions_with(&with_c_down(10_000), &lopsided(), &streams, off());
    assert_eq!(started(&decided), 0, "{decided:?}");
    let decided = decisions_with(
        &with_c_down(DEFAULT_RESTORE_AFTER_MILLIS),
        &lopsided(),
        &streams,
        off(),
    );
    assert_eq!(started(&decided), 1, "{decided:?}");
}

//! Restoring the replication factor: a follower whose broker is gone past the
//! restore delay is replaced, a set a failover left short is topped up, and a
//! restore survives its destination failing and the control plane restarting.
use super::reconciler::{cluster, shard_zero};
use super::*;
use crate::config::NodeLivenessConfig;
use crate::model::MoveReason;
use crate::store::ControlPlaneStore;

const NOW: u64 = 10_000_000_000;
const MINUTE: u64 = 60_000;

/// Reports as of `NOW` at generation 3, which `assigned` uses.
#[derive(Default)]
struct Reports {
    level: BTreeSet<String>,
    offsets: BTreeMap<String, u64>,
    tail: Option<u64>,
}

impl Reports {
    fn level(nodes: &[&str]) -> Self {
        Self {
            level: nodes.iter().map(|n| n.to_string()).collect(),
            ..Self::default()
        }
    }
}

impl CaughtUp for Reports {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.level.contains(node_id)
    }

    fn reported_offset(&self, _key: &ShardKey, node_id: &str) -> Option<u64> {
        self.offsets.get(node_id).copied()
    }

    fn lag_records(&self, _key: &ShardKey, node_id: &str) -> Option<u64> {
        if self.level.contains(node_id) {
            return Some(0);
        }
        Some(self.tail? - self.offsets.get(node_id)?)
    }

    fn leader_offset(&self, _key: &ShardKey) -> Option<u64> {
        self.tail
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }
}

/// `id`, not serving, last heard from `silent_for` before `NOW`.
fn gone(id: &str, silent_for: u64) -> Node {
    let mut node = node(id, NodeLifecycle::Down, None);
    node.status.last_heartbeat_at_millis = NOW - silent_for;
    node
}

fn quorum(name: &str) -> Stream {
    Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream(name, 1, 3)
    }
}

fn restoring(leader: &str, replicas: &[&str], joining: &str) -> ShardAssignment {
    ShardAssignment {
        joining: Some(joining.to_string()),
        move_started_at_millis: Some(NOW - 1_000),
        move_reason: Some(MoveReason::Restore),
        ..assigned("orders", leader, replicas)
    }
}

/// Plan one pass. A written assignment comes back at generation 0, the
/// store's to set; it is put at the reports' generation here, as if written.
fn decide(
    stream: Stream,
    nodes: &[Node],
    mut existing: ShardAssignment,
    reports: &dyn CaughtUp,
) -> (Decision, Plan) {
    existing.generation = 3;
    let plan = plan(&[stream], &[], nodes, &[existing], reports);
    assert_eq!(plan.shards.len(), 1);
    (plan.shards[0].decision.clone(), plan)
}

/// **Lost follower.** broker-c has been down for ten minutes, past the
/// five-minute default: a copy starts on broker-d, beside it, and the shard
/// is counted under-replicated until it is seated.
#[test]
fn a_follower_gone_past_the_delay_is_replaced() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 10 * MINUTE),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let (decision, plan) = decide(
        replicated_stream("orders", 1, 3),
        &nodes,
        existing,
        &Reports::default(),
    );

    let Decision::Move(MoveStep::Restore { replacing, to }, next) = decision else {
        panic!("expected a restore, got {decision:?}");
    };
    assert_eq!(replacing.as_deref(), Some("broker-c"));
    assert_eq!(to, "broker-d");
    assert_eq!(next.joining.as_deref(), Some("broker-d"));
    assert_eq!(next.replicas, ["broker-b", "broker-c", "broker-d"]);
    assert_eq!(next.move_reason, Some(MoveReason::Restore));
    assert_eq!(next.move_started_at_millis, Some(NOW));
    assert_eq!(plan.under_replicated, [shard_zero()]);
    assert_eq!(plan.missing_copies, 1);
}

/// A broker down for less than the delay is a restart, not a loss. Nothing is
/// copied, but the shard still shows as under-replicated.
#[test]
fn a_follower_down_briefly_is_waited_for() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 10_000),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let (decision, plan) = decide(
        replicated_stream("orders", 1, 3),
        &nodes,
        existing,
        &Reports::default(),
    );
    assert_eq!(decision, Decision::Kept);
    assert_eq!(plan.under_replicated, [shard_zero()]);
}

/// With no delay configured, a lost follower is never replaced.
#[test]
fn no_restore_delay_never_replaces_a_follower() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 60 * MINUTE),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let plan = plan_with(
        &[replicated_stream("orders", 1, 3)],
        &[],
        &nodes,
        &[assigned("orders", "broker-a", &["broker-b", "broker-c"])],
        &Reports::default(),
        MovePolicy {
            restore_after_millis: None,
            ..MovePolicy::default()
        },
    );
    assert_eq!(plan.shards[0].decision, Decision::Kept);
}

/// **No live destination.** The only broker outside the set is down too, so
/// nothing starts; the shard waits, visibly under-replicated.
#[test]
fn no_destination_while_the_only_candidate_is_down() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 10 * MINUTE),
        gone("broker-d", 10 * MINUTE),
    ];
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let (decision, plan) = decide(
        replicated_stream("orders", 1, 3),
        &nodes,
        existing,
        &Reports::default(),
    );
    assert_eq!(decision, Decision::Kept);
    assert_eq!(plan.missing_copies, 1);
}

/// A draining broker is leaving, so it is no destination either.
#[test]
fn no_destination_on_a_draining_broker() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 10 * MINUTE),
        node("broker-d", NodeLifecycle::Draining, None),
    ];
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let (decision, _) = decide(
        replicated_stream("orders", 1, 3),
        &nodes,
        existing,
        &Reports::default(),
    );
    assert_eq!(decision, Decision::Kept);
}

/// **Short set.** A failover with two live brokers left the set at two of
/// three copies. When a third broker is live it is added, with nobody to
/// replace, and seated once level.
#[test]
fn a_set_short_of_the_factor_is_topped_up() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = assigned("orders", "broker-a", &["broker-b"]);
    let (decision, _) = decide(
        replicated_stream("orders", 1, 3),
        &nodes,
        existing,
        &Reports::default(),
    );
    let Decision::Move(MoveStep::Restore { replacing, to }, next) = decision else {
        panic!("expected a restore, got {decision:?}");
    };
    assert_eq!((replacing, to.as_str()), (None, "broker-c"));

    let (decision, _) = decide(
        replicated_stream("orders", 1, 3),
        &nodes,
        next,
        &Reports::level(&["broker-b", "broker-c"]),
    );
    let Decision::Move(MoveStep::Seat { from, to }, seated) = decision else {
        panic!("expected a seat, got {decision:?}");
    };
    assert_eq!((from, to.as_str()), (None, "broker-c"));
    assert_eq!(seated.replicas, ["broker-b", "broker-c"]);
    assert_eq!(seated.joining, None);
    assert_eq!(seated.move_reason, None);
}

/// A set short of the factor grows before a lost follower in it is replaced:
/// beside a set of two, the leader and the newcomer alone are a majority, and
/// seating the newcomer in the lost follower's place could leave a record on
/// half the set. The replacement comes once the set is back at three.
#[test]
fn a_short_set_grows_before_a_lost_follower_is_replaced() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        gone("broker-c", 10 * MINUTE),
        node("broker-d", NodeLifecycle::Live, None),
        node("broker-e", NodeLifecycle::Live, None),
    ];
    let existing = assigned("orders", "broker-a", &["broker-c"]);
    let (decision, _) = decide(quorum("orders"), &nodes, existing, &Reports::default());
    let Decision::Move(MoveStep::Restore { replacing, to }, next) = decision else {
        panic!("expected a restore, got {decision:?}");
    };
    assert_eq!(replacing, None);

    let level = Reports {
        offsets: [(to.clone(), 100)].into(),
        tail: Some(100),
        ..Reports::level(&[&to])
    };
    let (decision, _) = decide(quorum("orders"), &nodes, next, &level);
    let Decision::Move(MoveStep::Seat { from, .. }, grown) = decision else {
        panic!("expected a seat, got {decision:?}");
    };
    assert_eq!(from, None, "broker-c is kept until the set is odd");
    assert_eq!(grown.replicas, ["broker-c".to_string(), to]);

    let (decision, _) = decide(quorum("orders"), &nodes, grown, &Reports::default());
    let Decision::Move(MoveStep::Restore { replacing, .. }, _) = decision else {
        panic!("expected the replacement, got {decision:?}");
    };
    assert_eq!(replacing.as_deref(), Some("broker-c"));
}

/// A leader that has not reported at its generation may still be fencing;
/// a new set now would be the set it fences. Nothing starts.
#[test]
fn no_restore_until_the_leader_reports_at_its_generation() {
    struct Old;
    impl CaughtUp for Old {
        fn is_caught_up(&self, _: &ShardKey, _: &str) -> bool {
            false
        }
        fn reported_generation(&self, _: &ShardKey) -> Option<u64> {
            Some(2)
        }
        fn as_of_millis(&self) -> Option<u64> {
            Some(NOW)
        }
    }
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = assigned("orders", "broker-a", &["broker-b"]);
    let (decision, _) = decide(replicated_stream("orders", 1, 3), &nodes, existing, &Old);
    assert_eq!(decision, Decision::Kept);
}

/// Seated once level, and the lost follower leaves the set in the same write.
#[test]
fn a_restore_seats_the_copy_and_drops_the_lost_follower() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 10 * MINUTE),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let existing = restoring(
        "broker-a",
        &["broker-b", "broker-c", "broker-d"],
        "broker-d",
    );

    let (decision, _) = decide(
        replicated_stream("orders", 1, 3),
        &nodes,
        existing.clone(),
        &Reports::default(),
    );
    assert_eq!(
        decision,
        Decision::Waiting(Blocked::DestinationCatchingUp {
            successor: "broker-d".to_string()
        })
    );

    let (decision, _) = decide(
        replicated_stream("orders", 1, 3),
        &nodes,
        existing,
        &Reports::level(&["broker-b", "broker-d"]),
    );
    let Decision::Move(MoveStep::Seat { from, to }, seated) = decision else {
        panic!("expected a seat, got {decision:?}");
    };
    assert_eq!(
        (from.as_deref(), to.as_str()),
        (Some("broker-c"), "broker-d")
    );
    assert_eq!(seated.replicas, ["broker-b", "broker-d"]);
    assert_eq!(seated.joining, None);
}

/// **Quorum.** The old set's majority rule holds for a restore as for a
/// drain: the copy is seated only once it holds what a majority of the set
/// held. The lost follower is left out of the report, so it counts as level
/// with the leader, and the copy has to reach the tail.
#[test]
fn a_quorum_restore_waits_for_what_the_old_set_holds() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 10 * MINUTE),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let existing = restoring(
        "broker-a",
        &["broker-b", "broker-c", "broker-d"],
        "broker-d",
    );
    // Within the lag bound, but short of what the leader and broker-c might
    // both hold.
    let behind = Reports {
        offsets: [("broker-b".to_string(), 90), ("broker-d".to_string(), 95)].into(),
        tail: Some(100),
        ..Reports::default()
    };
    let (decision, _) = decide(quorum("orders"), &nodes, existing.clone(), &behind);
    assert!(
        matches!(
            decision,
            Decision::Waiting(Blocked::DestinationCatchingUp { .. })
        ),
        "seated early on a Quorum stream: {decision:?}"
    );

    let level = Reports {
        offsets: [("broker-b".to_string(), 90), ("broker-d".to_string(), 100)].into(),
        tail: Some(100),
        ..Reports::default()
    };
    let (decision, _) = decide(quorum("orders"), &nodes, existing, &level);
    assert!(
        matches!(decision, Decision::Move(MoveStep::Seat { .. }, _)),
        "{decision:?}"
    );
}

/// **The destination fails, repeatedly.** Each time the broker being copied
/// into goes down the copy is dropped and the next pass picks another live
/// broker, never a dead one, until one finishes.
#[test]
fn a_destination_that_fails_is_dropped_and_another_chosen() {
    let stream = replicated_stream("orders", 1, 3);
    let mut nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 10 * MINUTE),
        node("broker-d", NodeLifecycle::Live, None),
        node("broker-e", NodeLifecycle::Live, None),
        node("broker-f", NodeLifecycle::Live, None),
    ];
    let mut current = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let mut tried = Vec::new();
    for _ in 0..2 {
        let (decision, _) = decide(stream.clone(), &nodes, current, &Reports::default());
        let Decision::Move(MoveStep::Restore { to, .. }, next) = decision else {
            panic!("expected a restore, got {decision:?}");
        };
        assert!(!tried.contains(&to), "{to} failed already");
        tried.push(to.clone());
        // It dies mid-copy.
        let dead = nodes.iter_mut().find(|n| n.node_id == to).unwrap();
        *dead = gone(&to, 1_000);
        let (decision, _) = decide(stream.clone(), &nodes, next, &Reports::default());
        let Decision::Move(MoveStep::Abandon { successor }, undone) = decision else {
            panic!("expected the copy to be dropped, got {decision:?}");
        };
        assert_eq!(successor, to);
        assert_eq!(undone.joining, None);
        assert_eq!(undone.replicas, ["broker-b", "broker-c"]);
        current = undone;
    }

    let (decision, _) = decide(stream.clone(), &nodes, current, &Reports::default());
    let Decision::Move(MoveStep::Restore { to, .. }, next) = decision else {
        panic!("expected a third restore, got {decision:?}");
    };
    assert!(!tried.contains(&to));
    let (decision, _) = decide(stream, &nodes, next, &Reports::level(&["broker-b", &to]));
    let Decision::Move(MoveStep::Seat { .. }, seated) = decision else {
        panic!("expected a seat, got {decision:?}");
    };
    assert_eq!(seated.replicas, ["broker-b".to_string(), to]);
}

/// The lost broker comes back while its replacement copies: it holds its
/// copy again, so the new one is dropped.
#[test]
fn a_lost_follower_that_returns_ends_the_restore() {
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    let existing = restoring(
        "broker-a",
        &["broker-b", "broker-c", "broker-d"],
        "broker-d",
    );
    let (decision, _) = decide(
        replicated_stream("orders", 1, 3),
        &nodes,
        existing,
        &Reports::default(),
    );
    let Decision::Move(MoveStep::Abandon { successor }, undone) = decision else {
        panic!("expected the restore to be dropped, got {decision:?}");
    };
    assert_eq!(successor, "broker-d");
    assert_eq!(undone.replicas, ["broker-b", "broker-c"]);
}

/// **Leader change mid-restore.** The leader dies while a copy is joining. The
/// failover writes the set first and ends the restore with it (a staged copy
/// was never counted), and the restore starts again from the new leader's set
/// in a later write, never inside the promotion.
#[test]
fn a_failover_mid_restore_ends_it_and_the_next_pass_starts_again() {
    let nodes = vec![
        gone("broker-a", 10_000),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 10 * MINUTE),
        node("broker-d", NodeLifecycle::Live, None),
        node("broker-e", NodeLifecycle::Live, None),
    ];
    let existing = restoring(
        "broker-a",
        &["broker-b", "broker-c", "broker-d"],
        "broker-d",
    );
    let (decision, _) = decide(
        quorum("orders"),
        &nodes,
        existing,
        &Reports::level(&["broker-b"]),
    );
    let Decision::Place(leader, replicas) = decision else {
        panic!("expected a failover, got {decision:?}");
    };
    assert_eq!(leader, "broker-b");
    assert_eq!(
        replicas,
        ["broker-c", "broker-a"],
        "the old set, the copy left out"
    );

    let promoted = assigned("orders", "broker-b", &["broker-c", "broker-a"]);
    let (decision, _) = decide(quorum("orders"), &nodes, promoted, &Reports::default());
    let Decision::Move(MoveStep::Restore { replacing, .. }, _) = decision else {
        panic!("expected the restore to start again, got {decision:?}");
    };
    assert_eq!(replacing.as_deref(), Some("broker-c"));
}

/// **Leader change while topping up a set of two.** The copy joining counted
/// toward the quorum, and with three members the leader and the newcomer alone
/// were a majority, so a record may be on them and not on broker-b. The
/// failover keeps the newcomer in the set, so the new leader's fence has to
/// reach it or the old leader.
#[test]
fn a_failover_while_topping_up_keeps_the_newcomer() {
    let nodes = vec![
        gone("broker-a", 10_000),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = restoring("broker-a", &["broker-b", "broker-c"], "broker-c");
    let (decision, _) = decide(
        quorum("orders"),
        &nodes,
        existing,
        &Reports::level(&["broker-b"]),
    );
    let Decision::Place(leader, replicas) = decision else {
        panic!("expected a failover, got {decision:?}");
    };
    assert_eq!(leader, "broker-b");
    assert_eq!(replicas, ["broker-c", "broker-a"]);
}

/// Paused placement starts no restore of its own.
#[test]
fn a_pause_holds_a_restore() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let plan = plan_with(
        &[replicated_stream("orders", 1, 3)],
        &[],
        &nodes,
        &[assigned("orders", "broker-a", &["broker-b"])],
        &Reports::default(),
        MovePolicy {
            paused: true,
            ..MovePolicy::default()
        },
    );
    assert_eq!(plan.shards[0].decision, Decision::Waiting(Blocked::Paused));
}

/// A restore goes before rebalancing: broker-a leads more than its share,
/// and the one slot goes to the shard that is a copy short.
#[test]
fn a_restore_takes_the_slot_before_a_rebalance() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let mut short = pinned("short", 0, "broker-a");
    short.replicas = vec!["broker-b".to_string()];
    let streams = vec![
        replicated_stream("aaa", 1, 1),
        replicated_stream("bbb", 1, 1),
        replicated_stream("short", 1, 3),
    ];
    let existing = vec![
        pinned("aaa", 0, "broker-a"),
        pinned("bbb", 0, "broker-a"),
        short,
    ];
    let plan = plan(&streams, &[], &nodes, &existing, &Reports::default());
    let moving: Vec<_> = plan
        .moves()
        .map(|(key, step, _)| (key.stream.as_str(), step.label()))
        .collect();
    assert_eq!(moving, [("short", "restore")]);
}

/// **Resumable.** The restore is in the assignment, so a control plane that
/// restarts, or another instance that takes the placement lease, picks it up
/// where it was: it waits for the same copy and seats it, and never starts a
/// second one.
#[tokio::test]
async fn a_restore_resumes_after_a_control_plane_restart() {
    let store = cluster(&["broker-a", "broker-b", "broker-c", "broker-d"]).await;
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the three-shard stream");
    store
        .create_stream(replicated_stream("orders", 1, 3))
        .await
        .expect("stream");
    store
        .put_shard_assignment(ShardAssignment {
            generation: 0,
            state: ShardState::Assigning,
            ..assigned("orders", "broker-a", &["broker-b", "broker-c"])
        })
        .await
        .expect("placed");
    let now = store.now_millis().await.expect("clock");
    store
        .record_node_heartbeat("broker-c", 0, now - 10 * MINUTE)
        .await
        .expect("beat");
    store
        .set_node_lifecycle("broker-c", NodeLifecycle::Down)
        .await
        .expect("down");
    let liveness = NodeLivenessConfig::default();

    // Nothing until the leader has reported at its generation.
    reconcile_once(&store, &liveness, MovePolicy::default()).await;
    let placed = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(placed.joining, None, "a restore before the leader reported");
    super::reconciler::report(&store, placed.generation, &["broker-b"], false).await;

    reconcile_once(&store, &liveness, MovePolicy::default()).await;
    let started = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    let joining = started.joining.clone().expect("a restore started");
    assert_eq!(joining, "broker-d");
    assert_eq!(started.move_reason, Some(MoveReason::Restore));

    // A new process: nothing but the store carries over.
    let liveness = NodeLivenessConfig::default();
    reconcile_once(&store, &liveness, MovePolicy::default()).await;
    let waiting = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(
        waiting, started,
        "a second restore started, or the first moved"
    );

    super::reconciler::report(&store, started.generation, &["broker-b", "broker-d"], false).await;
    reconcile_once(&store, &liveness, MovePolicy::default()).await;
    let seated = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(seated.replicas, ["broker-b", "broker-d"]);
    assert_eq!(seated.joining, None);
}

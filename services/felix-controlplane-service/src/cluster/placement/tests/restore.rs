//! Restoring the replication factor: a follower whose broker is gone past the
//! restore delay is replaced, a set a failover left short is topped up, and a
//! restore survives its destination failing and the control plane restarting.
use super::reconciler::{cluster, shard_zero};
use super::*;
use crate::cluster::placement::moves::Moves;
use crate::config::NodeLivenessConfig;
use crate::model::{HaltedCopy, MoveReason};
use crate::store::ControlPlaneStore;

const NOW: u64 = 10_000_000_000;
const MINUTE: u64 = 60_000;

/// Reports as of `NOW` at generation 3, which `assigned` uses.
#[derive(Default)]
struct Reports {
    level: BTreeSet<String>,
    offsets: BTreeMap<String, u64>,
    tail: Option<u64>,
    /// The set the report was made for, and its generation when not 3.
    members: Vec<String>,
    generation: Option<u64>,
    /// The leader reported drained at the report's generation.
    drained: bool,
    halted: BTreeMap<String, HaltedCopy>,
    /// When the fence at the report's generation was first seen.
    fenced_at: Option<u64>,
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
        Some(self.generation.unwrap_or(3))
    }

    fn reported_members(&self, _key: &ShardKey) -> Vec<&str> {
        self.members.iter().map(String::as_str).collect()
    }

    fn is_drained(&self, _key: &ShardKey, generation: u64) -> bool {
        self.drained && Some(generation) == self.reported_generation(_key)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }

    fn halted(&self, _key: &ShardKey, node_id: &str) -> Option<&HaltedCopy> {
        self.halted.get(node_id)
    }

    fn fenced_at_millis(&self, key: &ShardKey, generation: u64) -> Option<u64> {
        self.fenced_at
            .filter(|_| Some(generation) == self.reported_generation(key))
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
        ..Reports::level(&["broker-d"])
    };
    let (decision, _) = decide(quorum("orders"), &nodes, existing, &level);
    assert!(
        matches!(decision, Decision::Move(MoveStep::Seat { .. }, _)),
        "{decision:?}"
    );
}

/// **A `Quorum` cache's counters.** The report's offsets are the cache log's
/// alone. A copy level on it but missing an acknowledged counter update is
/// left out of the caught-up set, and it is not seated until it is named
/// there: seated, it would count toward a majority it could not lead.
#[test]
fn a_quorum_cache_restore_waits_for_the_counter_log() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        gone("broker-c", 10 * MINUTE),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let mut existing = restoring(
        "broker-a",
        &["broker-b", "broker-c", "broker-d"],
        "broker-d",
    );
    existing.key.stream = "sessions".to_string();
    existing.key.kind = ShardKind::Cache;
    existing.generation = 3;
    let cache = Cache {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "sessions".to_string(),
        display_name: "sessions".to_string(),
        shards: 1,
        replication_factor: 3,
        consistency: ConsistencyLevel::Quorum,
        incarnation: 0,
    };
    let decide = |reports: &Reports| {
        let plan = plan(
            &[],
            std::slice::from_ref(&cache),
            &nodes,
            std::slice::from_ref(&existing),
            reports,
        );
        assert_eq!(plan.shards.len(), 1);
        plan.shards[0].decision.clone()
    };
    // Level on the cache log, which is empty when the cache holds only
    // counters, but not on the counter log.
    let offsets: BTreeMap<String, u64> =
        [("broker-b".to_string(), 0), ("broker-d".to_string(), 0)].into();
    let without_counters = Reports {
        offsets: offsets.clone(),
        tail: Some(0),
        ..Reports::level(&["broker-b"])
    };
    let decision = decide(&without_counters);
    assert!(
        matches!(
            decision,
            Decision::Waiting(Blocked::DestinationCatchingUp { .. })
        ),
        "seated without the counter log: {decision:?}"
    );

    let with_counters = Reports {
        offsets,
        tail: Some(0),
        ..Reports::level(&["broker-b", "broker-d"])
    };
    let decision = decide(&with_counters);
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

/// **Leader dies right after an RF 2 seat.** broker-c was seated and
/// broker-b dropped, but broker-a's last report is from the joining
/// generation, when broker-a and broker-b were a majority on their own. The
/// failover keeps broker-b, still serving, so the new leader's fence can take
/// a record only the two of them held (#1487).
#[test]
fn a_failover_before_the_seat_is_reported_keeps_the_leaving_copy() {
    let nodes = vec![
        gone("broker-a", 10_000),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let stream = Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 2)
    };
    let joined = Reports {
        members: vec!["broker-a".into(), "broker-b".into(), "broker-c".into()],
        generation: Some(2),
        ..Reports::level(&["broker-c"])
    };
    let seated = assigned("orders", "broker-a", &["broker-c"]);
    let (decision, _) = decide(stream.clone(), &nodes, seated.clone(), &joined);
    let Decision::Place(leader, replicas) = decision else {
        panic!("expected a failover, got {decision:?}");
    };
    assert_eq!(leader, "broker-c");
    assert_eq!(replicas, ["broker-a", "broker-b"]);

    // A report at the seat's generation names the set as it is.
    let reported = Reports {
        generation: Some(3),
        ..joined
    };
    let (decision, _) = decide(stream, &nodes, seated, &reported);
    let Decision::Place(leader, replicas) = decision else {
        panic!("expected a failover, got {decision:?}");
    };
    assert_eq!(leader, "broker-c");
    assert_eq!(replicas, ["broker-a"]);
}

/// The keep needs the leaving copy's broker to serve: a gone one could only
/// raise the fence's majority, so the set stays as it was.
#[test]
fn a_failover_after_the_seat_drops_a_leaving_copy_that_is_gone() {
    let nodes = vec![
        gone("broker-a", 10_000),
        gone("broker-b", 10_000),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let stream = Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 2)
    };
    let joined = Reports {
        members: vec!["broker-a".into(), "broker-b".into(), "broker-c".into()],
        generation: Some(2),
        ..Reports::level(&["broker-c"])
    };
    let seated = assigned("orders", "broker-a", &["broker-c"]);
    let (decision, _) = decide(stream, &nodes, seated, &joined);
    let Decision::Place(leader, replicas) = decision else {
        panic!("expected a failover, got {decision:?}");
    };
    assert_eq!(leader, "broker-c");
    assert_eq!(replicas, ["broker-a"]);
}

/// Only a set of two keeps the copy it dropped. Beside three, every majority
/// that acknowledged still holds a majority of the seated set.
#[test]
fn a_failover_after_a_seat_into_three_keeps_only_the_set() {
    let nodes = vec![
        gone("broker-a", 10_000),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let joined = Reports {
        members: ["broker-a", "broker-b", "broker-c", "broker-d"]
            .map(String::from)
            .to_vec(),
        generation: Some(2),
        ..Reports::level(&["broker-c", "broker-d"])
    };
    let seated = assigned("orders", "broker-a", &["broker-c", "broker-d"]);
    let (decision, _) = decide(quorum("orders"), &nodes, seated, &joined);
    let Decision::Place(leader, mut replicas) = decision else {
        panic!("expected a failover, got {decision:?}");
    };
    replicas.push(leader);
    replicas.sort();
    assert_eq!(replicas, ["broker-a", "broker-c", "broker-d"]);
}

/// Reports at generation 3 from broker-c, leading `followers`, all level at
/// offset 100.
fn level_at_100(followers: &[&str]) -> Reports {
    Reports {
        offsets: followers.iter().map(|f| (f.to_string(), 100)).collect(),
        tail: Some(100),
        ..Reports::level(followers)
    }
}

/// The same reports, as a leader that stopped for a trim's fence makes them.
fn drained(reports: Reports) -> Reports {
    Reports {
        drained: true,
        ..reports
    }
}

/// `existing` fenced for a trim, as the store writes it.
fn trim_fenced(existing: &ShardAssignment) -> ShardAssignment {
    ShardAssignment {
        state: ShardState::Draining,
        generation: 0,
        ..existing.clone()
    }
}

/// `existing` without `from`, its leader serving again.
fn trimmed(existing: &ShardAssignment, from: &str) -> Decision {
    let mut replicas = existing.replicas.clone();
    replicas.retain(|replica| replica != from);
    Decision::Move(
        MoveStep::Trim {
            from: from.to_string(),
        },
        ShardAssignment {
            replicas,
            generation: 0,
            state: ShardState::Assigning,
            ..existing.clone()
        },
    )
}

fn rf2(name: &str) -> Stream {
    Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream(name, 1, 2)
    }
}

/// **RF 2 drain in three brokers.** A failover right after the seat kept
/// broker-b, which is draining, so the set is `{c; a, b}` and there is no
/// fourth broker to reseat it on. Once broker-c reports at this generation
/// with broker-a level, broker-c is fenced, and once it reports drained
/// broker-b goes and its drain can finish. Before that report nothing
/// happens, and with broker-a behind, broker-a goes instead.
#[test]
fn a_set_above_its_factor_drops_the_draining_copy() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let kept = assigned("orders", "broker-c", &["broker-a", "broker-b"]);
    let fenced = trim_fenced(&kept);
    let decided = |existing: &ShardAssignment, reports: &Reports| {
        decide(rf2("orders"), &nodes, existing.clone(), reports).0
    };
    let fence = Decision::Move(MoveStep::TrimFence, fenced.clone());

    let level = level_at_100(&["broker-a", "broker-b"]);
    assert_eq!(decided(&kept, &level), fence);
    assert_eq!(
        decided(&fenced, &drained(level)),
        trimmed(&fenced, "broker-b")
    );

    let unreported = Reports {
        generation: Some(2),
        ..level_at_100(&["broker-a", "broker-b"])
    };
    assert_eq!(
        decided(&kept, &unreported),
        Decision::Waiting(Blocked::OverFactor)
    );

    let mut behind = level_at_100(&["broker-b"]);
    behind.offsets.insert("broker-a".to_string(), 50);
    assert_eq!(decided(&kept, &behind), fence);
    assert_eq!(
        decided(&fenced, &drained(behind)),
        trimmed(&fenced, "broker-a")
    );

    // Neither can go while the other is not known to be caught up.
    let unknown = level_at_100(&[]);
    assert_eq!(
        decided(&kept, &unknown),
        Decision::Waiting(Blocked::OverFactor)
    );
    assert_eq!(
        decided(&fenced, &drained(unknown)),
        Decision::Waiting(Blocked::OverFactor)
    );
}

/// **Acknowledged after the report.** At the report, broker-a is behind
/// and broker-b level, so broker-a looks like the copy to drop. Then broker-c
/// and broker-a acknowledge 101..150 while broker-b lags. Dropped there,
/// broker-a would leave those records on broker-c alone of `{c; b}`, and a
/// failover before broker-b caught up could not fence without broker-c. So
/// the set changes only once broker-c has stopped and says who holds its
/// log: there broker-b goes, and broker-a, which holds it all, stays.
#[test]
fn a_trim_waits_for_records_acknowledged_after_its_report() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let kept = assigned("orders", "broker-c", &["broker-a", "broker-b"]);
    let mut report = level_at_100(&["broker-b"]);
    report.offsets.insert("broker-a".to_string(), 50);
    let (decision, _) = decide(rf2("orders"), &nodes, kept.clone(), &report);
    let fenced = trim_fenced(&kept);
    assert_eq!(
        decision,
        Decision::Move(MoveStep::TrimFence, fenced.clone())
    );

    let after = drained(Reports {
        offsets: [("broker-a", 150), ("broker-b", 100)]
            .map(|(node, at)| (node.to_string(), at))
            .into(),
        tail: Some(150),
        ..Reports::level(&["broker-a"])
    });
    let (decision, _) = decide(rf2("orders"), &nodes, fenced.clone(), &after);
    assert_eq!(decision, trimmed(&fenced, "broker-b"));
}

/// **A dead follower under steady writes.** broker-a stopped following, so
/// the report gives it no position, and the leader's tail is always ahead of
/// broker-b's copy. broker-b holds everything acknowledged, which is what the
/// trim needs, so broker-a goes.
#[test]
fn a_set_above_its_factor_drops_a_dead_copy_under_steady_writes() {
    let nodes = vec![
        gone("broker-a", 10_000),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let kept = assigned("orders", "broker-c", &["broker-a", "broker-b"]);
    let writing = Reports {
        offsets: [("broker-b".to_string(), 140)].into(),
        tail: Some(150),
        ..Reports::level(&["broker-b"])
    };
    let (decision, _) = decide(rf2("orders"), &nodes, kept.clone(), &writing);
    let fenced = trim_fenced(&kept);
    assert_eq!(
        decision,
        Decision::Move(MoveStep::TrimFence, fenced.clone())
    );

    let stopped = drained(Reports {
        offsets: [("broker-b".to_string(), 150)].into(),
        ..writing
    });
    let (decision, _) = decide(rf2("orders"), &nodes, fenced.clone(), &stopped);
    assert_eq!(decision, trimmed(&fenced, "broker-a"));
}

/// **Past the mark on the leader alone.** A cancelled write left record 101
/// on broker-c only, and broker-c reported drained with its tail at 101 and
/// both followers at the acknowledged mark, 100, so both count as caught up.
/// broker-b can still confirm 101 after that report and readers see it, and
/// dropping either follower then leaves 101 on broker-c and one copy that may
/// not hold it. The trim waits for a drained report with every follower that
/// stays at broker-c's tail.
#[test]
fn a_trim_waits_for_the_followers_to_hold_the_whole_drained_log() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let kept = assigned("orders", "broker-c", &["broker-a", "broker-b"]);
    let fenced = trim_fenced(&kept);
    let at_mark = drained(Reports {
        tail: Some(101),
        ..level_at_100(&["broker-a", "broker-b"])
    });
    let (decision, _) = decide(rf2("orders"), &nodes, fenced.clone(), &at_mark);
    assert_eq!(decision, Decision::Waiting(Blocked::OverFactor));

    let mut one_level = drained(Reports {
        tail: Some(101),
        ..level_at_100(&["broker-a", "broker-b"])
    });
    one_level.offsets.insert("broker-b".to_string(), 101);
    let (decision, _) = decide(rf2("orders"), &nodes, fenced.clone(), &one_level);
    assert_eq!(decision, trimmed(&fenced, "broker-a"));
}

/// In a larger cluster, a set above its factor is trimmed one follower a
/// fence, back to the factor, and nothing else starts on it meanwhile.
#[test]
fn a_set_above_its_factor_is_trimmed_back_to_it() {
    let names = ["broker-a", "broker-b", "broker-c", "broker-d", "broker-e"];
    let nodes: Vec<Node> = names
        .iter()
        .map(|id| node(id, NodeLifecycle::Live, None))
        .chain(std::iter::once(node("broker-f", NodeLifecycle::Live, None)))
        .collect();
    for (factor, followers) in [(2, &names[..2]), (3, &names[..4])] {
        let stream = Stream {
            consistency: ConsistencyLevel::Quorum,
            ..replicated_stream("orders", 1, factor)
        };
        let mut existing = assigned("orders", "broker-e", followers);
        for _ in 0..2 * followers.len() {
            let reports = level_at_100(followers);
            let reports = if existing.state == ShardState::Draining {
                drained(reports)
            } else {
                reports
            };
            match decide(stream.clone(), &nodes, existing.clone(), &reports).0 {
                Decision::Move(MoveStep::TrimFence, assignment) => existing = assignment,
                Decision::Move(MoveStep::Trim { from }, assignment) => {
                    assert!(followers.contains(&from.as_str()));
                    existing = assignment;
                }
                _ => break,
            }
        }
        assert_eq!(existing.nodes().count(), factor as usize, "factor {factor}");
    }
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
    store.forget_deleted_names().await;
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

/// **Placed with its leader alone.** A shard placed while one broker was live
/// gets brokers later. Its leader reports with no followers to name (#1153),
/// and that report is what starts the first copy; each copy then joins and is
/// seated in turn until the shard is at its factor.
#[tokio::test]
async fn a_shard_placed_with_its_leader_alone_is_topped_up_once_brokers_join() {
    let store = cluster(&["broker-a"]).await;
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
        .create_stream(replicated_stream("orders", 1, 3))
        .await
        .expect("stream");
    let liveness = NodeLivenessConfig::default();
    let assignment = || async {
        store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get")
    };

    reconcile_once(&store, &liveness, MovePolicy::default()).await;
    let placed = assignment().await;
    assert_eq!(
        (placed.leader.as_str(), placed.replicas.len()),
        ("broker-a", 0)
    );

    for (i, id) in ["broker-b", "broker-c"].iter().enumerate() {
        let mut joined = node(id, NodeLifecycle::Live, None);
        joined.spec.advertise_addr = format!("10.0.0.5:{}", 7600 + i);
        store.register_node(joined).await.expect("node");
    }
    reconcile_once(&store, &liveness, MovePolicy::default()).await;
    assert_eq!(
        assignment().await,
        placed,
        "a copy was added before the leader reported at its generation"
    );

    let mut seated = Vec::new();
    let mut current = placed;
    for _ in 0..2 {
        // What the leader reports with nobody, or only seated copies, behind it.
        super::reconciler::report(&store, current.generation, &[], false).await;
        reconcile_once(&store, &liveness, MovePolicy::default()).await;
        let started = assignment().await;
        let joining = started.joining.clone().expect("a restore started");
        assert_eq!(started.move_reason, Some(MoveReason::Restore));
        assert!(!seated.contains(&joining) && joining != "broker-a");

        seated.push(joining);
        let level: Vec<&str> = seated.iter().map(String::as_str).collect();
        super::reconciler::report(&store, started.generation, &level, false).await;
        reconcile_once(&store, &liveness, MovePolicy::default()).await;
        current = assignment().await;
        assert_eq!(current.joining, None, "the copy was not seated");
        assert_eq!(current.replicas, seated);
    }
    assert_eq!(current.leader, "broker-a");
    let mut held = current.replicas.clone();
    held.sort();
    assert_eq!(held, ["broker-b", "broker-c"]);
}

/// broker-b, on a draining node, being replaced by broker-c in an RF 2 set.
fn replacing_b(leader: &str) -> ShardAssignment {
    ShardAssignment {
        joining: Some("broker-c".to_string()),
        move_started_at_millis: Some(NOW - 1_000),
        move_reason: Some(MoveReason::Replace),
        ..assigned("orders", leader, &["broker-b", "broker-c"])
    }
}

/// **RF 2 seat after the report (#1616).** A report at the joining
/// generation has broker-c level at 100. Seated then, broker-a and broker-b
/// can still acknowledge 101 before broker-a takes the seat's generation,
/// and `{a, c}` would hold it on broker-a alone. So broker-a is fenced
/// first, and broker-c is seated only once broker-a's drained report has it
/// at broker-a's tail, 101 included.
#[test]
fn an_rf2_seat_waits_for_the_drained_leaders_whole_log() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let joined = replacing_b("broker-a");
    let (decision, _) = decide(
        rf2("orders"),
        &nodes,
        joined.clone(),
        &level_at_100(&["broker-b", "broker-c"]),
    );
    let fenced = trim_fenced(&joined);
    assert_eq!(
        decision,
        Decision::Move(
            MoveStep::SeatFence {
                to: "broker-c".to_string()
            },
            fenced.clone()
        )
    );

    // 101 acknowledged on broker-a and broker-b after that report.
    let mut acked_after = drained(level_at_100(&["broker-b", "broker-c"]));
    acked_after.tail = Some(101);
    acked_after.offsets.insert("broker-b".to_string(), 101);
    let (decision, _) = decide(rf2("orders"), &nodes, fenced.clone(), &acked_after);
    assert_eq!(decision, Decision::Waiting(Blocked::LeaderStopping));

    acked_after.offsets.insert("broker-c".to_string(), 101);
    let (decision, _) = decide(rf2("orders"), &nodes, fenced.clone(), &acked_after);
    assert_eq!(
        decision,
        Decision::Move(
            MoveStep::Seat {
                from: Some("broker-b".to_string()),
                to: "broker-c".to_string(),
            },
            ShardAssignment {
                replicas: vec!["broker-c".to_string()],
                generation: 0,
                state: ShardState::Assigning,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
                ..joined
            }
        )
    );
}

/// A newcomer that goes down while the leader is fenced for its seat is
/// dropped, and the leader serves again.
#[test]
fn a_fenced_seat_whose_newcomer_died_gives_the_shard_back() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        gone("broker-c", 10_000),
    ];
    let fenced = trim_fenced(&replacing_b("broker-a"));
    let (decision, _) = decide(
        rf2("orders"),
        &nodes,
        fenced.clone(),
        &drained(level_at_100(&["broker-b"])),
    );
    let Decision::Move(MoveStep::Abandon { successor }, next) = decision else {
        panic!("expected the newcomer dropped, got {decision:?}");
    };
    assert_eq!(successor, "broker-c");
    assert_eq!(next.replicas, ["broker-b"]);
    assert_eq!(next.joining, None);
    assert_eq!(next.state, ShardState::Assigning);
}

/// A seat into an odd set needs no fence: a record acknowledged on three of
/// the four survives losing the leaving copy.
#[test]
fn an_rf3_seat_does_not_fence() {
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    let mut nodes = nodes;
    nodes[1] = node("broker-b", NodeLifecycle::Draining, None);
    let joined = ShardAssignment {
        joining: Some("broker-d".to_string()),
        move_started_at_millis: Some(NOW - 1_000),
        move_reason: Some(MoveReason::Replace),
        ..assigned("orders", "broker-a", &["broker-b", "broker-c", "broker-d"])
    };
    let (decision, _) = decide(
        quorum("orders"),
        &nodes,
        joined,
        &level_at_100(&["broker-b", "broker-c", "broker-d"]),
    );
    let Decision::Move(MoveStep::Seat { from, to }, next) = decision else {
        panic!("expected a seat, got {decision:?}");
    };
    assert_eq!(
        (from.as_deref(), to.as_str()),
        (Some("broker-b"), "broker-d")
    );
    assert_ne!(next.state, ShardState::Draining);
}

/// A restore that drops a lost follower from an RF 2 set fences too.
#[test]
fn an_rf2_restore_in_place_of_a_lost_copy_fences() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        gone("broker-b", 10 * MINUTE),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let joined = restoring("broker-a", &["broker-b", "broker-c"], "broker-c");
    let (decision, _) = decide(
        rf2("orders"),
        &nodes,
        joined.clone(),
        &level_at_100(&["broker-c"]),
    );
    assert_eq!(
        decision,
        Decision::Move(
            MoveStep::SeatFence {
                to: "broker-c".to_string()
            },
            trim_fenced(&joined)
        )
    );
}

/// A fenced restore seats its newcomer in the lost copy's place once the
/// drained leader's report has it at the leader's tail.
#[test]
fn a_fenced_rf2_restore_seats_on_the_drained_report() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        gone("broker-b", 10 * MINUTE),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let joined = restoring("broker-a", &["broker-b", "broker-c"], "broker-c");
    let (decision, _) = decide(
        rf2("orders"),
        &nodes,
        trim_fenced(&joined),
        &drained(level_at_100(&["broker-c"])),
    );
    assert_eq!(
        decision,
        Decision::Move(
            MoveStep::Seat {
                from: Some("broker-b".to_string()),
                to: "broker-c".to_string(),
            },
            ShardAssignment {
                replicas: vec!["broker-c".to_string()],
                generation: 0,
                state: ShardState::Assigning,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
                ..joined
            }
        )
    );
}

/// **Factor raised mid-seat.** broker-c is restoring in place of the lost
/// broker-b, and records acknowledged on `{a, c}` alone are not on broker-b.
/// The operator raises the factor to 3 while broker-a is fenced: the set is
/// now short and replaces nobody. Undoing the restore would leave those
/// records on broker-a only, so broker-c is seated as a top-up instead, and
/// only on the drained report, as before.
#[test]
fn a_factor_raised_during_a_restore_seat_fence_tops_up() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        gone("broker-b", 10 * MINUTE),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let joined = restoring("broker-a", &["broker-b", "broker-c"], "broker-c");
    let fenced = trim_fenced(&joined);
    let (decision, _) = decide(
        quorum("orders"),
        &nodes,
        fenced.clone(),
        &level_at_100(&["broker-c"]),
    );
    assert_eq!(decision, Decision::Waiting(Blocked::LeaderStopping));

    let (decision, _) = decide(
        quorum("orders"),
        &nodes,
        fenced,
        &drained(level_at_100(&["broker-c"])),
    );
    assert_eq!(
        decision,
        Decision::Move(
            MoveStep::Seat {
                from: None,
                to: "broker-c".to_string(),
            },
            ShardAssignment {
                generation: 0,
                state: ShardState::Assigning,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
                ..joined
            }
        )
    );
}

/// A seat-fenced leader that never reports drained serves again at the
/// fence timeout, with the replacement still going.
#[test]
fn a_seat_fence_that_times_out_gives_the_shard_back_with_the_newcomer() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let joined = replacing_b("broker-a");
    let reports = Reports {
        fenced_at: Some(NOW - DEFAULT_FENCE_TIMEOUT_MILLIS - 1),
        ..level_at_100(&["broker-b", "broker-c"])
    };
    let (decision, _) = decide(rf2("orders"), &nodes, trim_fenced(&joined), &reports);
    assert_eq!(
        decision,
        Decision::Move(
            MoveStep::FenceTimedOut { successor: None },
            ShardAssignment {
                generation: 0,
                state: ShardState::Assigning,
                ..joined
            }
        )
    );
}

/// A newcomer its leader reports halted while fenced for its seat is
/// dropped, and the leader serves again.
#[test]
fn a_fenced_seat_whose_newcomer_halted_gives_the_shard_back() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let mut reports = drained(level_at_100(&["broker-b", "broker-c"]));
    reports.halted.insert(
        "broker-c".to_string(),
        HaltedCopy {
            reason: "diverged".to_string(),
            generation: 3,
            since_millis: NOW - MINUTE,
        },
    );
    let fenced = trim_fenced(&replacing_b("broker-a"));
    let (decision, _) = decide(rf2("orders"), &nodes, fenced, &reports);
    let Decision::Move(MoveStep::Halted { successor, reason }, next) = decision else {
        panic!("expected the newcomer dropped, got {decision:?}");
    };
    assert_eq!(
        (successor.as_str(), reason.as_str()),
        ("broker-c", "diverged")
    );
    assert_eq!(next.replicas, ["broker-b"]);
    assert_eq!(next.joining, None);
    assert_eq!(next.state, ShardState::Assigning);
}

/// A seat fence is one copy in flight, not two.
#[test]
fn a_seat_fence_takes_one_move_slot() {
    let fenced = trim_fenced(&replacing_b("broker-a"));
    let mut moves = Moves::counting(
        &[fenced],
        MovePolicy {
            max_concurrent: 2,
            ..MovePolicy::default()
        },
        &|_| false,
    );
    assert_eq!(moves.begin("broker-d", "broker-e"), Ok(()));
}

/// Reports from broker-a fenced at generation 3: tail 101, which broker-a
/// acknowledged on broker-c alone, broker-c at 101 and broker-b at 100.
fn acked_on_a_and_c() -> Reports {
    let mut reports = drained(level_at_100(&["broker-b", "broker-c"]));
    reports.tail = Some(101);
    reports.offsets.insert("broker-c".to_string(), 101);
    reports
}

/// **Undo beside an even set (#1628).** broker-c joins `{a, b}` to replace
/// broker-b, and broker-b is undrained, so the replacement is undone. Two of
/// the three acknowledge, so a record can be on broker-a and broker-c alone
/// after any report; dropped while broker-a serves, `{a, b}` would hold it
/// on broker-a only. So broker-a is fenced first, and broker-c goes only once
/// the drained report has broker-b at broker-a's tail.
#[test]
fn an_undo_beside_an_even_set_fences_first() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let joined = replacing_b("broker-a");
    let (decision, _) = decide(
        rf2("orders"),
        &nodes,
        joined.clone(),
        &level_at_100(&["broker-b", "broker-c"]),
    );
    let fenced = trim_fenced(&joined);
    assert_eq!(
        decision,
        Decision::Move(
            MoveStep::UndoFence {
                successor: "broker-c".to_string()
            },
            fenced.clone()
        )
    );

    let mut reports = acked_on_a_and_c();
    let (decision, _) = decide(rf2("orders"), &nodes, fenced.clone(), &reports);
    assert_eq!(decision, Decision::Waiting(Blocked::LeaderStopping));

    reports.offsets.insert("broker-b".to_string(), 101);
    let (decision, _) = decide(rf2("orders"), &nodes, fenced, &reports);
    assert_eq!(
        decision,
        Decision::Move(
            MoveStep::Abandon {
                successor: "broker-c".to_string()
            },
            ShardAssignment {
                replicas: vec!["broker-b".to_string()],
                generation: 0,
                state: ShardState::Assigning,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
                ..joined
            }
        )
    );
}

/// An undo beside an odd set needs no fence: a record acknowledged on three
/// of the four is still on two of the three left.
#[test]
fn an_undo_beside_an_odd_set_does_not_fence() {
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    let joined = ShardAssignment {
        joining: Some("broker-d".to_string()),
        move_started_at_millis: Some(NOW - 1_000),
        move_reason: Some(MoveReason::Replace),
        ..assigned("orders", "broker-a", &["broker-b", "broker-c", "broker-d"])
    };
    let (decision, _) = decide(
        quorum("orders"),
        &nodes,
        joined,
        &level_at_100(&["broker-b", "broker-c", "broker-d"]),
    );
    let Decision::Move(MoveStep::Abandon { successor }, next) = decision else {
        panic!("expected the newcomer dropped, got {decision:?}");
    };
    assert_eq!(successor, "broker-d");
    assert_eq!(next.replicas, ["broker-b", "broker-c"]);
}

/// **Halted while seat-fenced (#1628).** broker-c halts while broker-a is
/// fenced for its seat, after 101 was acknowledged on broker-a and broker-c
/// alone. Dropping broker-c then leaves 101 on broker-a only, so it waits
/// until the drained report has broker-b at 101.
#[test]
fn a_fenced_seat_whose_newcomer_halted_waits_for_the_rest_to_hold_the_log() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let mut reports = acked_on_a_and_c();
    reports.halted.insert(
        "broker-c".to_string(),
        HaltedCopy {
            reason: "diverged".to_string(),
            generation: 3,
            since_millis: NOW - MINUTE,
        },
    );
    let fenced = trim_fenced(&replacing_b("broker-a"));
    let (decision, _) = decide(rf2("orders"), &nodes, fenced.clone(), &reports);
    assert_eq!(decision, Decision::Waiting(Blocked::LeaderStopping));

    reports.offsets.insert("broker-b".to_string(), 101);
    let (decision, _) = decide(rf2("orders"), &nodes, fenced, &reports);
    let Decision::Move(MoveStep::Halted { successor, .. }, next) = decision else {
        panic!("expected the newcomer dropped, got {decision:?}");
    };
    assert_eq!(successor, "broker-c");
    assert_eq!(next.replicas, ["broker-b"]);
    assert_eq!(next.state, ShardState::Assigning);
}

/// **Lost copy back while seat-fenced (#1628).** broker-c is restoring in
/// place of the lost broker-b, and broker-b comes back while broker-a is
/// fenced, so the restore is not needed. 101 is on broker-a and broker-c
/// alone, so broker-c goes only once broker-b holds 101 too.
#[test]
fn a_fenced_restore_whose_lost_copy_came_back_waits_for_it_to_hold_the_log() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let fenced = trim_fenced(&restoring(
        "broker-a",
        &["broker-b", "broker-c"],
        "broker-c",
    ));
    let mut reports = acked_on_a_and_c();
    let (decision, _) = decide(rf2("orders"), &nodes, fenced.clone(), &reports);
    assert_eq!(decision, Decision::Waiting(Blocked::LeaderStopping));

    reports.offsets.insert("broker-b".to_string(), 101);
    let (decision, _) = decide(rf2("orders"), &nodes, fenced, &reports);
    let Decision::Move(MoveStep::Abandon { successor }, next) = decision else {
        panic!("expected the newcomer dropped, got {decision:?}");
    };
    assert_eq!(successor, "broker-c");
    assert_eq!(next.replicas, ["broker-b"]);
    assert_eq!(next.state, ShardState::Assigning);
}

/// An operator's cancel beside an even set fences with no move reason, and
/// placement then drops the newcomer even though it could be seated.
#[test]
fn a_fenced_cancel_drops_the_newcomer_rather_than_seating_it() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let cancelled = ShardAssignment {
        move_reason: None,
        ..trim_fenced(&replacing_b("broker-a"))
    };
    let mut reports = acked_on_a_and_c();
    reports.offsets.insert("broker-b".to_string(), 101);
    let (decision, _) = decide(rf2("orders"), &nodes, cancelled, &reports);
    let Decision::Move(MoveStep::Abandon { successor }, next) = decision else {
        panic!("expected the newcomer dropped, got {decision:?}");
    };
    assert_eq!(successor, "broker-c");
    assert_eq!(next.replicas, ["broker-b"]);
}

/// A seat-fenced replacement past its move timeout is still seated when it
/// can be, and dropped otherwise only once the rest hold the leader's log.
#[test]
fn a_fenced_seat_past_its_timeout_drops_the_newcomer_on_evidence() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let timeout = MovePolicy::default().timeout_millis.expect("a default");
    let fenced = ShardAssignment {
        move_started_at_millis: Some(NOW - timeout - 1),
        ..trim_fenced(&replacing_b("broker-a"))
    };
    let mut reports = drained(level_at_100(&["broker-b", "broker-c"]));
    reports.tail = Some(101);
    reports.offsets.insert("broker-b".to_string(), 101);
    let (decision, _) = decide(rf2("orders"), &nodes, fenced.clone(), &reports);
    let Decision::Move(MoveStep::TimedOut { successor }, next) = decision else {
        panic!("expected the newcomer dropped, got {decision:?}");
    };
    assert_eq!(successor, "broker-c");
    assert_eq!(next.replicas, ["broker-b"]);

    reports.offsets.insert("broker-b".to_string(), 100);
    let (decision, _) = decide(rf2("orders"), &nodes, fenced, &reports);
    assert_eq!(decision, Decision::Waiting(Blocked::LeaderStopping));
}

//! Copies a leader reports halted: never a destination, given up when a move
//! was waiting on one, not counted as a live copy, and replaced once they
//! have been halted for the restore delay.
use super::*;
use crate::model::{HaltedCopy, MoveReason};

const NOW: u64 = 10_000_000_000;
const MINUTE: u64 = 60_000;

/// Reports as of `NOW` at generation 3, which `assigned` uses.
#[derive(Default)]
struct Reports {
    level: BTreeSet<String>,
    halted: BTreeMap<String, HaltedCopy>,
}

impl Reports {
    /// `node` halted at generation 3 since `ago` before `NOW`.
    fn halted(mut self, node: &str, ago: u64) -> Self {
        self.halted.insert(node.to_string(), halt(3, NOW - ago));
        self
    }

    /// `node` listed at an earlier generation: dropped from the set for it.
    fn carried(mut self, node: &str) -> Self {
        self.halted.insert(node.to_string(), halt(2, NOW - MINUTE));
        self
    }

    fn level(mut self, nodes: &[&str]) -> Self {
        self.level = nodes.iter().map(|n| n.to_string()).collect();
        self
    }
}

fn halt(generation: u64, since_millis: u64) -> HaltedCopy {
    HaltedCopy {
        reason: "diverged".to_string(),
        generation,
        since_millis,
    }
}

impl CaughtUp for Reports {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.level.contains(node_id)
    }

    fn lag_records(&self, _key: &ShardKey, node_id: &str) -> Option<u64> {
        self.level.contains(node_id).then_some(0)
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }

    fn halted(&self, _key: &ShardKey, node_id: &str) -> Option<&HaltedCopy> {
        self.halted.get(node_id)
    }
}

fn decide(nodes: &[Node], existing: ShardAssignment, reports: &dyn CaughtUp) -> (Decision, Plan) {
    let plan = plan(
        &[replicated_stream("orders", 1, 3)],
        &[],
        nodes,
        &[existing],
        reports,
    );
    assert_eq!(plan.shards.len(), 1);
    (plan.shards[0].decision.clone(), plan)
}

/// The destination a drain of broker-a picks with nothing halted.
fn natural_destination(nodes: &[Node], existing: &ShardAssignment) -> String {
    match decide(nodes, existing.clone(), &Reports::default()).0 {
        Decision::Move(MoveStep::Stage { successor }, _) => successor,
        Decision::Move(MoveStep::Fence, next) => next.successor.expect("a successor"),
        other => panic!("expected a move to start, got {other:?}"),
    }
}

fn draining_a() -> Vec<Node> {
    vec![
        node("broker-a", NodeLifecycle::Draining, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
        node("broker-e", NodeLifecycle::Live, None),
    ]
}

/// **The #863 drain.** Whichever node a drain would pick, it picks another
/// once that node's copy is halted: a halted copy never catches up, and the
/// move would wait out its whole timeout.
#[test]
fn a_drain_never_picks_a_halted_copy() {
    let nodes = draining_a();
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let natural = natural_destination(&nodes, &existing);

    let (decision, _) = decide(
        &nodes,
        existing,
        &Reports::default().halted(&natural, 1_000),
    );
    let Decision::Move(MoveStep::Stage { successor }, _) = decision else {
        panic!("expected a move to start, got {decision:?}");
    };
    assert_ne!(successor, natural, "the drain picked the halted copy");
}

/// Nor a caught-up follower whose copy has since halted.
#[test]
fn a_drain_does_not_promote_a_halted_follower() {
    let nodes = draining_a();
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let reports = Reports::default()
        .level(&["broker-b"])
        .halted("broker-b", 1_000);

    let (decision, _) = decide(&nodes, existing, &reports);
    let Decision::Move(MoveStep::Stage { successor }, _) = decision else {
        panic!("expected a move to start, got {decision:?}");
    };
    assert_ne!(successor, "broker-b");
}

/// A node dropped from the set for a halt is still listed for a generation,
/// and is not picked again in it.
#[test]
fn a_node_just_dropped_for_a_halt_is_not_picked_again() {
    let nodes = draining_a();
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let natural = natural_destination(&nodes, &existing);
    assert!(
        natural != "broker-b" && natural != "broker-c",
        "the carried entry only matters for a node outside the set"
    );

    let (decision, _) = decide(&nodes, existing, &Reports::default().carried(&natural));
    let Decision::Move(MoveStep::Stage { successor }, _) = decision else {
        panic!("expected a move to start, got {decision:?}");
    };
    assert_ne!(successor, natural);
}

/// With nowhere else to go, the drain waits and says which copy is halted.
#[test]
fn a_drain_blocked_by_a_halted_copy_says_so() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Draining, None),
        node("broker-b", NodeLifecycle::Live, None),
    ];
    let existing = assigned("orders", "broker-a", &["broker-b"]);
    let (decision, _) = decide(
        &nodes,
        existing,
        &Reports::default().halted("broker-b", 1_000),
    );
    assert_eq!(
        decision,
        Decision::Waiting(Blocked::DestinationHalted {
            node: "broker-b".to_string(),
            reason: "diverged".to_string(),
        })
    );
    assert_eq!(
        Blocked::DestinationHalted {
            node: "broker-b".to_string(),
            reason: "diverged".to_string(),
        }
        .to_string(),
        "no live node can take this shard: the copy on broker-b is halted (diverged)"
    );
}

/// **A destination that halts is given up**, through the same write that
/// drops one that died, and the next pass picks again.
#[test]
fn a_staged_destination_that_halts_is_given_up() {
    let nodes = draining_a();
    let existing = ShardAssignment {
        successor: Some("broker-d".to_string()),
        move_started_at_millis: Some(NOW - 1_000),
        move_reason: Some(MoveReason::Drain),
        ..assigned("orders", "broker-a", &["broker-b", "broker-c", "broker-d"])
    };
    let (decision, _) = decide(
        &nodes,
        existing,
        &Reports::default().halted("broker-d", 1_000),
    );
    let Decision::Move(step, next) = decision else {
        panic!("expected the move to be given up, got {decision:?}");
    };
    assert_eq!(
        step,
        MoveStep::Halted {
            successor: "broker-d".to_string(),
            reason: "diverged".to_string(),
        }
    );
    assert_eq!(next.successor, None);
    assert_eq!(next.replicas, ["broker-b", "broker-c"]);
    assert_eq!(next.move_reason, None);
}

/// Fenced already: the halted destination is dropped and the leader hands
/// over to a follower it has, as for one that died.
#[test]
fn a_fenced_move_drops_a_halted_destination() {
    let nodes = draining_a();
    let existing = ShardAssignment {
        state: ShardState::Draining,
        successor: Some("broker-d".to_string()),
        move_reason: Some(MoveReason::Drain),
        ..assigned("orders", "broker-a", &["broker-b", "broker-c", "broker-d"])
    };
    let (decision, _) = decide(
        &nodes,
        existing,
        &Reports::default().halted("broker-d", 1_000),
    );
    let Decision::Move(MoveStep::Halted { successor, .. }, next) = decision else {
        panic!("expected the destination to be dropped, got {decision:?}");
    };
    assert_eq!(successor, "broker-d");
    assert_eq!(next.successor, None);
    assert_eq!(next.state, ShardState::Draining);
    assert_eq!(next.replicas, ["broker-b", "broker-c"]);
}

/// A copy being added that halts is given up the same way.
#[test]
fn a_joining_copy_that_halts_is_given_up() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let existing = ShardAssignment {
        joining: Some("broker-d".to_string()),
        move_started_at_millis: Some(NOW - 1_000),
        move_reason: Some(MoveReason::Restore),
        ..assigned("orders", "broker-a", &["broker-b", "broker-d"])
    };
    let (decision, _) = decide(
        &nodes,
        existing,
        &Reports::default().halted("broker-d", 1_000),
    );
    let Decision::Move(MoveStep::Halted { successor, reason }, next) = decision else {
        panic!("expected the copy to be given up, got {decision:?}");
    };
    assert_eq!(
        (successor.as_str(), reason.as_str()),
        ("broker-d", "diverged")
    );
    assert_eq!(next.joining, None);
    assert_eq!(next.replicas, ["broker-b"]);
}

fn whole() -> Vec<Node> {
    vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ]
}

/// **Not a live copy.** A halted follower is in no quorum, so the shard
/// counts as a copy short at once. It is not replaced yet: the leader's own
/// rebuild may bring it back.
#[test]
fn a_halted_follower_counts_as_missing_but_is_waited_for() {
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let (decision, plan) = decide(
        &whole(),
        existing,
        &Reports::default().halted("broker-c", 10_000),
    );
    assert_eq!(decision, Decision::Kept);
    assert_eq!(plan.under_replicated.len(), 1);
    assert_eq!(plan.missing_copies, 1);
    assert_eq!(plan.halted_copies, 1);
}

/// **Replaced after the restore delay**, beside it, as for a broker that is
/// gone: a copy joins elsewhere and the halted one leaves once it is seated.
#[test]
fn a_follower_halted_past_the_delay_is_replaced() {
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let (decision, _) = decide(
        &whole(),
        existing,
        &Reports::default().halted("broker-c", 10 * MINUTE),
    );
    let Decision::Move(MoveStep::Restore { replacing, to }, next) = decision else {
        panic!("expected a restore, got {decision:?}");
    };
    assert_eq!(replacing.as_deref(), Some("broker-c"));
    assert_eq!(to, "broker-d");
    assert_eq!(next.joining.as_deref(), Some("broker-d"));
}

/// A halt from further back than the carry window says nothing about this
/// set: the node is not a member being replaced.
#[test]
fn a_halt_from_long_ago_is_not_replaced() {
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let mut reports = Reports::default();
    reports
        .halted
        .insert("broker-c".to_string(), halt(0, NOW - 10 * MINUTE));
    let (decision, plan) = decide(&whole(), existing, &reports);
    assert_eq!(decision, Decision::Kept);
    assert!(plan.under_replicated.is_empty());
}

/// The replacement is seated once it holds the log, and the halted copy
/// leaves the set.
#[test]
fn a_replacement_for_a_halted_follower_is_seated() {
    let existing = ShardAssignment {
        joining: Some("broker-d".to_string()),
        move_started_at_millis: Some(NOW - 1_000),
        move_reason: Some(MoveReason::Restore),
        ..assigned("orders", "broker-a", &["broker-b", "broker-c", "broker-d"])
    };
    let reports = Reports::default()
        .level(&["broker-b", "broker-d"])
        .halted("broker-c", 10 * MINUTE);
    let (decision, _) = decide(&whole(), existing, &reports);
    let Decision::Move(MoveStep::Seat { from, to }, next) = decision else {
        panic!("expected the copy to be seated, got {decision:?}");
    };
    assert_eq!(
        (from.as_deref(), to.as_str()),
        (Some("broker-c"), "broker-d")
    );
    assert_eq!(next.replicas, ["broker-b", "broker-d"]);
}

/// A restore that finds its halted follower following again is undone.
#[test]
fn a_restore_is_undone_when_the_halt_clears() {
    let existing = ShardAssignment {
        joining: Some("broker-d".to_string()),
        move_started_at_millis: Some(NOW - 1_000),
        move_reason: Some(MoveReason::Restore),
        ..assigned("orders", "broker-a", &["broker-b", "broker-c", "broker-d"])
    };
    let (decision, _) = decide(&whole(), existing, &Reports::default());
    let Decision::Move(MoveStep::Abandon { successor }, next) = decision else {
        panic!("expected the restore to be undone, got {decision:?}");
    };
    assert_eq!(successor, "broker-d");
    assert_eq!(next.replicas, ["broker-b", "broker-c"]);
}

/// An operator is told why, rather than starting a move that cannot finish.
#[test]
fn an_operator_move_to_a_halted_copy_is_refused() {
    let nodes = whole();
    let streams = [replicated_stream("orders", 1, 3)];
    let existing = [assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let reports = Reports::default().halted("broker-c", 1_000);
    let catalog = Catalog {
        streams: &streams,
        caches: &[],
        nodes: &nodes,
        existing: &existing,
        caught_up: &reports,
        policy: MovePolicy::default(),
    };
    let refused = start_move(&catalog, &existing[0].key, "broker-c").expect_err("refused");
    assert_eq!(refused.code(), "destination_halted");
    assert!(start_move(&catalog, &existing[0].key, "broker-d").is_ok());
}

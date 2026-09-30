//! Pacing moves: every copy counts against the limits, drains go first, a
//! move fences within a lag bound, and a stuck one gives its slot back.
use super::*;

const NOW: u64 = 10_000_000_000;

/// Reports as of `NOW`, at the generation `assigned` uses: who is exactly
/// level, how far behind the others are, and whether the leader drained.
#[derive(Default)]
struct Positions {
    level: BTreeSet<String>,
    behind: BTreeMap<String, u64>,
    drained: bool,
}

impl Positions {
    fn behind(node: &str, records: u64) -> Self {
        Self {
            behind: [(node.to_string(), records)].into(),
            ..Self::default()
        }
    }
}

impl CaughtUp for Positions {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.level.contains(node_id)
    }

    fn lag_records(&self, _key: &ShardKey, node_id: &str) -> Option<u64> {
        if self.level.contains(node_id) {
            return Some(0);
        }
        self.behind.get(node_id).copied()
    }

    fn is_drained(&self, _key: &ShardKey, generation: u64) -> bool {
        self.drained && generation == 3
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }
}

fn policy(max_concurrent: usize) -> MovePolicy {
    MovePolicy {
        max_concurrent,
        ..MovePolicy::default()
    }
}

fn shard(stream: &str, shard: u32, leader: &str, replicas: &[&str]) -> ShardAssignment {
    let mut assignment = pinned(stream, shard, leader);
    assignment.replicas = replicas.iter().map(|r| r.to_string()).collect();
    assignment
}

fn staged(stream: &str, leader: &str, successor: &str, started: u64) -> ShardAssignment {
    let mut assignment = shard(stream, 0, leader, &[successor]);
    assignment.successor = Some(successor.to_string());
    assignment.move_started_at_millis = Some(started);
    assignment
}

fn decision_for<'a>(plan: &'a Plan, stream: &str, shard: u32) -> &'a Decision {
    &plan
        .shards
        .iter()
        .find(|p| p.key.stream == stream && p.key.shard == shard)
        .expect("planned")
        .decision
}

fn draining_a() -> Vec<Node> {
    vec![
        node("broker-a", NodeLifecycle::Draining, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ]
}

/// A follower being replaced is a copy like any other, and holds a slot
/// until it has caught up. Before, the replacement wrote no successor and
/// the next pass started another move beside it.
#[test]
fn a_follower_replacement_holds_a_move_slot() {
    let streams = vec![replicated_stream("orders", 1, 2), stream("audit", 1)];
    let mut replacing = shard("orders", 0, "broker-b", &["broker-a", "broker-c"]);
    replacing.joining = Some("broker-c".to_string());
    replacing.move_started_at_millis = Some(NOW);
    let existing = vec![replacing, shard("audit", 0, "broker-a", &[])];

    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Positions::default(),
        policy(1),
    );

    assert_eq!(
        decision_for(&plan, "audit", 0),
        &Decision::Waiting(Blocked::MoveLimit)
    );
}

/// The replacement is copied in beside the follower it replaces, so the
/// shard never has fewer copies than it asked for while the copy runs.
#[test]
fn a_replacement_is_copied_in_before_the_departing_follower_leaves() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![shard("orders", 0, "broker-a", &["broker-b"])];

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &Positions::default(),
        policy(1),
    );
    let next = match decision_for(&plan, "orders", 0) {
        Decision::Move(MoveStep::Reseat { from, to }, next) => {
            assert_eq!((from.as_str(), to.as_str()), ("broker-b", "broker-c"));
            next.clone()
        }
        other => panic!("expected a replacement, got {other:?}"),
    };
    assert_eq!(next.replicas, vec!["broker-b", "broker-c"]);
    assert_eq!(next.joining.as_deref(), Some("broker-c"));
    assert_eq!(next.move_started_at_millis, Some(NOW));

    // Still copying: it waits, and the departing follower stays.
    let mut written = next.clone();
    written.generation = 3;
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        std::slice::from_ref(&written),
        &Positions::behind("broker-c", 50_000),
        policy(1),
    );
    assert_eq!(
        decision_for(&plan, "orders", 0),
        &Decision::Waiting(Blocked::DestinationCatchingUp {
            successor: "broker-c".to_string()
        })
    );

    // Within the lag bound: the departing follower leaves and the slot is
    // given back.
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[written],
        &Positions::behind("broker-c", 10),
        policy(1),
    );
    match decision_for(&plan, "orders", 0) {
        Decision::Move(MoveStep::Seat { from, to }, next) => {
            assert_eq!(
                (from.as_deref(), to.as_str()),
                (Some("broker-b"), "broker-c")
            );
            assert_eq!(next.replicas, vec!["broker-c"]);
            assert_eq!(next.joining, None);
            assert_eq!(next.move_started_at_millis, None);
        }
        other => panic!("expected the replacement to be seated, got {other:?}"),
    }
}

/// A report at generation 3 with the leader's tail and each follower's
/// position, and every follower named caught up, as a leader reports before
/// its generation has anything counted.
struct Tailed {
    tail: u64,
    offsets: BTreeMap<String, u64>,
}

impl Tailed {
    fn at(tail: u64, offsets: &[(&str, u64)]) -> Self {
        Self {
            tail,
            offsets: offsets.iter().map(|(n, o)| (n.to_string(), *o)).collect(),
        }
    }
}

impl CaughtUp for Tailed {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        true
    }

    fn reported_offset(&self, _key: &ShardKey, node_id: &str) -> Option<u64> {
        self.offsets.get(node_id).copied()
    }

    fn lag_records(&self, _key: &ShardKey, node_id: &str) -> Option<u64> {
        Some(self.tail - self.offsets.get(node_id)?)
    }

    fn leader_offset(&self, _key: &ShardKey) -> Option<u64> {
        Some(self.tail)
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }
}

/// **On a `Quorum` stream the replacement is seated only once it holds what
/// a majority of the set it joined holds.** The leader and the departing
/// follower may hold records the lagging follower lacks; seating a joiner
/// short of them leaves the joiner and the lagging follower a majority of the
/// new set without them. Within the lag bound is not enough.
#[test]
fn a_quorum_replacement_waits_for_what_the_old_set_holds() {
    use crate::model::MoveReason;

    let streams = vec![Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let mut joining = shard(
        "orders",
        0,
        "broker-a",
        &["broker-b", "broker-c", "broker-d"],
    );
    joining.generation = 3;
    joining.joining = Some("broker-d".to_string());
    joining.move_started_at_millis = Some(NOW);
    joining.move_reason = Some(MoveReason::Replace);
    let seat = |report: &Tailed| {
        let plan = plan_with(
            &streams,
            &[],
            &nodes,
            std::slice::from_ref(&joining),
            report,
            policy(1),
        );
        matches!(
            decision_for(&plan, "orders", 0),
            Decision::Move(MoveStep::Seat { .. }, _)
        )
    };

    // {a, b} hold 10, c is at 4: the joiner at 6 is within the lag bound but
    // short of what the old set holds.
    let short = Tailed::at(10, &[("broker-b", 10), ("broker-c", 4), ("broker-d", 6)]);
    assert!(!seat(&short), "seated a joiner missing what a and b hold");
    // Level with the majority of the old set.
    assert!(seat(&Tailed::at(
        12,
        &[("broker-b", 10), ("broker-c", 4), ("broker-d", 10)]
    )));
    // A follower the report leaves out may hold everything the leader does.
    assert!(!seat(&Tailed::at(10, &[("broker-c", 4), ("broker-d", 6)])));
    assert!(seat(&Tailed::at(10, &[("broker-c", 4), ("broker-d", 10)])));
    // A `Leader` stream keeps the lag bound alone.
    let plan = plan_with(
        &[replicated_stream("orders", 1, 3)],
        &[],
        &nodes,
        std::slice::from_ref(&joining),
        &short,
        policy(1),
    );
    assert!(matches!(
        decision_for(&plan, "orders", 0),
        Decision::Move(MoveStep::Seat { .. }, _)
    ));
}

/// A node copies at most `max_per_node` shards in or out at once, even
/// with cluster-wide slots to spare.
#[test]
fn copies_into_or_out_of_one_node_are_bounded() {
    let streams = vec![stream("orders", 3)];
    let existing: Vec<ShardAssignment> = (0..3).map(|n| pinned("orders", n, "broker-a")).collect();

    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Positions::default(),
        MovePolicy {
            max_concurrent: 3,
            max_per_node: Some(1),
            ..MovePolicy::default()
        },
    );

    assert_eq!(plan.moves().count(), 1, "{plan:?}");
    assert_eq!(
        plan.waiting()
            .filter(|(_, why)| matches!(why, Blocked::NodeMoveLimit { node } if node == "broker-a"))
            .count(),
        2
    );

    // A move already in flight counts against both of its nodes.
    let mut existing = existing;
    existing[0] = staged("orders", "broker-a", "broker-b", NOW);
    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Positions::default(),
        MovePolicy {
            max_concurrent: 3,
            max_per_node: Some(1),
            ..MovePolicy::default()
        },
    );
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
}

/// With one slot, a drain is served before a rebalance, whatever order
/// the shards sort in. A draining node is waiting to leave; an imbalance
/// only costs evenness.
#[test]
fn a_drain_is_moved_before_a_rebalance() {
    // "audit" sorts first and is on an over-share live node; "zebra" is on
    // the draining one.
    let streams = vec![stream("audit", 3), stream("zebra", 1)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Draining, None),
    ];
    let mut existing: Vec<ShardAssignment> =
        (0..3).map(|n| pinned("audit", n, "broker-a")).collect();
    existing.push(pinned("zebra", 0, "broker-d"));

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &Positions::default(),
        policy(1),
    );

    assert!(
        matches!(
            decision_for(&plan, "zebra", 0),
            Decision::Move(MoveStep::Stage { .. }, _)
        ),
        "{plan:?}"
    );
    assert_eq!(plan.moves().count(), 1);
}

/// A draining node's leaderships move before its follower copies are
/// replaced. Clients feel the leader, not a missing follower, and a broker
/// stopping for a restart waits only for its leaders: a replacement holding
/// the one slot would leave them to fail over.
#[test]
fn a_draining_leader_is_moved_before_its_follower_is_replaced() {
    // "audit" sorts first and only has a follower on the draining node.
    let streams = vec![replicated_stream("audit", 1, 2), stream("zebra", 1)];
    let existing = vec![
        shard("audit", 0, "broker-b", &["broker-a"]),
        pinned("zebra", 0, "broker-a"),
    ];

    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Positions::default(),
        policy(1),
    );

    assert!(
        matches!(
            decision_for(&plan, "zebra", 0),
            Decision::Move(MoveStep::Stage { .. }, _)
        ),
        "{plan:?}"
    );
    assert_eq!(
        decision_for(&plan, "audit", 0),
        &Decision::Waiting(Blocked::MoveLimit)
    );
}

/// Under steady writes the destination is never exactly level. Within the
/// lag bound the leader is fenced, and the drained report then waits for
/// the rest, so nothing is lost by not waiting for exactly level first.
#[test]
fn a_destination_within_the_lag_bound_is_fenced() {
    let streams = vec![stream("orders", 1)];
    let existing = vec![staged("orders", "broker-a", "broker-b", NOW)];
    let bound = MovePolicy::default().fence_max_lag_records;

    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Positions::behind("broker-b", bound),
        policy(1),
    );
    match decision_for(&plan, "orders", 0) {
        Decision::Move(MoveStep::Fence, next) => {
            assert_eq!(next.state, ShardState::Draining);
            assert_eq!(next.successor.as_deref(), Some("broker-b"));
            assert_eq!(next.move_started_at_millis, Some(NOW), "carried");
        }
        other => panic!("expected a fence, got {other:?}"),
    }

    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Positions::behind("broker-b", bound + 1),
        policy(1),
    );
    assert!(
        matches!(
            decision_for(&plan, "orders", 0),
            Decision::Waiting(Blocked::DestinationCatchingUp { .. })
        ),
        "{plan:?}"
    );

    // A leader that does not report its tail gives no lag to judge: only
    // exactly level fences.
    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Reported {
            level: BTreeSet::new(),
        },
        policy(1),
    );
    assert!(matches!(
        decision_for(&plan, "orders", 0),
        Decision::Waiting(Blocked::DestinationCatchingUp { .. })
    ));
}

/// A leader too old to report its tail: only `is_caught_up` is known.
struct Reported {
    level: BTreeSet<String>,
}

impl CaughtUp for Reported {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.level.contains(node_id)
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }
}

/// A staged move that has not reached the fence within the timeout is
/// undone, so it stops holding a slot other moves are waiting for. The
/// start time stays, which sends the shard to the back of the queue.
#[test]
fn a_staged_move_past_its_timeout_is_abandoned() {
    let streams = vec![stream("orders", 1)];
    let timeout = MovePolicy::default().timeout_millis.expect("a default");
    let started = NOW - timeout - 1;
    let existing = vec![staged("orders", "broker-a", "broker-b", started)];

    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Positions::behind("broker-b", 50_000),
        policy(1),
    );
    match decision_for(&plan, "orders", 0) {
        Decision::Move(MoveStep::TimedOut { successor }, next) => {
            assert_eq!(successor, "broker-b");
            assert_eq!(next.leader, "broker-a");
            assert!(next.replicas.is_empty(), "the partial copy is dropped");
            assert_eq!(next.successor, None);
            assert_eq!(next.move_started_at_millis, Some(started));
        }
        other => panic!("expected the move to be abandoned, got {other:?}"),
    }

    // Not yet: it keeps copying.
    let existing = vec![staged("orders", "broker-a", "broker-b", NOW - timeout)];
    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Positions::behind("broker-b", 50_000),
        policy(1),
    );
    assert!(matches!(
        decision_for(&plan, "orders", 0),
        Decision::Waiting(Blocked::DestinationCatchingUp { .. })
    ));
}

/// A replacement that does not catch up in time is dropped, and the
/// follower it was replacing stays.
#[test]
fn a_replacement_past_its_timeout_is_abandoned() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let timeout = MovePolicy::default().timeout_millis.expect("a default");
    let mut replacing = shard("orders", 0, "broker-a", &["broker-b", "broker-c"]);
    replacing.joining = Some("broker-c".to_string());
    replacing.move_started_at_millis = Some(NOW - timeout - 1);

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[replacing],
        &Positions::behind("broker-c", 50_000),
        policy(1),
    );
    match decision_for(&plan, "orders", 0) {
        Decision::Move(MoveStep::TimedOut { successor }, next) => {
            assert_eq!(successor, "broker-c");
            assert_eq!(next.replicas, vec!["broker-b"]);
            assert_eq!(next.joining, None);
        }
        other => panic!("expected the replacement to be abandoned, got {other:?}"),
    }
}

/// A destination that was already a replica before the move is not
/// dropped with it: it is one of the copies the stream asked for.
#[test]
fn an_abandoned_move_keeps_a_replica_the_stream_had() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let timeout = MovePolicy::default().timeout_millis.expect("a default");
    let existing = vec![staged("orders", "broker-a", "broker-b", NOW - timeout - 1)];

    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Positions::behind("broker-b", 50_000),
        policy(1),
    );
    match decision_for(&plan, "orders", 0) {
        Decision::Move(MoveStep::TimedOut { .. }, next) => {
            assert_eq!(next.replicas, vec!["broker-b"]);
        }
        other => panic!("expected the move to be abandoned, got {other:?}"),
    }
}

/// Past the fence the leader has stopped serving, and going back means
/// a new generation and every client following the shard twice. The
/// remainder is bounded by the lag bound, so the move is finished instead.
#[test]
fn a_fenced_move_past_its_timeout_is_finished() {
    let streams = vec![stream("orders", 1)];
    let timeout = MovePolicy::default().timeout_millis.expect("a default");
    let mut fenced = staged("orders", "broker-a", "broker-b", NOW - timeout - 1);
    fenced.state = ShardState::Draining;

    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &[fenced],
        &Positions::behind("broker-b", 5),
        policy(1),
    );
    assert_eq!(
        decision_for(&plan, "orders", 0),
        &Decision::Waiting(Blocked::LeaderStopping)
    );
}

/// A shard whose last move timed out waits behind the others for a slot,
/// so one move that keeps failing cannot hold up the rest of a drain.
#[test]
fn a_shard_whose_move_timed_out_goes_to_the_back() {
    let streams = vec![stream("orders", 2)];
    let mut timed_out = pinned("orders", 0, "broker-a");
    timed_out.move_started_at_millis = Some(1);
    let existing = vec![timed_out, pinned("orders", 1, "broker-a")];

    let plan = plan_with(
        &streams,
        &[],
        &draining_a(),
        &existing,
        &Positions::default(),
        policy(1),
    );

    assert_eq!(
        decision_for(&plan, "orders", 0),
        &Decision::Waiting(Blocked::MoveLimit)
    );
    assert!(matches!(
        decision_for(&plan, "orders", 1),
        Decision::Move(MoveStep::Stage { .. }, _)
    ));
    // The plan still lists shards in key order.
    let order: Vec<u32> = plan.shards.iter().map(|p| p.key.shard).collect();
    assert_eq!(order, vec![0, 1]);
}

/// `max_shards` caps roles, leaders and followers together. A node leading
/// nothing but full of follower roles is not a destination.
#[test]
fn a_destination_full_of_follower_roles_is_skipped() {
    let streams = vec![stream("orders", 1), replicated_stream("audit", 2, 2)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Draining, None),
        node("broker-b", NodeLifecycle::Live, Some(2)),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![
        pinned("orders", 0, "broker-a"),
        shard("audit", 0, "broker-c", &["broker-b"]),
        shard("audit", 1, "broker-c", &["broker-b"]),
    ];

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &Positions::default(),
        policy(1),
    );
    match decision_for(&plan, "orders", 0) {
        Decision::Move(MoveStep::Stage { successor }, _) => {
            assert_eq!(successor, "broker-c", "broker-b is at its cap");
        }
        other => panic!("expected a stage, got {other:?}"),
    }
}

/// Fenced within the lag bound, the destination may still be copying when it
/// dies, and the leader's drained report waits for the destination to be
/// level, so it would never come. The dead destination is dropped at a new,
/// still fenced generation; the leader then reports drained against the
/// followers it has, and the cut-over picks one of them or the leader.
#[test]
fn a_destination_lost_after_the_fence_is_dropped_before_it_is_level() {
    let streams = vec![stream("orders", 1)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Draining, None),
        node("broker-b", NodeLifecycle::Down, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let mut fenced = staged("orders", "broker-a", "broker-b", NOW);
    fenced.state = ShardState::Draining;

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[fenced],
        &Positions::behind("broker-b", 5),
        policy(1),
    );
    match decision_for(&plan, "orders", 0) {
        Decision::Move(MoveStep::Abandon { successor }, next) => {
            assert_eq!(successor, "broker-b");
            assert_eq!(next.leader, "broker-a");
            assert_eq!(next.state, ShardState::Draining, "still fenced");
            assert_eq!(next.successor, None);
            assert!(next.replicas.is_empty());
        }
        other => panic!("expected the dead destination to be dropped, got {other:?}"),
    }
}

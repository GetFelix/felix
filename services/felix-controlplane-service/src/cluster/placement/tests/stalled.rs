//! A restore whose new copy stops moving gives up its slot after the stall
//! window rather than the move timeout, and shards whose restores keep being
//! given up take turns at the slot.
use super::*;
use crate::model::MoveReason;

const NOW: u64 = 10_000_000_000;
const MINUTE: u64 = 60_000;

/// Reports as of `NOW` at generation 3, with when this instance last saw each
/// restore's copy move, by stream.
#[derive(Default)]
struct Watched {
    progressed: BTreeMap<String, u64>,
}

impl CaughtUp for Watched {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        false
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }

    fn progressed_at_millis(&self, key: &ShardKey, _node_id: &str) -> Option<u64> {
        self.progressed.get(&key.stream).copied()
    }
}

fn short(name: &str, leader: &str, follower: &str) -> ShardAssignment {
    assigned(name, leader, &[follower])
}

/// `short`, with a restore into broker-c begun five minutes ago: well inside
/// the thirty-minute move timeout.
fn restoring(name: &str, leader: &str, follower: &str) -> ShardAssignment {
    ShardAssignment {
        replicas: vec![follower.to_string(), "broker-c".to_string()],
        joining: Some("broker-c".to_string()),
        move_started_at_millis: Some(NOW - 5 * MINUTE),
        move_reason: Some(MoveReason::Restore),
        ..assigned(name, leader, &[])
    }
}

fn decisions(existing: &[ShardAssignment], reports: &dyn CaughtUp) -> BTreeMap<String, Decision> {
    let streams = [
        replicated_stream("orders", 1, 3),
        replicated_stream("payments", 1, 3),
    ];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    plan(&streams, &[], &nodes, existing, reports)
        .shards
        .into_iter()
        .map(|shard| (shard.key.stream, shard.decision))
        .collect()
}

/// Copies in flight once a pass's decisions are written: restores already
/// running that the pass did not end, and the ones it starts.
fn copies_after(existing: &[ShardAssignment], decided: &BTreeMap<String, Decision>) -> usize {
    existing
        .iter()
        .filter(|assignment| {
            let ended = matches!(
                decided.get(&assignment.key.stream),
                Some(Decision::Move(
                    MoveStep::Stalled { .. } | MoveStep::TimedOut { .. } | MoveStep::Seat { .. },
                    _
                ))
            );
            assignment.joining.is_some() && !ended
        })
        .count()
        + decided
            .values()
            .filter(|decision| matches!(decision, Decision::Move(MoveStep::Restore { .. }, _)))
            .count()
}

/// **The canvas case.** `orders`' restore into broker-c has not moved for
/// three minutes, past the two-minute stall window, while `payments` is a
/// copy short and waiting for the only slot. The restore is given up, and the
/// next pass hands the slot to `payments`. Never more than one copy is in
/// flight.
#[test]
fn a_restore_that_stops_moving_gives_its_slot_to_the_next_shard() {
    let existing = vec![
        restoring("orders", "broker-a", "broker-b"),
        short("payments", "broker-b", "broker-a"),
    ];
    let reports = Watched {
        progressed: [("orders".to_string(), NOW - 3 * MINUTE)].into(),
    };

    let decided = decisions(&existing, &reports);
    let Some(Decision::Move(MoveStep::Stalled { successor }, undone)) = decided.get("orders")
    else {
        panic!("expected the restore given up, got {decided:?}");
    };
    assert_eq!(successor, "broker-c");
    assert_eq!(undone.replicas, ["broker-b"]);
    assert_eq!(undone.joining, None);
    // Kept, so the shard waits behind the others for its next slot.
    assert_eq!(undone.move_started_at_millis, Some(NOW - 5 * MINUTE));
    // The slot frees with the write, not within the pass that decides it.
    assert_eq!(
        decided.get("payments"),
        Some(&Decision::Waiting(Blocked::MoveLimit))
    );
    assert_eq!(copies_after(&existing, &decided), 0);

    let existing = vec![
        ShardAssignment {
            generation: 3,
            ..undone.clone()
        },
        short("payments", "broker-b", "broker-a"),
    ];
    let decided = decisions(&existing, &Watched::default());
    let Some(Decision::Move(MoveStep::Restore { to, .. }, _)) = decided.get("payments") else {
        panic!("expected payments restored, got {decided:?}");
    };
    assert_eq!(to, "broker-c");
    assert_eq!(
        decided.get("orders"),
        Some(&Decision::Waiting(Blocked::MoveLimit))
    );
    assert_eq!(copies_after(&existing, &decided), 1);
}

/// Inside the window the restore keeps its slot: a copy that moved a minute
/// ago may just be between batches.
#[test]
fn a_restore_that_moved_inside_the_window_keeps_its_slot() {
    let existing = vec![
        restoring("orders", "broker-a", "broker-b"),
        short("payments", "broker-b", "broker-a"),
    ];
    let reports = Watched {
        progressed: [("orders".to_string(), NOW - MINUTE)].into(),
    };
    let decided = decisions(&existing, &reports);
    assert_eq!(
        decided.get("orders"),
        Some(&Decision::Waiting(Blocked::DestinationCatchingUp {
            successor: "broker-c".to_string()
        }))
    );
    assert_eq!(
        decided.get("payments"),
        Some(&Decision::Waiting(Blocked::MoveLimit))
    );
}

/// A pass that has not watched the copy, after a restart or on an instance
/// that just took the lease, cannot tell a stuck copy from a moving one, and
/// leaves it to the move timeout.
#[test]
fn a_restore_nobody_watched_waits_for_the_move_timeout() {
    let existing = vec![
        restoring("orders", "broker-a", "broker-b"),
        short("payments", "broker-b", "broker-a"),
    ];
    let decided = decisions(&existing, &Watched::default());
    assert!(
        matches!(
            decided.get("orders"),
            Some(Decision::Waiting(Blocked::DestinationCatchingUp { .. }))
        ),
        "{decided:?}"
    );
}

/// Turned off, a stalled restore waits for the move timeout as before.
#[test]
fn no_stall_window_waits_for_the_move_timeout() {
    let streams = [
        replicated_stream("orders", 1, 3),
        replicated_stream("payments", 1, 3),
    ];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = vec![
        restoring("orders", "broker-a", "broker-b"),
        short("payments", "broker-b", "broker-a"),
    ];
    let reports = Watched {
        progressed: [("orders".to_string(), NOW - 20 * MINUTE)].into(),
    };
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &reports,
        MovePolicy {
            restore_stall_millis: None,
            ..MovePolicy::default()
        },
    );
    assert!(
        matches!(
            plan.shards[0].decision,
            Decision::Waiting(Blocked::DestinationCatchingUp { .. })
        ),
        "{:?}",
        plan.shards[0].decision
    );
}

/// **Fairness.** Two short shards whose restores were both given up: the one
/// that last had the slot longest ago goes first, whatever the key order, so a
/// restore that never finishes cannot take the slot every time.
#[test]
fn shards_whose_restores_keep_stalling_take_turns() {
    let given_up = |name: &str, leader: &str, follower: &str, started: u64| ShardAssignment {
        move_started_at_millis: Some(started),
        ..short(name, leader, follower)
    };
    for (orders, payments, first) in [
        (NOW - 10 * MINUTE, NOW - 20 * MINUTE, "payments"),
        (NOW - 20 * MINUTE, NOW - 10 * MINUTE, "orders"),
    ] {
        let existing = vec![
            given_up("orders", "broker-a", "broker-b", orders),
            given_up("payments", "broker-b", "broker-a", payments),
        ];
        let decided = decisions(&existing, &Watched::default());
        let restored: Vec<&str> = decided
            .iter()
            .filter(|(_, decision)| matches!(decision, Decision::Move(MoveStep::Restore { .. }, _)))
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(restored, [first], "{decided:?}");
    }
}

/// One report: the generation it was made at, where it puts broker-c, and
/// the store's clock.
struct Report {
    generation: u64,
    offset: Option<u64>,
    now: u64,
}

impl CaughtUp for Report {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        false
    }

    fn reported_offset(&self, _key: &ShardKey, _node_id: &str) -> Option<u64> {
        self.offset
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(self.generation)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(self.now)
    }
}

/// Progress is the copy's position going up. A report that leaves the copy
/// out, or names it where it was, is not progress; a new generation starts
/// the watch again, since offsets are only comparable within one.
#[test]
fn only_a_position_going_up_counts_as_progress() {
    let mut progress = super::super::progress::CopyProgress::default();
    let mut existing = vec![restoring("orders", "broker-a", "broker-b")];
    let key = existing[0].key.clone();
    let mut watch = |existing: &[ShardAssignment], generation, offset, now| {
        progress
            .observe(
                existing,
                &Report {
                    generation,
                    offset,
                    now,
                },
            )
            .get(&(key.clone(), "broker-c".to_string()))
            .copied()
    };

    assert_eq!(watch(&existing, 3, None, 1_000), Some(1_000));
    assert_eq!(watch(&existing, 3, None, 2_000), Some(1_000));
    assert_eq!(watch(&existing, 3, Some(10), 3_000), Some(3_000));
    assert_eq!(watch(&existing, 3, None, 4_000), Some(3_000));
    assert_eq!(watch(&existing, 3, Some(10), 5_000), Some(3_000));
    assert_eq!(watch(&existing, 3, Some(11), 6_000), Some(6_000));
    // A report from another generation says nothing about this one's copy.
    assert_eq!(watch(&existing, 2, Some(50), 7_000), Some(6_000));

    existing[0].generation = 4;
    assert_eq!(watch(&existing, 4, Some(5), 8_000), Some(8_000));

    // Seated or undone: forgotten.
    existing[0].joining = None;
    assert_eq!(watch(&existing, 4, Some(5), 9_000), None);
}

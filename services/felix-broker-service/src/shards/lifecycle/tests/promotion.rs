//! A promoted shard waits for the fence before it serves:
//! `fencing[b]` in `docs/formal/FelixShard.tla`, which `Serving` excludes.

use super::*;

fn fencing_lifecycle() -> ShardLifecycle {
    let mut own = lifecycle();
    own.fence_promotions();
    own
}

/// **A promoted shard does not serve until replication says it is fenced**,
/// and the write fence refuses a write that got admitted meanwhile.
#[test]
fn a_promotion_waits_in_fencing_until_the_fence_is_done() {
    let mut own = fencing_lifecycle();

    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 3))),
        Action::Open {
            key: key(0),
            generation: 3,
            new_term: true,
            fence: true,
        }
    );
    assert_eq!(own.opened(&key(0), 3), Opened::Fencing);
    assert_eq!(own.phase(&key(0)), Phase::Fencing);
    assert!(!own.may_serve(&key(0)));
    assert!(own.servable().is_empty());
    assert_eq!(own.fence().awaiting_promotion(&key(0)), Some(3));
    assert!(own.fence().admit(&key(0), 3).is_err());

    // Re-delivered while fencing: nothing new to do.
    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 3))),
        Action::None
    );
    assert!(
        !own.fenced(&key(0), 2),
        "a stale generation must not open it"
    );

    assert!(own.fenced(&key(0), 3));
    assert_eq!(own.phase(&key(0)), Phase::Active);
    assert_eq!(own.fence().awaiting_promotion(&key(0)), None);
    assert!(own.fence().admit(&key(0), 3).is_ok());
}

/// A move's destination takes over from a leader that drained into it, which
/// the model does not fence and neither does the broker.
#[test]
fn a_moves_destination_is_not_held_for_the_fence() {
    let mut own = fencing_lifecycle();
    own.observe(&key(0), Some(&moving_to("broker-a", 2, "active")));

    assert!(matches!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 4))),
        Action::Open { fence: false, .. }
    ));
    assert_eq!(own.opened(&key(0), 4), Opened::Activated);
}

/// A cancelled move hands the shard back to the leader that was draining it,
/// with its fenced writes; the model's `Retake`, unfenced.
#[test]
fn a_cancelled_move_is_not_held_for_the_fence() {
    let mut own = fencing_lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    own.opened(&key(0), 2);
    own.fenced(&key(0), 2);
    let draining = ShardAssignment {
        leader: "broker-a".to_string(),
        replicas: vec!["broker-b".to_string()],
        state: "draining".to_string(),
        successor: Some("broker-b".to_string()),
        ..assigned_to("broker-a", 3)
    };
    own.observe(&key(0), Some(&draining));
    assert_eq!(own.opened(&key(0), 3), Opened::Draining);

    assert!(matches!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 4))),
        Action::Open { fence: false, .. }
    ));
}

/// Reassigned away while fencing: it never served, and it lets go like an
/// open shard does.
#[test]
fn a_shard_reassigned_while_fencing_is_released() {
    let mut own = fencing_lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    own.opened(&key(0), 3);

    assert!(matches!(
        own.observe(&key(0), Some(&assigned_to("broker-b", 4))),
        Action::Release { .. }
    ));
    assert_eq!(own.fence().awaiting_promotion(&key(0)), None);
    assert!(!own.fenced(&key(0), 3));
}

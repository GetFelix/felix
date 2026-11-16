//! When a Raft shard's offset floor rises, and when a write keeps it.
use super::*;
use crate::model::{PlacedInEpoch, SealReport, SealedSideLog, ShardKind, ShardState, ShardSwitch};

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "payments".to_string(),
        stream: "orders".to_string(),
        shard: 2,
        kind: ShardKind::Stream,
    }
}

/// A shard at stream incarnation 4 that switched to Raft.
fn switched() -> ShardAssignment {
    let side = SealedSideLog {
        start: 0,
        end: 0,
        digest: "0".repeat(64),
    };
    ShardAssignment {
        key: key(),
        leader: "broker-a".to_string(),
        replicas: vec!["broker-b".to_string()],
        generation: 7,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 4,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        learners: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        placed: None,
        switch: Some(ShardSwitch {
            generation: 7,
            sealer: "broker-a".to_string(),
            sealer_copy: None,
            report: Some(SealReport {
                generation: 7,
                offset: 0,
                start: 0,
                digest: "0".repeat(64),
                dead_letters: side.clone(),
                cursors: side,
                voters: Default::default(),
            }),
        }),
        raft_copy_ids: Default::default(),
        raft_target: None,
        raft_floor: None,
        raft_ordered: None,
    }
}

fn report(index: u64, term: u64) -> RaftFloorReport {
    RaftFloorReport {
        key: key(),
        incarnation: 4,
        epoch: 0,
        index,
        term,
    }
}

fn floor(a: &ShardAssignment) -> Option<(u64, u64)> {
    a.raft_floor.as_ref().map(|floor| (floor.index, floor.term))
}

#[test]
fn the_floor_only_rises() {
    let mut a = switched();
    assert_eq!(a.raise_raft_floor(&report(10, 2)), Ok(true));
    assert_eq!(floor(&a), Some((10, 2)));
    // Equal and lower indexes are no-ops, at any term.
    assert_eq!(a.raise_raft_floor(&report(10, 3)), Ok(false));
    assert_eq!(a.raise_raft_floor(&report(9, 5)), Ok(false));
    assert_eq!(floor(&a), Some((10, 2)));
    assert_eq!(a.raise_raft_floor(&report(11, 2)), Ok(true));
    assert_eq!(a.raise_raft_floor(&report(20, 3)), Ok(true));
    assert_eq!(floor(&a), Some((20, 3)));
}

#[test]
fn a_higher_index_at_a_lower_term_is_refused() {
    let mut a = switched();
    a.raise_raft_floor(&report(10, 3)).expect("first");
    assert_eq!(
        a.raise_raft_floor(&report(12, 2)),
        Err(RaftFloorError::LowerTerm {
            index: 12,
            term: 2,
            held_index: 10,
            held_term: 3,
        })
    );
    assert_eq!(floor(&a), Some((10, 3)));
}

#[test]
fn only_a_switched_shard_at_the_reports_incarnation_and_epoch_keeps_one() {
    let mut hybrid = switched();
    hybrid.switch = None;
    assert_eq!(
        hybrid.raise_raft_floor(&report(1, 1)),
        Err(RaftFloorError::NotSwitched)
    );

    let mut a = switched();
    let other = RaftFloorReport {
        incarnation: 3,
        ..report(50, 9)
    };
    assert_eq!(
        a.raise_raft_floor(&other),
        Err(RaftFloorError::OtherIncarnation {
            reported: 3,
            current: 4
        })
    );

    // The reform epoch is the placed record's, 0 without one.
    let ahead = RaftFloorReport {
        epoch: 1,
        ..report(50, 9)
    };
    assert_eq!(
        a.raise_raft_floor(&ahead),
        Err(RaftFloorError::OtherEpoch {
            reported: 1,
            current: 0
        })
    );
    a.placed = Some(PlacedInEpoch {
        epoch: 2,
        brokers: Default::default(),
    });
    assert_eq!(
        a.raise_raft_floor(&ahead),
        Err(RaftFloorError::OtherEpoch {
            reported: 1,
            current: 2
        })
    );
    assert_eq!(floor(&a), None);
    let at = RaftFloorReport {
        epoch: 2,
        ..report(5, 1)
    };
    assert_eq!(a.raise_raft_floor(&at), Ok(true));
}

/// A new epoch's first report replaces the exact log id with its own and
/// keeps the higher `offset_floor`: the offsets the earlier epoch gave out
/// stay given out. No field ever pairs an index from one epoch with a term
/// from another.
#[test]
fn a_new_epoch_keeps_the_offset_floor_and_stores_its_own_log_id() {
    let floor = |a: &ShardAssignment| {
        a.raft_floor
            .as_ref()
            .map(|f| (f.offset_floor, (f.epoch, f.index, f.term)))
    };
    let mut a = switched();
    a.raise_raft_floor(&report(100, 9)).expect("epoch 0");
    a.placed = Some(PlacedInEpoch {
        epoch: 1,
        brokers: Default::default(),
    });
    let in_epoch_1 = |index, term| RaftFloorReport {
        epoch: 1,
        ..report(index, term)
    };
    assert_eq!(a.raise_raft_floor(&in_epoch_1(3, 1)), Ok(true));
    assert_eq!(floor(&a), Some((100, (1, 3, 1))));
    // Compared within the epoch only: term 1, not epoch 0's 9.
    assert_eq!(a.raise_raft_floor(&in_epoch_1(50, 2)), Ok(true));
    assert_eq!(floor(&a), Some((100, (1, 50, 2))));
    assert_eq!(a.raise_raft_floor(&in_epoch_1(120, 2)), Ok(true));
    assert_eq!(floor(&a), Some((120, (1, 120, 2))));
    assert_eq!(a.raise_raft_floor(&in_epoch_1(120, 2)), Ok(false));
}

#[test]
fn a_write_at_the_same_incarnation_keeps_the_floor() {
    let mut held = switched();
    held.raise_raft_floor(&report(10, 2)).expect("floor");

    let mut same = switched();
    same.carry_raft_floor(Some(&held));
    assert_eq!(same.raft_floor, held.raft_floor);

    let mut next = ShardAssignment {
        incarnation: 5,
        ..switched()
    };
    next.carry_raft_floor(Some(&held));
    assert_eq!(next.raft_floor, None);

    let mut fresh = switched();
    fresh.raft_floor = held.raft_floor.clone();
    fresh.carry_raft_floor(None);
    assert_eq!(fresh.raft_floor, None, "a caller's value is replaced");
}

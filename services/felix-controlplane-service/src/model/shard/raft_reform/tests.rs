//! The re-form planner's record, built from a shard assignment.
use std::collections::{BTreeMap, BTreeSet};

use felix_storage::raft_copy::EpochLogId;

use super::*;
use crate::model::{
    PlacedInEpoch, RaftFloor, RaftOrdered, RaftTarget, ShardKey, ShardKind, ShardState,
};

fn set(nodes: &[&str]) -> BTreeSet<String> {
    nodes.iter().map(|n| n.to_string()).collect()
}

fn target(counter: u64, pairs: &[(&str, u64)]) -> RaftTarget {
    RaftTarget {
        counter,
        members: pairs.iter().map(|(n, id)| (n.to_string(), *id)).collect(),
    }
}

/// A Raft shard at incarnation 3, epoch 0, moved from {a, b, c} to
/// {a, b, d}: d is placed and both targets were ordered.
fn assignment() -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "payments".to_string(),
            stream: "orders".to_string(),
            shard: 2,
            kind: ShardKind::Stream,
        },
        leader: "a".to_string(),
        replicas: vec!["b".to_string(), "d".to_string()],
        generation: 7,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 3,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        learners: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        placed: Some(PlacedInEpoch {
            epoch: 0,
            brokers: set(&["a", "b", "c", "d"]),
        }),
        switch: None,
        raft_copy_ids: BTreeMap::from([
            ("a".to_string(), 11),
            ("b".to_string(), 12),
            ("d".to_string(), 14),
        ]),
        raft_target_counter: 0,
        raft_target: Some(target(2, &[("a", 11), ("b", 12), ("d", 14)])),
        raft_floor: Some(RaftFloor {
            incarnation: 3,
            offset_floor: 40,
            epoch: 0,
            index: 40,
            term: 5,
        }),
        raft_formed: None,
        raft_voters: None,
        raft_seed: None,
        raft_bases: None,
        raft_ordered: Some(RaftOrdered {
            incarnation: 3,
            epoch: 0,
            targets: vec![
                target(1, &[("a", 11), ("b", 12), ("c", 13)]),
                target(2, &[("a", 11), ("b", 12), ("d", 14)]),
            ],
            lost: false,
        }),
    }
}

fn voters(pairs: &[(&str, u64)]) -> BTreeSet<(String, u64)> {
    pairs.iter().map(|(n, id)| (n.to_string(), *id)).collect()
}

/// Every field the record holds reaches the planner: the removed broker c
/// stays placed, the floor keeps its term, both targets are voter sets,
/// and the brokers declared dead are passed through.
#[test]
fn the_record_carries_what_the_shard_holds() {
    let record = assignment().reform_record(&set(&["c"])).expect("epoch 0");
    assert_eq!(
        record,
        GroupRecord {
            incarnation: 3,
            bases: Vec::new(),
            floor: Some(EpochLogId {
                index: 40,
                epoch: 0,
                term: 5,
            }),
            offset_floor: Some(40),
            placed: Some(set(&["a", "b", "c", "d"])),
            dead: set(&["c"]),
            seed: None,
            ordered: Some(vec![
                voters(&[("a", 11), ("b", 12), ("c", 13)]),
                voters(&[("a", 11), ("b", 12), ("d", 14)]),
            ]),
            restore: None,
        }
    );
}

/// A floor left from before the stream was created again belongs to
/// another group. Read as this group's, it would cut the new group's base
/// or reserve offsets it never gave out.
#[test]
fn a_floor_of_another_incarnation_is_dropped() {
    let mut a = assignment();
    a.raft_floor.as_mut().expect("floor").incarnation = 2;
    let record = a.reform_record(&BTreeSet::new()).expect("epoch 0");
    assert_eq!(record.floor, None);
    assert_eq!(record.offset_floor, None);
}

/// A floor reported in another epoch is not this group's floor.
#[test]
fn a_floor_of_another_epoch_is_dropped() {
    let mut a = assignment();
    a.raft_floor.as_mut().expect("floor").epoch = 1;
    let record = a.reform_record(&BTreeSet::new()).expect("epoch 0");
    assert_eq!(record.floor, None);
}

/// Past epoch 0 without the bases of earlier re-forms (a record written
/// below metadata level 47), no record is built rather than one that claims
/// the group never re-formed.
#[test]
fn a_group_past_epoch_zero_is_refused() {
    let mut a = assignment();
    a.placed.as_mut().expect("placed").epoch = 1;
    assert_eq!(
        a.reform_record(&BTreeSet::new()),
        Err(ReformRecordError::BasesNotRecorded(1))
    );
}

/// A shard with no complete record of its placed brokers (first assigned
/// below the level that keeps it, or restored) and a list of targets that
/// lost one stay unknown, so the planner blocks or marks the group.
#[test]
fn what_the_record_cannot_vouch_for_stays_unknown() {
    let mut a = assignment();
    a.placed = None;
    a.raft_ordered.as_mut().expect("ordered").lost = true;
    let record = a.reform_record(&BTreeSet::new()).expect("epoch 0");
    assert_eq!(record.placed, None);
    assert_eq!(record.ordered, None);
    assert_eq!(record.seed, None);
}

fn copies(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs.iter().map(|(n, id)| (n.to_string(), *id)).collect()
}

fn seed(incarnation: u64, epoch: u64, pairs: &[(&str, u64)]) -> RaftSeed {
    RaftSeed {
        incarnation,
        epoch,
        members: copies(pairs),
        starts: BTreeMap::new(),
    }
}

/// A seed reaches the planner only for this incarnation and epoch: one left
/// from before a re-create or a re-form names another group's voters.
#[test]
fn only_the_current_epochs_seed_is_read() {
    let mut a = assignment();
    a.raft_seed = Some(seed(3, 0, &[("a", 11), ("b", 12), ("c", 13)]));
    let record = a.reform_record(&BTreeSet::new()).expect("epoch 0");
    assert_eq!(
        record.seed,
        Some(voters(&[("a", 11), ("b", 12), ("c", 13)]))
    );

    for stale in [seed(2, 0, &[("a", 11)]), seed(3, 1, &[("a", 11)])] {
        a.raft_seed = Some(stale);
        let record = a.reform_record(&BTreeSet::new()).expect("epoch 0");
        assert_eq!(record.seed, None);
    }
}

/// The seed is the epoch's first target, taken in the write that stores
/// it. A list holding more than that target, or one lost, started too late
/// to show which target came first, so no seed is written.
#[test]
fn the_first_target_of_the_epoch_is_its_seed() {
    let first = target(1, &[("a", 11), ("b", 12), ("c", 13)]);
    let mut a = assignment();
    a.raft_target = Some(first.clone());
    a.raft_ordered.as_mut().expect("ordered").targets = vec![first.clone()];
    a.seed_from_first_target();
    assert_eq!(
        a.raft_seed,
        Some(seed(3, 0, &[("a", 11), ("b", 12), ("c", 13)]))
    );

    // A later target, such as a wiped copy's new id, leaves it alone.
    let second = target(2, &[("a", 11), ("b", 12), ("c", 23)]);
    a.raft_target = Some(second.clone());
    a.raft_ordered
        .as_mut()
        .expect("ordered")
        .targets
        .push(second);
    a.seed_from_first_target();
    assert_eq!(
        a.raft_seed,
        Some(seed(3, 0, &[("a", 11), ("b", 12), ("c", 13)]))
    );

    // Even when the target in force is the list's first again.
    let mut late = assignment();
    late.raft_target = late
        .raft_ordered
        .as_ref()
        .and_then(|o| o.targets.first().cloned());
    late.seed_from_first_target();
    assert_eq!(late.raft_seed, None, "two targets on the list");

    let mut lost = assignment();
    lost.raft_target = Some(first.clone());
    let ordered = lost.raft_ordered.as_mut().expect("ordered");
    ordered.targets = vec![first];
    ordered.lost = true;
    lost.seed_from_first_target();
    assert_eq!(lost.raft_seed, None, "lost list");
}

fn base(last: Option<(u64, u64, u64)>, first_index: u64) -> RaftEpochBase {
    RaftEpochBase {
        base_last: last.map(|(index, epoch, term)| RaftLogId { index, epoch, term }),
        first_index,
    }
}

/// The re-form's write starts the next epoch in one step: its placed
/// brokers, the base appended, the plan's voters as its seed and an empty
/// list of targets. The record of the new epoch then builds, and a second
/// re-form can follow it.
#[test]
fn a_re_form_writes_the_next_epochs_record() {
    let mut a = assignment();
    let dead = set(&["c"]);
    a.start_reformed_epoch(
        0,
        &dead,
        Some(40),
        base(Some((40, 0, 5)), 45),
        copies(&[("a", 11), ("b", 12)]),
    )
    .expect("epoch 1");
    assert_eq!(a.placed.as_ref().map(|placed| placed.epoch), Some(1));
    assert_eq!(
        a.placed.as_ref().map(|placed| placed.brokers.clone()),
        Some(set(&["a", "b", "d"]))
    );
    assert_eq!(a.raft_seed, Some(seed(3, 1, &[("a", 11), ("b", 12)])));
    // The target stays: it is what tells the new leader to add the learner
    // `d` and promote it once it has caught up.
    assert_eq!(
        a.raft_target,
        Some(target(2, &[("a", 11), ("b", 12), ("d", 14)]))
    );
    let ordered = a.raft_ordered.as_ref().expect("ordered");
    assert_eq!((ordered.epoch, ordered.targets.len()), (1, 0));

    let record = a.reform_record(&BTreeSet::new()).expect("bases recorded");
    assert_eq!(
        record.bases,
        vec![felix_storage::raft_copy::EpochBase {
            base_last: Some(EpochLogId {
                index: 40,
                epoch: 0,
                term: 5
            }),
            first_index: 45,
        }]
    );
    assert_eq!(record.seed, Some(voters(&[("a", 11), ("b", 12)])));

    a.start_reformed_epoch(
        1,
        &BTreeSet::new(),
        Some(40),
        base(None, 60),
        copies(&[("a", 11)]),
    )
    .expect("epoch 2");
    let record = a.reform_record(&BTreeSet::new()).expect("both bases");
    assert_eq!(record.bases.len(), 2);
}

/// A stale plan, a base that would give out an offset again, a voter the
/// new epoch does not place and an epoch whose earlier bases are missing
/// are all refused, and the record is left as it was.
#[test]
fn a_re_form_write_refuses_what_it_cannot_vouch_for() {
    let voters = || copies(&[("a", 11), ("b", 12)]);
    let refused = |mut a: ShardAssignment, expected, base, voters| {
        let before = a.clone();
        let err = a
            .start_reformed_epoch(expected, &BTreeSet::new(), Some(40), base, voters)
            .expect_err("refused");
        assert_eq!(a, before);
        err
    };
    assert_eq!(
        refused(assignment(), 1, base(None, 45), voters()),
        ReformWriteError::EpochMoved {
            expected: 1,
            current: 0
        }
    );
    assert!(matches!(
        refused(assignment(), 0, base(Some((45, 0, 5)), 45), voters()),
        ReformWriteError::BaseBehind(_)
    ));
    assert!(matches!(
        refused(assignment(), 0, base(Some((40, 1, 5)), 45), voters()),
        ReformWriteError::BaseBehind(_)
    ));
    assert_eq!(
        refused(assignment(), 0, base(None, 45), copies(&[("e", 1)])),
        ReformWriteError::VoterNotPlaced("e".to_string())
    );
    assert_eq!(
        refused(assignment(), 0, base(None, 45), BTreeMap::new()),
        ReformWriteError::NoVoter
    );

    let mut once = assignment();
    once.start_reformed_epoch(0, &BTreeSet::new(), Some(40), base(None, 45), voters())
        .expect("epoch 1");
    assert!(matches!(
        refused(once.clone(), 1, base(None, 45), voters()),
        ReformWriteError::BaseBehind(_)
    ));
    let mut unrecorded = once;
    unrecorded.raft_bases = None;
    assert_eq!(
        refused(unrecorded, 1, base(None, 60), voters()),
        ReformWriteError::Record(ReformRecordError::BasesNotRecorded(1))
    );
}

/// A leader that reports a higher floor after the plan read the record
/// refuses the start: the plan may cut entries up to it, and its first
/// index may be at or below it. A first index at the floor is refused even
/// for a plan that read it.
#[test]
fn a_floor_raised_after_the_plan_refuses_the_start() {
    let voters = || copies(&[("a", 11), ("b", 12)]);
    let mut raised = assignment();
    if let Some(floor) = raised.raft_floor.as_mut() {
        floor.offset_floor = 50;
        floor.index = 50;
    }
    let before = raised.clone();
    assert_eq!(
        raised.start_reformed_epoch(
            0,
            &BTreeSet::new(),
            Some(40),
            base(Some((40, 0, 5)), 45),
            voters()
        ),
        Err(ReformWriteError::FloorRaised {
            planned: 40,
            stored: 50
        })
    );
    assert_eq!(
        raised.start_reformed_epoch(0, &BTreeSet::new(), None, base(None, 55), voters()),
        Err(ReformWriteError::FloorRaised {
            planned: 0,
            stored: 50
        })
    );
    assert_eq!(raised, before);

    let mut at_floor = assignment();
    assert!(matches!(
        at_floor.start_reformed_epoch(0, &BTreeSet::new(), Some(40), base(None, 40), voters()),
        Err(ReformWriteError::BaseBehind(_))
    ));
}

/// A base whose last entry is from a re-formed epoch but below that
/// epoch's first index names an entry no copy can hold, and is refused.
#[test]
fn a_base_below_its_epochs_first_index_is_refused() {
    let mut a = assignment();
    a.start_reformed_epoch(
        0,
        &BTreeSet::new(),
        Some(40),
        base(None, 45),
        copies(&[("a", 11)]),
    )
    .expect("epoch 1");
    let before = a.clone();
    assert!(matches!(
        a.start_reformed_epoch(
            1,
            &BTreeSet::new(),
            Some(40),
            base(Some((44, 1, 6)), 60),
            copies(&[("a", 11)])
        ),
        Err(ReformWriteError::BaseBehind(_))
    ));
    assert_eq!(a, before);
    a.start_reformed_epoch(
        1,
        &BTreeSet::new(),
        Some(40),
        base(Some((45, 1, 6)), 60),
        copies(&[("a", 11)]),
    )
    .expect("an entry at the epoch's first index");
}

/// Seed and bases are store-owned: a write at the same incarnation keeps
/// the stored ones whatever it passes, and a new incarnation starts none,
/// so a caller can neither forge nor roll back a base.
#[test]
fn a_write_keeps_the_stored_seed_and_bases() {
    let mut held = assignment();
    held.start_reformed_epoch(
        0,
        &BTreeSet::new(),
        Some(40),
        base(None, 45),
        copies(&[("a", 11)]),
    )
    .expect("epoch 1");
    let mut write = assignment();
    write.raft_seed = Some(seed(3, 1, &[("e", 1)]));
    write.raft_bases = None;
    write.carry_raft_epochs(Some(&held));
    assert_eq!(
        (&write.raft_seed, &write.raft_bases),
        (&held.raft_seed, &held.raft_bases)
    );

    write.incarnation = 4;
    write.carry_raft_epochs(Some(&held));
    assert_eq!((write.raft_seed, write.raft_bases), (None, None));
}

/// A plan made for an earlier incarnation of the stream never starts the
/// new incarnation's group: both are at epoch 0, so only the incarnation
/// tells them apart, and the old plan's base would sit below offsets the
/// new group has not handed out yet.
#[test]
fn a_plan_for_another_incarnation_is_refused() {
    let mut a = assignment();
    let start = ReformEpochStart {
        incarnation: 2,
        expected_epoch: 0,
        planned_floor: Some(40),
        base: base(Some((40, 0, 5)), 45),
        voters: copies(&[("a", 11), ("b", 12)]),
        dead: set(&["c"]),
        starts: BTreeMap::new(),
    };
    let before = a.clone();
    assert_eq!(
        a.start_reform(&start),
        Err(ReformWriteError::OtherIncarnation {
            planned: 2,
            current: 3
        })
    );
    assert_eq!(a, before);
    a.start_reform(&ReformEpochStart {
        incarnation: 3,
        ..start
    })
    .expect("this incarnation");
    assert_eq!(a.group_epoch(), 1);
}

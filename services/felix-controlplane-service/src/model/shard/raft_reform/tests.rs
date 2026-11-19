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
        raft_target: Some(target(2, &[("a", 11), ("b", 12), ("d", 14)])),
        raft_floor: Some(RaftFloor {
            incarnation: 3,
            offset_floor: 40,
            epoch: 0,
            index: 40,
            term: 5,
        }),
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

/// Past epoch 0 the record would need the bases of earlier re-forms, which
/// it does not keep, so no record is built rather than one that claims the
/// group never re-formed.
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

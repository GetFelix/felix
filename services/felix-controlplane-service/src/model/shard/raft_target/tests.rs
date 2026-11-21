//! When a Raft shard's target counter rises.
use std::collections::BTreeMap;

use super::*;
use crate::model::{ShardKey, ShardKind, ShardState};

fn assignment() -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "payments".to_string(),
            stream: "orders".to_string(),
            shard: 2,
            kind: ShardKind::Stream,
        },
        leader: "broker-a".to_string(),
        replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
        generation: 7,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        learners: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        placed: None,
        switch: None,
        raft_copy_ids: Default::default(),
        raft_target: None,
        raft_floor: None,
        raft_formed: None,
        raft_voters: None,
        raft_ordered: None,
    }
}

fn ids(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs.iter().map(|(n, id)| (n.to_string(), *id)).collect()
}

fn reported(mut a: ShardAssignment, pairs: &[(&str, u64)]) -> ShardAssignment {
    a.raft_copy_ids.extend(ids(pairs));
    a
}

#[test]
fn no_target_until_every_broker_in_the_set_reported() {
    let mut a = reported(assignment(), &[("broker-a", 1), ("broker-b", 2)]);
    assert_eq!(a.settle_raft_target(), Ok(false));
    assert_eq!(a.raft_target, None);
    a.raft_copy_ids.insert("broker-c".into(), 3);
    assert_eq!(a.settle_raft_target(), Ok(true));
    let target = a.raft_target.clone().expect("target");
    assert_eq!(target.counter, 1);
    assert_eq!(
        target.members,
        ids(&[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)])
    );
}

#[test]
fn the_same_set_keeps_the_counter() {
    let mut a = reported(
        assignment(),
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
    );
    a.settle_raft_target().expect("first");
    // A write naming the same set, its leader swapped among the members.
    a.leader = "broker-b".into();
    a.replicas = vec!["broker-a".into(), "broker-c".into()];
    assert_eq!(a.settle_raft_target(), Ok(false));
    assert_eq!(a.raft_target.as_ref().map(|t| t.counter), Some(1));
}

#[test]
fn a_set_change_takes_the_next_counter_and_drops_the_leavers_id() {
    let mut a = reported(
        assignment(),
        &[
            ("broker-a", 1),
            ("broker-b", 2),
            ("broker-c", 3),
            ("broker-d", 4),
        ],
    );
    a.settle_raft_target().expect("first");
    assert!(!a.raft_copy_ids.contains_key("broker-d"));
    a.replicas = vec!["broker-b".into(), "broker-d".into()];
    a.raft_copy_ids.insert("broker-d".into(), 4);
    assert_eq!(a.settle_raft_target(), Ok(true));
    let target = a.raft_target.expect("target");
    assert_eq!(target.counter, 2);
    assert_eq!(
        target.members,
        ids(&[("broker-a", 1), ("broker-b", 2), ("broker-d", 4)])
    );
    assert!(!a.raft_copy_ids.contains_key("broker-c"));
}

#[test]
fn a_new_copy_id_takes_the_next_counter() {
    let mut a = reported(
        assignment(),
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
    );
    a.settle_raft_target().expect("first");
    a.raft_copy_ids.insert("broker-b".into(), 9);
    assert_eq!(a.settle_raft_target(), Ok(true));
    assert_eq!(a.raft_target.as_ref().map(|t| t.counter), Some(2));
    // The same id again changes nothing.
    a.raft_copy_ids.insert("broker-b".into(), 9);
    assert_eq!(a.settle_raft_target(), Ok(false));
    assert_eq!(a.raft_target.as_ref().map(|t| t.counter), Some(2));
}

#[test]
fn the_counter_is_never_reused_at_its_limit() {
    let mut a = reported(
        assignment(),
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
    );
    a.raft_target = Some(RaftTarget {
        counter: u64::MAX,
        members: ids(&[("broker-a", 1)]),
    });
    assert_eq!(
        a.settle_raft_target(),
        Err(RaftTargetError::CounterExhausted)
    );
}

/// Only a leader of the record's reform epoch marks the group formed: a
/// leader of an older epoch, cut off before the re-form, still reports and
/// must not release the new target's hold (#2794).
#[test]
fn only_a_report_from_the_placed_epoch_marks_the_group_formed() {
    let mut a = assignment();
    a.placed = Some(crate::model::PlacedInEpoch {
        epoch: 1,
        brokers: Default::default(),
    });
    assert!(!a.mark_raft_formed(0, 0), "an old epoch's leader");
    assert!(!a.mark_raft_formed(1, 1), "another incarnation");
    assert_eq!(a.raft_formed, None);
    assert!(a.mark_raft_formed(0, 1));
    assert_eq!(
        a.raft_formed,
        Some(RaftFormed {
            incarnation: 0,
            epoch: 1
        })
    );
}

/// The mark is per epoch: after a re-form, the new epoch's first target is
/// held again until a leader of the new epoch reports.
#[test]
fn a_reform_holds_the_new_epochs_first_target() {
    let mut a = reported(
        assignment(),
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
    );
    assert_eq!(a.settle_raft_target(), Ok(true));
    let placed = crate::model::PlacedInEpoch {
        epoch: 0,
        brokers: a.placed_nodes().cloned().collect(),
    };
    a.placed = Some(placed.clone());
    assert!(a.mark_raft_formed(0, 0));
    assert!(!a.holds_unformed_set());

    a.placed = Some(placed.next_epoch(&a, &Default::default()));
    let mut next = a.clone();
    next.carry_raft_formed(Some(&a));
    assert!(
        next.holds_unformed_set(),
        "the old epoch's mark holds nothing"
    );
    assert!(!next.mark_raft_formed(0, 0));
    assert!(next.mark_raft_formed(0, 1));
    assert!(!next.holds_unformed_set());
}

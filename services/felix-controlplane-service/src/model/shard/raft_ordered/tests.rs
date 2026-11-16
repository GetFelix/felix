//! The targets a Raft shard ordered in its epoch.
use std::collections::BTreeSet;

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
        raft_ordered: None,
    }
}

/// Report `pairs` and write the target they make, as a copy-id report does.
fn order(a: &mut ShardAssignment, pairs: &[(&str, u64)], kept: bool) {
    for (node, id) in pairs {
        a.raft_copy_ids.insert(node.to_string(), *id);
    }
    if a.settle_raft_target().expect("counter") {
        a.order_raft_target(kept);
    }
}

fn set(nodes: &[&str]) -> BTreeSet<String> {
    nodes.iter().map(|n| n.to_string()).collect()
}

/// A shard switched with the list started, and its first target written.
fn voters(pairs: &[(&str, u64)]) -> BTreeSet<(String, u64)> {
    pairs.iter().map(|(n, id)| (n.to_string(), *id)).collect()
}

/// A restore marks the list lost, so a list as old as the backup is never
/// read as complete.
#[test]
fn a_lost_list_reads_as_unknown() {
    let mut a = started();
    assert!(a.ordered_voter_sets().is_some());
    a.lose_raft_ordered();
    assert_eq!(a.ordered_voter_sets(), None);
}

fn started() -> ShardAssignment {
    let mut a = assignment();
    a.start_raft_ordered();
    order(
        &mut a,
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
        true,
    );
    a
}

#[test]
fn a_new_copy_id_appends_and_the_first_target_stays() {
    let mut a = started();
    order(&mut a, &[("broker-b", 9)], true);
    assert_eq!(a.raft_target.as_ref().map(|t| t.counter), Some(2));
    let ordered = a.raft_ordered.as_ref().expect("list");
    assert_eq!(
        ordered
            .targets
            .iter()
            .map(|t| t.counter)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        a.ordered_voter_sets(),
        Some(vec![
            voters(&[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)]),
            voters(&[("broker-a", 1), ("broker-b", 9), ("broker-c", 3)]),
        ])
    );
}

/// A set change drops the target and restarts its counter, but the epoch
/// goes on, so the old set's target must still count.
#[test]
fn a_set_change_keeps_the_list() {
    let before = started();
    let mut after = ShardAssignment {
        replicas: vec!["broker-b".to_string(), "broker-d".to_string()],
        raft_copy_ids: Default::default(),
        raft_target: None,
        raft_ordered: None,
        ..before.clone()
    };
    after.carry_raft_target(Some(&before));
    after.carry_raft_ordered(Some(&before));
    assert_eq!(after.raft_target, None);
    order(
        &mut after,
        &[("broker-a", 1), ("broker-b", 2), ("broker-d", 4)],
        true,
    );
    assert_eq!(after.raft_target.as_ref().map(|t| t.counter), Some(1));
    assert_eq!(
        after.ordered_voter_sets(),
        Some(vec![
            voters(&[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)]),
            voters(&[("broker-a", 1), ("broker-b", 2), ("broker-d", 4)]),
        ])
    );
}

#[test]
fn a_new_incarnation_or_epoch_reads_as_unknown_until_started_again() {
    let before = started();
    let mut recreated = ShardAssignment {
        incarnation: before.incarnation + 1,
        ..before.clone()
    };
    recreated.carry_raft_ordered(Some(&before));
    assert_eq!(recreated.raft_ordered, None);

    let mut reformed = before.clone();
    reformed.placed = Some(crate::model::PlacedInEpoch {
        epoch: 1,
        brokers: set(&["broker-a", "broker-b", "broker-c"]),
    });
    assert_eq!(reformed.ordered_voter_sets(), None, "epoch 0's list");
    reformed.start_raft_ordered();
    assert_eq!(reformed.ordered_voter_sets(), Some(vec![]));
}

#[test]
fn never_started_reads_as_unknown() {
    let mut a = assignment();
    order(
        &mut a,
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
        true,
    );
    assert!(a.raft_target.is_some());
    assert_eq!(a.raft_ordered, None);
    assert_eq!(a.ordered_voter_sets(), None);
}

#[test]
fn a_target_not_kept_marks_the_list_lost() {
    let mut a = started();
    order(&mut a, &[("broker-b", 9)], false);
    assert!(a.raft_ordered.as_ref().expect("list").lost);
    assert_eq!(a.ordered_voter_sets(), None);
    // Lost stays lost for the epoch.
    order(&mut a, &[("broker-b", 10)], true);
    assert_eq!(a.ordered_voter_sets(), None);
}

#[test]
fn a_target_ordered_again_is_listed_once() {
    let mut a = started();
    order(&mut a, &[("broker-b", 9)], true);
    order(&mut a, &[("broker-b", 2)], true);
    assert_eq!(a.raft_target.as_ref().map(|t| t.counter), Some(3));
    assert_eq!(a.raft_ordered.as_ref().expect("list").targets.len(), 2);
}

#[test]
fn past_the_cap_the_list_is_lost() {
    let mut a = started();
    for id in 0..RAFT_ORDERED_CAP as u64 - 1 {
        order(&mut a, &[("broker-b", 100 + id)], true);
    }
    assert_eq!(
        a.raft_ordered.as_ref().expect("list").targets.len(),
        RAFT_ORDERED_CAP
    );
    assert!(a.ordered_voter_sets().is_some());
    order(&mut a, &[("broker-b", 99)], true);
    assert_eq!(a.ordered_voter_sets(), None);
}

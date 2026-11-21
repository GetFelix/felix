//! When a Raft shard's target counter rises.
use std::collections::{BTreeMap, BTreeSet};

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
        raft_seed: None,
        raft_bases: None,
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
    assert_eq!(a.settle_raft_target(true), Ok(false));
    assert_eq!(a.raft_target, None);
    a.raft_copy_ids.insert("broker-c".into(), 3);
    assert_eq!(a.settle_raft_target(true), Ok(true));
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
    a.settle_raft_target(true).expect("first");
    // A write naming the same set, its leader swapped among the members.
    a.leader = "broker-b".into();
    a.replicas = vec!["broker-a".into(), "broker-c".into()];
    assert_eq!(a.settle_raft_target(true), Ok(false));
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
    a.settle_raft_target(true).expect("first");
    assert!(!a.raft_copy_ids.contains_key("broker-d"));
    a.replicas = vec!["broker-b".into(), "broker-d".into()];
    a.raft_copy_ids.insert("broker-d".into(), 4);
    assert_eq!(a.settle_raft_target(true), Ok(true));
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
    a.settle_raft_target(true).expect("first");
    a.raft_copy_ids.insert("broker-b".into(), 9);
    assert_eq!(a.settle_raft_target(true), Ok(true));
    assert_eq!(a.raft_target.as_ref().map(|t| t.counter), Some(2));
    // The same id again changes nothing.
    a.raft_copy_ids.insert("broker-b".into(), 9);
    assert_eq!(a.settle_raft_target(true), Ok(false));
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
        a.settle_raft_target(true),
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
    assert_eq!(a.settle_raft_target(true), Ok(true));
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

fn set(nodes: &[&str]) -> BTreeSet<String> {
    nodes.iter().map(|node| format!("broker-{node}")).collect()
}

/// Place `nodes`, each reporting copy 1.
fn place(record: &mut ShardAssignment, nodes: &[&str]) {
    record.raft_copy_ids = set(nodes).into_iter().map(|node| (node, 1)).collect();
    let mut nodes = set(nodes).into_iter();
    record.leader = nodes.next().expect("a leader");
    record.replicas = nodes.collect();
}

fn report(record: &mut ShardAssignment, index: u64, voters: &[&str]) -> bool {
    let report = super::super::RaftFloorReport {
        key: record.key.clone(),
        incarnation: record.incarnation,
        epoch: 0,
        index,
        term: 1,
        voters: None,
    };
    let voters = set(voters).into_iter().map(|node| (node, 1)).collect();
    record.note_raft_voters(&report, &voters)
}

fn formed(nodes: &[&str]) -> ShardAssignment {
    let mut record = assignment();
    place(&mut record, nodes);
    record.raft_formed = Some(RaftFormed {
        incarnation: record.incarnation,
        epoch: 0,
    });
    record
}

/// A chain of one-member swaps the group never applies would end on a set
/// of brokers it never reached (#2905). Each step waits for the voters
/// that keep a majority of it.
#[test]
fn a_swap_chain_waits_for_the_group_to_report_its_voters() {
    let mut record = formed(&["1", "2", "3"]);
    assert!(
        record.holds_formed_set(&set(&["1", "2", "4"])),
        "no voters reported yet"
    );
    assert!(report(&mut record, 5, &["1", "2", "3"]));
    assert!(!record.holds_formed_set(&set(&["1", "2", "4"])));
    place(&mut record, &["1", "2", "4"]);
    assert!(
        record.holds_formed_set(&set(&["1", "4", "5"])),
        "the group has not added broker-4"
    );
    assert!(
        !record.holds_formed_set(&set(&["1", "2", "5"])),
        "replacing the unapplied broker-4 keeps a majority"
    );
    assert!(report(&mut record, 9, &["1", "2", "4"]));
    assert!(
        !report(&mut record, 6, &["1", "2", "3"]),
        "a late report brings back no older voters"
    );
    assert!(!record.holds_formed_set(&set(&["1", "4", "5"])));
    place(&mut record, &["1", "4", "5"]);
    assert!(record.holds_formed_set(&set(&["4", "5", "6"])));
    assert!(
        !record.holds_formed_set(&set(&["1", "4", "5"])),
        "the current set is no change"
    );
}

/// At replication factor 1 any re-placement replaces every member, so it
/// is refused: the set does not move, and a re-form is the way out.
#[test]
fn a_replication_factor_1_set_does_not_move() {
    let mut record = formed(&["1"]);
    assert!(record.holds_formed_set(&set(&["2"])), "no voters reported");
    assert!(report(&mut record, 3, &["1"]));
    assert!(record.holds_formed_set(&set(&["2"])));
}

/// An even set keeps exactly half: two of four reported voters still hold
/// a copy with the group's log, since at most one of four can be empty.
#[test]
fn an_even_set_moves_keeping_half_of_its_voters() {
    let mut record = formed(&["1", "2", "3", "4"]);
    assert!(report(&mut record, 5, &["1", "2", "3", "4"]));
    assert!(!record.holds_formed_set(&set(&["1", "2", "5", "6"])));
    assert!(
        record.holds_formed_set(&set(&["1", "5", "6", "7"])),
        "one of four"
    );
}

/// A report from another reform epoch is an old group's and counts nothing.
#[test]
fn voters_count_only_at_the_placed_epoch() {
    let mut record = formed(&["1", "2", "3"]);
    record.placed = Some(crate::model::PlacedInEpoch {
        epoch: 2,
        brokers: set(&["1", "2", "3"]),
    });
    record.raft_formed = Some(RaftFormed {
        incarnation: record.incarnation,
        epoch: 2,
    });
    assert!(!report(&mut record, 5, &["1", "2", "3"]), "epoch 0 report");
    assert!(record.holds_formed_set(&set(&["1", "2", "4"])));
}

/// A voter that reopened empty comes back under a new copy id, and the old
/// copy the report names is gone. Counting brokers would let two wiped
/// voters and one new broker make an all-empty set that forms a second
/// group, so only a broker under its reported copy counts.
#[test]
fn a_voter_that_reopened_empty_does_not_count() {
    let mut record = formed(&["1", "2", "3"]);
    assert!(report(&mut record, 5, &["1", "2", "3"]));
    assert!(!record.holds_formed_set(&set(&["2", "3", "4"])));
    record.raft_copy_ids.insert("broker-2".to_string(), 2);
    assert!(
        record.holds_formed_set(&set(&["2", "3", "4"])),
        "broker-2 counts only under the copy it opened"
    );
    assert!(
        !record.holds_formed_set(&set(&["1", "3", "4"])),
        "broker-1 and broker-3 are still the reported copies"
    );
    record.raft_copy_ids.insert("broker-3".to_string(), 2);
    assert!(
        record.holds_formed_set(&set(&["1", "3", "4"])),
        "only broker-1 is left of the reported copies"
    );
}

/// A move is checked against the copies known when it is made, and a
/// voter can reopen empty after that. The target is checked against the
/// copies it would name, so a set whose voters all reopened gets no target
/// and none of its brokers can form it. One reopened voter of three still
/// gets one, so the group can replace that copy.
#[test]
fn a_target_waits_for_reported_voters_to_keep_a_majority_of_its_copies() {
    let mut record = formed(&["1", "2", "3"]);
    assert!(report(&mut record, 5, &["1", "2", "3"]));
    assert_eq!(record.settle_raft_target(true), Ok(true));
    record.raft_copy_ids.insert("broker-2".to_string(), 2);
    assert_eq!(
        record.settle_raft_target(true),
        Ok(true),
        "one copy replaced"
    );
    let kept = record.raft_target.clone();
    record.raft_copy_ids.insert("broker-3".to_string(), 2);
    assert_eq!(record.settle_raft_target(true), Ok(false));
    assert_eq!(record.raft_target, kept, "the target stays as it was");
    assert_eq!(
        record.settle_raft_target(false),
        Ok(true),
        "below the level that knows the hold"
    );

    // A replication factor 1 copy that reopened empty is the whole group.
    let mut single = formed(&["1"]);
    assert!(report(&mut single, 3, &["1"]));
    single.raft_copy_ids.insert("broker-1".to_string(), 2);
    assert_eq!(single.settle_raft_target(true), Ok(false));

    // No report yet holds no target of the current set.
    let mut quiet = formed(&["1", "2", "3"]);
    quiet.raft_copy_ids.insert("broker-2".to_string(), 2);
    assert_eq!(quiet.settle_raft_target(true), Ok(true));
}

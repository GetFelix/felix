//! The placement incarnation file: an acknowledgement survives a reopen and
//! never goes back.

use super::*;

use std::collections::{BTreeMap, BTreeSet};

use crate::raft_copy::{
    MAX_PLACEMENT_ACK_AHEAD, ack_placement_incarnation, acked_placement_incarnation,
    all_acked_placement_incarnation, copy_delete_is_void, placement_ack_in_bound,
    restored_placement_incarnation,
};

#[test]
fn an_acknowledged_incarnation_reads_back_and_never_lowers() {
    let root = Root::new();
    assert_eq!(acked_placement_incarnation(root.path()).expect("none"), 0);
    assert_eq!(ack_placement_incarnation(root.path(), 2).expect("ack"), 2);
    // A late word from before the restore must not lower it, or a delete
    // from the old incarnation would pass again.
    assert_eq!(ack_placement_incarnation(root.path(), 1).expect("lower"), 2);
    assert_eq!(acked_placement_incarnation(root.path()).expect("read"), 2);
    assert_eq!(ack_placement_incarnation(root.path(), 3).expect("raise"), 3);
    assert_eq!(acked_placement_incarnation(root.path()).expect("read"), 3);
}

#[test]
fn a_damaged_placement_incarnation_file_is_an_error() {
    let root = Root::new();
    ack_placement_incarnation(root.path(), 4).expect("ack");
    let path = root.path().join(super::super::placement_ack::FILE_NAME);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[8] ^= 1;
    std::fs::write(&path, bytes).expect("write");
    acked_placement_incarnation(root.path()).expect_err("damage is not 0");
}

// FelixRaftPlacementStaleWipeRewindSameInc: a restore that kept the
// incarnation lets a reissued version pass while an old delete is pending.
#[test]
fn a_restore_raises_the_incarnation_above_both_sides() {
    assert_eq!(restored_placement_incarnation(0, 0), Some(1));
    assert_eq!(restored_placement_incarnation(3, 1), Some(4));
    // An older control plane restoring a newer backup still moves past it.
    assert_eq!(restored_placement_incarnation(1, 5), Some(6));
}

// Saturating at the top would hand every later restore the same incarnation.
#[test]
fn a_restore_at_the_top_has_no_incarnation_to_raise_to() {
    assert_eq!(
        restored_placement_incarnation(u64::MAX - 1, 0),
        Some(u64::MAX)
    );
    assert_eq!(restored_placement_incarnation(u64::MAX, 0), None);
    assert_eq!(restored_placement_incarnation(0, u64::MAX), None);
}

#[test]
fn an_acknowledgement_far_above_the_incarnation_is_out_of_bound() {
    assert!(placement_ack_in_bound(5, 0));
    assert!(placement_ack_in_bound(5, 5 + MAX_PLACEMENT_ACK_AHEAD));
    assert!(!placement_ack_in_bound(5, 6 + MAX_PLACEMENT_ACK_AHEAD));
    assert!(!placement_ack_in_bound(0, u64::MAX));
}

// FelixRaftPlacementStaleWipeRewindNoAck: a target or doom sent before every
// broker has the incarnation.
#[test]
fn nothing_goes_out_until_every_broker_has_acknowledged() {
    let brokers: BTreeSet<&str> = ["a", "b", "c"].into();
    let mut acked = BTreeMap::from([("a", 2), ("b", 2)]);
    assert!(!all_acked_placement_incarnation(2, &brokers, &acked));
    acked.insert("c", 1);
    assert!(!all_acked_placement_incarnation(2, &brokers, &acked));
    acked.insert("c", 2);
    assert!(all_acked_placement_incarnation(2, &brokers, &acked));
    // Acknowledged at a control plane this one was not restored from.
    acked.insert("c", 3);
    assert!(!all_acked_placement_incarnation(2, &brokers, &acked));
}

// Every broker starts at 0 (`ack` in FelixRaftPlacement's Init), so one that
// never reported holds back only an incarnation a restore raised.
#[test]
fn a_broker_that_never_acknowledged_counts_as_zero() {
    let brokers: BTreeSet<&str> = ["a"].into();
    let none = BTreeMap::new();
    assert!(all_acked_placement_incarnation(0, &brokers, &none));
    assert!(!all_acked_placement_incarnation(1, &brokers, &none));
}

#[test]
fn a_delete_below_the_highest_incarnation_seen_is_void() {
    assert!(copy_delete_is_void(1, 2));
    assert!(!copy_delete_is_void(2, 2));
    assert!(!copy_delete_is_void(3, 2));
}

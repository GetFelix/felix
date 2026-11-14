//! The placement incarnation file: an acknowledgement survives a reopen and
//! never goes back.

use super::*;

use std::collections::{BTreeMap, BTreeSet};

use crate::raft_copy::{
    ack_placement_incarnation, acked_placement_incarnation, all_acked_placement_incarnation,
    copy_delete_is_void, restored_placement_incarnation,
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
    assert_eq!(restored_placement_incarnation(0, 0), 1);
    assert_eq!(restored_placement_incarnation(3, 1), 4);
    // An older control plane restoring a newer backup still moves past it.
    assert_eq!(restored_placement_incarnation(1, 5), 6);
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
}

#[test]
fn a_delete_below_the_highest_incarnation_seen_is_void() {
    assert!(copy_delete_is_void(1, 2));
    assert!(!copy_delete_is_void(2, 2));
    assert!(!copy_delete_is_void(3, 2));
}

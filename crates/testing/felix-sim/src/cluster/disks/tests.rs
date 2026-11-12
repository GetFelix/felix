use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use super::{Observed, Thinned, judge, missing, thin};

fn set(values: &[u64]) -> BTreeSet<u64> {
    values.iter().copied().collect()
}

fn disks(nodes: &[usize]) -> BTreeSet<usize> {
    nodes.iter().copied().collect()
}

fn run(
    faulted: &[usize],
    loses_synced: bool,
    before: &[&[u64]],
    after: &[&[u64]],
) -> super::DiskEvent {
    let before: Vec<_> = before.iter().map(|v| set(v)).collect();
    let after: Vec<_> = after.iter().map(|v| set(v)).collect();
    let acked = BTreeMap::from([(0, vec![1, 2])]);
    judge(Observed {
        at: Duration::ZERO,
        what: "fault".into(),
        faulted,
        loses_synced,
        before: &before,
        after: &after,
        acked: &acked,
        gone: &BTreeSet::new(),
        thinned: &Thinned::new(),
    })
}

#[test]
fn a_record_another_disk_still_holds_is_not_lost() {
    let event = run(&[0], true, &[&[1, 2], &[1, 2], &[]], &[&[], &[1, 2], &[]]);
    assert!(event.excused.is_empty() && event.unexcused.is_empty());
}

#[test]
fn a_record_every_holder_lost_is_excused_when_the_fault_can_lose_synced_bytes() {
    let event = run(&[0, 1], true, &[&[1, 2], &[1, 2], &[]], &[&[], &[], &[]]);
    assert_eq!(event.excused, BTreeMap::from([(0, vec![1, 2])]));
    assert!(event.unexcused.is_empty());
}

#[test]
fn a_synced_record_lost_to_a_power_cut_is_not_excused() {
    let event = run(&[0, 1], false, &[&[1, 2], &[1, 2], &[]], &[&[], &[], &[]]);
    assert!(event.excused.is_empty());
    assert_eq!(event.unexcused.len(), 2);
}

#[test]
fn a_record_acknowledged_with_one_copy_is_not_excused() {
    let event = run(&[0], true, &[&[1, 2], &[], &[]], &[&[], &[], &[]]);
    assert!(event.excused.is_empty());
    assert_eq!(event.unexcused.len(), 2);
}

#[test]
fn a_record_an_earlier_fault_took_a_copy_of_may_be_on_one_disk() {
    let before = [set(&[1]), set(&[]), set(&[])];
    let after = [set(&[]), set(&[]), set(&[])];
    let acked = BTreeMap::from([(0, vec![1])]);
    let thinned = Thinned::from([(1, disks(&[1]))]);
    let event = judge(Observed {
        at: Duration::ZERO,
        what: "fault".into(),
        faulted: &[0],
        loses_synced: true,
        before: &before,
        after: &after,
        acked: &acked,
        gone: &BTreeSet::new(),
        thinned: &thinned,
    });
    assert_eq!(event.excused.len(), 1);
}

#[test]
fn a_record_no_disk_held_is_not_excused() {
    let event = run(&[0], true, &[&[], &[], &[]], &[&[], &[], &[]]);
    assert_eq!(event.unexcused.len(), 2);
}

fn sets(disks: &[&[u64]]) -> Vec<BTreeSet<u64>> {
    disks.iter().map(|v| set(v)).collect()
}

/// A fault that leaves a broker's copy in place takes nothing, so a later
/// one-broker fault that takes the other copy is still a finding.
#[test]
fn a_copy_the_fault_did_not_take_is_not_thinned() {
    let acked = BTreeMap::from([(0, vec![1])]);
    let mut thinned = Thinned::new();
    let held = sets(&[&[1], &[1], &[]]);
    thin(&mut thinned, &[0], &acked, &[], &held, &held);
    assert!(thinned.is_empty(), "{thinned:?}");

    let before = sets(&[&[], &[1], &[]]);
    let after = sets(&[&[], &[], &[]]);
    let event = judge(Observed {
        at: Duration::ZERO,
        what: "fault".into(),
        faulted: &[1],
        loses_synced: true,
        before: &before,
        after: &after,
        acked: &acked,
        gone: &BTreeSet::new(),
        thinned: &thinned,
    });
    assert!(event.excused.is_empty());
    assert_eq!(event.unexcused.len(), 1);
}

/// A copy taken and then copied back no longer counts.
#[test]
fn a_record_two_disks_hold_again_is_no_longer_thinned() {
    let acked = BTreeMap::from([(0, vec![1])]);
    let mut thinned = Thinned::new();
    thin(
        &mut thinned,
        &[0],
        &acked,
        &[],
        &sets(&[&[1], &[1], &[]]),
        &sets(&[&[], &[1], &[]]),
    );
    assert_eq!(thinned, Thinned::from([(1, disks(&[0]))]));
    thin(
        &mut thinned,
        &[2],
        &acked,
        &[],
        &sets(&[&[1], &[1], &[]]),
        &sets(&[&[1], &[1], &[]]),
    );
    assert!(thinned.is_empty(), "{thinned:?}");
}

/// Two disks must have lost a record for its loss to be excused. A thinned
/// entry that names the last holder itself is one disk.
#[test]
fn a_loss_on_one_disk_is_never_excused() {
    let before = sets(&[&[], &[1], &[]]);
    let after = sets(&[&[], &[], &[]]);
    let acked = BTreeMap::from([(0, vec![1])]);
    let thinned = Thinned::from([(1, disks(&[1]))]);
    let event = judge(Observed {
        at: Duration::ZERO,
        what: "fault".into(),
        faulted: &[1],
        loses_synced: true,
        before: &before,
        after: &after,
        acked: &acked,
        gone: &BTreeSet::new(),
        thinned: &thinned,
    });
    assert!(event.excused.is_empty());
    assert_eq!(event.unexcused.len(), 1);
}

#[test]
fn an_acknowledged_record_no_disk_holds_and_no_fault_took_is_missing() {
    let acked = BTreeMap::from([(0, vec![1, 2, 3])]);
    let disks = sets(&[&[1], &[], &[]]);
    assert_eq!(missing(&acked, &disks, &set(&[2])), vec![(0, 3)]);
}

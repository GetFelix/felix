use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use super::{Observed, judge};

fn set(values: &[u64]) -> BTreeSet<u64> {
    values.iter().copied().collect()
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
        thinned: &BTreeSet::new(),
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
    let thinned = set(&[1]);
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

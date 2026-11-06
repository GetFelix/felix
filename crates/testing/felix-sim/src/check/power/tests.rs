use std::time::Duration;

use super::*;

fn loss(node: usize, at: u64, mid_append: bool, repaired_before: u64) -> PowerLoss {
    PowerLoss {
        node,
        at: Duration::from_secs(at),
        mid_append,
        repaired_before,
        view: Default::default(),
        restarted_at: Some(Duration::from_secs(at + 5)),
    }
}

fn nodes(missed: &[PowerLoss]) -> Vec<(usize, u64)> {
    missed
        .iter()
        .map(|loss| (loss.node, loss.at.as_secs()))
        .collect()
}

#[test]
fn a_torn_append_with_a_repair_after_it_passes() {
    let losses = [loss(0, 10, true, 0)];
    assert!(unrepaired(&losses, |_| 1).is_empty());
}

#[test]
fn a_torn_append_with_no_repair_after_it_fails() {
    let losses = [loss(0, 10, true, 2)];
    assert_eq!(nodes(&unrepaired(&losses, |_| 2)), [(0, 10)]);
}

#[test]
fn a_cut_with_no_append_in_flight_needs_no_repair() {
    let losses = [loss(0, 10, false, 0)];
    assert!(unrepaired(&losses, |_| 0).is_empty());
}

#[test]
fn each_cut_is_held_to_the_window_before_the_next_one_on_its_broker() {
    // b1's first cut was repaired before its second; the second never was.
    // b2's repair in between does not count for b1.
    let losses = [
        loss(0, 10, true, 0),
        loss(1, 20, true, 0),
        loss(0, 30, true, 1),
    ];
    assert_eq!(nodes(&unrepaired(&losses, |_| 1)), [(0, 30)]);

    // A repair the first cut never got cannot be borrowed from the second.
    let losses = [loss(0, 10, true, 0), loss(0, 30, true, 0)];
    assert_eq!(nodes(&unrepaired(&losses, |_| 1)), [(0, 10)]);
}

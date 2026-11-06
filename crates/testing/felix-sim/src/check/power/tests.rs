use std::path::PathBuf;
use std::time::Duration;

use super::*;

const SEG: &str = "sim/ns/events/0/00000000000000000000.log";
const OTHER: &str = "sim/ns/events/1/00000000000000000000.log";

fn loss(node: usize, at: u64, torn: Option<&str>, repaired_before: usize) -> PowerLoss {
    PowerLoss {
        node,
        at: Duration::from_secs(at),
        torn: torn.map(PathBuf::from),
        repaired_before,
        view: Default::default(),
        restarted_at: Some(Duration::from_secs(at + 5)),
    }
}

fn paths(list: &[&str]) -> Vec<PathBuf> {
    list.iter().map(PathBuf::from).collect()
}

fn missed(missed: &[PowerLoss]) -> Vec<(usize, u64)> {
    missed
        .iter()
        .map(|loss| (loss.node, loss.at.as_secs()))
        .collect()
}

#[test]
fn a_torn_segment_repaired_after_the_cut_passes() {
    let losses = [loss(0, 10, Some(SEG), 0)];
    assert!(unrepaired(&losses, |_| paths(&[SEG])).is_empty());
}

#[test]
fn a_repair_of_another_segment_does_not_count() {
    let losses = [loss(0, 10, Some(SEG), 0)];
    assert_eq!(missed(&unrepaired(&losses, |_| paths(&[OTHER]))), [(0, 10)]);
}

#[test]
fn a_repair_from_before_the_cut_does_not_count() {
    let losses = [loss(0, 10, Some(SEG), 1)];
    assert_eq!(missed(&unrepaired(&losses, |_| paths(&[SEG]))), [(0, 10)]);
}

#[test]
fn a_cut_with_no_append_in_flight_needs_no_repair() {
    let losses = [loss(0, 10, None, 0)];
    assert!(unrepaired(&losses, |_| Vec::new()).is_empty());
}

#[test]
fn each_cut_is_held_to_the_window_before_the_next_one_on_its_broker() {
    // b1's first cut was repaired before its second; the second never was.
    // b2's repairs are its own.
    let losses = [
        loss(0, 10, Some(SEG), 0),
        loss(1, 20, Some(SEG), 0),
        loss(0, 30, Some(OTHER), 1),
    ];
    let repaired = |node: usize| {
        if node == 0 {
            paths(&[SEG])
        } else {
            paths(&[SEG, OTHER])
        }
    };
    assert_eq!(missed(&unrepaired(&losses, repaired)), [(0, 30)]);

    // A repair after the second cut does not cover the first.
    let losses = [loss(0, 10, Some(SEG), 0), loss(0, 30, Some(OTHER), 0)];
    assert_eq!(
        missed(&unrepaired(&losses, |_| paths(&[OTHER, SEG]))),
        [(0, 10)]
    );
}

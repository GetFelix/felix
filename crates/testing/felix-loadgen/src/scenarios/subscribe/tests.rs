use std::time::Duration;

use super::dropped;
use crate::scenarios::framing::{read_send_time, stamp_send_time, unix_micros};
use crate::stats::Reservoir;

#[test]
fn consecutive_offsets_drop_nothing() {
    assert_eq!(dropped(None, 7, 0), 0);
    assert_eq!(dropped(Some(7), 8, 0), 0);
}

#[test]
fn a_jump_counts_the_records_in_between() {
    assert_eq!(dropped(Some(7), 11, 0), 3);
}

#[test]
fn offsets_that_hold_no_event_are_not_drops() {
    // A leader change's generation-start record took offset 8.
    assert_eq!(dropped(Some(7), 9, 1), 0);
    assert_eq!(dropped(Some(7), 12, 1), 3);
}

#[test]
fn a_send_stamp_reads_back() {
    let mut body = vec![0xffu8; 32];
    let before = unix_micros();
    stamp_send_time(&mut body);
    let sent = read_send_time(&body).expect("stamped");
    assert!(sent >= before && sent <= unix_micros());
    assert_eq!(body[8..], [0xffu8; 24]);
    assert_eq!(read_send_time(&body[..7]), None);
}

#[test]
fn a_reservoir_keeps_at_most_its_capacity() {
    let mut reservoir = Reservoir::new(100, 1);
    for micros in 0..10_000 {
        reservoir.offer(Duration::from_micros(micros));
    }
    assert_eq!(reservoir.seen(), 10_000);
    let mut samples = reservoir.into_samples();
    assert_eq!(samples.len(), 100);
    // Uniform over 0..10000, so the median of the sample is nowhere near
    // the first hundred values it started with.
    let p50 = samples.percentiles().p50_us;
    assert!((2_000..8_000).contains(&p50), "p50 = {p50}");
}

use super::*;

fn at(base: Instant, ms: u64) -> Instant {
    base + Duration::from_millis(ms)
}

#[test]
fn an_acknowledged_write_read_back_once_is_clean() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.begin((0, 0), t0);
    ledger.ack((0, 0), at(t0, 3));
    ledger.delivered(Some((0, 0)));
    let counters = ledger.counters(at(t0, 60_000));
    assert_eq!(
        (counters.acked, counters.delivered, counters.gaps),
        (1, 1, 0)
    );
    let counts = HashMap::from([((0, 0), 1)]);
    assert!(judge(&ledger.snapshot(), &counts, 0).holds());
}

#[test]
fn an_acknowledged_write_missing_from_the_stream_is_a_gap() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.begin((1, 7), t0);
    ledger.ack((1, 7), at(t0, 2));
    let verdict = judge(&ledger.snapshot(), &HashMap::new(), 0);
    assert_eq!(verdict.gaps, vec![(1, 7)]);
    assert!(!verdict.holds());
}

#[test]
fn a_gap_shows_live_only_after_the_reader_grace() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.begin((0, 0), t0);
    ledger.ack((0, 0), t0);
    assert_eq!(ledger.counters(at(t0, 1_000)).gaps, 0);
    assert_eq!(ledger.counters(at(t0, 6_000)).gaps, 1);
    ledger.delivered(Some((0, 0)));
    assert_eq!(ledger.counters(at(t0, 6_000)).gaps, 0);
}

#[test]
fn a_record_read_before_its_ack_is_not_a_gap() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.begin((0, 0), t0);
    ledger.delivered(Some((0, 0)));
    ledger.ack((0, 0), at(t0, 5));
    assert_eq!(ledger.counters(at(t0, 60_000)).gaps, 0);
}

#[test]
fn a_repeated_write_is_a_duplicate() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.begin((0, 0), t0);
    ledger.ack((0, 0), t0);
    ledger.delivered(Some((0, 0)));
    ledger.delivered(Some((0, 0)));
    assert_eq!(ledger.counters(t0).duplicates, 1);
    let counts = HashMap::from([((0, 0), 2)]);
    assert_eq!(
        judge(&ledger.snapshot(), &counts, 0).duplicates,
        vec![(0, 0)]
    );
}

#[test]
fn a_write_in_doubt_is_never_a_gap() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.begin((0, 0), t0);
    ledger.begin((0, 1), t0);
    ledger.failed((0, 0));
    ledger.failed((0, 1));
    let counters = ledger.counters(at(t0, 100));
    assert_eq!((counters.in_doubt, counters.in_flight), (2, 0));
    // One of them landed after all, one did not.
    let counts = HashMap::from([((0, 0), 1)]);
    let verdict = judge(&ledger.snapshot(), &counts, 0);
    assert_eq!((verdict.in_doubt_landed, verdict.in_doubt_absent), (1, 1));
    assert!(verdict.gaps.is_empty());
    assert!(verdict.holds());
}

#[test]
fn a_retry_that_is_acknowledged_leaves_doubt_and_counts_as_recovered() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.begin((0, 0), t0);
    ledger.failed((0, 0));
    ledger.retrying((0, 0));
    assert_eq!(ledger.counters(t0).in_flight, 1);
    ledger.ack((0, 0), at(t0, 900));
    let snapshot = ledger.snapshot();
    assert_eq!((snapshot.recovered, snapshot.in_doubt.len()), (1, 0));
}

#[test]
fn a_record_nobody_sent_is_unexpected() {
    let t0 = Instant::now();
    let ledger = Ledger::new(t0);
    assert!(!judge(&ledger.snapshot(), &HashMap::new(), 1).holds());
}

#[test]
fn a_stall_runs_from_the_oldest_waiting_publish_to_the_next_ack() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.begin((0, 0), at(t0, 10_000));
    ledger.begin((1, 0), at(t0, 10_020));
    assert_eq!(ledger.watch(at(t0, 10_100)), None);
    assert_eq!(ledger.watch(at(t0, 10_600)), Some(at(t0, 10_000)));
    // Flagged once, not again on every tick.
    assert_eq!(ledger.watch(at(t0, 10_700)), None);
    let stall = ledger
        .ack((0, 0), at(t0, 11_500))
        .expect("the ack ends the stall");
    assert_eq!(stall.blocked, Duration::from_millis(1_500));
    assert_eq!(stall.at_secs, 10);
    assert!(ledger.ack((1, 0), at(t0, 11_510)).is_none());
    assert_eq!(ledger.stalls().len(), 1);
}

#[test]
fn quick_publishes_are_not_a_stall() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.begin((0, 0), t0);
    assert_eq!(ledger.watch(at(t0, 200)), None);
    assert!(ledger.ack((0, 0), at(t0, 210)).is_none());
}

#[test]
fn publishes_still_waiting_after_the_first_ack_are_not_a_second_stall() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.begin((0, 0), at(t0, 1_000));
    ledger.begin((1, 0), at(t0, 1_010));
    assert!(ledger.watch(at(t0, 1_600)).is_some());
    assert!(ledger.ack((0, 0), at(t0, 5_000)).is_some());
    // The other producer is still waiting; that is the same stall.
    assert_eq!(ledger.watch(at(t0, 5_100)), None);
    assert!(ledger.ack((1, 0), at(t0, 5_200)).is_none());
    assert_eq!(ledger.watch(at(t0, 5_300)), None);
    assert_eq!(
        ledger.stalls()[0].drained,
        Some(Duration::from_millis(4_300))
    );
    // Everything drained, so a later wait is a new stall.
    ledger.begin((0, 1), at(t0, 8_000));
    assert!(ledger.watch(at(t0, 8_600)).is_some());
    assert_eq!(ledger.stalls().len(), 1);
}

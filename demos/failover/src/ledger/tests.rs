use super::*;

/// A ledger that expects `n` producers, all started.
fn ledger_with(t0: Instant, n: u16) -> Ledger {
    let mut ledger = Ledger::new(t0);
    ledger.expect_producers(n);
    for _ in 0..n {
        ledger.producer_started();
    }
    ledger
}

fn at(base: Instant, ms: u64) -> Instant {
    base + Duration::from_millis(ms)
}

#[test]
fn an_acknowledged_write_read_back_once_is_clean() {
    let t0 = Instant::now();
    let mut ledger = ledger_with(t0, 1);
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
    let mut ledger = ledger_with(t0, 1);
    ledger.begin((0, 9), t0);
    ledger.ack((0, 9), t0);
    ledger.begin((0, 0), t0);
    ledger.begin((0, 1), t0);
    ledger.failed((0, 0));
    ledger.failed((0, 1));
    let counters = ledger.counters(at(t0, 100));
    assert_eq!((counters.in_doubt, counters.in_flight), (2, 0));
    // One of them landed after all, one did not.
    let counts = HashMap::from([((0, 9), 1), ((0, 0), 1)]);
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

#[test]
fn a_run_that_acknowledged_nothing_does_not_pass() {
    let t0 = Instant::now();
    let ledger = ledger_with(t0, 4);
    let verdict = judge(&ledger.snapshot(), &HashMap::new(), 0);
    assert!(verdict.violation().is_none());
    assert!(!verdict.holds());
    assert_eq!(
        verdict.failure().as_deref(),
        Some("nothing was acknowledged")
    );
}

#[test]
fn a_producer_that_never_started_fails_the_run() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.expect_producers(4);
    for _ in 0..3 {
        ledger.producer_started();
    }
    ledger.begin((0, 0), t0);
    ledger.ack((0, 0), t0);
    let counts = HashMap::from([((0, 0), 1)]);
    let verdict = judge(&ledger.snapshot(), &counts, 0);
    assert!(!verdict.holds());
    assert!(verdict.failure().expect("failure").contains("3 of 4"));
}

#[test]
fn a_producer_that_died_fails_the_run_with_its_reason() {
    let t0 = Instant::now();
    let mut ledger = ledger_with(t0, 2);
    ledger.begin((0, 0), t0);
    ledger.ack((0, 0), t0);
    ledger.begin((1, 0), t0);
    ledger.producer_died(1, "the broker refused it".to_string(), Some((1, 0)));
    let counts = HashMap::from([((0, 0), 1)]);
    let verdict = judge(&ledger.snapshot(), &counts, 0);
    assert!(!verdict.holds());
    assert!(
        verdict
            .failure()
            .expect("failure")
            .contains("producer 1 stopped early")
    );
}

#[test]
fn a_refused_producers_write_stops_counting_as_waiting() {
    let t0 = Instant::now();
    let mut ledger = ledger_with(t0, 2);
    ledger.begin((0, 0), t0);
    ledger.begin((1, 0), at(t0, 10));
    ledger.producer_died(0, "refused".to_string(), Some((0, 0)));
    // The dead producer's write is in doubt, not in flight and not a stall.
    let counters = ledger.counters(at(t0, 100));
    assert_eq!((counters.in_flight, counters.in_doubt), (1, 1));
    assert_eq!(ledger.watch(at(t0, 5_000)), Some(at(t0, 10)));
    ledger.ack((1, 0), at(t0, 5_100));
    assert_eq!(ledger.watch(at(t0, 5_200)), None);
    assert_eq!(ledger.counters(at(t0, 5_200)).in_flight, 0);
}

#[test]
fn the_read_stops_past_the_highest_acknowledged_offset() {
    let t0 = Instant::now();
    let mut ledger = ledger_with(t0, 1);
    assert_eq!(ledger.snapshot().read_to, None);
    ledger.landed_at(7);
    ledger.landed_at(3);
    assert_eq!(ledger.snapshot().read_to, Some(8));
}

/// A pinned writer that has lost its connection once.
fn pinned_ledger(t0: Instant) -> Ledger {
    let mut ledger = Ledger::new(t0);
    ledger.expect_pinned_writer();
    ledger.reconnected();
    ledger
}

#[test]
fn an_acked_write_missing_from_the_stream_is_a_violation_for_a_pinned_writer() {
    let t0 = Instant::now();
    let mut ledger = pinned_ledger(t0);
    ledger.begin((0, 0), t0);
    ledger.ack((0, 0), at(t0, 1));
    let verdict = judge(&ledger.snapshot(), &HashMap::new(), 0);
    assert_eq!(verdict.gaps, vec![(0, 0)]);
    assert!(verdict.violation().is_some());
    assert!(!verdict.holds());
}

#[test]
fn a_refused_write_that_is_in_the_stream_is_a_violation() {
    let t0 = Instant::now();
    let mut ledger = pinned_ledger(t0);
    ledger.begin((0, 0), t0);
    ledger.refuse((0, 0));
    ledger.begin((0, 1), t0);
    ledger.ack((0, 1), at(t0, 1));
    let counts = HashMap::from([((0, 0), 1), ((0, 1), 1)]);
    let verdict = judge(&ledger.snapshot(), &counts, 0);
    assert_eq!(verdict.refused_present, vec![(0, 0)]);
    assert!(verdict.violation().is_some());
    assert!(!verdict.holds());
}

#[test]
fn a_refused_write_absent_from_the_stream_is_clean() {
    let t0 = Instant::now();
    let mut ledger = pinned_ledger(t0);
    ledger.begin((0, 0), t0);
    ledger.refuse((0, 0));
    ledger.begin((0, 1), t0);
    ledger.ack((0, 1), at(t0, 1));
    let counts = HashMap::from([((0, 1), 1)]);
    let verdict = judge(&ledger.snapshot(), &counts, 0);
    assert_eq!((verdict.refused, verdict.refused_present.len()), (1, 0));
    assert_eq!((verdict.acked, verdict.acked_once), (1, 1));
    assert!(verdict.holds());
}

#[test]
fn an_indeterminate_write_is_clean_whether_it_landed_or_not() {
    let t0 = Instant::now();
    for landed in [true, false] {
        let mut ledger = pinned_ledger(t0);
        ledger.begin((0, 0), t0);
        ledger.abandon((0, 0));
        ledger.begin((0, 1), t0);
        ledger.ack((0, 1), at(t0, 1));
        let mut counts = HashMap::from([((0, 1), 1)]);
        if landed {
            counts.insert((0, 0), 1);
        }
        let verdict = judge(&ledger.snapshot(), &counts, 0);
        assert_eq!(
            (verdict.in_doubt_landed, verdict.in_doubt_absent),
            (u64::from(landed), u64::from(!landed))
        );
        assert!(verdict.holds());
    }
}

#[test]
fn an_abandoned_write_is_no_longer_waiting_but_is_still_known() {
    let t0 = Instant::now();
    let mut ledger = pinned_ledger(t0);
    ledger.begin((0, 0), t0);
    ledger.abandon((0, 0));
    assert_eq!(ledger.watch(at(t0, 60_000)), None);
    assert!(ledger.was_sent((0, 0)));
    ledger.begin((0, 1), t0);
    ledger.refuse((0, 1));
    assert!(ledger.was_sent((0, 1)));
}

#[test]
fn an_acked_write_present_twice_is_not_counted_as_once() {
    let t0 = Instant::now();
    let mut ledger = pinned_ledger(t0);
    ledger.begin((0, 0), t0);
    ledger.ack((0, 0), at(t0, 1));
    let counts = HashMap::from([((0, 0), 2)]);
    let verdict = judge(&ledger.snapshot(), &counts, 0);
    assert_eq!((verdict.acked_once, verdict.duplicates.len()), (0, 1));
    assert!(!verdict.holds());
}

#[test]
fn a_pinned_run_that_never_reconnected_or_never_heard_back_proves_nothing() {
    let t0 = Instant::now();
    let mut ledger = Ledger::new(t0);
    ledger.expect_pinned_writer();
    ledger.begin((0, 0), t0);
    ledger.ack((0, 0), at(t0, 1));
    let counts = HashMap::from([((0, 0), 1)]);
    let verdict = judge(&ledger.snapshot(), &counts, 0);
    assert!(verdict.failure().unwrap().contains("never dropped"));

    ledger.reconnected();
    let verdict = judge(&ledger.snapshot(), &counts, 0);
    assert!(verdict.failure().unwrap().contains("no acknowledgement"));

    ledger.begin((0, 1), t0);
    ledger.ack((0, 1), at(t0, 2));
    let counts = HashMap::from([((0, 0), 1), ((0, 1), 1)]);
    assert!(judge(&ledger.snapshot(), &counts, 0).holds());
}

#[test]
fn forwarded_writes_are_counted_and_still_acked() {
    let t0 = Instant::now();
    let mut ledger = pinned_ledger(t0);
    ledger.begin((0, 0), t0);
    ledger.was_forwarded();
    ledger.ack((0, 0), at(t0, 1));
    let verdict = judge(&ledger.snapshot(), &HashMap::from([((0, 0), 1)]), 0);
    assert_eq!((verdict.forwarded, verdict.acked), (1, 1));
}

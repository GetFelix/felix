use bytes::Bytes;

use super::*;
use crate::log::RecordMark;

fn record(offset: Offset, len: usize) -> LogRecord {
    LogRecord {
        offset,
        timestamp_micros: 0,
        checksum: 0,
        payload: Bytes::from(vec![b'x'; len]),
        mark: RecordMark::None,
        publisher: None,
        headers: None,
        generation: Some(1),
    }
}

fn offsets(records: Option<Vec<LogRecord>>) -> Option<Vec<Offset>> {
    records.map(|records| records.iter().map(|record| record.offset).collect())
}

#[test]
fn a_range_is_served_only_from_its_start() {
    let mut cache = TailCache::default();
    cache.extend(7, (10..15).map(|offset| record(offset, 8)));

    assert_eq!(
        offsets(cache.range(7, 10, 15)),
        Some(vec![10, 11, 12, 13, 14])
    );
    assert_eq!(offsets(cache.range(7, 12, 14)), Some(vec![12, 13]));
    assert_eq!(offsets(cache.range(7, 13, 99)), Some(vec![13, 14]));
    assert_eq!(offsets(cache.range(7, 9, 15)), None, "starts below");
    assert_eq!(offsets(cache.range(7, 15, 16)), None, "starts past");
}

#[test]
fn a_cut_log_is_served_nothing() {
    let mut cache = TailCache::default();
    cache.extend(7, (10..15).map(|offset| record(offset, 8)));
    assert_eq!(offsets(cache.range(8, 10, 15)), None);

    // The next append is after the cut, and what was held goes.
    cache.extend(8, [record(12, 8)]);
    assert_eq!(offsets(cache.range(8, 10, 15)), None);
    assert_eq!(offsets(cache.range(8, 12, 15)), Some(vec![12]));
}

#[test]
fn records_that_do_not_follow_replace_what_is_held() {
    let mut cache = TailCache::default();
    cache.extend(7, (10..15).map(|offset| record(offset, 8)));
    cache.extend(7, [record(20, 8), record(21, 8)]);
    assert_eq!(offsets(cache.range(7, 10, 30)), None);
    assert_eq!(offsets(cache.range(7, 20, 30)), Some(vec![20, 21]));
}

#[test]
fn the_oldest_records_go_first_at_capacity() {
    let mut cache = TailCache::default();
    let each = size_of_record(&record(0, 100));
    let fit = CAPACITY_BYTES / each;
    cache.extend(7, (0..fit + 3).map(|offset| record(offset, 100)));

    assert_eq!(offsets(cache.range(7, 2, 4)), None);
    assert_eq!(offsets(cache.range(7, 3, 5)), Some(vec![3, 4]));
    assert!(cache.bytes <= CAPACITY_BYTES);

    // One record over the capacity is not kept.
    cache.extend(7, [record(fit + 3, CAPACITY_BYTES as usize)]);
    assert_eq!(offsets(cache.range(7, fit + 3, fit + 4)), None);
    assert_eq!(cache.bytes, 0);
}

fn tail(budget: &Arc<TailBudget>) -> Arc<Tail> {
    Tail::new(Arc::clone(budget))
}

/// Append `count` records of `len` payload bytes to `tail`, after what it
/// holds.
fn append(tail: &Tail, from: Offset, count: u64, len: usize) {
    tail.extend(1, (from..from + count).map(|offset| record(offset, len)));
}

fn each() -> u64 {
    size_of_record(&record(0, 100))
}

#[test]
fn many_logs_over_the_budget_stay_under_it() {
    let budget = TailBudget::new(each() * 40);
    let logs: Vec<_> = (0..30).map(|_| tail(&budget)).collect();
    for round in 0..10 {
        for log in &logs {
            append(log, round * 3, 3, 100);
            assert!(
                budget.held() <= budget.limit + CAPACITY_BYTES,
                "held {}",
                budget.held()
            );
        }
    }
    // Each append settles to the limit before it returns.
    assert!(budget.held() <= budget.limit, "held {}", budget.held());
    let sum: u64 = logs.iter().map(|log| log.cache.lock().bytes).sum();
    assert_eq!(sum, budget.held());
}

#[test]
fn the_least_recently_appended_log_gives_up_first() {
    let budget = TailBudget::new(each() * 6);
    let logs: Vec<_> = (0..7).map(|_| tail(&budget)).collect();
    for log in &logs[..6] {
        append(log, 0, 1, 100);
    }
    assert_eq!(
        budget.held(),
        each() * 6,
        "at the limit, nothing is evicted"
    );

    // The first log is appended to again, so the second is now the least
    // recent.
    append(&logs[0], 1, 1, 100);
    append(&logs[6], 0, 1, 100);

    assert!(budget.held() <= budget.limit);
    assert_eq!(logs[1].held(), 0, "the least recent log was emptied");
    assert_eq!(offsets(logs[1].range(1, 0, 1)), None);
    assert!(
        logs[0].held() > 0,
        "a log appended to again keeps its cache"
    );
    assert!(
        logs[6].range(1, 0, 1).is_some(),
        "so does the one appending"
    );
}

#[test]
fn a_busy_log_keeps_its_share_next_to_idle_ones() {
    let budget = TailBudget::new(each() * 20);
    let busy = tail(&budget);
    let others: Vec<_> = (0..3).map(|_| tail(&budget)).collect();
    for other in &others {
        append(other, 0, 2, 100);
    }
    for start in (0..30).step_by(2) {
        append(&busy, start, 2, 100);
    }
    assert!(budget.held() <= budget.limit);
    assert!(
        busy.range(1, 28, 30).is_some(),
        "the newest records are served from memory"
    );
    assert!(busy.held() > 0);
}

#[test]
fn a_dropped_log_returns_its_bytes() {
    let budget = TailBudget::new(1 << 20);
    let log = tail(&budget);
    append(&log, 0, 3, 100);
    assert!(budget.held() > 0);
    drop(log);
    assert_eq!(budget.held(), 0);
}

#[test]
fn logs_appending_and_evicting_each_other_finish() {
    let budget = TailBudget::new(each() * 8);
    let logs: Vec<_> = (0..8).map(|_| tail(&budget)).collect();
    let threads: Vec<_> = logs
        .iter()
        .map(|log| {
            let log = Arc::clone(log);
            std::thread::spawn(move || {
                for next in 0..2_000u64 {
                    log.extend(1, [record(next, 100)]);
                    if next % 64 == 0 {
                        log.clear();
                        log.extend(1, [record(next + 1, 100)]);
                    }
                    let _ = log.range(1, next, next + 1);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().expect("no panic, no deadlock");
    }
    let sum: u64 = logs.iter().map(|log| log.cache.lock().bytes).sum();
    assert_eq!(sum, budget.held());
}

/// Run a pass with nothing to free, so what is appended after it counts as
/// appended since the previous pass.
fn mark_pass(budget: &Arc<TailBudget>) {
    budget.evict(&tail(budget));
}

#[test]
fn nothing_is_evicted_under_the_limit() {
    let budget = TailBudget::new(each() * 1_000);
    let idle: Vec<_> = (0..500).map(|_| tail(&budget)).collect();
    for log in &idle {
        append(log, 0, 1, 1);
    }
    let busy = tail(&budget);
    for offset in 0..3 {
        append(&busy, offset, 1, 400);
    }
    assert!(budget.held() < budget.limit);
    assert_eq!(offsets(busy.range(1, 0, 3)), Some(vec![0, 1, 2]));
    assert!(idle.iter().all(|log| log.held() > 0), "nothing was evicted");
}

#[test]
fn idle_logs_are_emptied_before_a_busy_one_loses_any() {
    let budget = TailBudget::new(each() * 12);
    let idle: Vec<_> = (0..4).map(|_| tail(&budget)).collect();
    for log in &idle {
        append(log, 0, 1, 100);
    }
    mark_pass(&budget);
    let busy = [tail(&budget), tail(&budget)];
    let mut next = 0;
    while budget.passes.load(Ordering::Relaxed) < 2 {
        for log in &busy {
            append(log, next, 1, 100);
        }
        next += 1;
    }

    assert!(budget.held() <= budget.limit);
    assert_eq!(idle[0].held(), 0, "the least recent idle log went first");
    assert!(idle[3].held() > 0, "the pass stopped at the low-water mark");
    for log in &busy {
        let all: Vec<Offset> = (0..next).collect();
        assert_eq!(offsets(log.range(1, 0, next)), Some(all), "busy lost none");
    }
}

#[test]
fn only_busy_logs_over_budget_all_keep_their_newest() {
    // Small entries, so a pass frees more than a round of 45 appends adds
    // and every log appends between two passes. The logs fill the cache
    // capacity more than the limit allows.
    let budget = TailBudget::new(CAPACITY_BYTES * 45 * 9 / 10);
    let logs: Vec<_> = (0..45).map(|_| tail(&budget)).collect();
    mark_pass(&budget);
    let mut newest = vec![None; logs.len()];
    let mut passes = budget.passes.load(Ordering::Relaxed);
    for round in 0..80 {
        for (at, log) in logs.iter().enumerate() {
            append(log, round, 1, 10);
            newest[at] = Some(round);
            assert!(budget.held() <= budget.limit, "held {}", budget.held());
            let now = budget.passes.load(Ordering::Relaxed);
            if now != passes {
                passes = now;
                // Right after a pass, every log still serves its newest.
                for (log, newest) in logs.iter().zip(&newest) {
                    if let Some(newest) = newest {
                        assert!(log.range(1, *newest, newest + 1).is_some());
                    }
                }
            }
        }
    }
    assert!(passes > 2, "the load was over the budget");
}

#[test]
fn a_busy_log_next_to_many_quiet_ones_keeps_a_full_cache() {
    // Quiet logs that together fill the budget, then one log appends alone.
    let quiet_each = each() * 8;
    let budget = TailBudget::new(quiet_each * 600 + each());
    let quiet: Vec<_> = (0..600).map(|_| tail(&budget)).collect();
    for log in &quiet {
        append(log, 0, 8, 100);
    }
    let busy = tail(&budget);
    for next in 0..2_000 {
        append(&busy, next, 1, 100);
        if next >= 200 {
            assert!(
                busy.held() + each() > CAPACITY_BYTES,
                "cache fell to {} at {next}",
                busy.held()
            );
        }
    }
    assert!(budget.held() <= budget.limit);
    assert!(busy.range(1, 1_999, 2_000).is_some());
    assert!(quiet[0].held() < quiet_each, "the quiet logs gave up bytes");
}

#[test]
fn a_sustained_over_budget_load_runs_few_passes() {
    let budget = TailBudget::new(each() * 400);
    let logs: Vec<_> = (0..600).map(|_| tail(&budget)).collect();
    let mut appends = 0;
    for round in 0..30 {
        for log in &logs {
            append(log, round, 1, 100);
            appends += 1;
        }
    }
    let passes = budget.passes.load(Ordering::Relaxed);
    assert!(passes > 0, "the load was over the budget");
    assert!(
        passes * 20 < appends,
        "{passes} passes for {appends} appends"
    );
    assert!(budget.held() <= budget.limit);
}

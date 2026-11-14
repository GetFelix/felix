//! Reads sharing read-index rounds, and a follower catching up in few
//! appends. The two `#[ignore]` benchmarks at the end are what
//! `.github/workflows/raft-read-perf.yml` runs.

use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use openraft::ReadPolicy;

use super::commit::{Led, led};
use super::*;

/// Reads that arrive together are confirmed by a few rounds, not one each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_that_arrive_together_share_read_index_rounds() {
    let mut led = led().await;
    let member = led
        .cluster
        .runtime(&led.leader.broker)
        .group(&shard(0))
        .expect("group");
    let before = member.read_rounds_asked();
    let key = shard(0);
    let reads = (0..64).map(|_| led.engine.confirm_read(&key, led.term));
    for confirmed in futures::future::join_all(reads).await {
        assert_eq!(confirmed, Ok(()));
    }
    let asked = member.read_rounds_asked() - before;
    // All 64 enter in one poll, so one or two rounds is the usual count.
    assert!((1..8).contains(&asked), "{asked} rounds for 64 reads");
    led.cluster.stop().await;
}

/// 1 KiB records, written while one follower was cut off.
const CATCH_UP_RECORD: usize = 1024;

/// Write `records` records of 1 KiB while one follower is cut off, and
/// return that follower and the last index.
async fn leave_one_behind(led: &Led, records: usize) -> (String, u64) {
    let behind = led.followers().next().expect("a follower");
    led.cluster.net.cut.lock().insert(behind.clone());
    let record = Record {
        timestamp_micros: 1_700_000_000,
        payload: Bytes::from(vec![7u8; CATCH_UP_RECORD]),
    };
    let mut left = records;
    while left > 0 {
        let n = left.min(1000);
        let batch = Batch {
            records: vec![record.clone(); n],
            ack: AckAt::Flushed,
        };
        led.engine.propose(&shard(0), batch).await.expect("written");
        left -= n;
    }
    let last = eventually(|| {
        let status = led.status();
        let last = status.last_index?;
        (status.committed >= Some(last)).then_some(last)
    })
    .await;
    (behind, last)
}

/// A follower behind by 6,000 records of 1 KiB catches up in a few
/// appends of about 4 MiB each. openraft's default of 300 entries an
/// append would take 20.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_behind_by_small_records_catches_up_in_few_appends() {
    let mut led = led().await;
    let (_, last) = leave_one_behind(&led, 6000).await;
    led.cluster.net.appends.store(0, Relaxed);
    led.heal();
    led.cluster.committed_everywhere(0, last).await;
    let appends = led.cluster.net.appends.load(Relaxed);
    assert!(appends <= 6, "{appends} appends to catch up 6,000 records");
    led.cluster.stop().await;
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let at = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[at]
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied().unwrap_or(0.0)
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

/// One reader's loop: confirm reads one after another until `until`.
async fn read_loop(led: Arc<Led>, shared: bool, until: Instant) -> Vec<Duration> {
    let member = led
        .cluster
        .runtime(&led.leader.broker)
        .group(&shard(0))
        .expect("group");
    let mut took = Vec::new();
    while Instant::now() < until {
        let start = Instant::now();
        if shared {
            led.engine
                .confirm_read(&shard(0), led.term)
                .await
                .expect("confirmed");
        } else {
            // A round per read: what `confirm_read` did before reads shared
            // rounds, without the lapsed-lease retry no run here needs.
            let (read, _) = member
                .raft()
                .get_read_log_id(ReadPolicy::ReadIndex)
                .await
                .expect("confirmed");
            assert_eq!(**read.committed_leader_id(), led.term);
        }
        took.push(start.elapsed());
    }
    took
}

/// Confirmed reads per second and latency, with a round per read and with
/// shared rounds, at several numbers of concurrent readers. The sides
/// alternate, and the one that goes first flips each round.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "benchmark; run by raft-read-perf.yml"]
async fn read_index_rounds_benchmark() {
    let seconds: f64 = env_or("RAFT_READ_BENCH_SECONDS", 5.0);
    let rounds: usize = env_or("RAFT_READ_BENCH_ROUNDS", 6);
    let readers = [1usize, 16, 128];
    let led = Arc::new(led().await);
    let member = led
        .cluster
        .runtime(&led.leader.broker)
        .group(&shard(0))
        .expect("group");
    // (shared, readers) -> per round: reads/s, p50 ms, p99 ms.
    let mut results: HashMap<(bool, usize), Vec<(f64, f64, f64)>> = HashMap::new();
    for round in 0..rounds {
        let sides = if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        for shared in sides {
            for &n in &readers {
                let asked = member.read_rounds_asked();
                let until = Instant::now() + Duration::from_secs_f64(seconds);
                let tasks: Vec<_> = (0..n)
                    .map(|_| tokio::spawn(read_loop(Arc::clone(&led), shared, until)))
                    .collect();
                let mut took = Vec::new();
                for task in tasks {
                    took.extend(task.await.expect("reader"));
                }
                took.sort_unstable();
                let rate = took.len() as f64 / seconds;
                let (p50, p99) = (ms(percentile(&took, 0.5)), ms(percentile(&took, 0.99)));
                let side = if shared { "shared" } else { "per-read" };
                let per_round = if shared {
                    let asked = member.read_rounds_asked() - asked;
                    format!(
                        ", {:.1} reads a round",
                        took.len() as f64 / asked.max(1) as f64
                    )
                } else {
                    String::new()
                };
                println!(
                    "read round {round} {side} readers={n}: {rate:.0} reads/s, p50 {p50:.3} ms, p99 {p99:.3} ms{per_round}"
                );
                results
                    .entry((shared, n))
                    .or_default()
                    .push((rate, p50, p99));
            }
        }
    }
    println!("read summary (medians over {rounds} rounds of {seconds} s):");
    println!("| readers | side | reads/s | p50 ms | p99 ms |");
    println!("| --- | --- | --- | --- | --- |");
    for &n in &readers {
        for shared in [false, true] {
            let runs = &results[&(shared, n)];
            let side = if shared { "shared" } else { "per-read" };
            println!(
                "| {n} | {side} | {:.0} | {:.3} | {:.3} |",
                median(runs.iter().map(|run| run.0).collect()),
                median(runs.iter().map(|run| run.1).collect()),
                median(runs.iter().map(|run| run.2).collect()),
            );
        }
    }
    match Arc::try_unwrap(led) {
        Ok(mut led) => led.cluster.stop().await,
        Err(_) => panic!("a reader still holds the cluster"),
    }
}

/// How long a follower behind by `RAFT_CATCH_UP_RECORDS` records of 1 KiB
/// takes to catch up over a simulated 1 Gbit/s link, and in how many
/// appends, at openraft's default of 300 entries an append and at the
/// runtime's. A fresh cluster per run; the sides alternate.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "benchmark; run by raft-read-perf.yml"]
async fn catch_up_benchmark() {
    let records: usize = env_or("RAFT_CATCH_UP_RECORDS", 50_000);
    let rounds: usize = env_or("RAFT_CATCH_UP_ROUNDS", 4);
    let sides = [300u64, RuntimeConfig::default().append_entries];
    let mut results: HashMap<u64, Vec<(f64, u64)>> = HashMap::new();
    for round in 0..rounds {
        let order = if round % 2 == 0 {
            sides
        } else {
            [sides[1], sides[0]]
        };
        for entries in order {
            let mut led = super::commit::led_with(RuntimeConfig {
                append_entries: entries,
                ..timers()
            })
            .await;
            let (_, last) = leave_one_behind(&led, records).await;
            led.cluster.net.appends.store(0, Relaxed);
            led.cluster.net.gigabit.store(true, Relaxed);
            let start = Instant::now();
            led.heal();
            led.cluster.committed_everywhere(0, last).await;
            let took = start.elapsed().as_secs_f64();
            let appends = led.cluster.net.appends.load(Relaxed);
            println!(
                "catch-up round {round} entries={entries}: {records} records in {took:.3} s, {appends} appends"
            );
            results.entry(entries).or_default().push((took, appends));
            led.cluster.stop().await;
        }
    }
    println!("catch-up summary (medians over {rounds} rounds, {records} records of 1 KiB):");
    println!("| entries an append | seconds | appends | records/s |");
    println!("| --- | --- | --- | --- |");
    for entries in sides {
        let runs = &results[&entries];
        let took = median(runs.iter().map(|run| run.0).collect());
        let appends = median(runs.iter().map(|run| run.1 as f64).collect());
        println!(
            "| {entries} | {took:.3} | {appends:.0} | {:.0} |",
            records as f64 / took
        );
    }
}

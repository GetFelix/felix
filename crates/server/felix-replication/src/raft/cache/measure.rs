//! Read-path and snapshot-build measurement for a cache group's state, run on demand.

use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::Bytes;

use super::{CacheCommand, CacheState, Condition, Read, Request};

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
            line.split_whitespace().nth(1)?.parse().ok()
        })
        .unwrap_or(0)
}

/// Best of five passes, in nanoseconds per call.
fn best(calls: usize, mut once: impl FnMut()) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..5 {
        let started = Instant::now();
        for _ in 0..calls {
            once();
        }
        best = best.min(started.elapsed().as_nanos() as f64 / calls as f64);
    }
    best
}

fn keys() -> u64 {
    std::env::var("MEASURE_KEYS")
        .ok()
        .and_then(|keys| keys.parse().ok())
        .unwrap_or(1_000)
}

fn command(index: u64, keys: u64) -> CacheCommand {
    let mut value = vec![0u8; 256];
    value[..8].copy_from_slice(&index.to_le_bytes());
    let expires_at = if index % 2 == 0 {
        1_000_000_000_000 + index
    } else {
        0
    };
    CacheCommand::Put {
        key: format!("key-{:08}", 1 + (index - 1) % keys),
        value: Bytes::from(value),
        expires_at,
        cond: Condition::None,
        request: Request {
            producer: 7,
            sequence: index,
            floor: 0,
        },
    }
}

#[test]
#[ignore = "a measurement"]
fn cache_read_path() {
    let keys = keys();
    let label = std::env::var("MEASURE_LABEL").unwrap_or_default();
    let before = rss_kb();
    let mut state = CacheState::default();
    let started = Instant::now();
    for index in 1..=keys {
        state.apply(index, index * 10, &command(index, keys));
    }
    let fill = started.elapsed();
    let rss_mb = rss_kb().saturating_sub(before) as f64 / 1024.0;
    let names: Vec<String> = (1..=keys).map(|index| format!("key-{index:08}")).collect();

    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let read_ns = best(2_000_000, || {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let key = &names[((x >> 33) % keys) as usize];
        if let Read::Value { version, .. } = state.read(key, 0) {
            black_box(version);
        }
    });
    let due_none_ns = best(1_000_000, || {
        black_box(state.next_expiry());
        black_box(state.due(0, 256));
    });
    let due_256_ns = best(20_000, || {
        black_box(state.due(u64::MAX, 256));
    });
    // Overwrites, the steady apply rate on a full state.
    let mut index = keys;
    let overwrite_ns = best((keys as usize).min(200_000), || {
        index += 1;
        state.apply(index, index * 10, &command(index, keys));
    });
    println!(
        "cache_read_path label={label} keys={keys} rss_mb={rss_mb:.1} fill_per_s={:.0} \
         read_ns={read_ns:.1} due_none_ns={due_none_ns:.1} due_256_us={:.2} overwrite_per_s={:.0}",
        keys as f64 / fill.as_secs_f64(),
        due_256_ns / 1000.0,
        1e9 / overwrite_ns,
    );
}

/// What a snapshot build costs a group's state machine: how long taking
/// the builder holds apply, the apply rate and read cost while the build
/// runs, and memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement"]
async fn cache_build() {
    use openraft::entry::RaftEntry;
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    use openraft::type_config::alias::{EntryOf, LogIdOf};

    use super::super::machine::Machine;
    use super::super::types::{Command, ShardTypes};
    use crate::ShardKind;

    const RUN: u64 = 10_000;
    let keys = keys();
    let label = std::env::var("MEASURE_LABEL").unwrap_or_default();
    let entries = move |first: u64, last: u64| {
        let entries: Vec<_> = (first..=last)
            .map(|index| {
                let command = Command {
                    timestamp_micros: index * 10,
                    payload: command(index, keys).encode(),
                    written: None,
                };
                let id = LogIdOf::<ShardTypes>::new(1.into(), index);
                Ok((EntryOf::<ShardTypes>::new_normal(id, command), None))
            })
            .collect();
        futures::stream::iter(entries)
    };
    let dir = tempfile::tempdir().expect("dir");
    let snapshots = felix_storage::raft_log::SnapshotStore::open(dir.path()).expect("store");
    let (mut machine, _, cache) = Machine::open(snapshots, ShardKind::Cache)
        .await
        .expect("open");
    let cache = cache.expect("cache");
    let rss_before = rss_kb();
    let mut last = 0;
    while last < keys {
        machine
            .apply(entries(last + 1, last + RUN))
            .await
            .expect("apply");
        last += RUN;
    }
    // Steady apply rate on the full state, before any build.
    let started = Instant::now();
    for _ in 0..20 {
        machine
            .apply(entries(last + 1, last + RUN))
            .await
            .expect("apply");
        last += RUN;
    }
    let steady = (20 * RUN) as f64 / started.elapsed().as_secs_f64();
    let rss_full = rss_kb();
    let names: Vec<String> = (1..=keys).map(|index| format!("key-{index:08}")).collect();
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut read = |reads: usize| {
        let started = Instant::now();
        for _ in 0..reads {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let key = &names[((x >> 33) % keys) as usize];
            black_box(cache.read().read(key, 0));
        }
        started.elapsed().as_nanos() as f64 / reads as f64
    };
    let read_before = read(200_000);

    let started = Instant::now();
    let mut builder = machine.get_snapshot_builder().await;
    let stall = started.elapsed();
    let build = tokio::spawn(async move {
        let started = Instant::now();
        builder.build_snapshot().await.expect("build");
        started.elapsed()
    });
    let (mut applied, mut applying, mut slowest) = (0, Duration::ZERO, Duration::ZERO);
    let (mut reads_ns, mut read_runs, mut rss_peak) = (0.0, 0, rss_kb());
    while !build.is_finished() {
        let started = Instant::now();
        machine
            .apply(entries(last + 1, last + RUN))
            .await
            .expect("apply");
        let took = started.elapsed();
        applying += took;
        slowest = slowest.max(took);
        applied += RUN;
        last += RUN;
        reads_ns += read(20_000);
        read_runs += 1;
        rss_peak = rss_peak.max(rss_kb());
    }
    let built = build.await.expect("task");
    // Once the build lets go, its writes fold back; then reads and apply
    // should be as before it.
    let started = Instant::now();
    let mut runs_after = 0;
    while started.elapsed() < Duration::from_secs(2) {
        machine
            .apply(entries(last + 1, last + RUN))
            .await
            .expect("apply");
        last += RUN;
        runs_after += 1;
    }
    let after_rate = (runs_after * RUN) as f64 / started.elapsed().as_secs_f64();
    let read_after = read(200_000);
    println!(
        "cache_build label={label} keys={keys} rss_full_mb={:.1} rss_peak_mb={:.1} \
         steady_per_s={steady:.0} builder_stall_us={:.1} build_ms={:.0} \
         during_per_s={:.0} slowest_run_ms={:.1} read_before_ns={read_before:.1} \
         read_during_ns={:.1} after_per_s={after_rate:.0} read_after_ns={read_after:.1}",
        rss_full.saturating_sub(rss_before) as f64 / 1024.0,
        rss_peak.saturating_sub(rss_before) as f64 / 1024.0,
        stall.as_secs_f64() * 1e6,
        built.as_secs_f64() * 1e3,
        applied as f64 / applying.as_secs_f64().max(f64::EPSILON),
        slowest.as_secs_f64() * 1e3,
        reads_ns / read_runs.max(1) as f64,
    );
}

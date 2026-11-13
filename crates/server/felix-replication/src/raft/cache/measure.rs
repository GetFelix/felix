//! Read-path measurement for a cache group's state, run on demand.

use std::hint::black_box;
use std::time::Instant;

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

#[test]
#[ignore = "a measurement"]
fn cache_read_path() {
    let keys: u64 = std::env::var("MEASURE_KEYS")
        .ok()
        .and_then(|keys| keys.parse().ok())
        .unwrap_or(1_000);
    let label = std::env::var("MEASURE_LABEL").unwrap_or_default();
    let before = rss_kb();
    let mut state = CacheState::default();
    let started = Instant::now();
    for index in 1..=keys {
        let mut value = vec![0u8; 256];
        value[..8].copy_from_slice(&index.to_le_bytes());
        let expires_at = if index % 2 == 0 {
            1_000_000_000_000 + index
        } else {
            0
        };
        let command = CacheCommand::Put {
            key: format!("key-{index:08}"),
            value: Bytes::from(value),
            expires_at,
            cond: Condition::None,
            request: Request {
                producer: 7,
                sequence: index,
                floor: 0,
            },
        };
        state.apply(index, index * 10, &command);
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
    let clone_ns = best(if keys > 100_000 { 5 } else { 2_000 }, || {
        black_box(state.clone());
    });
    println!(
        "cache_read_path label={label} keys={keys} rss_mb={rss_mb:.1} fill_per_s={:.0} \
         read_ns={read_ns:.1} due_none_ns={due_none_ns:.1} due_256_us={:.2} clone_us={:.2}",
        keys as f64 / fill.as_secs_f64(),
        due_256_ns / 1000.0,
        clone_ns / 1000.0,
    );
}

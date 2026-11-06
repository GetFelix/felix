//! Scratch timing for #1968, never merged.
#![allow(unreachable_pub, missing_docs, clippy::all)]

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

pub const L_TOTAL: usize = 0;
pub const L_ENQ: usize = 1;
pub const L_QUEUED: usize = 2;
pub const L_WRITE: usize = 3;
pub const L_REMOTE: usize = 4;
pub const L_WAKE: usize = 5;
pub const L_DEPTH: usize = 6;
pub const L_BYTES: usize = 7;
pub const F_IDLE: usize = 8;
pub const F_SPAWN: usize = 9;
pub const F_HANDLE_REC: usize = 10;
pub const F_HANDLE_OTHER: usize = 11;
pub const F_JOIN: usize = 12;
pub const F_WRITE: usize = 13;
pub const F_CYCLE: usize = 14;
pub const F_READ: usize = 15;
pub const SCHED_LAG: usize = 16;
pub const L_TOTAL_REC: usize = 17;
pub const F_BACKLOG: usize = 18;
pub const L_PERMIT_CONN: usize = 19;
pub const L_ENCODE: usize = 20;
const KINDS: usize = 21;
const NAMES: [&str; KINDS] = [
    "l_total",
    "l_enq",
    "l_queued",
    "l_write",
    "l_remote",
    "l_wake",
    "l_depth",
    "l_bytes",
    "f_idle",
    "f_spawn",
    "f_handle_rec",
    "f_handle_other",
    "f_join",
    "f_write",
    "f_cycle",
    "f_read",
    "sched_lag",
    "l_total_rec",
    "f_backlog",
    "l_permit_conn",
    "l_encode",
];
static N: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static SUM: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static MAX: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static STARTED: std::sync::Once = std::sync::Once::new();

fn start() {
    STARTED.call_once(|| {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async {
            let mut last = Instant::now();
            let mut next = Instant::now();
            loop {
                let before = Instant::now();
                tokio::time::sleep(Duration::from_millis(10)).await;
                value(
                    SCHED_LAG,
                    before
                        .elapsed()
                        .saturating_sub(Duration::from_millis(10))
                        .as_micros() as u64,
                );
                if next.elapsed() < Duration::from_secs(5) {
                    continue;
                }
                next = Instant::now();
                let window = last.elapsed().as_micros() as u64;
                last = Instant::now();
                let mut line = String::new();
                for k in 0..KINDS {
                    let n = N[k].swap(0, Relaxed);
                    let sum = SUM[k].swap(0, Relaxed);
                    let max = MAX[k].swap(0, Relaxed);
                    if n == 0 {
                        continue;
                    }
                    line.push_str(&format!(" {}=n{}/mean{}/max{}", NAMES[k], n, sum / n, max));
                }
                tracing::info!(window_us = window, "rttprof{line}");
            }
        });
    });
}

pub fn value(kind: usize, v: u64) {
    start();
    N[kind].fetch_add(1, Relaxed);
    SUM[kind].fetch_add(v, Relaxed);
    MAX[kind].fetch_max(v, Relaxed);
}

pub fn add(kind: usize, d: Duration) {
    value(kind, d.as_micros() as u64);
}

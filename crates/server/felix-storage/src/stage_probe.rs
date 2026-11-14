//! Measurement only, not for merge: per-stage latency histograms for the
//! Raft publish path, logged every few seconds when FELIX_STAGE_PROBE is set.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const BUCKET_MICROS: u64 = 25;
const BUCKETS: usize = 800;

pub struct Stage {
    name: &'static str,
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

impl Stage {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
            buckets: [const { AtomicU64::new(0) }; BUCKETS],
        }
    }

    pub fn value(&self, value: u64) {
        if !enabled() {
            return;
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.max.fetch_max(value, Ordering::Relaxed);
        let bucket = ((value / BUCKET_MICROS) as usize).min(BUCKETS - 1);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    pub fn since(&self, start: Instant) {
        self.value(start.elapsed().as_micros() as u64);
    }

    fn take(&self) -> Option<String> {
        let count = self.count.swap(0, Ordering::Relaxed);
        let sum = self.sum.swap(0, Ordering::Relaxed);
        let max = self.max.swap(0, Ordering::Relaxed);
        let buckets: Vec<u64> = self
            .buckets
            .iter()
            .map(|bucket| bucket.swap(0, Ordering::Relaxed))
            .collect();
        if count == 0 {
            return None;
        }
        let total: u64 = buckets.iter().sum();
        let at = |share: f64| {
            let want = (total as f64 * share) as u64;
            let mut seen = 0;
            for (i, n) in buckets.iter().enumerate() {
                seen += n;
                if seen > want {
                    return i as u64 * BUCKET_MICROS;
                }
            }
            max
        };
        Some(format!(
            "{}: n={} mean={} p50={} p99={} max={}",
            self.name,
            count,
            sum / count,
            at(0.5),
            at(0.99),
            max
        ))
    }
}

macro_rules! stages {
    ($($ident:ident),* $(,)?) => {
        $(pub static $ident: Stage = Stage::new(stringify!($ident));)*
        static ALL: &[&Stage] = &[$(&$ident),*];
    };
}

// Times are microseconds; *_N stages are counts.
stages!(
    PROPOSE_TO_DRAIN,
    DRAIN_BATCHES_N,
    DRAIN_TO_HANDED,
    PROPOSE_TO_WRITTEN,
    WRITTEN_TO_APPLIED,
    PROPOSE_TO_ANSWER,
    LOG_APPEND_WRITE,
    LOG_APPEND_ENTRIES_N,
    APPEND_TO_FLUSHED,
    FSYNC,
    FLUSH_ROUND_APPENDS_N,
    RPC_ROUND_TRIP,
    RPC_ENTRIES_N,
    RPC_GAP_SAME_TARGET,
    FOLLOWER_APPEND_HANDLE,
    APPLY_ENTRIES_N,
    PUBLISH_TOTAL,
);

pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        let on = std::env::var_os("FELIX_STAGE_PROBE").is_some();
        if on {
            std::thread::spawn(|| {
                loop {
                    std::thread::sleep(Duration::from_secs(5));
                    let lines: Vec<String> = ALL.iter().filter_map(|stage| stage.take()).collect();
                    if !lines.is_empty() {
                        eprintln!("STAGE_PROBE\n  {}", lines.join("\n  "));
                    }
                }
            });
        }
        on
    })
}

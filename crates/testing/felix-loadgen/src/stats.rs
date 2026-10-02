//! Latency samples and the two report shapes every scenario emits.
//!
//! Human-readable lines in the format `scripts/perf/run_latency_matrix.py`
//! already parses — so eyeballing a remote run reads like a local one — and a
//! single `LOADGEN_JSON {...}` line per case for the Azure runner, which is
//! the machine contract: the runner never scrapes the prose.

use std::time::Duration;

/// Microsecond samples, percentiled by sort. Sample counts here are at most a
/// few hundred thousand, where sorting is cheaper than being clever.
#[derive(Debug, Default)]
pub(crate) struct Samples {
    micros: Vec<u64>,
}

impl Samples {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            micros: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn record(&mut self, elapsed: Duration) {
        self.micros.push(elapsed.as_micros() as u64);
    }

    pub(crate) fn merge(&mut self, other: Samples) {
        self.micros.extend(other.micros);
    }

    pub(crate) fn len(&self) -> usize {
        self.micros.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.micros.is_empty()
    }

    pub(crate) fn percentiles(&mut self) -> Percentiles {
        self.micros.sort_unstable();
        let at = |q: f64| -> u64 {
            if self.micros.is_empty() {
                return 0;
            }
            let idx = ((self.micros.len() as f64 - 1.0) * q).round() as usize;
            self.micros[idx.min(self.micros.len() - 1)]
        };
        Percentiles {
            p50_us: at(0.50),
            p99_us: at(0.99),
            p999_us: at(0.999),
            max_us: self.micros.last().copied().unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Percentiles {
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}

/// `123.4 us` / `12.3 ms`, matching what the matrix runner's duration parser
/// accepts.
pub(crate) fn fmt_us(micros: u64) -> String {
    if micros >= 10_000 {
        format!("{:.1} ms", micros as f64 / 1000.0)
    } else {
        format!("{micros:.1} us")
    }
}

/// The machine contract: one JSON object on one line, prefixed so it survives
/// being embedded in a log full of prose.
pub fn emit_json(value: &serde_json::Value) {
    println!("LOADGEN_JSON {value}");
}

/// `println!`, when the run was asked for the human-readable lines.
macro_rules! report {
    ($common:expr, $($arg:tt)*) => {
        if $common.prose {
            println!($($arg)*);
        }
    };
}
pub(crate) use report;

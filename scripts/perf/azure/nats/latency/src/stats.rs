//! felix-loadgen's `stats.rs`, unchanged: microsecond samples, percentiles by
//! sort at index round((n - 1) * q), and the `LOADGEN_JSON` line.

use std::time::Duration;

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

pub(crate) fn fmt_us(micros: u64) -> String {
    if micros >= 10_000 {
        format!("{:.1} ms", micros as f64 / 1000.0)
    } else {
        format!("{micros:.1} us")
    }
}

pub(crate) fn emit_json(value: &serde_json::Value) {
    println!("LOADGEN_JSON {value}");
}

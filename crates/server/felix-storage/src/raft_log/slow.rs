//! A warning when one of openraft's calls into the log outlasts a
//! heartbeat. openraft runs them on its core, so a slow one stops the
//! member's heartbeats and writes for as long.

use std::time::{Duration, Instant};

use crate::metrics_names;

/// The least time between two warnings of one log.
const WARN_EVERY: Duration = Duration::from_secs(10);

#[derive(Default)]
pub(super) struct SlowCalls {
    /// Set by [`super::RaftLog::warn_slow_calls`]; nothing is timed before.
    threshold: std::sync::OnceLock<Duration>,
    pub(super) warned: parking_lot::Mutex<Option<Instant>>,
}

impl SlowCalls {
    pub(super) fn set(&self, threshold: Duration) {
        let _ = self.threshold.set(threshold);
    }

    /// Time `call` until the returned guard drops.
    pub(super) fn time<'a>(&'a self, label: &'a str, call: &'static str) -> Timed<'a> {
        Timed {
            slow: self,
            label,
            call,
            began: Instant::now(),
        }
    }
}

pub(super) struct Timed<'a> {
    slow: &'a SlowCalls,
    label: &'a str,
    call: &'static str,
    began: Instant,
}

impl Drop for Timed<'_> {
    fn drop(&mut self) {
        let Some(&threshold) = self.slow.threshold.get() else {
            return;
        };
        let took = self.began.elapsed();
        if took <= threshold {
            return;
        }
        metrics::counter!(metrics_names::RAFT_LOG_SLOW_CALLS_TOTAL, "call" => self.call)
            .increment(1);
        let mut warned = self.slow.warned.lock();
        if warned.is_some_and(|at| at.elapsed() < WARN_EVERY) {
            return;
        }
        *warned = Some(Instant::now());
        drop(warned);
        tracing::warn!(
            shard = %self.label,
            call = self.call,
            ?took,
            ?threshold,
            "a Raft log call held the Raft core for longer than a heartbeat"
        );
    }
}

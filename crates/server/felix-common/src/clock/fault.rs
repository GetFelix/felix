//! Test-only clock faults: a fixed offset, a step, a clock that runs fast or
//! slow.
//!
//! Off unless `FELIX_CLOCK_FAULT_FILE` names a file, or a process hosting a
//! service in-process calls [`follow`] (the cluster harness does, for its
//! control plane). Like the peer partition file, it needs no cooperation from
//! the code under test beyond reading time through [`super`].
//!
//! The file holds `key=value` lines:
//!
//! - `offset_ms=<i64>` is added to every reading. Changing it is a step,
//!   forwards or backwards.
//! - `rate=<f64>` is how fast this process's clock runs against the real one,
//!   from the moment the change is seen. Drift already accumulated is kept
//!   when the rate changes again, so the clock does not jump.
//!
//! Missing or empty means the true clock, so healing is a delete. The skew
//! applies to both [`super::boottime`] and [`super::wall_millis`]: a real
//! clock fault does not pick one.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Once, PoisonError};
use std::time::{Duration, Instant};

/// How long a reading of the file is reused. Short enough that a test sees a
/// step land without coordinating, long enough that a lease check is not a
/// file read.
const REREAD_AFTER: Duration = Duration::from_millis(50);

static FROM_ENV: Once = Once::new();
/// The fast path. False in every process that never asked for a fault.
static ENABLED: AtomicBool = AtomicBool::new(false);
static FOLLOWER: Mutex<Option<Follower>> = Mutex::new(None);

/// Skew this process's clock by whatever `path` says, from now on.
///
/// For a service hosted in a process whose environment the caller does not
/// control, such as a test binary. Replaces any file followed before, and
/// starts from the true clock.
pub fn follow(path: impl Into<PathBuf>) {
    // Settled first, so the environment read later cannot undo this.
    FROM_ENV.call_once(|| {});
    install(path.into());
}

/// Stop following `path`, if it is the file being followed, and go back to
/// the true clock.
pub fn stop_following(path: &Path) {
    let mut follower = FOLLOWER.lock().unwrap_or_else(PoisonError::into_inner);
    if follower.as_ref().is_some_and(|f| f.path == path) {
        *follower = None;
        ENABLED.store(false, Ordering::Release);
    }
}

/// How far this process's clock is from the true one right now, in nanos.
pub(super) fn offset_nanos() -> i128 {
    FROM_ENV.call_once(|| {
        if let Some(path) = std::env::var_os("FELIX_CLOCK_FAULT_FILE") {
            install(path.into());
        }
    });
    if !ENABLED.load(Ordering::Acquire) {
        return 0;
    }
    let mut follower = FOLLOWER.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(follower) = follower.as_mut() else {
        return 0;
    };
    let raw = super::raw_boottime();
    if follower
        .read_at
        .is_none_or(|read_at| read_at.elapsed() >= REREAD_AFTER)
    {
        let setting = std::fs::read_to_string(&follower.path)
            .ok()
            .and_then(|body| Setting::parse(&body));
        follower.skew.set(setting, raw);
        follower.read_at = Some(Instant::now());
    }
    follower.skew.offset_at(raw)
}

fn install(path: PathBuf) {
    // On stderr: this crate has no logger, and a process running on a
    // doctored clock should say so somewhere.
    eprintln!(
        "clock fault injection is ENABLED from {}; this is a test-only facility",
        path.display()
    );
    *FOLLOWER.lock().unwrap_or_else(PoisonError::into_inner) = Some(Follower {
        path,
        read_at: None,
        skew: Skew::default(),
    });
    ENABLED.store(true, Ordering::Release);
}

struct Follower {
    path: PathBuf,
    read_at: Option<Instant>,
    skew: Skew,
}

/// What the file asks for.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Setting {
    pub(crate) offset_ms: i64,
    pub(crate) rate: f64,
}

impl Setting {
    /// `None` for a file that asks for nothing, which is the true clock.
    /// Lines that do not parse are ignored rather than failing the reading:
    /// a half-written file must not stop the clock.
    pub(crate) fn parse(body: &str) -> Option<Self> {
        let mut setting = Setting {
            offset_ms: 0,
            rate: 1.0,
        };
        let mut any = false;
        for line in body.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            match key.trim() {
                "offset_ms" => {
                    if let Ok(offset) = value.trim().parse() {
                        setting.offset_ms = offset;
                        any = true;
                    }
                }
                "rate" => {
                    if let Ok(rate) = value.trim().parse::<f64>()
                        && rate.is_finite()
                        && rate >= 0.0
                    {
                        setting.rate = rate;
                        any = true;
                    }
                }
                _ => {}
            }
        }
        any.then_some(setting)
    }
}

/// The offset a [`Setting`] produces over time, measured against the raw
/// boottime clock.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Skew {
    offset_ms: i64,
    rate: f64,
    /// Drift accumulated before `since`, under earlier rates.
    drift_nanos: i128,
    /// Raw reading at which `rate` took effect.
    since: Duration,
}

impl Default for Skew {
    fn default() -> Self {
        Self {
            offset_ms: 0,
            rate: 1.0,
            drift_nanos: 0,
            since: Duration::ZERO,
        }
    }
}

impl Skew {
    /// Adopt `setting` as of raw reading `raw`. `None` is the true clock and
    /// forgets any drift.
    pub(crate) fn set(&mut self, setting: Option<Setting>, raw: Duration) {
        let Some(setting) = setting else {
            *self = Skew::default();
            return;
        };
        if setting.rate != self.rate {
            self.drift_nanos = self.drift_at(raw);
            self.since = raw;
            self.rate = setting.rate;
        }
        self.offset_ms = setting.offset_ms;
    }

    pub(crate) fn offset_at(&self, raw: Duration) -> i128 {
        i128::from(self.offset_ms) * 1_000_000 + self.drift_at(raw)
    }

    fn drift_at(&self, raw: Duration) -> i128 {
        if self.rate == 1.0 {
            return self.drift_nanos;
        }
        let elapsed = raw.saturating_sub(self.since).as_nanos() as f64;
        self.drift_nanos + (elapsed * (self.rate - 1.0)) as i128
    }
}

#[cfg(test)]
mod tests;

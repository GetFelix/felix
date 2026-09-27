//! The clock the lease is measured on.
//!
//! `CLOCK_MONOTONIC` (and so `tokio::time::Instant`) stops while the host or VM
//! is suspended, so a broker that slept through its expiry would wake still
//! counting a valid lease. The control plane's clock kept running. On Linux the
//! lease reads `CLOCK_BOOTTIME`, which counts suspended time too; elsewhere it
//! falls back to `std::time::Instant`, which is the best the platform offers.
//!
//! Scheduling (sleeps, tickers) stays on tokio: a late tick costs latency, not
//! safety. Only validity is judged on this clock.

use std::time::Duration;

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

/// A reading of a [`LeaseClock`]. Comparable only with readings of the same
/// clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LeaseInstant(Duration);

impl LeaseInstant {
    pub(crate) fn saturating_duration_since(self, earlier: LeaseInstant) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}

/// Where lease time comes from.
#[derive(Clone, Debug)]
pub(crate) enum LeaseClock {
    /// `CLOCK_BOOTTIME` on Linux, `std::time::Instant` elsewhere.
    System,
    /// Tokio's clock, so a paused-time test can drive the lease with
    /// `tokio::time::advance`.
    #[cfg(test)]
    Tokio(tokio::time::Instant),
    /// Moved only by the test holding it, in millis.
    #[cfg(test)]
    Manual(Arc<AtomicU64>),
}

impl LeaseClock {
    pub(crate) fn now(&self) -> LeaseInstant {
        match self {
            LeaseClock::System => LeaseInstant(system_now()),
            #[cfg(test)]
            LeaseClock::Tokio(origin) => LeaseInstant(origin.elapsed()),
            #[cfg(test)]
            LeaseClock::Manual(millis) => {
                LeaseInstant(Duration::from_millis(millis.load(Ordering::Acquire)))
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn system_now() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable timespec; CLOCK_BOOTTIME exists on
    // every kernel this runs on (2.6.39+), so the call cannot fail.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_BOOTTIME) failed");
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

#[cfg(not(target_os = "linux"))]
fn system_now() -> Duration {
    static ORIGIN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    ORIGIN.get_or_init(std::time::Instant::now).elapsed()
}

#[cfg(test)]
mod tests;

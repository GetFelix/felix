//! The clocks leases and liveness are judged on.
//!
//! Two readings, one per side of the lease:
//!
//! - [`boottime`] is what a broker measures its lease on. `CLOCK_MONOTONIC`
//!   (and so `tokio::time::Instant`) stops while the host or VM is suspended,
//!   so a broker that slept through its expiry would wake still counting a
//!   valid lease while the control plane's clock kept running. On Linux this
//!   reads `CLOCK_BOOTTIME`, which counts suspended time too; elsewhere it
//!   falls back to `std::time::Instant`, the best the platform offers.
//! - [`wall_millis`] is what the control plane stamps heartbeats with and
//!   judges expiry against.
//!
//! In debug builds and builds with the `fault-injection` feature, both pass
//! through [`fault`], so a test can skew, step or speed up one process's view
//! of time. That is off, and costs one atomic load, unless
//! `FELIX_CLOCK_FAULT_FILE` is set. A plain release build has no such module
//! and reads the real clocks directly.

#[cfg(any(debug_assertions, test, feature = "fault-injection"))]
pub mod fault;

use std::time::Duration;

/// The lease clock: monotonic, and counting through suspend where the
/// platform allows. Readings are comparable only within one process.
pub fn boottime() -> Duration {
    skewed(raw_boottime())
}

/// Wall-clock milliseconds since the Unix epoch.
///
/// Clamped at zero so a clock behind the epoch cannot panic the caller.
pub fn wall_millis() -> u64 {
    let raw = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    skewed(raw).as_millis() as u64
}

/// `raw` as this process's clock fault has it.
#[cfg(any(debug_assertions, test, feature = "fault-injection"))]
fn skewed(raw: Duration) -> Duration {
    shift(raw, fault::offset_nanos())
}

#[cfg(not(any(debug_assertions, test, feature = "fault-injection")))]
#[inline]
fn skewed(raw: Duration) -> Duration {
    raw
}

#[cfg(target_os = "linux")]
fn raw_boottime() -> Duration {
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
fn raw_boottime() -> Duration {
    static ORIGIN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    ORIGIN.get_or_init(std::time::Instant::now).elapsed()
}

/// `raw` moved by `nanos`, floored at zero.
#[cfg(any(debug_assertions, test, feature = "fault-injection"))]
fn shift(raw: Duration, nanos: i128) -> Duration {
    if nanos == 0 {
        return raw;
    }
    let shifted = (raw.as_nanos() as i128).saturating_add(nanos).max(0);
    Duration::from_nanos(u64::try_from(shifted).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests;

//! Test-only faults at the I/O seam: a slow device.
//!
//! Compiled into debug builds and into builds with the `fault-injection`
//! feature, never into a plain release build. Nothing here does anything until
//! a caller sets it, so a debug broker that is never told about a fault
//! behaves exactly as a release one.
//!
//! Every flush the crate issues goes through `io` (`sync_data`, `sync_all`,
//! `sync_dir`, and the `io_uring` submission), and each of those consults this
//! module first. That is what makes a fault here reach every durability path,
//! including the macOS `F_FULLFSYNC` branch and the ring.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Microseconds each flush waits before it is issued. Zero means no delay.
static FSYNC_DELAY_MICROS: AtomicU64 = AtomicU64::new(0);

/// Make every flush in this process wait `delay` before it reaches the device.
///
/// Process-wide, because the thing it stands in for is a slow disk, and a
/// broker has one. `Duration::ZERO` turns it off.
pub fn set_fsync_delay(delay: Duration) {
    let micros = u64::try_from(delay.as_micros()).unwrap_or(u64::MAX);
    if FSYNC_DELAY_MICROS.swap(micros, Ordering::Relaxed) != micros {
        tracing::warn!(
            delay_ms = delay.as_millis() as u64,
            "storage fault injection: every fsync is delayed (test-only facility)",
        );
    }
}

/// The delay currently injected before each flush, if any.
pub fn fsync_delay() -> Option<Duration> {
    match FSYNC_DELAY_MICROS.load(Ordering::Relaxed) {
        0 => None,
        micros => Some(Duration::from_micros(micros)),
    }
}

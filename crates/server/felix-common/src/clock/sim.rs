//! Clocks driven by a simulator.
//!
//! A deterministic simulation runs several brokers and a control plane in one
//! process on virtual time. Their leases must age on that time too, or a
//! simulated pause would never expire one. [`install`] hands both clocks to
//! the simulator for the rest of the process's life: [`super::boottime`]
//! reads the simulator's elapsed time and [`super::wall_millis`] reads it
//! counted from a fixed [`EPOCH`], so every run sees the same dates.
//!
//! Until something installs a clock this costs one atomic load per reading.

use std::sync::OnceLock;
use std::time::Duration;

/// What [`super::wall_millis`] reads at the start of a simulation:
/// 2026-01-01T00:00:00Z.
pub const EPOCH: Duration = Duration::from_millis(1_767_225_600_000);

static CLOCK: OnceLock<fn() -> Duration> = OnceLock::new();

/// Drive this process's clocks from `elapsed`, the simulated time since the
/// simulation began. It must never go backwards.
///
/// Installing the same function again is a no-op.
///
/// # Panics
///
/// If a different clock is already installed: two simulators in one process
/// would each see the other's time.
pub fn install(elapsed: fn() -> Duration) {
    let installed = *CLOCK.get_or_init(|| elapsed);
    assert!(
        std::ptr::fn_addr_eq(installed, elapsed),
        "a different simulated clock is already installed"
    );
}

/// Whether a simulator drives this process.
pub fn installed() -> bool {
    CLOCK.get().is_some()
}

/// The simulator's elapsed time, if one drives this process.
pub(super) fn elapsed() -> Option<Duration> {
    CLOCK.get().map(|elapsed| elapsed())
}

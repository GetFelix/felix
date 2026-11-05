//! `clock::sim` in its own test binary: an installed clock stays installed
//! for the life of the process, so it cannot share one with tests that read
//! the real clocks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use felix_common::clock;

static NOW_MS: AtomicU64 = AtomicU64::new(0);

fn simulated() -> Duration {
    Duration::from_millis(NOW_MS.load(Ordering::SeqCst))
}

fn other() -> Duration {
    Duration::ZERO
}

#[test]
fn an_installed_clock_drives_both_readings() {
    NOW_MS.store(1_500, Ordering::SeqCst);
    clock::sim::install(simulated);
    assert_eq!(clock::boottime(), Duration::from_millis(1_500));
    let epoch_ms = clock::sim::EPOCH.as_millis() as u64;
    assert_eq!(clock::wall_millis(), epoch_ms + 1_500);

    NOW_MS.store(61_500, Ordering::SeqCst);
    assert_eq!(clock::boottime(), Duration::from_millis(61_500));
    assert_eq!(clock::wall_millis(), epoch_ms + 61_500);

    // The harness installs once per seed; the same clock again is fine.
    clock::sim::install(simulated);
    let second = std::panic::catch_unwind(|| clock::sim::install(other));
    assert!(second.is_err(), "a second, different clock must be refused");
    assert_eq!(clock::boottime(), Duration::from_millis(61_500));
}

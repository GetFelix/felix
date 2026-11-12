//! `clock::sim` in its own test binary: an installed clock stays installed
//! for the life of the process, so it cannot share one with tests that read
//! the real clocks.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use felix_common::clock;

static NOW_MS: AtomicU64 = AtomicU64::new(0);
static WALL_OFFSET_MS: AtomicI64 = AtomicI64::new(0);

fn simulated() -> Duration {
    Duration::from_millis(NOW_MS.load(Ordering::SeqCst))
}

fn wall_offset() -> i128 {
    i128::from(WALL_OFFSET_MS.load(Ordering::SeqCst)) * 1_000_000
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

    // A wall offset moves the wall clock and leaves the lease clock alone.
    NOW_MS.store(10_000, Ordering::SeqCst);
    clock::sim::install_wall_offset(wall_offset);

    WALL_OFFSET_MS.store(-2_500, Ordering::SeqCst);
    assert_eq!(clock::wall_millis(), epoch_ms + 7_500);
    assert_eq!(
        clock::system_time(),
        std::time::UNIX_EPOCH + clock::sim::EPOCH + Duration::from_millis(7_500)
    );
    assert_eq!(clock::boottime(), Duration::from_millis(10_000));

    WALL_OFFSET_MS.store(0, Ordering::SeqCst);
    assert_eq!(clock::wall_millis(), epoch_ms + 10_000);
}

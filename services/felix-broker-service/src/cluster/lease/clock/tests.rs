use super::*;

#[test]
fn the_system_clock_never_goes_backwards() {
    let clock = LeaseClock::System;
    let mut last = clock.now();
    for _ in 0..10_000 {
        let now = clock.now();
        assert!(now >= last, "{now:?} is before {last:?}");
        last = now;
    }
    std::thread::sleep(Duration::from_millis(5));
    assert!(clock.now().saturating_duration_since(last) >= Duration::from_millis(5));
}

/// The lease must run on a clock that keeps counting through suspend. On Linux
/// that is `CLOCK_BOOTTIME`, which is never behind `CLOCK_MONOTONIC` (they
/// differ by the time spent suspended).
#[cfg(target_os = "linux")]
#[test]
fn on_linux_the_system_clock_is_boottime() {
    let read = |id| {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: valid timespec, clock ids known to the kernel.
        assert_eq!(unsafe { libc::clock_gettime(id, &mut ts) }, 0);
        Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    };
    let monotonic = read(libc::CLOCK_MONOTONIC);
    let lease = LeaseClock::System.now().0;
    let boottime = read(libc::CLOCK_BOOTTIME);
    assert!(lease >= monotonic, "lease clock is behind CLOCK_MONOTONIC");
    assert!(lease <= boottime, "lease clock is ahead of CLOCK_BOOTTIME");
}

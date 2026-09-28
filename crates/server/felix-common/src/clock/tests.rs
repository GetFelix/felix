use super::*;

// These read the raw clock: the one test that installs a fault follower runs
// in parallel with them, and would otherwise move time under their feet.

#[test]
fn the_boottime_clock_never_goes_backwards() {
    let mut last = raw_boottime();
    for _ in 0..10_000 {
        let now = raw_boottime();
        assert!(now >= last, "{now:?} is before {last:?}");
        last = now;
    }
    std::thread::sleep(Duration::from_millis(5));
    assert!(raw_boottime().saturating_sub(last) >= Duration::from_millis(5));
}

#[test]
fn a_shift_is_floored_at_zero() {
    let raw = Duration::from_secs(10);
    assert_eq!(shift(raw, 0), raw);
    assert_eq!(shift(raw, 1_000_000_000), Duration::from_secs(11));
    assert_eq!(shift(raw, -20_000_000_000), Duration::ZERO);
}

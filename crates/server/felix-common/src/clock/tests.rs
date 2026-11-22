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

#[test]
fn system_time_follows_the_host_clock_without_a_simulator() {
    let before = std::time::SystemTime::now();
    let read = system_time();
    let after = std::time::SystemTime::now();
    assert!(before <= read && read <= after);
}

#[test]
fn a_past_instant_is_due_now() {
    assert!(until_unix(0).is_zero());
    assert!(until_unix(i64::MIN).is_zero());
    assert!(!until_unix(i64::MAX).is_zero());
}

#[cfg(feature = "wall-sleep")]
#[tokio::test(start_paused = true)]
async fn an_absent_end_never_returns_and_a_past_one_returns_at_once() {
    sleep_until_unix_or_never(Some(0)).await;
    let never = tokio::time::timeout(Duration::from_secs(3600), sleep_until_unix_or_never(None));
    assert!(never.await.is_err());
}

use std::time::Duration;

use super::{check_sharded_watch, non_negative_duration, positive_duration, reconnect_policy};

const YEAR: f64 = 365.0 * 24.0 * 3600.0;

#[test]
fn a_delay_may_be_zero_but_not_negative_or_unbounded() {
    assert_eq!(non_negative_duration(0.0, "delay").unwrap(), Duration::ZERO);
    assert_eq!(
        non_negative_duration(1.5, "delay").unwrap(),
        Duration::from_millis(1500)
    );
    assert!(non_negative_duration(YEAR, "delay").is_ok());
    for bad in [-0.001, -1.0, YEAR + 1.0, f64::NAN, f64::INFINITY] {
        assert!(non_negative_duration(bad, "delay").is_err(), "{bad}");
    }
}

#[test]
fn a_visibility_or_extension_must_be_positive() {
    assert!(positive_duration(0.001, "visibility").is_ok());
    for bad in [0.0, -1.0, YEAR + 1.0, f64::NAN, f64::INFINITY] {
        assert!(positive_duration(bad, "visibility").is_err(), "{bad}");
    }
}

#[test]
fn a_retained_watch_refuses_a_resume() {
    let resume = Some(std::collections::BTreeMap::from([(0_u32, 5_u64)]));
    assert!(check_sharded_watch(true, &resume).is_err());
    assert!(check_sharded_watch(true, &None::<()>).is_ok());
    assert!(check_sharded_watch(false, &resume).is_ok());
}

#[test]
fn a_read_timeout_must_be_finite_and_not_negative() {
    use crate::producer::timeout_of;

    assert_eq!(timeout_of(None).unwrap(), None);
    assert_eq!(timeout_of(Some(0.0)).unwrap(), Some(Duration::ZERO));
    for bad in [-1.0, YEAR + 1.0, f64::NAN, f64::INFINITY] {
        assert!(timeout_of(Some(bad)).is_err(), "{bad}");
    }
}

#[test]
fn the_backoffs_reach_the_policy_and_a_max_below_the_backoff_is_kept() {
    let policy = reconnect_policy(None, None, Some(0.05), Some(0.4)).unwrap();
    assert_eq!(policy.backoff, Duration::from_millis(50));
    assert_eq!(policy.max_backoff, Duration::from_millis(400));

    let policy = reconnect_policy(None, None, Some(5.0), Some(0.1)).unwrap();
    assert_eq!(policy.backoff, Duration::from_secs(5));
    assert_eq!(policy.max_backoff, Duration::from_millis(100));

    let default = reconnect_policy(None, None, None, None).unwrap();
    assert_eq!(default.backoff, Duration::from_millis(200));
    assert_eq!(default.max_backoff, Duration::from_secs(2));
}

#[test]
fn a_backoff_must_be_above_zero_and_at_most_a_year() {
    for bad in [0.0, -1.0, YEAR + 1.0, f64::NAN, f64::INFINITY] {
        assert!(
            reconnect_policy(None, None, Some(bad), None).is_err(),
            "{bad}"
        );
        assert!(
            reconnect_policy(None, None, None, Some(bad)).is_err(),
            "{bad}"
        );
    }
}

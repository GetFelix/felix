use std::time::Duration;

use super::{check_sharded_watch, non_negative_duration, positive_duration};

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

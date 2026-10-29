use napi::bindgen_prelude::BigInt;

use super::{duration_ms, reconnect_policy, u64_of};

#[test]
fn a_bigint_in_range_converts() {
    assert_eq!(u64_of(BigInt::from(7u64)).unwrap(), 7);
    assert_eq!(u64_of(BigInt::from(u64::MAX)).unwrap(), u64::MAX);
}

#[test]
fn a_negative_bigint_is_refused_rather_than_read_as_its_magnitude() {
    assert!(u64_of(BigInt::from(-1i64)).is_err());
}

#[test]
fn a_bigint_wider_than_u64_is_refused() {
    let wide = BigInt {
        sign_bit: false,
        words: vec![0, 1],
    };
    assert!(u64_of(wide).is_err());
}

#[test]
fn a_duration_is_kept_between_its_floor_and_a_year() {
    let year = 365.0 * 24.0 * 3600.0 * 1000.0;
    assert_eq!(duration_ms(1500.0, 1.0, "x").unwrap().as_millis(), 1500);
    assert!(duration_ms(0.0, 0.0, "x").is_ok());
    assert!(duration_ms(year, 1.0, "x").is_ok());
    for bad in [0.0, -1.0, f64::NAN, f64::INFINITY, year + 1.0] {
        assert!(duration_ms(bad, 1.0, "x").is_err(), "{bad} was accepted");
    }
    assert!(duration_ms(-1.0, 0.0, "x").is_err());
}

#[test]
fn the_backoffs_reach_the_policy_and_a_max_below_the_backoff_is_kept() {
    let policy = reconnect_policy(None, None, Some(50.0), Some(400.0)).unwrap();
    assert_eq!(policy.backoff.as_millis(), 50);
    assert_eq!(policy.max_backoff.as_millis(), 400);

    let policy = reconnect_policy(None, None, Some(5000.0), Some(100.0)).unwrap();
    assert_eq!(policy.backoff.as_millis(), 5000);
    assert_eq!(policy.max_backoff.as_millis(), 100);

    let default = reconnect_policy(None, None, None, None).unwrap();
    assert_eq!(default.backoff.as_millis(), 200);
    assert_eq!(default.max_backoff.as_millis(), 2000);
}

#[test]
fn a_backoff_must_be_a_millisecond_to_a_year() {
    let year = 365.0 * 24.0 * 3600.0 * 1000.0;
    for bad in [0.0, 0.5, -1.0, year + 1.0, f64::NAN, f64::INFINITY] {
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

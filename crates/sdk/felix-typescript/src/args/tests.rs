use napi::bindgen_prelude::BigInt;

use super::{duration_ms, u64_of};

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

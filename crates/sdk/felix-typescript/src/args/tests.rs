use napi::bindgen_prelude::BigInt;

use super::u64_of;

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

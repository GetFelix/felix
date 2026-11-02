use bytes::Bytes;

use super::*;

#[test]
fn accepts_what_the_specification_allows() {
    for ok in [
        "",
        "a",
        "topic/with/levels",
        "é€😀日本語",
        "\u{1}\u{7f}",
        "\u{FFFF}",
    ] {
        assert_eq!(Utf8::try_from(ok).unwrap().as_str(), ok);
    }
}

#[test]
fn refuses_what_it_forbids() {
    assert_eq!(
        Utf8::new(Bytes::from_static(&[0xFF])),
        Err(StringError::InvalidUtf8)
    );
    // Overlong NUL, a surrogate, and a truncated sequence.
    for bad in [&[0xC0, 0x80][..], &[0xED, 0xA0, 0x80], &[0xE2, 0x82]] {
        assert_eq!(
            Utf8::new(Bytes::copy_from_slice(bad)),
            Err(StringError::InvalidUtf8)
        );
    }
    assert_eq!(Utf8::try_from("a\0"), Err(StringError::ContainsNull));
    assert_eq!(
        Utf8::try_from("x".repeat(65_536)),
        Err(StringError::TooLong)
    );
    assert!(Utf8::try_from("x".repeat(65_535)).is_ok());
}

#[test]
fn compares_with_str() {
    let s = Utf8::try_from("abc").unwrap();
    assert_eq!(s, "abc");
    assert!(s == *"abc");
    assert_eq!(format!("{s} {s:?}"), "abc \"abc\"");
    assert_eq!(s.len(), 3);
    assert!(!s.is_empty());
    assert_eq!(s.clone().into_bytes(), Bytes::from_static(b"abc"));
}

use super::*;

#[test]
fn ordinary_names_are_accepted() {
    for name in [
        "t1",
        "tenant-a",
        "payments",
        "orders.v2",
        "user_events",
        "9lives",
    ] {
        assert!(validate_identifier("stream", name).is_ok(), "{name}");
    }
    assert!(validate_identifier("stream", &"a".repeat(MAX_IDENTIFIER_LEN)).is_ok());
}

/// Every character that means something in an RBAC object, a URL or a path.
#[test]
fn names_that_would_change_meaning_elsewhere_are_refused() {
    for name in [
        "", "*", "a*", "a/b", "a:b", ".", "..", ".hidden", "-flag", "a b", "a%2Fb", "a#b", "a?b",
        "naïve",
    ] {
        assert!(validate_identifier("stream", name).is_err(), "{name:?}");
    }
    assert!(validate_identifier("stream", &"a".repeat(MAX_IDENTIFIER_LEN + 1)).is_err());
}

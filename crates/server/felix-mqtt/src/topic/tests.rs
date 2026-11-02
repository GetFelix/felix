use super::*;

#[test]
fn a_plain_name_is_valid() {
    for name in [
        "a",
        "plant/line1/temp",
        "/leading",
        "trailing/",
        "$SYS/x",
        "päivä/ä",
    ] {
        assert_eq!(check_topic_name(name), Ok(()), "{name}");
    }
    assert_eq!(check_topic_name(&"a".repeat(MAX_TOPIC_NAME_BYTES)), Ok(()));
}

#[test]
fn a_name_that_cannot_be_published_to_is_refused() {
    assert_eq!(check_topic_name(""), Err(InvalidTopicName::Empty));
    assert_eq!(check_topic_name("a/+/b"), Err(InvalidTopicName::Wildcard));
    assert_eq!(check_topic_name("a/#"), Err(InvalidTopicName::Wildcard));
    assert_eq!(check_topic_name("a\0b"), Err(InvalidTopicName::Nul));
    assert_eq!(
        check_topic_name(&"a".repeat(MAX_TOPIC_NAME_BYTES + 1)),
        Err(InvalidTopicName::TooLong)
    );
}

#[test]
fn the_length_is_in_bytes() {
    // 32768 two-byte characters are 65536 bytes.
    assert_eq!(
        check_topic_name(&"é".repeat(32_768)),
        Err(InvalidTopicName::TooLong)
    );
    assert_eq!(check_topic_name(&"é".repeat(32_767)), Ok(()));
}

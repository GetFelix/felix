use super::*;

const BOUNDS: TopicBounds = TopicBounds {
    max_bytes: 32,
    max_levels: 4,
};

fn filter(f: &str) -> Result<(), TopicError> {
    check_topic_filter(f, Version::V5, BOUNDS)
}

#[test]
fn topic_names() {
    assert_eq!(check_topic_name("a/b/c", BOUNDS), Ok(()));
    assert_eq!(check_topic_name("/", BOUNDS), Ok(()));
    assert_eq!(check_topic_name("$SYS/broker", BOUNDS), Ok(()));
    assert_eq!(check_topic_name("", BOUNDS), Err(TopicError::Empty));
    assert_eq!(check_topic_name("a/+", BOUNDS), Err(TopicError::Wildcard));
    assert_eq!(
        check_topic_name("a/b/c/d/e", BOUNDS),
        Err(TopicError::TooManyLevels { max: 4 })
    );
    assert_eq!(
        check_topic_name(&"a".repeat(33), BOUNDS),
        Err(TopicError::TooLong { max: 32 })
    );
    assert_eq!(check_topic_name("a/b/c/d", BOUNDS), Ok(()));
}

#[test]
fn filters_follow_the_wildcard_rules() {
    for ok in [
        "#", "+", "+/+", "a/+", "a/#", "+/a/#", "/", "/#", "$SYS/#", "a//b",
    ] {
        assert_eq!(filter(ok), Ok(()), "{ok}");
    }
    for bad in ["a#", "#/a", "a/#/b", "a+", "+a", "a/b+", "a/+b", "##", "#+"] {
        assert_eq!(filter(bad), Err(TopicError::BadWildcard), "{bad}");
    }
    assert_eq!(filter(""), Err(TopicError::Empty));
}

#[test]
fn shared_filters() {
    assert_eq!(split_shared_filter("$share/g/a/b"), Some(("g", "a/b")));
    assert_eq!(split_shared_filter("$share/g"), None);
    assert_eq!(split_shared_filter("a/b"), None);

    assert_eq!(filter("$share/g/a/+"), Ok(()));
    assert_eq!(filter("$share/g/#"), Ok(()));
    for bad in [
        "$share/",
        "$share/g",
        "$share//a",
        "$share/g+/a",
        "$share/#/a",
    ] {
        assert_eq!(filter(bad), Err(TopicError::BadShare), "{bad}");
    }
    assert_eq!(filter("$share/g/"), Err(TopicError::Empty));
    // The level bound counts the filter part, not the prefix.
    assert_eq!(filter("$share/g/a/b/c/d"), Ok(()));
    assert_eq!(
        filter("$share/g/a/b/c/d/e"),
        Err(TopicError::TooManyLevels { max: 4 })
    );
    // Before 5.0 the prefix is an ordinary topic.
    assert_eq!(
        check_topic_filter("$share/g", Version::V311, BOUNDS),
        Ok(())
    );
}

#[test]
fn discouraged_characters() {
    for bad in [
        "a\u{1}",
        "a\u{1F}",
        "a\u{7F}",
        "a\u{9F}",
        "a\u{FDD0}",
        "a\u{FDEF}",
        "a\u{FFFE}",
        "a\u{FFFF}",
        "a\u{1FFFE}",
        "a\u{10FFFF}",
    ] {
        assert_eq!(
            check_topic_name(bad, BOUNDS),
            Err(TopicError::DiscouragedCharacter),
            "{bad:?}"
        );
    }
    for ok in [
        "a b",
        "a\u{A0}",
        "é",
        "a\u{FDCF}",
        "a\u{FDF0}",
        "a\u{FFFD}",
        "a\u{10000}",
    ] {
        assert_eq!(check_topic_name(ok, BOUNDS), Ok(()), "{ok:?}");
    }
}

//! The config-file rules, pinned to what the files meant before the parser
//! changed: a file that loaded still loads to the same values, and a file
//! that was refused for these reasons is still refused.

use serde::Deserialize;

use super::from_str;

#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct Strict {
    flag: Option<bool>,
    count: Option<u64>,
    size: Option<usize>,
    name: Option<String>,
    list: Option<Vec<String>>,
}

#[derive(Debug, Default, PartialEq, Deserialize)]
struct Lenient {
    flag: Option<bool>,
}

fn strict(text: &str) -> Strict {
    from_str(text).unwrap_or_else(|err| panic!("{text:?}: {err}"))
}

#[test]
fn only_true_and_false_are_booleans() {
    for (text, want) in [
        ("flag: true", true),
        ("flag: false", false),
        ("flag: True", true),
        ("flag: FALSE", false),
    ] {
        assert_eq!(strict(text).flag, Some(want), "{text}");
    }
    for text in [
        "flag: yes",
        "flag: no",
        "flag: on",
        "flag: off",
        "flag: y",
        "flag: n",
        "flag: 1",
    ] {
        assert!(from_str::<Strict>(text).is_err(), "{text} was accepted");
    }
}

#[test]
fn yes_and_no_are_strings_where_a_string_is_wanted() {
    assert_eq!(strict("name: yes").name.as_deref(), Some("yes"));
    assert_eq!(strict("name: off").name.as_deref(), Some("off"));
    assert_eq!(strict("name: 0777").name.as_deref(), Some("0777"));
    assert_eq!(strict("name: 1e3").name.as_deref(), Some("1e3"));
}

#[test]
fn integers_are_decimal_hex_and_octal() {
    assert_eq!(strict("count: 1000").count, Some(1000));
    assert_eq!(strict("count: 0x10").count, Some(16));
    assert_eq!(strict("count: 0o17").count, Some(15));
    assert_eq!(strict("size: 18446744073709551615").size, Some(usize::MAX));
    // A leading zero is neither YAML 1.1 octal nor a guess at decimal.
    for text in [
        "count: 0777",
        "count: 1e3",
        "count: 1.5",
        "count: -1",
        "count: 1:30",
        "count: 18446744073709551616",
        "count: 64MiB",
        "count: 5s",
    ] {
        assert!(from_str::<Strict>(text).is_err(), "{text} was accepted");
    }
}

#[test]
fn null_and_empty_leave_a_setting_unset() {
    for text in ["count: ~", "count: null", "count:", "name: ~", "name:"] {
        assert_eq!(strict(text), Strict::default(), "{text}");
    }
}

#[test]
fn an_empty_or_comment_only_file_sets_nothing() {
    for text in ["", "\n", "# nothing here\n", "---\n", "{}"] {
        assert_eq!(strict(text), Strict::default(), "{text:?}");
    }
}

#[test]
fn unknown_and_duplicate_keys_are_refused() {
    assert!(from_str::<Strict>("cuont: 1").is_err());
    assert!(from_str::<Strict>("count: 1\ncount: 2").is_err());
}

/// `<<` merged nothing before, so a file relying on it would have been
/// refused (or ignored, without `deny_unknown_fields`). It stays that way
/// rather than starting to merge settings in.
#[test]
fn a_merge_key_is_an_ordinary_key() {
    assert!(from_str::<Strict>("<<: {count: 1}").is_err());
    assert_eq!(
        from_str::<Lenient>("<<: {flag: true}").expect("parse"),
        Lenient::default()
    );
}

#[test]
fn anchors_and_aliases_resolve() {
    let text = "name: &n broker-a\nlist: [*n, b]";
    assert_eq!(
        strict(text).list,
        Some(vec!["broker-a".to_string(), "b".to_string()])
    );
}

#[test]
fn more_than_one_document_is_refused() {
    assert!(from_str::<Strict>("count: 1\n---\ncount: 2\n").is_err());
}

#[test]
fn a_wrong_type_is_refused() {
    for text in [
        "count: abc",
        "count: [1]",
        "name: [a]",
        "list: a",
        "flag: {}",
        "- count: 1",
    ] {
        assert!(from_str::<Strict>(text).is_err(), "{text} was accepted");
    }
}

/// The file may hold a credential; an error must not quote it, whether it
/// is the bad value or a line near it.
#[test]
fn an_error_does_not_quote_the_file() {
    for (text, line) in [
        ("name: hunter2-secret\ncount: [1]\n", "line 2"),
        ("count: hunter2-secret\n", "line 1"),
        ("flag: hunter2-secret\n", "line 1"),
    ] {
        let err = from_str::<Strict>(text)
            .expect_err("wrong type")
            .to_string();
        assert!(!err.contains("hunter2"), "{err}");
        assert!(err.contains(line), "{err}");
    }
}

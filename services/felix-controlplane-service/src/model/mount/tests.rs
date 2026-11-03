use super::*;

fn key(stream: &str) -> StreamKey {
    StreamKey {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: stream.to_string(),
    }
}

#[test]
fn a_plain_prefix_is_valid() {
    for prefix in ["plant/", "plant", "a/b/c/", "päivä/", "x"] {
        assert_eq!(validate(prefix), Ok(()), "{prefix}");
    }
    assert_eq!(validate(&"a".repeat(MAX_MOUNT_BYTES)), Ok(()));
}

#[test]
fn a_prefix_that_no_topic_could_start_with_is_refused() {
    for prefix in [
        "",
        "plant/+/",
        "plant/#",
        "pl#ant",
        "$SYS/",
        "$",
        "pl\0ant",
        "pl\u{1}ant",
        "pl\u{7f}ant",
    ] {
        assert!(validate(prefix).is_err(), "{prefix:?}");
    }
    assert!(validate(&"a".repeat(MAX_MOUNT_BYTES + 1)).is_err());
}

#[test]
fn the_length_is_in_bytes() {
    // 513 two-byte characters are 1026 bytes.
    assert!(validate(&"é".repeat(513)).is_err());
    assert_eq!(validate(&"é".repeat(512)), Ok(()));
}

#[test]
fn a_dollar_inside_the_prefix_is_fine() {
    assert_eq!(validate("plant/$x/"), Ok(()));
}

#[test]
fn prefixes_overlap_when_either_begins_with_the_other() {
    assert!(overlaps("plant/", "plant/"));
    assert!(overlaps("plant/", "plant/line1/"));
    assert!(overlaps("plant/line1/", "plant/"));
    assert!(overlaps("plant", "plants/x"));
    assert!(!overlaps("plant/", "plants/"));
    assert!(!overlaps("a/", "b/"));
}

#[test]
fn a_stream_does_not_overlap_its_own_mount() {
    let mounted = [(key("a"), "plant/".to_string())];
    let iter = || mounted.iter().map(|(k, p)| (k, p.as_str()));
    assert!(find_overlap("plant/x/", &key("a"), iter()).is_none());
    let found = find_overlap("plant/x/", &key("b"), iter()).expect("overlap");
    assert_eq!(found.0, &key("a"));
}

#[test]
fn the_refusals_are_told_apart_from_other_conflicts() {
    let overlap = overlap("a/", &key("s"), "a/b/");
    let message = refusal(&overlap).expect("a mount refusal");
    assert!(
        message.contains("\"a/\"") && message.contains("n/s"),
        "{message}"
    );
    assert!(refusal(&needs_durable()).is_some());
    assert!(refusal(&crate::store::StoreError::Conflict("stream exists".into())).is_none());
}

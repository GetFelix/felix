use super::*;

fn change(value: Option<&[u8]>) -> CacheChange {
    CacheChange {
        key: "user-1".into(),
        value: value.map(bytes::Bytes::copy_from_slice),
        offset: 12,
        expires_at_millis: 0,
    }
}

#[test]
fn a_change_prints_as_key_and_value() {
    assert_eq!(
        change_line(false, "sessions", None, &change(Some(b"in"))),
        b"user-1\tin"
    );
    assert_eq!(
        change_line(false, "sessions", None, &change(None)),
        b"user-1\t(deleted)"
    );
}

#[test]
fn a_change_as_json_names_its_shard_and_offset() {
    let line = change_line(true, "sessions", Some(2), &change(Some(b"in")));
    let value: serde_json::Value = serde_json::from_slice(&line).expect("json");
    assert_eq!(
        value,
        serde_json::json!({
            "cache": "sessions", "shard": 2, "key": "user-1",
            "offset": 12, "deleted": false, "value": "in",
        })
    );
    let line = change_line(true, "sessions", None, &change(None));
    let value: serde_json::Value = serde_json::from_slice(&line).expect("json");
    assert_eq!(value["deleted"], true);
    assert!(value.get("value").is_none());
}

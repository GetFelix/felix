use super::*;

fn printer(format: FormatArg) -> Printer {
    Printer {
        out: Output { json: false },
        format,
        stream: "orders".into(),
    }
}

#[test]
fn raw_prints_the_payload_only() {
    assert_eq!(printer(FormatArg::Raw).line(Some(1), Some(7), b"hi"), b"hi");
}

#[test]
fn offsets_prefix_the_shard_and_offset() {
    assert_eq!(
        printer(FormatArg::Offsets).line(Some(1), Some(7), b"hi"),
        b"1\t7\thi"
    );
    assert_eq!(
        printer(FormatArg::Offsets).line(None, None, b"hi"),
        b"-\t-\thi"
    );
}

#[test]
fn json_is_one_object_per_message() {
    let line = printer(FormatArg::Json).line(Some(0), Some(3), b"hi");
    let value: serde_json::Value = serde_json::from_slice(&line).expect("json");
    assert_eq!(
        value,
        serde_json::json!({"stream": "orders", "shard": 0, "offset": 3, "payload": "hi"})
    );
    let line = printer(FormatArg::Json).line(Some(0), None, &[0xff]);
    let value: serde_json::Value = serde_json::from_slice(&line).expect("json");
    assert_eq!(value["payload_base64"], "/w==");
    assert_eq!(value["offset"], serde_json::Value::Null);
}

#[test]
fn from_maps_onto_start_positions() {
    assert_eq!(start_position(StartArg::Latest), None);
    assert_eq!(
        start_position(StartArg::Earliest),
        Some(StartPosition::Earliest)
    );
    assert_eq!(
        start_position(StartArg::Offset(9)),
        Some(StartPosition::Offset(9))
    );
}

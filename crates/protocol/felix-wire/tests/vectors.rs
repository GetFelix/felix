use bytes::Bytes;
use felix_wire::{AckMode, Frame, Message, binary};
use std::fs;

// Vectors are the cross-language source of truth, so the expected bytes are
// derived from the spec by hand rather than dumped from this encoder — that is
// what makes the comparison below a real check instead of a tautology.
//
// `kind` selects how a vector is validated. It is absent on the original vectors,
// which are all JSON `Message` frames with flags = 0.
#[test]
fn vectors_match_frame_encoding() {
    let dir = "tests/vectors";
    let mut checked = 0usize;
    for entry in fs::read_dir(dir).expect("read vectors dir") {
        let entry = entry.expect("entry");
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let contents = fs::read_to_string(&path).expect("read vector");
        let value: serde_json::Value = serde_json::from_str(&contents).expect("json");
        let payload_hex = value["payload_hex"].as_str().expect("payload_hex");
        let frame_hex = value["frame_hex"].as_str().expect("frame_hex");
        let payload = hex_to_bytes(payload_hex);
        let frame_expected = Bytes::from(hex_to_bytes(frame_hex));
        let kind = value["kind"].as_str().unwrap_or("json_message");
        checked += 1;

        // Every vector must frame identically, whatever its payload encoding.
        let flags = u16::try_from(value["flags"].as_u64().unwrap_or(0)).expect("flags fit u16");
        let frame = Frame::new(flags, payload.into()).expect("frame");
        assert_eq!(
            frame.encode().as_ref(),
            frame_expected.as_ref(),
            "frame mismatch for {path:?}"
        );

        let decoded = Frame::decode(frame_expected.clone()).expect("decode");
        assert_eq!(decoded.header.flags, flags, "flags mismatch for {path:?}");

        match kind {
            "json_message" => {
                let message = Message::decode(decoded).expect("message decode");
                let encoded = message.encode().expect("message encode");
                assert_eq!(
                    encoded.encode().as_ref(),
                    frame_expected.as_ref(),
                    "round trip mismatch for {path:?}"
                );
            }
            "binary_acked_publish_batch" => {
                let acked = binary::decode_acked_publish_batch(&decoded).expect("decode acked");
                assert_eq!(acked.request_id, value["request_id"].as_u64().unwrap());
                let expected_ack = match value["ack_mode"].as_str().unwrap() {
                    "per_message" => AckMode::PerMessage,
                    "per_batch" => AckMode::PerBatch,
                    other => panic!("unknown ack_mode {other} in {path:?}"),
                };
                assert_eq!(acked.ack, expected_ack);
                assert_eq!(acked.batch.tenant_id, value["tenant_id"].as_str().unwrap());
                assert_eq!(acked.batch.namespace, value["namespace"].as_str().unwrap());
                assert_eq!(acked.batch.stream, value["stream"].as_str().unwrap());
                let expected: Vec<Vec<u8>> = value["payloads_utf8"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| p.as_str().unwrap().as_bytes().to_vec())
                    .collect();
                let payloads: Vec<Vec<u8>> =
                    acked.batch.payloads.iter().map(|p| p.to_vec()).collect();
                assert_eq!(payloads, expected);
                let expected_key = value["key_utf8"].as_str();
                assert_eq!(
                    acked.batch.key.as_deref(),
                    expected_key.map(str::as_bytes),
                    "key mismatch for {path:?}"
                );

                let re_encoded = binary::encode_acked_publish_batch_bytes_keyed(
                    acked.request_id,
                    acked.ack,
                    acked.batch.key.as_deref(),
                    &acked.batch.tenant_id,
                    &acked.batch.namespace,
                    &acked.batch.stream,
                    &payloads,
                )
                .expect("re-encode acked");
                assert_eq!(
                    re_encoded.as_ref(),
                    frame_expected.as_ref(),
                    "round trip mismatch for {path:?}"
                );
            }
            "binary_publish_batch" => {
                let batch = binary::decode_publish_batch(&decoded).expect("decode batch");
                assert_eq!(batch.tenant_id, value["tenant_id"].as_str().unwrap());
                assert_eq!(batch.namespace, value["namespace"].as_str().unwrap());
                assert_eq!(batch.stream, value["stream"].as_str().unwrap());
                let expected_key = value["key_utf8"].as_str();
                assert_eq!(
                    batch.key.as_deref(),
                    expected_key.map(str::as_bytes),
                    "key mismatch for {path:?}"
                );
                let expected: Vec<Vec<u8>> = value["payloads_utf8"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| p.as_str().unwrap().as_bytes().to_vec())
                    .collect();
                let payloads: Vec<Vec<u8>> = batch.payloads.iter().map(|p| p.to_vec()).collect();
                assert_eq!(payloads, expected);

                let (re_encoded, _stats) = binary::encode_publish_batch_bytes_with_stats_keyed(
                    batch.key.as_deref(),
                    &batch.tenant_id,
                    &batch.namespace,
                    &batch.stream,
                    &payloads,
                )
                .expect("re-encode batch");
                assert_eq!(
                    re_encoded.as_ref(),
                    frame_expected.as_ref(),
                    "round trip mismatch for {path:?}"
                );
            }
            "binary_publish_ack" => {
                let ack = binary::decode_publish_ack(&decoded).expect("decode ack");
                assert_eq!(ack.request_id, value["request_id"].as_u64().unwrap());
                let expected_message = value["message"].as_str();
                assert_eq!(ack.error.as_deref(), expected_message);
                match value["status"].as_str().unwrap() {
                    "ok" => assert!(ack.error.is_none(), "ok vector carried an error"),
                    "error" => assert!(ack.error.is_some(), "error vector carried no message"),
                    other => panic!("unknown status {other} in {path:?}"),
                }

                let re_encoded =
                    binary::encode_publish_ack_bytes(ack.request_id, ack.error.as_deref())
                        .expect("re-encode ack");
                assert_eq!(
                    re_encoded.as_ref(),
                    frame_expected.as_ref(),
                    "round trip mismatch for {path:?}"
                );
            }
            other => panic!("unknown vector kind {other} in {path:?}"),
        }
    }
    // Guard against the loop silently matching nothing.
    assert!(
        checked >= 13,
        "expected all vectors to be checked, got {checked}"
    );
}

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    assert!(hex.len().is_multiple_of(2), "hex length must be even");
    hex.as_bytes()
        .chunks(2)
        .map(|pair| {
            let hi = from_hex_char(pair[0]) << 4;
            let lo = from_hex_char(pair[1]);
            hi | lo
        })
        .collect()
}

fn from_hex_char(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => panic!("invalid hex char"),
    }
}

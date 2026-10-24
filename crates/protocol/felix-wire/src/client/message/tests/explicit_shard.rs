//! `shard` on the writes that route by a key, and on `state_get`.

use bytes::Bytes;

use crate::{AckMode, Message};

fn writes(shard: Option<u32>) -> Vec<Message> {
    vec![
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "city".to_string(),
            payload: b"tick".to_vec(),
            key: Some(Bytes::from_static(b"d02")),
            shard,
            request_id: Some(1),
            ack: Some(AckMode::PerMessage),
        },
        Message::PublishBatch {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "city".to_string(),
            payloads: vec![b"tick".to_vec()],
            key: None,
            shard,
            request_id: Some(2),
            ack: Some(AckMode::PerBatch),
        },
        Message::PublishIdempotent {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "city".to_string(),
            payloads: vec![b"tick".to_vec()],
            key: None,
            shard,
            request_id: 3,
            producer_id: 9,
            sequence: 0,
        },
        Message::PublishIf {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "city".to_string(),
            payloads: vec![b"tick".to_vec()],
            key: Some(Bytes::from_static(b"d02")),
            shard,
            expected_offset: 4,
            request_id: 4,
        },
        Message::Commit {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "city".to_string(),
            entity_key: Bytes::from_static(b"d02"),
            event: Bytes::from_static(b"tick"),
            changes: Vec::new(),
            request_id: 5,
            expected_offset: Some(7),
            shard,
        },
        Message::StateGet {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "city".to_string(),
            entity_key: Bytes::new(),
            key: "district".to_string(),
            request_id: 6,
            shard,
        },
    ]
}

#[test]
fn a_named_shard_round_trips() {
    for shard in [Some(0), Some(2), Some(u32::MAX)] {
        for message in writes(shard) {
            let json = serde_json::to_string(&message).expect("json");
            assert!(json.contains("\"shard\":"), "{json}");
            let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
            assert_eq!(decoded, message);
        }
    }
}

/// Unset, the field is left out, so a new client sends an older broker the
/// frame it always did, and an older client's frame reads as `None`.
#[test]
fn no_shard_is_the_frame_an_older_peer_sends() {
    for message in writes(None) {
        let json = serde_json::to_string(&message).expect("json");
        assert!(!json.contains("shard"), "{json}");
        let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, message);
    }
    let older = r#"{"type":"commit","tenant_id":"t1","namespace":"ns","stream":"orders","entity_key":"aw==","event":"ZQ==","changes":[],"request_id":7}"#;
    let decoded: Message = serde_json::from_str(older).expect("decode");
    assert!(matches!(decoded, Message::Commit { shard: None, .. }));
}

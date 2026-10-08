use bytes::Bytes;

use crate::{Message, StateChange};

#[test]
fn commit_messages_round_trip() {
    for message in [
        Message::Commit {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            entity_key: Bytes::from_static(b"order-1"),
            event: Bytes::from_static(b"placed"),
            changes: vec![
                StateChange::Put {
                    key: "order-1".to_string(),
                    value: Bytes::from_static(b"placed"),
                },
                StateChange::Delete {
                    key: "cart-1".to_string(),
                },
            ],
            request_id: 7,
            expected_offset: None,
        },
        Message::CommitOk {
            request_id: 7,
            offset: 42,
        },
        Message::StateGet {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            entity_key: Bytes::from_static(b"order-1"),
            key: "order-1".to_string(),
            request_id: 8,
        },
        Message::StateValue {
            value: Some(Bytes::from_static(b"placed")),
            version: Some(42),
            as_of: Some(43),
            request_id: 8,
        },
        Message::StateValue {
            value: None,
            version: None,
            as_of: None,
            request_id: 9,
        },
    ] {
        let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, message);
    }
}

/// The change list is tagged by `op`, so a reader can tell a put from a
/// delete without guessing from which fields are present.
#[test]
fn a_state_change_names_its_operation() {
    let json = serde_json::to_string(&StateChange::Delete {
        key: "k".to_string(),
    })
    .expect("json");
    assert_eq!(json, r#"{"op":"delete","key":"k"}"#);
}

/// A conditional publish, an expected offset on a commit, and the refusal
/// round-trip.
#[test]
fn conditional_writes_round_trip() {
    for message in [
        Message::PublishIf {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "match".to_string(),
            payloads: vec![b"tick".to_vec(), b"tock".to_vec()],
            key: Some(Bytes::from_static(b"match-1")),
            expected_offset: 41,
            request_id: 3,
        },
        Message::PublishIf {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "match".to_string(),
            payloads: vec![b"tick".to_vec()],
            key: None,
            expected_offset: 0,
            request_id: 4,
        },
        Message::Commit {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            entity_key: Bytes::from_static(b"order-1"),
            event: Bytes::from_static(b"placed"),
            changes: Vec::new(),
            request_id: 5,
            expected_offset: Some(42),
        },
        Message::PublishRefused {
            request_id: 3,
            reason: crate::PublishRefusalReason::OffsetMismatch { tail: 44 },
            message: "expected 41".to_string(),
        },
    ] {
        let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, message);
    }
}

/// A commit without an expected offset is the frame every client sent before
/// the field existed, so an older broker reads it as it always did.
#[test]
fn a_commit_without_an_expected_offset_is_unchanged() {
    let message = Message::Commit {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        entity_key: Bytes::from_static(b"k"),
        event: Bytes::from_static(b"e"),
        changes: Vec::new(),
        request_id: 7,
        expected_offset: None,
    };
    let json = serde_json::to_string(&message).expect("json");
    assert!(!json.contains("expected_offset"), "{json}");
    assert_eq!(
        json,
        r#"{"type":"commit","tenant_id":"t1","namespace":"ns","stream":"orders","entity_key":"aw==","event":"ZQ==","changes":[],"request_id":7}"#
    );
}

/// The refusal names its reason and the tail, in the shape a client parses.
#[test]
fn an_offset_mismatch_carries_the_tail() {
    let json = serde_json::to_string(&crate::PublishRefusalReason::OffsetMismatch { tail: 9 })
        .expect("json");
    assert_eq!(json, r#"{"offset_mismatch":{"tail":9}}"#);
}

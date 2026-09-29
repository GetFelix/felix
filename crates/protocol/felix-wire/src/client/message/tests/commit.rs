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

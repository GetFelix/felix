use crate::{AckMode, Message};

#[test]
fn message_round_trip() {
    let message = Message::Publish {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "topic".to_string(),
        payload: b"payload".to_vec(),
        request_id: None,
        ack: None,
        key: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn message_error_round_trip() {
    let message = Message::error("oops");
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn message_all_variants_encode_decode() {
    // Test Subscribe message
    let message = Message::Subscribe {
        start: None,
        subscription_id: Some(42),
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream".to_string(),
        shard: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test Subscribed message
    let message = Message::Subscribed {
        subscription_id: 42,
        start_offset: None,
        live_offset: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test PublishOk message
    let message = Message::PublishOk {
        request_id: 123,
        offset: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test PublishError message
    let message = Message::publish_error(123, "error");
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test Ok message
    let message = Message::Ok;
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test EventStreamHello message
    let message = Message::EventStreamHello {
        subscription_id: 99,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn ack_mode_serialization() {
    // Test all AckMode variants serialize correctly
    let none = AckMode::None;
    let per_msg = AckMode::PerMessage;
    let per_batch = AckMode::PerBatch;

    // Just ensure they can be used in messages
    let msg = Message::Publish {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        payload: vec![1, 2, 3],
        request_id: Some(1),
        ack: Some(none),
        key: None,
    };
    assert!(msg.encode().is_ok());

    let msg2 = Message::Publish {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        payload: vec![1, 2, 3],
        request_id: Some(2),
        ack: Some(per_msg),
        key: None,
    };
    assert!(msg2.encode().is_ok());

    let msg3 = Message::PublishBatch {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        payloads: vec![vec![1, 2], vec![3, 4]],
        request_id: Some(3),
        ack: Some(per_batch),
        key: None,
    };
    assert!(msg3.encode().is_ok());
}

/// Without an offset a `PublishOk` is the frame an older broker sends, so an
/// old client reads it unchanged; with one, the offset round-trips.
#[test]
fn publish_ok_offset_is_optional_on_the_wire() {
    let plain = Message::PublishOk {
        request_id: 7,
        offset: None,
    };
    let frame = plain.clone().encode().expect("encode");
    assert_eq!(
        &frame.payload[..],
        br#"{"type":"publish_ok","request_id":7}"#
    );

    let placed = Message::PublishOk {
        request_id: 7,
        offset: Some(42),
    };
    let frame = placed.clone().encode().expect("encode");
    assert_eq!(
        &frame.payload[..],
        br#"{"type":"publish_ok","request_id":7,"offset":42}"#
    );
    assert_eq!(Message::decode(frame).expect("decode"), placed);
    assert_eq!(placed.without_ack_offset(), plain);
}

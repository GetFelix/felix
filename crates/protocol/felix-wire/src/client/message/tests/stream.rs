use crate::{AckMode, Message};

#[test]
fn message_event_variants() {
    // Test Event message
    let message = Message::Event {
        offset: None,
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream1".to_string(),
        payload: b"event data".to_vec(),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test EventBatch message
    let message = Message::EventBatch {
        base_offset: None,
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream1".to_string(),
        payloads: vec![b"event1".to_vec(), b"event2".to_vec()],
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test EventStreamHello
    let message = Message::EventStreamHello {
        subscription_id: 123,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

#[test]
fn message_publish_with_ack_modes() {
    // Test Publish with AckMode::None
    let message = Message::Publish {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream1".to_string(),
        payload: b"data".to_vec(),
        request_id: Some(1),
        ack: Some(AckMode::None),
        key: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test Publish with AckMode::PerMessage
    let message = Message::Publish {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream1".to_string(),
        payload: b"data".to_vec(),
        request_id: Some(2),
        ack: Some(AckMode::PerMessage),
        key: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test PublishBatch with AckMode::PerBatch
    let message = Message::PublishBatch {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream1".to_string(),
        payloads: vec![b"data1".to_vec(), b"data2".to_vec()],
        request_id: Some(3),
        ack: Some(AckMode::PerBatch),
        key: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

/// Without join offsets `subscribed` is the frame it always was, and an old
/// broker's frame decodes with none.
#[test]
fn subscribed_without_join_offsets_is_unchanged() {
    let plain = Message::Subscribed {
        subscription_id: 42,
        start_offset: None,
        live_offset: None,
    };
    assert_eq!(
        serde_json::to_string(&plain).expect("serialize"),
        r#"{"type":"subscribed","subscription_id":42}"#
    );
    let legacy: Message =
        serde_json::from_str(r#"{"type":"subscribed","subscription_id":42}"#).expect("decode");
    assert_eq!(legacy, plain);

    let joined = Message::Subscribed {
        subscription_id: 42,
        start_offset: Some(10),
        live_offset: Some(25),
    };
    let frame = joined.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), joined);
}

/// `shard_moved` round-trips, and its optional fields stay off the wire when
/// absent, so a move with no hint is exactly the fields every reader knows.
#[test]
fn shard_moved_round_trips_and_omits_absent_hints() {
    let full = Message::ShardMoved {
        subscription_id: 7,
        resume_from: Some(1234),
        node_id: Some("broker-b".to_string()),
        addr: Some("10.0.0.5:5000".to_string()),
        generation: 9,
    };
    let frame = full.encode().expect("encode");
    assert_eq!(frame.header.flags, 0, "a JSON frame carries no flag bits");
    let json = std::str::from_utf8(&frame.payload).expect("utf8");
    assert_eq!(
        json,
        r#"{"type":"shard_moved","subscription_id":7,"resume_from":1234,"node_id":"broker-b","addr":"10.0.0.5:5000","generation":9}"#
    );
    assert_eq!(Message::decode(frame).expect("decode"), full);

    let bare = Message::ShardMoved {
        subscription_id: 7,
        resume_from: None,
        node_id: None,
        addr: None,
        generation: 9,
    };
    let frame = bare.encode().expect("encode");
    let json = std::str::from_utf8(&frame.payload).expect("utf8");
    assert_eq!(
        json,
        r#"{"type":"shard_moved","subscription_id":7,"generation":9}"#
    );
    assert_eq!(Message::decode(frame).expect("decode"), bare);
}

/// `subscription_lagged` is a plain JSON frame naming where to resume.
#[test]
fn subscription_lagged_round_trips() {
    let lagged = Message::SubscriptionLagged {
        subscription_id: 7,
        resume_from: 1234,
    };
    let frame = lagged.encode().expect("encode");
    assert_eq!(frame.header.flags, 0, "a JSON frame carries no flag bits");
    assert_eq!(
        std::str::from_utf8(&frame.payload).expect("utf8"),
        r#"{"type":"subscription_lagged","subscription_id":7,"resume_from":1234}"#
    );
    assert_eq!(Message::decode(frame).expect("decode"), lagged);
}

#[test]
fn a_stream_read_round_trips() {
    let request = Message::StreamRead {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "matches".to_string(),
        shard: 2,
        from: 10,
        end: Some(20),
        max_records: 5,
        max_bytes: 4096,
        request_id: 7,
    };
    let decoded = Message::decode(request.encode().expect("encode")).expect("decode");
    assert_eq!(request, decoded);

    let answer = Message::StreamRecords {
        records: vec![
            crate::StreamRecord {
                offset: 10,
                payload: bytes::Bytes::from_static(b"one"),
                publisher: Some("alice".to_string()),
                timestamp_micros: 1_700_000_000_000_000,
            },
            crate::StreamRecord {
                offset: 12,
                payload: bytes::Bytes::from_static(b"two"),
                publisher: None,
                timestamp_micros: 1_700_000_000_000_001,
            },
        ],
        next_offset: 13,
        request_id: 7,
    };
    let decoded = Message::decode(answer.encode().expect("encode")).expect("decode");
    assert_eq!(answer, decoded);
}

/// The optional fields stay off the wire when unset, so the shortest request
/// is just the shard and where to start.
#[test]
fn a_stream_read_leaves_unset_bounds_out() {
    let request = Message::StreamRead {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "s".to_string(),
        shard: 0,
        from: 3,
        end: None,
        max_records: 0,
        max_bytes: 0,
        request_id: 1,
    };
    assert_eq!(
        serde_json::to_string(&request).expect("encode"),
        r#"{"type":"stream_read","tenant_id":"t1","namespace":"ns","stream":"s","shard":0,"from":3,"request_id":1}"#
    );
    let decoded: Message = serde_json::from_str(
        r#"{"type":"stream_read","tenant_id":"t1","namespace":"ns","stream":"s","shard":0,"from":3,"request_id":1}"#,
    )
    .expect("decode");
    assert_eq!(decoded, request);
}

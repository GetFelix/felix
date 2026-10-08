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
        queue_capacity: None,
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
        queue_capacity: None,
    };
    let frame = joined.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), joined);
}

/// A subscribe that does not ask for a queue capacity, and the answer to it,
/// are the frames every peer sent before the field existed. One that asks
/// round-trips the request and the granted value.
#[test]
fn subscribe_queue_capacity_is_absent_unless_asked_for() {
    let plain = Message::Subscribe {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
        stream: "s".to_string(),
        subscription_id: None,
        start: None,
        shard: None,
        queue_capacity: None,
    };
    let legacy = r#"{"type":"subscribe","tenant_id":"t","namespace":"ns","stream":"s"}"#;
    assert_eq!(serde_json::to_string(&plain).expect("serialize"), legacy);
    assert_eq!(
        serde_json::from_str::<Message>(legacy).expect("decode"),
        plain
    );

    let sized = Message::Subscribe {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
        stream: "s".to_string(),
        subscription_id: None,
        start: None,
        shard: None,
        queue_capacity: Some(4096),
    };
    let json = serde_json::to_string(&sized).expect("serialize");
    assert!(json.ends_with(r#","queue_capacity":4096}"#), "{json}");
    let frame = sized.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), sized);

    let granted = Message::Subscribed {
        subscription_id: 1,
        start_offset: None,
        live_offset: None,
        queue_capacity: Some(2048),
    };
    assert_eq!(
        serde_json::to_string(&granted).expect("serialize"),
        r#"{"type":"subscribed","subscription_id":1,"queue_capacity":2048}"#
    );
    let frame = granted.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), granted);
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

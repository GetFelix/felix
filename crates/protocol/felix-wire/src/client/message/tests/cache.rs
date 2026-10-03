use bytes::Bytes;

use crate::Message;

#[test]
fn message_cache_operations() {
    // Test CachePut
    let message = Message::CachePut {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "cache1".to_string(),
        key: "key1".to_string(),
        value: Bytes::from_static(b"value1"),
        request_id: Some(42),
        ttl_ms: Some(60000),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test CacheGet
    let message = Message::CacheGet {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "cache1".to_string(),
        key: "key1".to_string(),
        request_id: Some(42),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test the consumer-group messages
    let message = Message::GroupPoll {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "jobs".to_string(),
        shard: 3,
        group: "workers".to_string(),
        max_records: 32,
        wait_ms: 5_000,
        request_id: 42,
    };
    let frame = message.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), message);

    let message = Message::GroupRecords {
        records: vec![
            crate::GroupRecord {
                offset: 7,
                payload: Bytes::from_static(b"one"),
                attempts: 1,
                skipped_before: 0,
            },
            crate::GroupRecord {
                offset: 9,
                payload: Bytes::new(),
                attempts: 3,
                skipped_before: 1,
            },
        ],
        request_id: 42,
    };
    let frame = message.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), message);

    // An empty batch is an answer, not an error: nothing was available.
    let message = Message::GroupRecords {
        records: Vec::new(),
        request_id: 42,
    };
    let frame = message.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), message);

    for message in [
        Message::GroupAck {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 3,
            group: "workers".to_string(),
            offset: 11,
            request_id: 42,
        },
        Message::GroupNack {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 3,
            group: "workers".to_string(),
            offset: 11,
            request_id: 42,
        },
    ] {
        let frame = message.clone().encode().expect("encode");
        assert_eq!(Message::decode(frame).expect("decode"), message);
    }

    // Ack and nack differ on the wire, or a hand-back would finish the record.
    let ack = Message::GroupAck {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "jobs".to_string(),
        shard: 0,
        group: "g".to_string(),
        offset: 1,
        request_id: 1,
    };
    let nack = Message::GroupNack {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "jobs".to_string(),
        shard: 0,
        group: "g".to_string(),
        offset: 1,
        request_id: 1,
    };
    assert_ne!(
        ack.encode().expect("encode"),
        nack.encode().expect("encode"),
    );

    // A poll from a client that predates long-polling asks for no wait, which
    // is the behaviour every broker had before it.
    let legacy = r#"{"type":"group_poll","tenant_id":"t1","namespace":"ns",
        "stream":"jobs","shard":0,"group":"g","max_records":1,"request_id":1}"#;
    match serde_json::from_str::<Message>(legacy).expect("legacy poll") {
        Message::GroupPoll { wait_ms, .. } => assert_eq!(wait_ms, 0),
        other => panic!("expected a group poll, got {other:?}"),
    }

    // A record delivered by a broker that does not report attempts reads as
    // unknown rather than as a first attempt.
    let legacy = r#"{"offset":4,"payload":"YWJj"}"#;
    let record: crate::GroupRecord = serde_json::from_str(legacy).expect("legacy record");
    assert_eq!(record.attempts, 0);

    // Test CacheDelete
    let message = Message::CacheDelete {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "cache1".to_string(),
        key: "key1".to_string(),
        request_id: Some(42),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test CacheValue with value
    let message = Message::CacheValue {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "cache1".to_string(),
        key: "key1".to_string(),
        value: Some(Bytes::from_static(b"value1")),
        request_id: Some(42),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test CacheValue miss (no value)
    let message = Message::CacheValue {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "cache1".to_string(),
        key: "key1".to_string(),
        value: None,
        request_id: Some(42),
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);

    // Test CacheOk
    let message = Message::CacheOk { request_id: 42 };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

/// The watch messages survive an encode/decode round trip, and the optional
/// fields default the way an older peer's silence must be read.
#[test]
fn cache_watch_messages_round_trip() {
    let watch = Message::CacheWatch {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "sessions".to_string(),
        key: Some("user:42".to_string()),
        prefix: None,
        shard: None,
        from_offset: Some(7),
        retained: false,
        subscription_id: None,
    };
    let decoded = Message::decode(watch.encode().expect("encode")).expect("decode");
    assert_eq!(watch, decoded);

    let prefix_watch = Message::CacheWatch {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "sessions".to_string(),
        key: None,
        prefix: Some("user:".to_string()),
        shard: Some(3),
        from_offset: None,
        retained: false,
        subscription_id: Some(9),
    };
    let decoded = Message::decode(prefix_watch.encode().expect("encode")).expect("decode");
    assert_eq!(prefix_watch, decoded);

    let started = Message::CacheWatchStarted {
        subscription_id: 9,
        resume_offset: 12,
        resnapshot: true,
        retained_count: None,
    };
    let decoded = Message::decode(started.encode().expect("encode")).expect("decode");
    assert_eq!(started, decoded);

    let put = Message::CacheEvent {
        key: "user:42".to_string(),
        value: Some(Bytes::from_static(b"online")),
        offset: 12,
        expires_at_millis: 1_700_000_000_000,
    };
    let decoded = Message::decode(put.encode().expect("encode")).expect("decode");
    assert_eq!(put, decoded);

    // A delete carries no value, and the absent field must not appear on the
    // wire at all -- an old JSON reader sees exactly the fields it knows.
    let delete = Message::CacheEvent {
        key: "user:42".to_string(),
        value: None,
        offset: 13,
        expires_at_millis: 0,
    };
    let frame = delete.encode().expect("encode");
    let json = std::str::from_utf8(&frame.payload).expect("utf8");
    assert!(
        !json.contains("value"),
        "absent value must be omitted: {json}"
    );
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(delete, decoded);

    let lagged = Message::CacheWatchLagged { resume_from: 40 };
    let decoded = Message::decode(lagged.encode().expect("encode")).expect("decode");
    assert_eq!(lagged, decoded);

    // A `resnapshot` the sender omitted reads as false: a watch that did not
    // ask to resume was never resnapshotted.
    let legacy = r#"{"type":"cache_watch_started","subscription_id":1,"resume_offset":0}"#;
    match serde_json::from_str::<Message>(legacy).expect("legacy started") {
        Message::CacheWatchStarted { resnapshot, .. } => assert!(!resnapshot),
        other => panic!("expected cache_watch_started, got {other:?}"),
    }
}

/// Retained delivery rides the existing watch messages as optional fields, so
/// a watch that does not use it stays byte-identical to one that predates it.
#[test]
fn retained_watch_fields_round_trip_and_default_off_the_wire() {
    let watch = Message::CacheWatch {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "presence".to_string(),
        key: None,
        prefix: Some("user:".to_string()),
        shard: None,
        from_offset: None,
        retained: true,
        subscription_id: None,
    };
    let decoded = Message::decode(watch.encode().expect("encode")).expect("decode");
    assert_eq!(watch, decoded);

    // An unretained watch must not carry the field at all: an old broker sees
    // exactly the frame an old client would have sent.
    let plain = Message::CacheWatch {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "presence".to_string(),
        key: Some("k".to_string()),
        prefix: None,
        shard: None,
        from_offset: None,
        retained: false,
        subscription_id: None,
    };
    let frame = plain.encode().expect("encode");
    let json = std::str::from_utf8(&frame.payload).expect("utf8");
    assert!(
        !json.contains("retained"),
        "an unset flag must stay off the wire: {json}"
    );

    // A frame that predates the field reads as unretained.
    let legacy = r#"{"type":"cache_watch","tenant_id":"t1","namespace":"ns",
        "cache":"presence","key":"k"}"#;
    match serde_json::from_str::<Message>(legacy).expect("legacy watch") {
        Message::CacheWatch { retained, .. } => assert!(!retained),
        other => panic!("expected cache_watch, got {other:?}"),
    }

    // `Some(0)` is the "no retained value" signal, distinct from absent.
    let started = Message::CacheWatchStarted {
        subscription_id: 3,
        resume_offset: 8,
        resnapshot: false,
        retained_count: Some(0),
    };
    let decoded = Message::decode(started.encode().expect("encode")).expect("decode");
    assert_eq!(started, decoded);
    let legacy = r#"{"type":"cache_watch_started","subscription_id":1,"resume_offset":0}"#;
    match serde_json::from_str::<Message>(legacy).expect("legacy started") {
        Message::CacheWatchStarted { retained_count, .. } => assert_eq!(retained_count, None),
        other => panic!("expected cache_watch_started, got {other:?}"),
    }
}

/// The counter messages round trip, and their answers keep never-written and
/// zero apart.
#[test]
fn counter_messages_round_trip() {
    let add = Message::CounterAdd {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "metrics".to_string(),
        key: "page-views".to_string(),
        delta: -3,
        request_id: 9,
    };
    let decoded = Message::decode(add.encode().expect("encode")).expect("decode");
    assert_eq!(add, decoded);

    let get = Message::CounterGet {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "metrics".to_string(),
        key: "page-views".to_string(),
        request_id: 10,
    };
    let decoded = Message::decode(get.encode().expect("encode")).expect("decode");
    assert_eq!(get, decoded);

    // A sum of zero is a value; a counter never written has none, and the
    // absent field stays off the wire entirely.
    let zero = Message::CounterValue {
        value: Some(0),
        request_id: 10,
    };
    let decoded = Message::decode(zero.encode().expect("encode")).expect("decode");
    assert_eq!(zero, decoded);
    let missing = Message::CounterValue {
        value: None,
        request_id: 10,
    };
    let frame = missing.encode().expect("encode");
    let json = std::str::from_utf8(&frame.payload).expect("utf8");
    assert!(
        !json.contains("\"value\":"),
        "absent must be omitted: {json}"
    );
    assert_eq!(Message::decode(frame).expect("decode"), missing);
}

/// A group record that skipped nothing is the frame an older client always
/// got: the new field is left out rather than sent as zero.
#[test]
fn a_group_record_with_nothing_skipped_omits_the_field() {
    let record = crate::GroupRecord {
        offset: 7,
        payload: Bytes::from_static(b"one"),
        attempts: 1,
        skipped_before: 0,
    };
    let json = serde_json::to_string(&record).expect("encode");
    assert!(!json.contains("skipped_before"), "{json}");
    let skipped = crate::GroupRecord {
        skipped_before: 2,
        ..record
    };
    let json = serde_json::to_string(&skipped).expect("encode");
    assert!(json.contains("\"skipped_before\":2"), "{json}");
}

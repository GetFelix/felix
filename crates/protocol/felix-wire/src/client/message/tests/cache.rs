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
        consumer: None,
        reclaim: false,
        visibility_ms: 0,
    };
    let frame = message.encode().expect("encode");
    // Without a consumer the frame is the one an older broker always read.
    let text = std::str::from_utf8(&frame.payload)
        .expect("utf8")
        .to_string();
    assert!(
        !text.contains("consumer") && !text.contains("reclaim") && !text.contains("visibility"),
        "{text}"
    );
    assert_eq!(Message::decode(frame).expect("decode"), message);
    let named = Message::GroupPoll {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "jobs".to_string(),
        shard: 3,
        group: "workers".to_string(),
        max_records: 32,
        wait_ms: 0,
        request_id: 43,
        consumer: Some("snapshotter".to_string()),
        reclaim: true,
        visibility_ms: 0,
    };
    let frame = named.encode().expect("encode");
    assert_eq!(Message::decode(frame).expect("decode"), named);

    let message = Message::GroupRecords {
        records: vec![
            crate::GroupRecord {
                offset: 7,
                payload: Bytes::from_static(b"one"),
                attempts: 1,
                skipped_before: 0,
                publisher: None,
                timestamp_micros: None,
            },
            crate::GroupRecord {
                offset: 9,
                payload: Bytes::new(),
                attempts: 3,
                skipped_before: 1,
                publisher: None,
                timestamp_micros: None,
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
            delay_ms: 0,
            attempts: 0,
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
        delay_ms: 0,
        attempts: 0,
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
        version: None,
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
        version: None,
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
        publisher: None,
        timestamp_micros: None,
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

/// A group record without a time is byte for byte the record an older
/// client gets, and a time round-trips for one that asked.
#[test]
fn a_group_record_time_is_left_out_unless_set() {
    let record = crate::GroupRecord {
        offset: 7,
        payload: Bytes::from_static(b"one"),
        attempts: 1,
        skipped_before: 0,
        publisher: None,
        timestamp_micros: None,
    };
    assert_eq!(
        serde_json::to_string(&record).expect("encode"),
        r#"{"offset":7,"payload":"b25l","attempts":1}"#
    );
    let timed = crate::GroupRecord {
        timestamp_micros: Some(1_700_000_000_000_000),
        ..record
    };
    let json = serde_json::to_string(&timed).expect("encode");
    assert!(
        json.contains("\"timestamp_micros\":1700000000000000"),
        "{json}"
    );
    let back: crate::GroupRecord = serde_json::from_str(&json).expect("decode");
    assert_eq!(back, timed);
}

#[test]
fn offset_for_time_round_trips() {
    for message in [
        Message::OffsetForTime {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 1,
            at_micros: 1_700_000_000_000_000,
            request_id: 3,
        },
        Message::OffsetValue {
            offset: Some(12),
            request_id: 3,
        },
        Message::OffsetValue {
            offset: None,
            request_id: 4,
        },
    ] {
        let frame = message.encode().expect("encode");
        assert_eq!(Message::decode(frame).expect("decode"), message);
    }
    let json = serde_json::to_string(&Message::OffsetValue {
        offset: None,
        request_id: 4,
    })
    .unwrap();
    assert_eq!(json, r#"{"type":"offset_value","request_id":4}"#);
}

#[test]
fn group_admin_messages_round_trip() {
    for message in [
        Message::GroupSeek {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 2,
            group: "g".to_string(),
            start: crate::StartPosition::Offset(7),
            if_new: true,
            request_id: 1,
        },
        Message::GroupPosition {
            offset: 7,
            moved: true,
            request_id: 1,
        },
        Message::GroupDescribe {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 2,
            group: "g".to_string(),
            request_id: 2,
        },
        Message::GroupInfo {
            committed: Some(5),
            tail: 9,
            in_flight: 2,
            owed: 1,
            dead_letters: 0,
            request_id: 2,
        },
        Message::GroupDelete {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 2,
            group: "g".to_string(),
            request_id: 3,
        },
        Message::GroupDeleted {
            existed: false,
            request_id: 3,
        },
    ] {
        let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, message);
    }
}

/// The optional fields stay off the wire at their defaults.
#[test]
fn group_admin_defaults_are_left_out() {
    let seek = Message::GroupSeek {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "jobs".to_string(),
        shard: 0,
        group: "g".to_string(),
        start: crate::StartPosition::Latest,
        if_new: false,
        request_id: 1,
    };
    let json = String::from_utf8(seek.encode().expect("encode").payload.to_vec()).expect("utf8");
    assert!(!json.contains("if_new"), "{json}");
    assert!(json.contains(r#""start":"latest""#), "{json}");

    let info = Message::GroupInfo {
        committed: None,
        tail: 0,
        in_flight: 0,
        owed: 0,
        dead_letters: 0,
        request_id: 2,
    };
    let json = String::from_utf8(info.encode().expect("encode").payload.to_vec()).expect("utf8");
    assert!(!json.contains("committed"), "{json}");
}

#[test]
fn conditional_cache_messages_round_trip() {
    let messages = [
        Message::CachePutIf {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: "leases".to_string(),
            key: "endpoint-7".to_string(),
            value: Bytes::from_static(b"worker-2"),
            ttl_ms: Some(30_000),
            condition: crate::CacheCondition::Absent,
            request_id: 1,
        },
        Message::CachePutIf {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: "leases".to_string(),
            key: "endpoint-7".to_string(),
            value: Bytes::from_static(b"worker-2"),
            ttl_ms: None,
            condition: crate::CacheCondition::Version(41),
            request_id: 2,
        },
        Message::CacheDeleteIf {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: "leases".to_string(),
            key: "endpoint-7".to_string(),
            version: 41,
            request_id: 3,
        },
        Message::CacheConditionResult {
            applied: false,
            version: Some(41),
            request_id: 4,
        },
        Message::CacheConditionResult {
            applied: false,
            version: None,
            request_id: 5,
        },
    ];
    for message in messages {
        let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, message);
    }
}

/// The condition's JSON is what docs/protocol.md shows.
#[test]
fn a_condition_is_absent_or_a_version() {
    assert_eq!(
        serde_json::to_string(&crate::CacheCondition::Absent).unwrap(),
        "\"absent\""
    );
    assert_eq!(
        serde_json::to_string(&crate::CacheCondition::Version(9)).unwrap(),
        "{\"version\":9}"
    );
}

/// A get answered to a client that did not offer the bit carries no version,
/// so its frame is byte-identical to the one an older broker sends.
#[test]
fn a_cache_value_without_a_version_omits_the_field() {
    let plain = Message::CacheValue {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "c".to_string(),
        key: "k".to_string(),
        value: Some(Bytes::from_static(b"v")),
        request_id: Some(1),
        version: None,
    };
    let json = serde_json::to_string(&plain).expect("encode");
    assert!(!json.contains("version"), "{json}");

    // And a frame from an older broker, which has no field, still decodes.
    let decoded: Message = serde_json::from_str(&json).expect("decode");
    assert_eq!(decoded, plain);

    let versioned = Message::CacheValue {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "c".to_string(),
        key: "k".to_string(),
        value: Some(Bytes::from_static(b"v")),
        request_id: Some(1),
        version: Some(12),
    };
    let json = serde_json::to_string(&versioned).expect("encode");
    assert!(json.contains("\"version\":12"), "{json}");
}

#[test]
fn group_claim_control_messages_round_trip() {
    for message in [
        Message::GroupExtend {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 1,
            group: "g".to_string(),
            offset: 9,
            attempts: 2,
            extend_ms: 60_000,
            request_id: 1,
        },
        Message::GroupExtended {
            visible_ms: 60_000,
            request_id: 1,
        },
        Message::GroupDeadLetter {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 1,
            group: "g".to_string(),
            offset: 9,
            request_id: 2,
        },
        Message::GroupNack {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 1,
            group: "g".to_string(),
            offset: 9,
            request_id: 3,
            delay_ms: 5_000,
            attempts: 2,
        },
        Message::GroupPoll {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "jobs".to_string(),
            shard: 1,
            group: "g".to_string(),
            max_records: 4,
            wait_ms: 0,
            request_id: 4,
            consumer: None,
            reclaim: false,
            visibility_ms: 120_000,
        },
    ] {
        let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, message);
    }
}

/// A nack and a poll that use neither new field encode to the bytes they
/// always did, and an older peer's frames read as no delay and the broker's
/// own visibility.
#[test]
fn claim_control_fields_are_left_out_at_their_defaults() {
    let nack = Message::GroupNack {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "jobs".to_string(),
        shard: 0,
        group: "g".to_string(),
        offset: 1,
        request_id: 1,
        delay_ms: 0,
        attempts: 0,
    };
    let json = String::from_utf8(nack.encode().expect("encode").payload.to_vec()).expect("utf8");
    assert_eq!(
        json,
        r#"{"type":"group_nack","tenant_id":"t1","namespace":"ns","stream":"jobs","shard":0,"group":"g","offset":1,"request_id":1}"#
    );

    let legacy = r#"{"type":"group_nack","tenant_id":"t1","namespace":"ns",
        "stream":"jobs","shard":0,"group":"g","offset":1,"request_id":1}"#;
    match serde_json::from_str::<Message>(legacy).expect("legacy nack") {
        Message::GroupNack {
            delay_ms, attempts, ..
        } => assert_eq!((delay_ms, attempts), (0, 0)),
        other => panic!("expected a group nack, got {other:?}"),
    }
    let legacy = r#"{"type":"group_poll","tenant_id":"t1","namespace":"ns",
        "stream":"jobs","shard":0,"group":"g","max_records":1,"request_id":1}"#;
    match serde_json::from_str::<Message>(legacy).expect("legacy poll") {
        Message::GroupPoll { visibility_ms, .. } => assert_eq!(visibility_ms, 0),
        other => panic!("expected a group poll, got {other:?}"),
    }
}

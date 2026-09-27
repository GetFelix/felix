use crate::{Message, PublishRefusalReason};

#[test]
fn producer_init_round_trips() {
    for message in [
        Message::ProducerInit { request_id: 7 },
        Message::ProducerInitOk {
            request_id: 7,
            producer_id: 0xdead_beef_cafe_f00d,
        },
    ] {
        let frame = message.encode().expect("encode");
        assert_eq!(Message::decode(frame).expect("decode"), message);
    }
}

/// The producer id and sequence are not optional: a batch without them is
/// an ordinary `publish_batch`, and the broker must never guess which.
#[test]
fn publish_idempotent_round_trips_and_names_its_producer() {
    let message = Message::PublishIdempotent {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        payloads: vec![b"a".to_vec(), b"b".to_vec()],
        key: Some(bytes::Bytes::from_static(b"k")),
        request_id: 3,
        producer_id: 42,
        sequence: 9,
    };
    let frame = message.encode().expect("encode");
    let json = std::str::from_utf8(&frame.payload).expect("utf8");
    assert!(json.contains("\"type\":\"publish_idempotent\""), "{json}");
    assert!(json.contains("\"producer_id\":42"), "{json}");
    assert!(json.contains("\"sequence\":9"), "{json}");
    assert_eq!(Message::decode(frame).expect("decode"), message);

    let without_them = "{\"type\":\"publish_idempotent\",\"tenant_id\":\"t1\",\
         \"namespace\":\"ns\",\"stream\":\"orders\",\"payloads\":[],\"request_id\":1}";
    assert!(
        serde_json::from_str::<Message>(without_them).is_err(),
        "a producer publish without a producer decoded"
    );
}

/// A key is optional and omitted when absent, like every other publish.
#[test]
fn publish_idempotent_omits_an_absent_key() {
    let message = Message::PublishIdempotent {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        payloads: vec![],
        key: None,
        request_id: 3,
        producer_id: 1,
        sequence: 0,
    };
    let frame = message.encode().expect("encode");
    let json = std::str::from_utf8(&frame.payload).expect("utf8");
    assert!(!json.contains("\"key\""), "{json}");
    assert_eq!(Message::decode(frame).expect("decode"), message);
}

/// Every reason survives the wire, and the one carrying data carries it.
#[test]
fn every_refusal_reason_round_trips() {
    for reason in [
        PublishRefusalReason::SequenceGap { expected: 12 },
        PublishRefusalReason::UnknownProducer,
        PublishRefusalReason::SequenceExpired,
        PublishRefusalReason::SequenceReused,
        PublishRefusalReason::NotLeader {
            node_id: "broker-b".to_string(),
            addr: Some("10.0.0.2:5000".to_string()),
        },
        PublishRefusalReason::NotLeader {
            node_id: "broker-c".to_string(),
            addr: None,
        },
    ] {
        let message = Message::PublishRefused {
            request_id: 5,
            reason: reason.clone(),
            message: "why".to_string(),
        };
        let frame = message.encode().expect("encode");
        assert_eq!(Message::decode(frame).expect("decode"), message);
    }
    let gap =
        serde_json::to_string(&PublishRefusalReason::SequenceGap { expected: 12 }).expect("json");
    assert_eq!(gap, "{\"sequence_gap\":{\"expected\":12}}");
    let plain = serde_json::to_string(&PublishRefusalReason::UnknownProducer).expect("json");
    assert_eq!(plain, "\"unknown_producer\"");
    let reused = serde_json::to_string(&PublishRefusalReason::SequenceReused).expect("json");
    assert_eq!(reused, "\"sequence_reused\"");
}

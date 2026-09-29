use super::*;

/// The names these serialize under are what a broker parses. Pinned here as
/// well as in the broker, because the two are compiled separately and a
/// rename on this side would otherwise be found by a cluster rather than by
/// a test.
#[test]
fn the_levels_serialize_under_the_names_brokers_read() {
    assert_eq!(
        serde_json::to_string(&ConsistencyLevel::Leader).expect("serialize"),
        "\"Leader\"",
    );
    assert_eq!(
        serde_json::to_string(&ConsistencyLevel::Quorum).expect("serialize"),
        "\"Quorum\"",
    );
}

/// A stream serializes its level as a plain field, which is what lets a
/// broker read it without understanding the rest of the model.
#[test]
fn a_stream_carries_its_level_as_a_field() {
    let stream = Stream {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        kind: StreamKind::Stream,
        shards: 1,
        replication_factor: 3,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Quorum,
        delivery: DeliveryGuarantee::AtLeastOnce,
        durable: true,
        region: None,
        routing: Default::default(),
    };
    let json: serde_json::Value = serde_json::to_value(&stream).expect("serialize");
    assert_eq!(json["consistency"], "Quorum");
}

/// A stream without a home region serializes exactly as it did before the
/// field existed, and one written before it reads back without a region.
#[test]
fn region_is_absent_unless_set() {
    let json = serde_json::json!({
        "tenant_id": "t1",
        "namespace": "ns",
        "stream": "orders",
        "kind": "Stream",
        "shards": 1,
        "retention": { "max_age_seconds": null, "max_size_bytes": null },
        "consistency": "Leader",
        "delivery": "AtMostOnce",
        "durable": true,
    });
    let stream: Stream = serde_json::from_value(json).expect("deserialize");
    assert_eq!(stream.region, None);
    let written = serde_json::to_value(&stream).expect("serialize");
    assert!(written.get("region").is_none(), "{written}");

    let homed = Stream {
        region: Some("eu".to_string()),
        ..stream
    };
    assert_eq!(
        serde_json::to_value(&homed).expect("serialize")["region"],
        "eu"
    );
}

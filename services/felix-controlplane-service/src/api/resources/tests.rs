use crate::api::streams::same_stream;
use crate::model::{
    ConsistencyLevel, DeliveryGuarantee, RetentionPolicy, Stream, StreamKind, StreamRouting,
};

fn stream(routing: StreamRouting) -> Stream {
    Stream {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        kind: StreamKind::Stream,
        shards: 2,
        replication_factor: 1,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtLeastOnce,
        durable: true,
        region: None,
        routing,
        incarnation: 0,
    }
}

/// A stream made as modulo before the fleet finalized jump hash, retried
/// after: the server's choice changed, the caller's request did not.
#[test]
fn routing_left_to_the_server_does_not_count() {
    let existing = stream(StreamRouting::Modulo);
    let retried = stream(StreamRouting::JumpHash);
    assert!(same_stream(&existing, &retried, None));
    assert!(!same_stream(
        &existing,
        &retried,
        Some(StreamRouting::JumpHash)
    ));
    assert!(same_stream(
        &existing,
        &stream(StreamRouting::Modulo),
        Some(StreamRouting::Modulo)
    ));
}

#[test]
fn any_other_setting_counts() {
    let existing = stream(StreamRouting::Modulo);
    let mut wanted = existing.clone();
    wanted.shards = 3;
    assert!(!same_stream(&existing, &wanted, None));
    let mut wanted = existing.clone();
    wanted.region = Some("eu".to_string());
    assert!(!same_stream(&existing, &wanted, None));
}

/// A retry draws a new incarnation; the stream it would create is the one
/// that exists.
#[test]
fn the_incarnation_does_not_count() {
    let existing = Stream {
        incarnation: 7,
        ..stream(StreamRouting::Modulo)
    };
    let retried = Stream {
        incarnation: 8,
        ..stream(StreamRouting::Modulo)
    };
    assert!(same_stream(&existing, &retried, None));
}

#[test]
fn a_new_stream_gets_an_incarnation_once_the_fleet_finalized_them() {
    let request = || {
        serde_json::from_value::<crate::api::types::StreamCreateRequest>(serde_json::json!({
            "stream": "s", "kind": "Stream", "shards": 1, "retention": {},
            "consistency": "Leader", "delivery": "AtLeastOnce", "durable": true
        }))
        .expect("request")
    };
    let create = |finalized| {
        crate::api::streams::new_stream("t".into(), "n".into(), request(), false, finalized)
            .expect("stream")
            .incarnation
    };
    assert_eq!(create(false), 0);
    let (first, second) = (create(true), create(true));
    assert!(first > 0 && first < 1 << 53, "{first}");
    assert_ne!(first, second, "each creation draws its own");
}

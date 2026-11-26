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
        mount: None,
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

/// **A create asking for more brokers a shard than are registered is
/// warned about, not refused**: brokers register after streams when a
/// cluster bootstraps.
#[test]
fn a_replication_factor_over_the_registered_brokers_warns() {
    let mut wide = stream(StreamRouting::Modulo);
    wide.replication_factor = 3;
    let warning = crate::api::streams::short_warning(&wide, 1).expect("warns");
    assert!(
        warning.contains("3") && warning.contains("1 broker is"),
        "{warning}"
    );
    assert!(crate::api::streams::short_warning(&wide, 3).is_none());
    assert!(crate::api::streams::short_warning(&wide, 5).is_none());
    wide.durable = false;
    assert!(crate::api::streams::short_warning(&wide, 1).is_none());
}

/// **A broker without durable storage does not count as a host for a durable
/// stream's copies**: placement never names it, so two brokers with disks
/// and one without are short of an RF 3 stream.
#[test]
fn a_diskless_broker_does_not_count_toward_the_replication_factor() {
    let node = |id: &str, diskless: bool| {
        let mut node = crate::store::contract::nodes::node(id, 7000 + id.len() as u16);
        node.spec.labels.clear();
        if diskless {
            node.spec.labels.insert(
                felix_common::membership::NODE_STORAGE_LABEL.to_string(),
                felix_common::membership::NODE_STORAGE_NONE.to_string(),
            );
        }
        node
    };
    let nodes = vec![node("a", false), node("bb", false), node("ccc", true)];
    assert_eq!(crate::api::streams::durable_hosts(&nodes), 2);
    let mut wide = stream(StreamRouting::Modulo);
    wide.replication_factor = 3;
    let hosts = crate::api::streams::durable_hosts(&nodes);
    assert!(crate::api::streams::short_warning(&wide, hosts).is_some());
}

/// An old client sees the stream's own fields: the warnings field is absent
/// when empty, and a response with it still reads as a stream.
#[test]
fn the_warnings_field_is_absent_when_empty() {
    use crate::api::types::StreamCreateResponse;
    let bare = StreamCreateResponse {
        stream: stream(StreamRouting::Modulo),
        warnings: Vec::new(),
    };
    let value = serde_json::to_value(&bare).expect("json");
    assert_eq!(value, serde_json::to_value(&bare.stream).expect("json"));
    assert!(value.get("warnings").is_none());

    let warned = StreamCreateResponse {
        warnings: vec!["short".to_string()],
        ..bare
    };
    let value = serde_json::to_value(&warned).expect("json");
    assert_eq!(value["warnings"][0], "short");
    let back: StreamCreateResponse = serde_json::from_value(value.clone()).expect("round trip");
    assert_eq!(back.warnings, warned.warnings);
    let as_stream: Stream = serde_json::from_value(value).expect("an old client reads it");
    assert_eq!(as_stream, warned.stream);
}

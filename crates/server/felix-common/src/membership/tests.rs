use super::*;

#[test]
fn a_lifecycle_this_build_does_not_know_reads_as_not_placeable() {
    // A newer control plane adding a state must not leave an older broker
    // guessing that it may keep serving.
    let parsed: NodeLifecycle = serde_json::from_str("\"quiescing\"").expect("parse");
    assert_eq!(parsed, NodeLifecycle::Unknown);
    assert!(!parsed.is_placeable());
}

#[test]
fn the_known_states_round_trip_as_the_wire_spells_them() {
    for (text, value) in [
        ("\"live\"", NodeLifecycle::Live),
        ("\"draining\"", NodeLifecycle::Draining),
        ("\"down\"", NodeLifecycle::Down),
        ("\"left\"", NodeLifecycle::Left),
    ] {
        assert_eq!(
            serde_json::from_str::<NodeLifecycle>(text).expect("parse"),
            value,
        );
        assert_eq!(serde_json::to_string(&value).expect("write"), text);
    }
}

/// A broker that never drains sends the bytes it always sent.
#[test]
fn a_report_that_is_not_drained_omits_the_field() {
    let status = ShardReplicaStatus {
        tenant_id: "t1".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
        kind: ShardKind::Stream,
        generation: 1,
        caught_up: Vec::new(),
        replica_offsets: Vec::new(),
        drained: false,
        leader_offset: None,
        halted: Vec::new(),
        replica_counter_offsets: Vec::new(),
        stream_incarnation: 0,
        by_round: false,
        replica_incarnations: BTreeMap::new(),
        replica_storage_ids: BTreeMap::new(),
    };
    let json = serde_json::to_string(&status).expect("write");
    assert!(!json.contains("drained"), "{json}");
    assert!(!json.contains("by_round"), "{json}");
    assert!(!json.contains("incarnation"), "{json}");
    // Nor does one with nothing halted, or a stream's.
    assert!(!json.contains("halted"), "{json}");
    assert!(!json.contains("counter"), "{json}");
    let parsed: ShardReplicaStatus = serde_json::from_str(&json).expect("read");
    assert!(!parsed.drained);
}

/// **A report without storage ids is today's JSON, byte for byte**, and a
/// report from a broker that predates them reads as having none.
#[test]
fn a_report_without_storage_ids_is_unchanged() {
    let status = ShardReplicaStatus {
        tenant_id: "t1".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
        kind: ShardKind::Stream,
        generation: 1,
        caught_up: vec!["broker-b".into()],
        replica_offsets: Vec::new(),
        drained: false,
        leader_offset: Some(3),
        halted: Vec::new(),
        replica_counter_offsets: Vec::new(),
        stream_incarnation: 0,
        by_round: false,
        replica_incarnations: BTreeMap::new(),
        replica_storage_ids: BTreeMap::new(),
    };
    let today = r#"{"tenant_id":"t1","namespace":"ns","stream":"orders","shard":0,"kind":"stream","generation":1,"caught_up":["broker-b"],"replica_offsets":[],"leader_offset":3}"#;
    assert_eq!(serde_json::to_string(&status).expect("write"), today);
    let parsed: ShardReplicaStatus = serde_json::from_str(today).expect("read");
    assert!(parsed.replica_storage_ids.is_empty());

    let with = ShardReplicaStatus {
        replica_storage_ids: BTreeMap::from([("broker-b".into(), "ab".repeat(16))]),
        ..status
    };
    let json = serde_json::to_string(&with).expect("write");
    assert!(
        json.ends_with(&format!(
            r#""replica_storage_ids":{{"broker-b":"{}"}}}}"#,
            "ab".repeat(16)
        )),
        "{json}"
    );
}

#[test]
fn only_live_and_draining_are_placeable() {
    assert!(NodeLifecycle::Live.is_placeable());
    assert!(NodeLifecycle::Draining.is_placeable());
    assert!(!NodeLifecycle::Down.is_placeable());
    assert!(!NodeLifecycle::Left.is_placeable());
}

#[test]
fn a_report_round_trips() {
    let report = ReplicaStatusRequest {
        incarnation: 3,
        shards: vec![ShardReplicaStatus {
            tenant_id: "t1".into(),
            namespace: "ns".into(),
            stream: "orders".into(),
            shard: 0,
            kind: ShardKind::Cache,
            generation: 7,
            caught_up: vec!["broker-b".into()],
            replica_offsets: vec![ReplicaOffset {
                node_id: "broker-b".into(),
                durable_offset: 42,
            }],
            drained: true,
            leader_offset: None,
            halted: vec![HaltedReplicaStatus {
                node_id: "b".into(),
                reason: "diverged".into(),
            }],
            replica_counter_offsets: vec![ReplicaOffset {
                node_id: "broker-b".into(),
                durable_offset: 9,
            }],
            stream_incarnation: 5,
            by_round: true,
            replica_incarnations: BTreeMap::new(),
            replica_storage_ids: BTreeMap::from([(
                "broker-b".into(),
                "ab000000000000000000000000000000".into(),
            )]),
        }],
    };
    let json = serde_json::to_string(&report).expect("write");
    assert_eq!(
        serde_json::from_str::<ReplicaStatusRequest>(&json).expect("read"),
        report,
    );
}

/// A report from a broker predating cache placement omits `kind`, and must
/// still be read as a stream rather than rejected.
#[test]
fn an_older_report_without_a_kind_reads_as_a_stream() {
    let older = r#"{"incarnation":0,"shards":[{"tenant_id":"t1","namespace":"ns",
        "stream":"orders","shard":0,"generation":1,"caught_up":[]}]}"#;
    let parsed: ReplicaStatusRequest = serde_json::from_str(older).expect("parse");
    assert_eq!(parsed.shards[0].kind, ShardKind::Stream);
    assert!(parsed.shards[0].replica_offsets.is_empty());
    // And as a leader on its lease, so placement waits the lease out.
    assert!(!parsed.shards[0].by_round);
}

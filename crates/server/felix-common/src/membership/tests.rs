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
    };
    let json = serde_json::to_string(&status).expect("write");
    assert!(!json.contains("drained"), "{json}");
    // Nor does one with nothing halted.
    assert!(!json.contains("halted"), "{json}");
    let parsed: ShardReplicaStatus = serde_json::from_str(&json).expect("read");
    assert!(!parsed.drained);
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
}

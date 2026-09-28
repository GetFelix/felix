//! The parts of a backup point that do not need a cluster: finding a broker,
//! matching its answer to the assignments, and noticing leadership move.

use super::*;

fn assignment(stream: &str, shard: u32, leader: &str, generation: u64) -> Assignment {
    Assignment {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard,
        kind: "stream".to_string(),
        leader: leader.to_string(),
        generation,
    }
}

fn answer(body: serde_json::Value) -> BrokerOffsets {
    serde_json::from_value(body).expect("broker answer")
}

fn node(id: &str, client: Option<&str>, internal: &str) -> NodeAddress {
    NodeAddress {
        node_id: id.to_string(),
        client_addr: client.map(str::to_string),
        advertise_addr: internal.to_string(),
    }
}

#[test]
fn a_broker_is_reached_on_its_client_host_at_the_metrics_port() {
    let nodes = vec![
        node("a", Some("10.0.0.1:5000"), "10.1.0.1:6000"),
        node("b", None, "broker-b.internal:6000"),
        node("c", Some("[fd00::3]:5000"), "[fd00::3]:6000"),
    ];
    let overrides = HashMap::from([("d".to_string(), "http://proxy:9000".to_string())]);
    let url = |id| broker_url(id, &nodes, &overrides, 8080);

    assert_eq!(url("a").expect("a"), "http://10.0.0.1:8080");
    // No client address: the internal listener's host is the machine.
    assert_eq!(url("b").expect("b"), "http://broker-b.internal:8080");
    assert_eq!(url("c").expect("c"), "http://[fd00::3]:8080");
    assert_eq!(url("d").expect("d"), "http://proxy:9000");
    assert!(url("e").is_err(), "an unknown leader has no address");
}

#[test]
fn every_assigned_shard_at_its_generation_makes_a_point() {
    let assigned = [
        assignment("orders", 0, "a", 3),
        assignment("orders", 1, "a", 2),
    ];
    let refs: Vec<&Assignment> = assigned.iter().collect();
    let offsets = answer(serde_json::json!({
        "node_id": "a",
        "shards": [
            { "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 0, "kind": "stream",
              "generation": 3, "logs": { "records": 41, "group_cursors": 2, "group_dead_letters": 0 } },
            { "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 1, "kind": "stream",
              "generation": 2, "logs": { "records": 7 } }
        ],
        "skipped": []
    }));

    let shards = match_leader("a", &refs, &offsets).expect("complete");
    assert_eq!(shards.len(), 2);
    assert_eq!(shards[0].logs.records, 41);
    assert_eq!(shards[0].logs.group_cursors, Some(2));
    assert_eq!(shards[1].leader, "a");
    assert_eq!(shards[1].generation, 2);
}

#[test]
fn a_shard_at_another_generation_or_without_an_answer_is_missing() {
    let assigned = [
        assignment("orders", 0, "a", 4),
        assignment("orders", 1, "a", 2),
        assignment("orders", 2, "a", 2),
        assignment("scratch", 0, "a", 1),
    ];
    let refs: Vec<&Assignment> = assigned.iter().collect();
    let offsets = answer(serde_json::json!({
        "shards": [
            { "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 0, "kind": "stream",
              "generation": 3, "logs": { "records": 41 } }
        ],
        "skipped": [
            { "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 1, "kind": "stream",
              "reason": "settling" },
            { "tenant_id": "t", "namespace": "ns", "name": "scratch", "shard": 0, "kind": "stream",
              "reason": "not_durable" }
        ]
    }));

    let missing = match_leader("a", &refs, &offsets).expect_err("incomplete");
    assert_eq!(missing.len(), 3, "{missing:?}");
    assert!(
        missing[0].contains("generation 3 rather than 4"),
        "{missing:?}"
    );
    assert!(missing[1].contains("settling"), "{missing:?}");
    assert!(missing[2].contains("not led there"), "{missing:?}");
}

#[test]
fn an_in_memory_stream_is_left_out_rather_than_missing() {
    let assigned = [assignment("scratch", 0, "a", 1)];
    let refs: Vec<&Assignment> = assigned.iter().collect();
    let offsets = answer(serde_json::json!({
        "shards": [],
        "skipped": [
            { "tenant_id": "t", "namespace": "ns", "name": "scratch", "shard": 0, "kind": "stream",
              "reason": "not_durable" }
        ]
    }));
    assert_eq!(
        match_leader("a", &refs, &offsets).expect("nothing to back up"),
        vec![]
    );
}

#[test]
fn a_new_leader_or_generation_between_the_reads_starts_the_collection_over() {
    let before = vec![
        assignment("orders", 0, "a", 3),
        assignment("orders", 1, "b", 2),
    ];
    assert!(!leadership_changed(&before, &before.clone()));

    let mut moved = before.clone();
    moved[1].leader = "c".to_string();
    assert!(leadership_changed(&before, &moved));

    let mut bumped = before.clone();
    bumped[0].generation = 4;
    assert!(leadership_changed(&before, &bumped));

    let mut added = before.clone();
    added.push(assignment("orders", 2, "a", 1));
    assert!(leadership_changed(&before, &added));
}

#[test]
fn the_manifest_round_trips_and_leaves_out_logs_a_shard_does_not_have() {
    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        name: "nightly".to_string(),
        taken_at_millis: 1_700_000_000_000,
        metadata_version: 12,
        shards: vec![PointShard {
            tenant_id: "t".to_string(),
            namespace: "ns".to_string(),
            name: "prices".to_string(),
            shard: 0,
            kind: "cache".to_string(),
            leader: "a".to_string(),
            generation: 5,
            logs: PointLogs {
                records: 9,
                group_cursors: None,
                group_dead_letters: None,
                counters: Some(3),
            },
        }],
    };
    let json = serde_json::to_value(&manifest).expect("json");
    assert_eq!(json["format_version"], 1);
    assert_eq!(
        json["shards"][0]["logs"],
        serde_json::json!({ "records": 9, "counters": 3 })
    );
    let back: Manifest = serde_json::from_value(json).expect("parse");
    assert_eq!(back, manifest);
    assert!(render_summary(&back).contains("t/ns/prices/0 (cache)"));
}

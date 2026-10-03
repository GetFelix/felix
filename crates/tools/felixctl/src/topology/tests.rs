use serde_json::json;

use super::*;

#[test]
fn owners_are_the_named_streams_or_caches_assignments() {
    let assignment = |name: &str, kind: Option<&str>, shard: u32| {
        let mut value = json!({
            "tenant_id": "t1", "namespace": "default", "stream": name,
            "shard": shard, "leader": "b1",
        });
        if let Some(kind) = kind {
            value["kind"] = kind.into();
        }
        value
    };
    let all = vec![
        assignment("orders", Some("stream"), 0),
        assignment("orders", None, 1),
        assignment("orders", Some("cache"), 0),
        assignment("other", Some("stream"), 0),
    ];
    let streams = shard_owners(all.clone(), "t1", "default", "orders", "stream");
    assert_eq!(streams.len(), 2, "an assignment with no kind is a stream's");
    let caches = shard_owners(all.clone(), "t1", "default", "orders", "cache");
    assert_eq!(caches.len(), 1);
    assert!(shard_owners(all, "t2", "default", "orders", "stream").is_empty());
}

#[test]
fn a_row_takes_the_owner_from_the_broker_and_replicas_from_the_control_plane() {
    let owner = ShardOwner {
        shard: 0,
        node_id: Some("b2".into()),
        addr: Some("10.0.0.2:5000".into()),
        generation: 7,
        unavailable: None,
    };
    let assignment = json!({
        "shard": 0, "leader": "b1", "replicas": ["b1", "b2"],
        "generation": 6, "state": "active",
    });
    let brokers = vec![("b1".to_string(), "10.0.0.1:5000".to_string())];

    let row = shard_row(0, Some(&owner), Some(&assignment), &brokers);
    assert_eq!(
        row["leader"], "b2",
        "the broker's answer is the fresher one"
    );
    assert_eq!(row["leader_addr"], "10.0.0.2:5000");
    assert_eq!(row["generation"], 7);
    assert_eq!(row["replicas"], json!(["b1", "b2"]));
    assert_eq!(row["state"], "active");

    // A broker too old to say falls back on the assignment.
    let row = shard_row(0, None, Some(&assignment), &brokers);
    assert_eq!(row["leader"], "b1");
    assert_eq!(row["leader_addr"], "10.0.0.1:5000");
    assert_eq!(row["generation"], 6);
}

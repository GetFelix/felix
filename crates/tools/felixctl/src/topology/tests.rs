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

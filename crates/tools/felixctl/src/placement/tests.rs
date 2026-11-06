use serde_json::json;

use super::*;

#[test]
fn moves_are_a_table_with_the_pause_above_it() {
    let response = json!({
        "paused": true,
        "items": [{
            "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0,
            "kind": "stream", "leader": "broker-a", "destination": "broker-b",
            "step": "staged", "reason": "operator", "generation": 4,
            "lag_records": 12, "started_at_millis": 1000,
            "caught_up": false, "drained": false,
        }],
    });
    assert_eq!(
        render_moves(&response),
        "placement is paused: it starts no moves of its own\n\
         SHARD           STEP    REASON    LEADER    DESTINATION  LAG  STARTED_MS\n\
         t1/ns/orders/0  staged  operator  broker-a  broker-b     12   1000"
    );
    assert_eq!(
        render_moves(&json!({ "paused": false, "items": [] })),
        "no moves in progress"
    );
}

#[test]
fn replication_lists_the_short_shards() {
    let response = json!({
        "under_replicated": 1,
        "items": [{
            "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0,
            "kind": "stream", "leader": "broker-a", "desired_replicas": 3,
            "current_replicas": 2, "under_replicated": true,
            "unavailable": ["broker-c"], "restoring": "broker-d",
        }, {
            "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 1,
            "kind": "stream", "leader": "broker-a", "desired_replicas": 3,
            "current_replicas": 2, "under_replicated": true,
            "halted": [{
                "node_id": "broker-b", "reason": "diverged", "generation": 4,
                "since_millis": 1000,
            }],
        }],
    });
    assert_eq!(
        render_replication(&response),
        "SHARD           LEADER    COPIES  UNAVAILABLE  HALTED               RESTORING\n\
         t1/ns/orders/0  broker-a  2/3     broker-c     -                    broker-d\n\
         t1/ns/orders/1  broker-a  2/3     -            broker-b (diverged)  -"
    );
    assert_eq!(
        render_replication(&json!({ "under_replicated": 0, "items": [] })),
        "every shard has its replication factor"
    );
}

#[test]
fn a_plan_says_what_each_shard_would_get() {
    let response = json!({
        "paused": false,
        "items": [
            {
                "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 1,
                "kind": "cache", "action": "stage",
                "assignment": { "leader": "broker-a", "successor": "broker-c" },
            },
            {
                "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 2,
                "kind": "stream", "action": "waiting", "reason": "waiting for a move slot",
            },
        ],
    });
    assert_eq!(
        render_plan(&response),
        "SHARD                   ACTION   DETAIL\n\
         t1/ns/orders/1 (cache)  stage    leader broker-a, moving to broker-c\n\
         t1/ns/orders/2          waiting  waiting for a move slot"
    );
    assert_eq!(
        render_plan(&json!({ "paused": true, "items": [] })),
        "placement is paused: it starts no moves of its own\nnothing to do"
    );
}

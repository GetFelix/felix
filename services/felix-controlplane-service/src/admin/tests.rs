use serde_json::json;

use super::*;

#[test]
fn a_shard_is_named_by_its_path() {
    assert_eq!(
        ShardPath::parse("t1/ns/orders/3", false).expect("parse"),
        ShardPath {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            name: "orders".to_string(),
            shard: 3,
            cache: false,
        }
    );
    assert_eq!(
        ShardPath::parse("t1/ns/sessions/0", true)
            .expect("parse")
            .kind(),
        "cache"
    );
    for bad in ["t1/ns/orders", "t1/ns/orders/x", "t1/ns/orders/0/extra"] {
        assert!(ShardPath::parse(bad, false).is_err(), "{bad}");
    }
}

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
         t1/ns/orders/0  staged  operator  broker-a  broker-b     12   1000\n"
    );
    assert_eq!(
        render_moves(&json!({ "paused": false, "items": [] })),
        "no moves in progress\n"
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
         t1/ns/orders/2          waiting  waiting for a move slot\n"
    );
}

#[test]
fn a_move_says_what_it_does_to_the_zone_spread() {
    let response = json!({
        "step": "stage",
        "dry_run": true,
        "zones_before": 3,
        "zones_after": 2,
        "assignment": {
            "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0,
            "kind": "stream", "leader": "broker-a", "successor": "broker-b", "generation": 4,
        },
    });
    assert_eq!(
        render_step(&response),
        "dry run, nothing written: stage: t1/ns/orders/0 leader broker-a generation 4, moving to broker-b\n\
         zones: 3 -> 2 (the shard's copies will span fewer zones)\n"
    );
}

/// Against a running API: every command reaches its endpoint, and a refusal
/// comes back as the API's code and message.
#[tokio::test]
async fn each_command_drives_the_api() {
    use std::sync::Arc;

    use crate::model::{ShardAssignment, ShardState};
    use crate::store::ControlPlaneStore;
    use crate::test_support::{app_state_ready, one_shard_cluster, shard_zero, token};

    let (store, keys) = one_shard_cluster().await;
    store
        .put_shard_assignment(ShardAssignment {
            key: shard_zero(),
            leader: "broker-x".to_string(),
            replicas: vec![],
            generation: 0,
            state: ShardState::Assigning,
            successor: None,
            joining: None,
            move_started_at_millis: None,
            move_reason: None,
        })
        .await
        .expect("assign");
    let app = crate::api::build_router(app_state_ready(Arc::clone(&store) as _));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move { axum::serve(listener, app).await });
    let operator = token(&keys, &["node.view:cluster:*", "node.manage:cluster:*"]);
    let admin = |words: &[&str]| {
        let mut args = vec![
            "--url".to_string(),
            url.clone(),
            "--token".to_string(),
            operator.clone(),
        ];
        args.extend(words.iter().map(|w| w.to_string()));
        run(args)
    };

    admin(&["pause"]).await.expect("pause");
    assert!(store.moves_paused().await.expect("read"));
    admin(&["move", "t1/ns/orders/0", "broker-y", "--dry-run"])
        .await
        .expect("dry run");
    let untouched = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(untouched.successor, None, "a dry run writes nothing");
    admin(&["move", "t1/ns/orders/0", "broker-y"])
        .await
        .expect("move");
    let moving = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(moving.successor.as_deref(), Some("broker-y"));
    admin(&["moves"]).await.expect("moves");
    admin(&["--json", "plan"]).await.expect("plan");
    admin(&["cancel", "t1/ns/orders/0"]).await.expect("cancel");
    let back = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(back.successor, None);
    admin(&["resume"]).await.expect("resume");
    assert!(!store.moves_paused().await.expect("read"));

    let refused = admin(&["move", "t1/ns/orders/0", "broker-z"])
        .await
        .expect_err("an unknown node");
    assert!(refused.to_string().contains("unknown_node"), "{refused:#}");
    assert!(admin(&["bogus"]).await.is_err());

    // The brokers here report no features, so a finalize is refused and a
    // dry run changes nothing.
    admin(&["features"]).await.expect("features");
    admin(&["features", "finalize", "x", "--dry-run"])
        .await
        .expect("a dry run answers either way");
    let refused = admin(&["features", "finalize", "x"])
        .await
        .expect_err("no broker supports x");
    assert!(refused.to_string().contains("409"), "{refused:#}");
    assert!(
        store
            .enabled_fleet_features()
            .await
            .expect("read")
            .is_empty()
    );
}

#[test]
fn fleet_features_render_as_lines() {
    let response = json!({
        "supported": ["a", "b"], "enabled": [], "serving_nodes": 3,
    });
    assert_eq!(
        render_features(&response),
        "serving brokers: 3\nsupported: a, b\nenabled:   -\n"
    );
    let refused = json!({
        "feature": "a", "dry_run": true, "enabled": false, "would_enable": false,
        "lacking": ["broker-2"], "serving_nodes": 3,
    });
    assert_eq!(
        render_finalize(&refused),
        "dry run: a cannot be finalized: broker-2 do not support it\n"
    );
}

use std::collections::HashMap;

use axum::extract::Query;
use axum::{Json, Router, routing::get};
use tokio::net::TcpListener;

use super::*;
use crate::test_support::{spawn_axum_with_shutdown, wait_for_listen};

/// Two pages, then nothing: the follower drains both in one round.
async fn feed(Query(query): Query<HashMap<String, String>>) -> Json<serde_json::Value> {
    let since: u64 = query.get("since").and_then(|s| s.parse().ok()).unwrap_or(0);
    Json(match since {
        0 => serde_json::json!({
            "items": [{"seq": 1, "tenant_id": "t1", "principal_id": "alice", "revoked_at": 100}],
            "next_seq": 2,
        }),
        2 => serde_json::json!({
            "items": [{"seq": 2, "tenant_id": "t1", "principal_id": "*", "revoked_at": 50}],
            "next_seq": 3,
        }),
        _ => serde_json::json!({"items": [], "next_seq": since}),
    })
}

#[tokio::test]
async fn revocations_from_the_feed_are_applied() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_stop, _server) = spawn_axum_with_shutdown(
        listener,
        Router::new().route("/v1/principal-revocations/changes", get(feed)),
    );
    wait_for_listen(addr).await.unwrap();

    let revocations = Arc::new(Revocations::default());
    let shutdown = CancellationToken::new();
    let follower = tokio::spawn(follow_revocations(
        Arc::clone(&revocations),
        format!("http://{addr}"),
        Duration::from_millis(10),
        None,
        shutdown.clone(),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !revocations.revokes("t1", "bob", None, 50) {
        assert!(tokio::time::Instant::now() < deadline, "feed not applied");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(revocations.revokes("t1", "alice", None, 100));
    assert!(!revocations.revokes("t1", "alice", None, 101));
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), follower)
        .await
        .expect("stops on shutdown")
        .unwrap();
}

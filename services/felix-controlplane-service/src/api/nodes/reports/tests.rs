//! Which reports wake placement, and that a woken pass acts on them.
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crate::test_support::{app_state_ready, one_shard_cluster, shard_zero, token};

/// Only the broker itself reports its floors, and a batch whose entries are
/// all skipped is still a 200 that raised nothing.
#[tokio::test]
async fn a_floor_batch_is_answered_with_how_many_it_raised() {
    let (store, keys) = one_shard_cluster().await;
    let state = app_state_ready(Arc::clone(&store) as _);
    let post = |perm: &'static str, floors: serde_json::Value| {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/nodes/broker-x/raft-floors")
            .header("authorization", format!("Bearer {}", token(&keys, &[perm])))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({ "floors": floors }).to_string(),
            ))
            .expect("request");
        let router = crate::api::build_router(state.clone());
        async move {
            let response = router.oneshot(request).await.expect("response");
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .expect("body");
            (
                status,
                serde_json::from_slice::<serde_json::Value>(&body).ok(),
            )
        }
    };
    let unassigned = serde_json::json!([{
        "key": shard_zero(),
        "incarnation": 0,
        "index": 5,
        "term": 1,
    }]);
    let (status, _) = post("node.manage:node:broker-y", unassigned.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    for floors in [serde_json::json!([]), unassigned] {
        let (status, body) = post("node.manage:node:broker-x", floors).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.expect("json")["raised"], 0);
    }

    // As many as a broker sends in one tick, and one more.
    let many = |n: usize| {
        serde_json::Value::Array(
            (0..n)
                .map(|i| {
                    serde_json::json!({
                        "key": shard_zero(),
                        "incarnation": 0,
                        "index": i,
                        "term": 1,
                    })
                })
                .collect(),
        )
    };
    let (status, _) = post("node.manage:node:broker-x", many(1024)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = post("node.manage:node:broker-x", many(1025)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body.expect("json")["code"], "validation_error");
}

/// **A floor report whose voters changed wakes placement (#3337).** The
/// shrink of a one-member Raft move waits on it, so it runs now rather than
/// a placement interval later; the same voters again wake nothing.
#[tokio::test]
async fn a_floor_report_with_new_voters_requests_a_pass() {
    let (store, keys) = one_shard_cluster().await;
    let state = app_state_ready(Arc::clone(&store) as _);
    let post = |voters: serde_json::Value| {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/nodes/broker-x/raft-floors")
            .header(
                "authorization",
                format!("Bearer {}", token(&keys, &["node.manage:node:broker-x"])),
            )
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({ "floors": [{
                    "key": shard_zero(),
                    "incarnation": 0,
                    "index": 5,
                    "term": 1,
                    "voters": voters,
                }] })
                .to_string(),
            ))
            .expect("request");
        crate::api::build_router(state.clone()).oneshot(request)
    };
    let wakes = Arc::clone(&state.placement_wakes);
    let woken = || tokio::time::timeout(Duration::from_millis(100), wakes.pass_requested());

    let one = serde_json::json!({ "broker-x": 7 });
    let two = serde_json::json!({ "broker-x": 7, "broker-y": 9 });
    for (voters, wakes_placement) in [(one.clone(), true), (one, false), (two, true)] {
        let response = post(voters.clone()).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(woken().await.is_ok(), wakes_placement, "{voters}");
    }
}

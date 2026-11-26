use axum::http::StatusCode;

use super::*;

/// Brokers poll every 2 s during a rolling upgrade, so an older leader must
/// read as a refusal, not a storage fault logged on every poll.
#[test]
fn an_older_leader_is_a_conflict() {
    let older = StoreError::Conflict("no plans here".to_string());
    assert_eq!(refused(older).status, StatusCode::CONFLICT);
    let fault = StoreError::Unexpected(anyhow::anyhow!("disk"));
    assert_eq!(refused(fault).status, StatusCode::INTERNAL_SERVER_ERROR);
}

/// Applying a plan writes epochs, so it takes the cluster operator's
/// `node.manage`, as making the plan does: a viewer or a broker's own
/// `node.manage` is refused before the plan is looked up.
#[tokio::test]
async fn applying_a_plan_takes_the_cluster_operator() {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use crate::api::build_router;
    use crate::test_support::{app_state_ready, one_shard_cluster, token};

    let (store, keys) = one_shard_cluster().await;
    let app = build_router(app_state_ready(Arc::clone(&store) as _));
    let call = |bearer: String| {
        let request = Request::post("/v1/raft/reform/plans/nope/apply")
            .header("authorization", format!("Bearer {bearer}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"accept_loss":[]}"#))
            .expect("request");
        let app = app.clone();
        async move { app.oneshot(request).await.expect("answer").status() }
    };
    for bearer in [
        token(&keys, &["node.view:cluster:*"]),
        token(&keys, &["node.manage:node:broker-x"]),
    ] {
        let status = call(bearer).await;
        assert!(
            matches!(status, StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED),
            "{status}"
        );
    }
    let operator = token(&keys, &["node.manage:cluster:*"]);
    assert_eq!(call(operator).await, StatusCode::CONFLICT, "no such plan");
}

/// A restore plan with a gap below the minimum is refused before any plan
/// is made, unless the operator accepts the short gap.
#[tokio::test]
async fn a_short_offset_gap_is_refused_unless_accepted() {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use crate::api::build_router;
    use crate::test_support::{app_state_ready, one_shard_cluster, token};

    let (store, keys) = one_shard_cluster().await;
    let app = build_router(app_state_ready(Arc::clone(&store) as _));
    let operator = token(&keys, &["node.manage:cluster:*"]);
    let call = |restore: serde_json::Value| {
        let body = serde_json::json!({ "scope": "cluster", "restore": restore });
        let request = Request::post("/v1/raft/reform/plans")
            .header("authorization", format!("Bearer {operator}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request");
        let app = app.clone();
        async move { app.oneshot(request).await.expect("answer").status() }
    };
    let restore = |gap: u64, accept: bool| {
        serde_json::json!({
            "point": "nightly", "taken_at_millis": 1, "shards": [],
            "offset_gap": gap, "accept_short_gap": accept,
        })
    };
    assert_eq!(call(restore(1 << 20, false)).await, StatusCode::BAD_REQUEST);
    assert_eq!(call(restore(1 << 20, true)).await, StatusCode::OK);
    assert_eq!(call(restore(1 << 40, false)).await, StatusCode::OK);
}

/// A broker asks for its own group's epoch only: a token for another node
/// is refused before the shard is looked at, and the node's own token
/// reaches the store, which answers an unknown shard as not found.
#[tokio::test]
async fn a_broker_starts_only_its_own_groups_epoch() {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use crate::api::build_router;
    use crate::test_support::{app_state_ready, one_shard_cluster, token};

    let (store, keys) = one_shard_cluster().await;
    let app = build_router(app_state_ready(Arc::clone(&store) as _));
    let body = serde_json::json!({
        "key": {
            "tenant_id": "t1", "namespace": "ns", "stream": "orders",
            "shard": 0, "kind": "stream"
        },
        "start": {
            "incarnation": 0, "expected_epoch": 0,
            "base": { "first_index": 1 },
            "voters": { "broker-a": 7 }
        }
    })
    .to_string();
    let call = |bearer: String| {
        let request = Request::post("/v1/nodes/broker-a/reform/alone")
            .header("authorization", format!("Bearer {bearer}"))
            .header("content-type", "application/json")
            .body(Body::from(body.clone()))
            .expect("request");
        let app = app.clone();
        async move { app.oneshot(request).await.expect("answer").status() }
    };
    for bearer in [
        token(&keys, &["node.view:cluster:*"]),
        token(&keys, &["node.manage:node:broker-b"]),
    ] {
        let status = call(bearer).await;
        assert!(
            matches!(status, StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED),
            "{status}"
        );
    }
    let own = token(&keys, &["node.manage:node:broker-a"]);
    assert_eq!(call(own).await, StatusCode::NOT_FOUND, "no such shard");
}

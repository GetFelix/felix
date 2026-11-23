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

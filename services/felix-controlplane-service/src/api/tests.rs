use std::sync::Arc;

use super::*;
use crate::store::StoreConfig;
use crate::store::memory::InMemoryStore;

fn test_state() -> AppState {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: crate::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(crate::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    });
    crate::test_support::app_state_ready(Arc::new(store))
}

#[tokio::test]
async fn ensure_tenant_exists_returns_not_found() {
    let state = test_state();
    let err = ensure_tenant_exists(&state, "missing").await.unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ensure_tenant_namespace_checks_both() {
    let state = test_state();
    let err = ensure_tenant_namespace(&state, "missing", "ns")
        .await
        .unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::NOT_FOUND);

    // Insert a tenant and namespace, then validate success.
    state
        .store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant".to_string(),
        })
        .await
        .expect("tenant");
    state
        .store
        .create_namespace(crate::model::Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect("namespace");

    ensure_tenant_namespace(&state, "t1", "default")
        .await
        .expect("exists");
}

/// One request through the router, answered with its status and JSON body.
async fn send(
    store: Arc<InMemoryStore>,
    method: &str,
    uri: &str,
    token: &str,
) -> (axum::http::StatusCode, serde_json::Value) {
    use tower::ServiceExt;
    let request = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(axum::body::Body::empty())
        .expect("request");
    let response = build_router(crate::test_support::app_state_ready(store))
        .oneshot(request)
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// **Declaring a directory dead answers each store refusal with its own
/// status**: 404 for a node never registered, 409 for one still serving or
/// with no storage id to name. Once declared, the listing shows it.
#[tokio::test]
async fn declaring_a_node_dead_maps_each_refusal_to_its_status() {
    use axum::http::StatusCode;

    use crate::model::NodeLifecycle;
    use crate::store::ControlPlaneStore;
    use crate::store::contract::nodes::node;

    let (store, keys) = crate::test_support::one_shard_cluster().await;
    let manage = crate::test_support::token(&keys, &["node.manage:cluster:*"]);
    let declare = |id: &str| format!("/v1/nodes/{id}/declare-dead");

    let (status, _) = send(store.clone(), "POST", &declare("nobody"), &manage).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(store.clone(), "POST", &declare("broker-x"), &manage).await;
    assert_eq!(status, StatusCode::CONFLICT, "a live node is serving");

    let mut no_disk = node("broker-z", 7700);
    no_disk.status.storage_id = None;
    store.register_node(no_disk).await.expect("register");
    store
        .set_node_lifecycle("broker-z", NodeLifecycle::Down)
        .await
        .expect("down");
    let (status, _) = send(store.clone(), "POST", &declare("broker-z"), &manage).await;
    assert_eq!(status, StatusCode::CONFLICT, "no storage id to name");

    let mut on_disk = node("broker-w", 7701);
    on_disk.status.storage_id = Some("a".repeat(32));
    store.register_node(on_disk).await.expect("register");
    store
        .set_node_lifecycle("broker-w", NodeLifecycle::Left)
        .await
        .expect("left");
    let (status, body) = send(store.clone(), "POST", &declare("broker-w"), &manage).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["node_id"], "broker-w");
    assert_eq!(body["storage_id"], "a".repeat(32));

    // Listing is a view, a permission of its own.
    let view = crate::test_support::token(&keys, &["node.view:cluster:*"]);
    let (status, body) = send(store.clone(), "GET", "/v1/fleet/declared-dead", &view).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        serde_json::json!([{ "node_id": "broker-w", "storage_id": "a".repeat(32) }])
    );
}

/// **Declaring a node dead needs `node.manage`**: it cannot be undone, so a
/// token that may only view nodes is refused and nothing is recorded.
#[tokio::test]
async fn declaring_a_node_dead_needs_node_manage() {
    use crate::store::ControlPlaneStore;

    let (store, keys) = crate::test_support::one_shard_cluster().await;
    let view = crate::test_support::token(&keys, &["node.view:cluster:*"]);
    let (status, _) = send(
        store.clone(),
        "POST",
        "/v1/nodes/broker-x/declare-dead",
        &view,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN);
    assert!(store.declared_dead().await.expect("dead").is_empty());
}

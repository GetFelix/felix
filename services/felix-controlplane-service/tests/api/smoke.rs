use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_bootstrap_router, build_router};
use felix_controlplane_service::auth::felix_token::TenantSigningKeys;
use felix_controlplane_service::auth::idp_registry::IdpIssuerConfig;
use felix_controlplane_service::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use felix_controlplane_service::model::{
    Cache, CacheChange, CacheKey, CachePatchRequest, Namespace, NamespaceChange, NamespaceKey,
    Node, NodeChange, NodeLifecycle, NodePatchRequest, ReplicaReport, ShardAssignment,
    ShardAssignmentChange, ShardKey, Stream, StreamChange, StreamKey, StreamPatchRequest, Tenant,
    TenantChange,
};
use felix_controlplane_service::store::{
    AuthStore, ChangeSet, ControlPlaneStore, Page, PageRequest, Snapshot, StoreError, StoreResult,
};
use tower::ServiceExt;

use crate::common::read_json;
use crate::common::{Credentials, json_request_as, request_as, seed_credentials};

struct Harness {
    app: axum::routing::RouterIntoService<axum::body::Body, ()>,
    store: Arc<felix_controlplane_service::store::memory::InMemoryStore>,
    credentials: Credentials,
}

impl Harness {
    fn operator(&self) -> String {
        self.credentials.operator()
    }

    fn admin(&self, tenant_id: &str) -> String {
        self.credentials.tenant_admin(tenant_id)
    }

    /// Create `tenant_id` through the API, as an operator, and bind the test
    /// keys to it so the admin tokens minted here verify.
    async fn create_tenant(&self, tenant_id: &str) {
        let create = json_request_as(
            "POST",
            "/v1/tenants",
            &self.operator(),
            serde_json::json!({
                "tenant_id": tenant_id,
                "display_name": "Tenant One"
            }),
        );
        let response = self.app.clone().oneshot(create).await.expect("tenant");
        assert_eq!(response.status(), StatusCode::CREATED);
        self.credentials.adopt(self.store.as_ref(), tenant_id).await;
    }
}

async fn harness(region_id: &str) -> Harness {
    let store = Arc::new(
        felix_controlplane_service::store::memory::InMemoryStore::new(
            felix_controlplane_service::store::StoreConfig {
                changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
                change_retention_max_rows: Some(
                    felix_controlplane_service::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS,
                ),
            },
        ),
    );
    let credentials = seed_credentials(store.as_ref()).await;
    let state = AppState {
        region: Region {
            region_id: region_id.to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::clone(&store)
            as Arc<dyn felix_controlplane_service::store::ControlPlaneAuthStore + Send + Sync>,
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    Harness {
        app: build_router(state).into_service(),
        store,
        credentials,
    }
}

#[tokio::test]
async fn streams_crud_and_changes_smoke() {
    let h = harness("local").await;
    let app = h.app.clone();
    let op = h.operator();
    let admin = h.admin("t1");

    h.create_tenant("t1").await;

    let create_namespace = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces",
        &admin,
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        &admin,
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(create).await.expect("create");
    assert_eq!(response.status(), StatusCode::CREATED);

    let list = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("list");
    let response = app.clone().oneshot(list).await.expect("list");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["items"].as_array().unwrap().len(), 1);

    let get = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams/orders")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("get");
    let response = app.clone().oneshot(get).await.expect("get");
    assert_eq!(response.status(), StatusCode::OK);

    let patch = json_request_as(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/streams/orders",
        &admin,
        serde_json::json!({
            "retention": { "max_age_seconds": 7200, "max_size_bytes": null }
        }),
    );
    let response = app.clone().oneshot(patch).await.expect("patch");
    assert_eq!(response.status(), StatusCode::OK);

    let changes = Request::builder()
        .uri("/v1/streams/changes?since=0")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("changes");
    let response = app.clone().oneshot(changes).await.expect("changes");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert!(!payload["items"].as_array().unwrap().is_empty());

    let delete = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/streams/orders")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn caches_crud_and_changes_smoke() {
    let h = harness("local").await;
    let app = h.app.clone();
    let op = h.operator();
    let admin = h.admin("t1");

    h.create_tenant("t1").await;

    let create_namespace = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces",
        &admin,
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        &admin,
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary Cache"
        }),
    );
    let response = app.clone().oneshot(create).await.expect("create");
    assert_eq!(response.status(), StatusCode::CREATED);

    let list = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("list");
    let response = app.clone().oneshot(list).await.expect("list");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["items"].as_array().unwrap().len(), 1);

    let get = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches/primary")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("get");
    let response = app.clone().oneshot(get).await.expect("get");
    assert_eq!(response.status(), StatusCode::OK);

    let patch = json_request_as(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/caches/primary",
        &admin,
        serde_json::json!({
            "display_name": "Primary Cache Updated"
        }),
    );
    let response = app.clone().oneshot(patch).await.expect("patch");
    assert_eq!(response.status(), StatusCode::OK);

    let changes = Request::builder()
        .uri("/v1/caches/changes?since=0")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("changes");
    let response = app.clone().oneshot(changes).await.expect("changes");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert!(!payload["items"].as_array().unwrap().is_empty());

    let delete = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/caches/primary")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn system_and_region_endpoints() {
    let h = harness("local").await;
    let app = h.app.clone();

    let info = Request::builder()
        .uri("/v1/system/info")
        .body(Body::empty())
        .expect("info");
    let response = app.clone().oneshot(info).await.expect("info");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["region_id"], "local");
    assert_eq!(payload["api_version"], "v1");

    let health = Request::builder()
        .uri("/v1/system/health")
        .body(Body::empty())
        .expect("health");
    let response = app.clone().oneshot(health).await.expect("health");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["status"], "ok");

    let regions = Request::builder()
        .uri("/v1/regions")
        .body(Body::empty())
        .expect("regions");
    let response = app.clone().oneshot(regions).await.expect("regions");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["items"].as_array().unwrap().len(), 1);

    let get_region = Request::builder()
        .uri("/v1/regions/local")
        .body(Body::empty())
        .expect("get region");
    let response = app.clone().oneshot(get_region).await.expect("get region");
    assert_eq!(response.status(), StatusCode::OK);

    let missing_region = Request::builder()
        .uri("/v1/regions/missing")
        .body(Body::empty())
        .expect("missing region");
    let response = app
        .clone()
        .oneshot(missing_region)
        .await
        .expect("missing region");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn tenant_and_namespace_errors() {
    let h = harness("local").await;
    let app = h.app.clone();
    let op = h.operator();
    let admin = h.admin("t1");

    let list = Request::builder()
        .uri("/v1/tenants")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("list tenants");
    let response = app.clone().oneshot(list).await.expect("list tenants");
    assert_eq!(response.status(), StatusCode::OK);

    h.create_tenant("t1").await;

    let conflict = json_request_as(
        "POST",
        "/v1/tenants",
        &op,
        serde_json::json!({
            "tenant_id": "t1",
            "display_name": "Tenant One Again"
        }),
    );
    let response = app.clone().oneshot(conflict).await.expect("conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let list_missing_ns = Request::builder()
        .uri("/v1/tenants/missing/namespaces")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("list missing ns");
    let response = app
        .clone()
        .oneshot(list_missing_ns)
        .await
        .expect("list missing ns");
    // No keys, so no credential can be valid for it: 401, not a 404 that
    // would say whether the tenant exists.
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let create_missing_ns = json_request_as(
        "POST",
        "/v1/tenants/missing/namespaces",
        &admin,
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_missing_ns)
        .await
        .expect("missing ns");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let create_namespace = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces",
        &admin,
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let delete_namespace = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete namespace");
    let response = app
        .clone()
        .oneshot(delete_namespace)
        .await
        .expect("delete namespace");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let delete_namespace_missing = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete namespace missing");
    let response = app
        .clone()
        .oneshot(delete_namespace_missing)
        .await
        .expect("delete namespace missing");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let delete_tenant = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("delete tenant");
    let response = app
        .clone()
        .oneshot(delete_tenant)
        .await
        .expect("delete tenant");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let delete_tenant_missing = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("delete tenant missing");
    let response = app
        .clone()
        .oneshot(delete_tenant_missing)
        .await
        .expect("delete tenant missing");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn stream_and_cache_not_found_paths() {
    let h = harness("local").await;
    let app = h.app.clone();
    let admin = h.admin("t1");

    h.create_tenant("t1").await;

    let create_namespace = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces",
        &admin,
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let get_stream = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams/missing")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("get stream");
    let response = app.clone().oneshot(get_stream).await.expect("get stream");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let patch_stream = json_request_as(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/streams/missing",
        &admin,
        serde_json::json!({
            "durable": true
        }),
    );
    let response = app
        .clone()
        .oneshot(patch_stream)
        .await
        .expect("patch stream");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let get_cache = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches/missing")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("get cache");
    let response = app.clone().oneshot(get_cache).await.expect("get cache");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let patch_cache = json_request_as(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/caches/missing",
        &admin,
        serde_json::json!({
            "display_name": "Updated"
        }),
    );
    let response = app.clone().oneshot(patch_cache).await.expect("patch cache");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let list_missing_namespace_streams = Request::builder()
        .uri("/v1/tenants/t1/namespaces/missing/streams")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("list missing streams");
    let response = app
        .clone()
        .oneshot(list_missing_namespace_streams)
        .await
        .expect("list missing streams");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let list_missing_namespace_caches = Request::builder()
        .uri("/v1/tenants/t1/namespaces/missing/caches")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("list missing caches");
    let response = app
        .clone()
        .oneshot(list_missing_namespace_caches)
        .await
        .expect("list missing caches");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn snapshots_and_changes_endpoints() {
    let h = harness("local").await;
    let app = h.app.clone();
    let op = h.operator();
    let admin = h.admin("t1");

    h.create_tenant("t1").await;

    let create_namespace = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces",
        &admin,
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create_stream = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        &admin,
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(create_stream).await.expect("stream");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create_cache = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        &admin,
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary"
        }),
    );
    let response = app.clone().oneshot(create_cache).await.expect("cache");
    assert_eq!(response.status(), StatusCode::CREATED);

    for path in [
        "/v1/tenants/snapshot",
        "/v1/tenants/changes?since=0",
        "/v1/namespaces/snapshot",
        "/v1/namespaces/changes?since=0",
        "/v1/streams/snapshot",
        "/v1/streams/changes?since=0",
        "/v1/caches/snapshot",
        "/v1/caches/changes?since=0",
    ] {
        let req = request_as("GET", path, &op);
        let response = app.clone().oneshot(req).await.expect("snapshot/changes");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = read_json(response).await;
        assert!(!payload["items"].as_array().unwrap().is_empty());
    }
}

#[tokio::test]
async fn stream_and_cache_conflict_and_delete_errors() {
    let h = harness("local").await;
    let app = h.app.clone();
    let admin = h.admin("t1");

    h.create_tenant("t1").await;

    let create_namespace = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces",
        &admin,
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create_stream = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        &admin,
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(create_stream).await.expect("stream");
    assert_eq!(response.status(), StatusCode::CREATED);

    let conflict_stream = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        &admin,
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app
        .clone()
        .oneshot(conflict_stream)
        .await
        .expect("stream conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let patch_stream = json_request_as(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/streams/orders",
        &admin,
        serde_json::json!({
            "retention": { "max_age_seconds": 7200, "max_size_bytes": 1024 },
            "consistency": "Quorum",
            "delivery": "AtMostOnce",
            "durable": true
        }),
    );
    let response = app
        .clone()
        .oneshot(patch_stream)
        .await
        .expect("patch stream");
    assert_eq!(response.status(), StatusCode::OK);

    let create_cache = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        &admin,
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary"
        }),
    );
    let response = app.clone().oneshot(create_cache).await.expect("cache");
    assert_eq!(response.status(), StatusCode::CREATED);

    let conflict_cache = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        &admin,
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary"
        }),
    );
    let response = app
        .clone()
        .oneshot(conflict_cache)
        .await
        .expect("cache conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let patch_cache = json_request_as(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/caches/primary",
        &admin,
        serde_json::json!({ "display_name": "Primary Updated" }),
    );
    let response = app.clone().oneshot(patch_cache).await.expect("patch cache");
    assert_eq!(response.status(), StatusCode::OK);

    let delete_stream = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/streams/missing")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete stream");
    let response = app
        .clone()
        .oneshot(delete_stream)
        .await
        .expect("delete stream");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let delete_cache = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/caches/missing")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete cache");
    let response = app
        .clone()
        .oneshot(delete_cache)
        .await
        .expect("delete cache");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_endpoints_return_items() {
    let h = harness("local").await;
    let app = h.app.clone();
    let op = h.operator();
    let admin = h.admin("t1");

    h.create_tenant("t1").await;

    let create_namespace = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces",
        &admin,
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create_stream = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        &admin,
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(create_stream).await.expect("stream");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create_cache = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        &admin,
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary"
        }),
    );
    let response = app.clone().oneshot(create_cache).await.expect("cache");
    assert_eq!(response.status(), StatusCode::CREATED);

    let list_tenants = Request::builder()
        .uri("/v1/tenants")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("list tenants");
    let response = app
        .clone()
        .oneshot(list_tenants)
        .await
        .expect("list tenants");
    assert_eq!(response.status(), StatusCode::OK);

    let list_namespaces = Request::builder()
        .uri("/v1/tenants/t1/namespaces")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("list namespaces");
    let response = app
        .clone()
        .oneshot(list_namespaces)
        .await
        .expect("list namespaces");
    assert_eq!(response.status(), StatusCode::OK);

    let list_streams = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("list streams");
    let response = app
        .clone()
        .oneshot(list_streams)
        .await
        .expect("list streams");
    assert_eq!(response.status(), StatusCode::OK);

    let list_caches = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("list caches");
    let response = app.clone().oneshot(list_caches).await.expect("list caches");
    assert_eq!(response.status(), StatusCode::OK);
}

#[derive(Clone, Default)]
struct FailingStore {
    /// When set, the credential check passes and the failure surfaces from
    /// the operation itself, which is what these tests are about.
    signing_keys: Option<TenantSigningKeys>,
    tenant_exists: bool,
    namespace_exists: bool,
    stream_create_not_found: bool,
    cache_create_not_found: bool,
    tenant_bootstrapped: Option<bool>,
}

impl FailingStore {
    fn with_namespace_checks_succeeding() -> Self {
        Self {
            tenant_exists: true,
            namespace_exists: true,
            ..Self::default()
        }
    }

    fn with_create_not_found() -> Self {
        Self {
            tenant_exists: true,
            namespace_exists: true,
            stream_create_not_found: true,
            cache_create_not_found: true,
            ..Self::default()
        }
    }
}

#[async_trait]
impl ControlPlaneStore for FailingStore {
    async fn list_tenants(&self) -> StoreResult<Vec<Tenant>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_tenants_page(&self, _page: PageRequest<String>) -> StoreResult<Page<Tenant>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn create_tenant(&self, _tenant: Tenant) -> StoreResult<Tenant> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_tenant(&self, _tenant_id: &str) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn tenant_snapshot(&self) -> StoreResult<Snapshot<Tenant>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn tenant_changes(&self, _since: u64) -> StoreResult<ChangeSet<TenantChange>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_namespaces(&self, _tenant_id: &str) -> StoreResult<Vec<Namespace>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_namespaces_page(
        &self,
        _tenant_id: &str,
        _page: PageRequest<String>,
    ) -> StoreResult<Page<Namespace>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn create_namespace(&self, _namespace: Namespace) -> StoreResult<Namespace> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_namespace(&self, _key: &NamespaceKey) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn namespace_snapshot(&self) -> StoreResult<Snapshot<Namespace>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn namespace_changes(&self, _since: u64) -> StoreResult<ChangeSet<NamespaceChange>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_streams(&self, _tenant_id: &str, _namespace: &str) -> StoreResult<Vec<Stream>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_streams_page(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _page: PageRequest<String>,
    ) -> StoreResult<Page<Stream>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn get_stream(&self, _key: &StreamKey) -> StoreResult<Stream> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn create_stream(&self, _stream: Stream) -> StoreResult<Stream> {
        if self.stream_create_not_found {
            return Err(StoreError::NotFound("namespace".to_string()));
        }
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn patch_stream(
        &self,
        _key: &StreamKey,
        _patch: StreamPatchRequest,
    ) -> StoreResult<Stream> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_stream(&self, _key: &StreamKey) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn stream_changes(&self, _since: u64) -> StoreResult<ChangeSet<StreamChange>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_caches(&self, _tenant_id: &str, _namespace: &str) -> StoreResult<Vec<Cache>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_caches_page(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _page: PageRequest<String>,
    ) -> StoreResult<Page<Cache>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn get_cache(&self, _key: &CacheKey) -> StoreResult<Cache> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn create_cache(&self, _cache: Cache) -> StoreResult<Cache> {
        if self.cache_create_not_found {
            return Err(StoreError::NotFound("namespace".to_string()));
        }
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn patch_cache(&self, _key: &CacheKey, _patch: CachePatchRequest) -> StoreResult<Cache> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_cache(&self, _key: &CacheKey) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn cache_changes(&self, _since: u64) -> StoreResult<ChangeSet<CacheChange>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn register_node(&self, _node: Node) -> StoreResult<Node> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn enabled_fleet_features(&self) -> StoreResult<std::collections::BTreeSet<String>> {
        // A stream create reads the fleet gate first; let it through so the
        // create's own failure is the one that surfaces.
        if self.stream_create_not_found {
            return Ok(Default::default());
        }
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn finalize_fleet_feature(
        &self,
        _feature: &str,
    ) -> StoreResult<std::collections::BTreeSet<String>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn get_node(&self, _node_id: &str) -> StoreResult<Node> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_nodes(&self) -> StoreResult<Vec<Node>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_nodes_page(&self, _page: PageRequest<String>) -> StoreResult<Page<Node>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn patch_node(&self, _node_id: &str, _patch: NodePatchRequest) -> StoreResult<Node> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_node(&self, _node_id: &str) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn record_node_heartbeat(
        &self,
        _node_id: &str,
        _incarnation: u64,
        _at_millis: u64,
    ) -> StoreResult<Node> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn expire_stale_nodes(&self, _expiry_before_millis: u64) -> StoreResult<Vec<Node>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn clamp_future_heartbeats(&self, _now_millis: u64) -> StoreResult<u64> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn set_node_lifecycle(
        &self,
        _node_id: &str,
        _lifecycle: NodeLifecycle,
    ) -> StoreResult<Option<Node>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn node_snapshot(&self) -> StoreResult<Snapshot<Node>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn node_changes(&self, _since: u64) -> StoreResult<ChangeSet<NodeChange>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn put_shard_assignment(
        &self,
        _assignment: ShardAssignment,
    ) -> StoreResult<ShardAssignment> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn put_shard_assignment_if(
        &self,
        _assignment: ShardAssignment,
        _expected_generation: Option<u64>,
        _fence: u64,
    ) -> StoreResult<felix_controlplane_service::store::AssignmentWrite> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn get_shard_assignment(&self, _key: &ShardKey) -> StoreResult<ShardAssignment> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_shard_assignments(&self) -> StoreResult<Vec<ShardAssignment>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_shard_assignments_page(
        &self,
        _leader: Option<&str>,
        _page: PageRequest<ShardKey>,
    ) -> StoreResult<Page<ShardAssignment>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_shard_assignments_for_node(
        &self,
        _node_id: &str,
    ) -> StoreResult<Vec<ShardAssignment>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_shard_assignment(&self, _key: &ShardKey) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn record_replica_report(
        &self,
        _report: ReplicaReport,
        _leader: &str,
    ) -> StoreResult<felix_controlplane_service::store::ReportWrite> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_replica_reports(&self) -> StoreResult<Vec<ReplicaReport>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn moves_paused(&self) -> StoreResult<bool> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn set_moves_paused(&self, _paused: bool) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn placement_token(&self) -> StoreResult<u64> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn acquire_placement_lease(
        &self,
        _holder: &str,
        _ttl_millis: u64,
    ) -> StoreResult<Option<felix_controlplane_service::store::PlacementLease>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn release_placement_lease(&self, _holder: &str) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn shard_assignment_snapshot(&self) -> StoreResult<Snapshot<ShardAssignment>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn shard_assignment_changes(
        &self,
        _since: u64,
    ) -> StoreResult<ChangeSet<ShardAssignmentChange>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn tenant_exists(&self, _tenant_id: &str) -> StoreResult<bool> {
        Ok(self.tenant_exists)
    }

    async fn namespace_exists(&self, _key: &NamespaceKey) -> StoreResult<bool> {
        Ok(self.namespace_exists)
    }

    async fn health_check(&self) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    fn is_durable(&self) -> bool {
        false
    }

    fn backend_name(&self) -> &'static str {
        "fail"
    }
}

#[async_trait]
impl AuthStore for FailingStore {
    async fn list_idp_issuers(&self, _tenant_id: &str) -> StoreResult<Vec<IdpIssuerConfig>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn upsert_idp_issuer(
        &self,
        _tenant_id: &str,
        _issuer: IdpIssuerConfig,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_idp_issuer(&self, _tenant_id: &str, _issuer: &str) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_rbac_policies(&self, _tenant_id: &str) -> StoreResult<Vec<PolicyRule>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_rbac_groupings(&self, _tenant_id: &str) -> StoreResult<Vec<GroupingRule>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_rbac_policies_page(
        &self,
        _tenant_id: &str,
        _page: PageRequest<PolicyRule>,
    ) -> StoreResult<Page<PolicyRule>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_rbac_groupings_page(
        &self,
        _tenant_id: &str,
        _page: PageRequest<GroupingRule>,
    ) -> StoreResult<Page<GroupingRule>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn add_rbac_policy(&self, _tenant_id: &str, _policy: PolicyRule) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn add_rbac_grouping(
        &self,
        _tenant_id: &str,
        _grouping: GroupingRule,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn remove_rbac_policy(&self, _tenant_id: &str, _policy: PolicyRule) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn remove_rbac_grouping(
        &self,
        _tenant_id: &str,
        _grouping: GroupingRule,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn stage_signing_key(
        &self,
        _tenant_id: &str,
        _key: felix_controlplane_service::auth::felix_token::SigningKey,
    ) -> StoreResult<TenantSigningKeys> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn activate_signing_key(
        &self,
        _tenant_id: &str,
        _kid: &str,
    ) -> StoreResult<TenantSigningKeys> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn retire_signing_key(
        &self,
        _tenant_id: &str,
        _kid: &str,
    ) -> StoreResult<TenantSigningKeys> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn get_tenant_signing_keys(&self, _tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        self.signing_keys
            .clone()
            .ok_or_else(|| StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn set_tenant_signing_keys(
        &self,
        _tenant_id: &str,
        _keys: TenantSigningKeys,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn tenant_auth_is_bootstrapped(&self, _tenant_id: &str) -> StoreResult<bool> {
        self.tenant_bootstrapped
            .ok_or_else(|| StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn set_tenant_auth_bootstrapped(
        &self,
        _tenant_id: &str,
        _bootstrapped: bool,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn ensure_signing_key_current(&self, _tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn seed_rbac_policies_and_groupings(
        &self,
        _tenant_id: &str,
        _policies: Vec<PolicyRule>,
        _groupings: Vec<GroupingRule>,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn bootstrap_tenant_auth(
        &self,
        _tenant_id: &str,
        _seed: felix_controlplane_service::store::TenantAuthSeed,
    ) -> StoreResult<TenantSigningKeys> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }
    async fn insert_refresh_token(
        &self,
        _token: felix_controlplane_service::auth::refresh_token::RefreshToken,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn take_refresh_token(
        &self,
        _tenant_id: &str,
        _token_id: &str,
        _now_secs: i64,
    ) -> StoreResult<felix_controlplane_service::auth::refresh_token::RefreshTokenTake> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn revoke_refresh_family(&self, _tenant_id: &str, _family_id: &str) -> StoreResult<u64> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn revoke_refresh_tokens_for_principal(
        &self,
        _tenant_id: &str,
        _principal_id: &str,
    ) -> StoreResult<u64> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn purge_expired_refresh_tokens(&self, _before_secs: i64) -> StoreResult<u64> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }
}

/// Health answers 503 when the store does not, and it must probe the *real*
/// store to do so — a readiness wired to something that cannot fail would make
/// this pass while the endpoint reported ready during an outage.
#[tokio::test]
async fn system_health_reports_unavailable_on_store_failure() {
    let failing: Arc<dyn felix_controlplane_service::store::ControlPlaneAuthStore + Send + Sync> =
        Arc::new(FailingStore::default());
    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::clone(&failing),
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::StoreProbe(
                Arc::clone(&failing),
            )),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
        build_router(state).into_service();

    let health = Request::builder()
        .uri("/v1/system/health")
        .body(Body::empty())
        .expect("health");
    let response = app.clone().oneshot(health).await.expect("health");
    // 503, not 500: this says "do not send me traffic", which a load balancer
    // acts on. A 500 reads as a fault to page someone about.
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    // And liveness is unaffected, which is the whole point of the split: an
    // unreachable database must not restart the process.
    let live = Request::builder()
        .uri("/v1/system/live")
        .body(Body::empty())
        .expect("live");
    let response = app.clone().oneshot(live).await.expect("live");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn tenant_endpoints_report_internal_error_on_store_failure() {
    let credentials = Credentials {
        keys: felix_controlplane_service::auth::keys::generate_signing_keys().expect("keys"),
    };
    let op = credentials.operator();
    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::new(FailingStore {
            signing_keys: Some(credentials.keys.clone()),
            ..FailingStore::default()
        }),
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
        build_router(state).into_service();

    let list = Request::builder()
        .uri("/v1/tenants")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("list tenants");
    let response = app.clone().oneshot(list).await.expect("list tenants");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let create = json_request_as(
        "POST",
        "/v1/tenants",
        &op,
        serde_json::json!({
            "tenant_id": "t1",
            "display_name": "Tenant One"
        }),
    );
    let response = app.clone().oneshot(create).await.expect("create tenant");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let delete = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("delete tenant");
    let response = app.clone().oneshot(delete).await.expect("delete tenant");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let snapshot = Request::builder()
        .uri("/v1/tenants/snapshot")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("snapshot");
    let response = app.clone().oneshot(snapshot).await.expect("snapshot");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let changes = Request::builder()
        .uri("/v1/tenants/changes?since=0")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("changes");
    let response = app.clone().oneshot(changes).await.expect("changes");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn stream_and_cache_endpoints_report_internal_error_after_scope_checks() {
    let credentials = Credentials {
        keys: felix_controlplane_service::auth::keys::generate_signing_keys().expect("keys"),
    };
    let admin = credentials.tenant_admin("t1");
    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::new(FailingStore {
            signing_keys: Some(credentials.keys.clone()),
            ..FailingStore::with_namespace_checks_succeeding()
        }),
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
        build_router(state).into_service();

    let stream_create = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        &admin,
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app
        .clone()
        .oneshot(stream_create)
        .await
        .expect("create stream");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let stream_get = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams/orders")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("get stream");
    let response = app.clone().oneshot(stream_get).await.expect("get stream");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let stream_patch = json_request_as(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/streams/orders",
        &admin,
        serde_json::json!({ "durable": true }),
    );
    let response = app
        .clone()
        .oneshot(stream_patch)
        .await
        .expect("patch stream");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let stream_delete = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/streams/orders")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete stream");
    let response = app
        .clone()
        .oneshot(stream_delete)
        .await
        .expect("delete stream");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let cache_create = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        &admin,
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary"
        }),
    );
    let response = app
        .clone()
        .oneshot(cache_create)
        .await
        .expect("create cache");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let cache_get = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches/primary")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("get cache");
    let response = app.clone().oneshot(cache_get).await.expect("get cache");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let cache_patch = json_request_as(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/caches/primary",
        &admin,
        serde_json::json!({ "display_name": "Updated" }),
    );
    let response = app.clone().oneshot(cache_patch).await.expect("patch cache");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let cache_delete = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/caches/primary")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete cache");
    let response = app
        .clone()
        .oneshot(cache_delete)
        .await
        .expect("delete cache");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn stream_and_cache_create_report_not_found_when_store_reports_missing_namespace() {
    let credentials = Credentials {
        keys: felix_controlplane_service::auth::keys::generate_signing_keys().expect("keys"),
    };
    let admin = credentials.tenant_admin("t1");
    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::new(FailingStore {
            signing_keys: Some(credentials.keys.clone()),
            ..FailingStore::with_create_not_found()
        }),
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
        build_router(state).into_service();

    let stream_create = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        &admin,
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app
        .clone()
        .oneshot(stream_create)
        .await
        .expect("create stream");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let cache_create = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        &admin,
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary"
        }),
    );
    let response = app
        .clone()
        .oneshot(cache_create)
        .await
        .expect("create cache");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn bootstrap_initialize_reports_internal_error_when_signing_key_ensure_fails() {
    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::new(FailingStore {
            tenant_exists: true,
            tenant_bootstrapped: Some(false),
            ..FailingStore::default()
        }),
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: true,
        bootstrap_tokens: vec!["secret".to_string()],
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
        build_bootstrap_router(state).into_service();

    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            serde_json::json!({
                "display_name": "Tenant One",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

/// A stream keeps the region it was created with, through a patch; an empty
/// one is refused rather than read as "anywhere".
#[tokio::test]
async fn a_stream_keeps_its_home_region() {
    let h = harness("local").await;
    let app = h.app.clone();
    let admin = h.admin("t1");
    h.create_tenant("t1").await;
    let create_namespace = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces",
        &admin,
        serde_json::json!({ "namespace": "default", "display_name": "Default" }),
    );
    let response = app.clone().oneshot(create_namespace).await.expect("ns");
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = |name: &str, region: &str| {
        serde_json::json!({
            "stream": name,
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": null, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": true,
            "region": region,
        })
    };
    let streams = "/v1/tenants/t1/namespaces/default/streams";
    let blank = json_request_as("POST", streams, &admin, body("blank", " "));
    let response = app.clone().oneshot(blank).await.expect("blank");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let create = json_request_as("POST", streams, &admin, body("orders", "eu"));
    let response = app.clone().oneshot(create).await.expect("create");
    assert_eq!(response.status(), StatusCode::CREATED);

    let patch = json_request_as(
        "PATCH",
        &format!("{streams}/orders"),
        &admin,
        serde_json::json!({ "durable": false }),
    );
    let response = app.clone().oneshot(patch).await.expect("patch");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(read_json(response).await["region"], "eu");
}

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use felix_controlplane_service::api::types::FeatureFlags;
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::model::{
    Node, NodeCapacity, NodeLifecycle, NodeSpec, NodeStatus, RetentionPolicy, StreamKind,
};
use felix_controlplane_service::store::{ControlPlaneStore, StoreConfig};
use tower::ServiceExt;

use crate::common::read_json;
use crate::common::{Credentials, json_request_as, seed_credentials};

struct Harness {
    app: axum::routing::RouterIntoService<Body, ()>,
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
}

async fn harness() -> Harness {
    let store = Arc::new(
        felix_controlplane_service::store::memory::InMemoryStore::new(StoreConfig {
            changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: Some(
                felix_controlplane_service::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS,
            ),
        }),
    );
    let credentials = seed_credentials(store.as_ref()).await;
    let state = AppState {
        region: felix_controlplane_service::api::types::Region {
            region_id: "local".to_string(),
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

/// Create `t1` as an operator and bind the test keys to it, so the admin
/// tokens minted here verify against it.
async fn create_tenant(h: &Harness) {
    let req = json_request_as(
        "POST",
        "/v1/tenants",
        &h.operator(),
        serde_json::json!({
            "tenant_id": "t1",
            "display_name": "Tenant One"
        }),
    );
    let response = h.app.clone().oneshot(req).await.expect("tenant");
    assert_eq!(response.status(), StatusCode::CREATED);
    h.credentials.adopt(h.store.as_ref(), "t1").await;
}

async fn create_namespace(h: &Harness) {
    let req = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces",
        &h.admin("t1"),
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = h.app.clone().oneshot(req).await.expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn tenants_conflict_and_delete_not_found() {
    let h = harness().await;
    let app = h.app.clone();
    let op = h.operator();

    // First create should succeed.
    create_tenant(&h).await;

    // Second create should conflict.
    let req = json_request_as(
        "POST",
        "/v1/tenants",
        &op,
        serde_json::json!({
            "tenant_id": "t1",
            "display_name": "Tenant One"
        }),
    );
    let response = app.clone().oneshot(req).await.expect("conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // Delete existing tenant succeeds, second delete returns not found.
    let delete = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let delete_again = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete_again).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn namespaces_missing_tenant_and_conflict() {
    let h = harness().await;
    let app = h.app.clone();
    let admin = h.admin("t1");

    // A tenant that does not exist has no keys, so no credential can be
    // valid for it: 401, and not a 404 that would say whether it exists.
    let list_missing = Request::builder()
        .uri("/v1/tenants/missing/namespaces")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("list");
    let response = app.clone().oneshot(list_missing).await.expect("list");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    create_tenant(&h).await;

    create_namespace(&h).await;

    let create_conflict = json_request_as(
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
        .oneshot(create_conflict)
        .await
        .expect("conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let delete_missing = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/other")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete_missing).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn caches_error_paths_and_changes() {
    let h = harness().await;
    let app = h.app.clone();
    let op = h.operator();
    let admin = h.admin("t1");
    create_tenant(&h).await;
    create_namespace(&h).await;

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

    let conflict = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        &admin,
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary Cache"
        }),
    );
    let response = app.clone().oneshot(conflict).await.expect("conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let get_missing = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches/missing")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("get");
    let response = app.clone().oneshot(get_missing).await.expect("get");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let patch_missing = json_request_as(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/caches/missing",
        &admin,
        serde_json::json!({
            "display_name": "Missing"
        }),
    );
    let response = app.clone().oneshot(patch_missing).await.expect("patch");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let delete_missing = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/caches/missing")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete_missing).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let changes = Request::builder()
        .uri("/v1/caches/changes?since=0")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("changes");
    let response = app.clone().oneshot(changes).await.expect("changes");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert!(payload["items"].as_array().is_some());
}

#[tokio::test]
async fn streams_error_paths_and_changes() {
    let h = harness().await;
    let app = h.app.clone();
    let op = h.operator();
    let admin = h.admin("t1");
    create_tenant(&h).await;
    create_namespace(&h).await;

    let create = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        &admin,
        serde_json::json!({
            "stream": "orders",
            "kind": StreamKind::Stream,
            "shards": 1,
            "retention": RetentionPolicy { max_age_seconds: Some(3600), max_size_bytes: None },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(create).await.expect("create");
    assert_eq!(response.status(), StatusCode::CREATED);

    let conflict = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        &admin,
        serde_json::json!({
            "stream": "orders",
            "kind": StreamKind::Stream,
            "shards": 1,
            "retention": RetentionPolicy { max_age_seconds: Some(3600), max_size_bytes: None },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(conflict).await.expect("conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let get_missing = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams/missing")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("get");
    let response = app.clone().oneshot(get_missing).await.expect("get");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let patch_missing = json_request_as(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/streams/missing",
        &admin,
        serde_json::json!({
            "retention": { "max_age_seconds": 7200, "max_size_bytes": null }
        }),
    );
    let response = app.clone().oneshot(patch_missing).await.expect("patch");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let delete_missing = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/streams/missing")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete_missing).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let changes = Request::builder()
        .uri("/v1/streams/changes?since=0")
        .header("authorization", format!("Bearer {op}"))
        .body(Body::empty())
        .expect("changes");
    let response = app.clone().oneshot(changes).await.expect("changes");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert!(payload["items"].as_array().is_some());
}

/// A cache records the consistency it was created with, and one created without
/// saying reads back as `Leader`, which is what every cache was before.
#[tokio::test]
async fn a_cache_keeps_the_consistency_it_was_created_with() {
    let h = harness().await;
    let admin = h.admin("t1");
    create_tenant(&h).await;
    create_namespace(&h).await;

    for (cache, requested, expected) in [
        ("plain", None, "Leader"),
        ("strict", Some("Quorum"), "Quorum"),
    ] {
        let mut body = serde_json::json!({ "cache": cache, "display_name": cache });
        if let Some(level) = requested {
            body["consistency"] = serde_json::json!(level);
        }
        let create = json_request_as(
            "POST",
            "/v1/tenants/t1/namespaces/default/caches",
            &admin,
            body,
        );
        let response = h.app.clone().oneshot(create).await.expect("create");
        assert_eq!(response.status(), StatusCode::CREATED);

        let get = Request::builder()
            .uri(format!("/v1/tenants/t1/namespaces/default/caches/{cache}"))
            .header("authorization", format!("Bearer {admin}"))
            .body(Body::empty())
            .expect("get");
        let response = h.app.clone().oneshot(get).await.expect("get");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            read_json(response).await["consistency"],
            expected,
            "{cache}"
        );
    }
}

/// A name that means something in an RBAC object, a URL or a path is refused
/// at creation: a stream named `*` would turn every grant written for it into
/// a namespace-wide one.
#[tokio::test]
async fn names_outside_the_identifier_grammar_are_refused() {
    let h = harness().await;
    let tenant = json_request_as(
        "POST",
        "/v1/tenants",
        &h.operator(),
        serde_json::json!({"tenant_id": "a/b", "display_name": "Slash"}),
    );
    let response = h.app.clone().oneshot(tenant).await.expect("tenant");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    create_tenant(&h).await;
    let namespace = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces",
        &h.admin("t1"),
        serde_json::json!({"namespace": "*", "display_name": "Star"}),
    );
    let response = h.app.clone().oneshot(namespace).await.expect("namespace");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    create_namespace(&h).await;
    let stream = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        &h.admin("t1"),
        serde_json::json!({
            "stream": "*",
            "kind": "Stream",
            "shards": 1,
            "retention": {"max_age_seconds": null, "max_size_bytes": null},
            "consistency": "Leader",
            "delivery": "AtMostOnce",
            "durable": false
        }),
    );
    let response = h.app.clone().oneshot(stream).await.expect("stream");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let cache = json_request_as(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        &h.admin("t1"),
        serde_json::json!({"cache": "a:b", "display_name": "Colon"}),
    );
    let response = h.app.clone().oneshot(cache).await.expect("cache");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// A stream's routing is decided once, when it is created: modulo until the
/// fleet finalizes `jump_hash_routing`, jump hash after, and a stream created
/// before keeps modulo.
#[tokio::test]
async fn stream_routing_is_fixed_by_the_fleet_gate_at_creation() {
    let h = harness().await;
    let admin = h.admin("t1");
    create_tenant(&h).await;
    create_namespace(&h).await;
    let create = |name: &str, routing: Option<&str>| {
        let mut body = serde_json::json!({
            "stream": name,
            "kind": StreamKind::Stream,
            "shards": 4,
            "retention": RetentionPolicy { max_age_seconds: None, max_size_bytes: None },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        });
        if let Some(routing) = routing {
            body["routing"] = serde_json::json!(routing);
        }
        json_request_as(
            "POST",
            "/v1/tenants/t1/namespaces/default/streams",
            &admin,
            body,
        )
    };

    let response = h
        .app
        .clone()
        .oneshot(create("legacy", None))
        .await
        .expect("create");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert!(
        read_json(response).await.get("routing").is_none(),
        "modulo is omitted"
    );
    let response = h
        .app
        .clone()
        .oneshot(create("early", Some("jump_hash")))
        .await
        .expect("create");
    assert_eq!(response.status(), StatusCode::CONFLICT, "not finalized yet");

    let feature = felix_common::fleet::JUMP_HASH_ROUTING.name();
    h.store
        .register_node(Node {
            node_id: "broker-a".to_string(),
            spec: NodeSpec {
                advertise_addr: "10.0.0.4:7000".to_string(),
                client_addr: None,
                kafka_addr: None,
                region: "local".to_string(),
                zone: None,
                labels: Default::default(),
                capacity: NodeCapacity::default(),
            },
            status: NodeStatus {
                lifecycle: NodeLifecycle::Live,
                last_heartbeat_at_millis: 1,
                registered_at_millis: 1,
                incarnation: 0,
                features: [feature.to_string()].into(),
            },
        })
        .await
        .expect("node");
    h.store
        .finalize_fleet_feature(feature)
        .await
        .expect("finalize");

    let response = h
        .app
        .clone()
        .oneshot(create("fresh", None))
        .await
        .expect("create");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(read_json(response).await["routing"], "jump_hash");
    let response = h
        .app
        .clone()
        .oneshot(create("optout", Some("modulo")))
        .await
        .expect("create");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert!(read_json(response).await.get("routing").is_none());

    let get = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams/legacy")
        .header("authorization", format!("Bearer {admin}"))
        .body(Body::empty())
        .expect("get");
    let response = h.app.clone().oneshot(get).await.expect("get");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        read_json(response).await.get("routing").is_none(),
        "finalizing leaves an existing stream on modulo"
    );
}

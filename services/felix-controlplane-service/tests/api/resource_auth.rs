//! The resource API takes a credential, and the right one.
//!
//! Before this, tenants, namespaces, streams and caches were created and
//! deleted with no token at all while `/v1/nodes` next to them answered 401.
//! Each case here is a request that used to succeed and must not.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::model::{Namespace, Tenant};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{ControlPlaneStore, StoreConfig};
use tower::ServiceExt;

use crate::common::{
    Credentials, json_request, json_request_as, read_json, request_as, seed_credentials,
};

pub(crate) struct Harness {
    pub(crate) app: axum::routing::RouterIntoService<Body, ()>,
    pub(crate) store: Arc<InMemoryStore>,
    pub(crate) credentials: Credentials,
}

/// A control plane with tenant `t1`, holding namespaces `payments` and
/// `billing`, bound to the test keys.
pub(crate) async fn harness() -> Harness {
    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(
            felix_controlplane_service::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS,
        ),
    }));
    let credentials = seed_credentials(store.as_ref()).await;
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    credentials.adopt(store.as_ref(), "t1").await;
    for namespace in ["payments", "billing"] {
        store
            .create_namespace(Namespace {
                tenant_id: "t1".to_string(),
                namespace: namespace.to_string(),
                display_name: namespace.to_string(),
            })
            .await
            .expect("namespace");
    }
    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
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

pub(crate) fn stream_body(name: &str) -> serde_json::Value {
    serde_json::json!({
        "stream": name,
        "kind": "Stream",
        "shards": 1,
        "retention": { "max_age_seconds": null, "max_size_bytes": null },
        "consistency": "Leader",
        "delivery": "AtLeastOnce",
        "durable": false
    })
}

fn tenant_body(id: &str) -> serde_json::Value {
    serde_json::json!({ "tenant_id": id, "display_name": id })
}

async fn status(h: &Harness, request: Request<Body>) -> StatusCode {
    h.app
        .clone()
        .oneshot(request)
        .await
        .expect("response")
        .status()
}

/// The reproduction from the report: create a tenant, a namespace and a
/// stream, then delete the tenant, all with no credential.
#[tokio::test]
async fn nothing_in_the_resource_api_works_without_a_token() {
    let h = harness().await;

    let cases = [
        json_request("POST", "/v1/tenants", tenant_body("t2")),
        Request::builder()
            .uri("/v1/tenants")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .method("DELETE")
            .uri("/v1/tenants/t1")
            .body(Body::empty())
            .unwrap(),
        json_request(
            "POST",
            "/v1/tenants/t1/namespaces",
            serde_json::json!({ "namespace": "ns", "display_name": "ns" }),
        ),
        Request::builder()
            .uri("/v1/tenants/t1/namespaces")
            .body(Body::empty())
            .unwrap(),
        json_request(
            "POST",
            "/v1/tenants/t1/namespaces/payments/streams",
            stream_body("orders"),
        ),
        Request::builder()
            .uri("/v1/tenants/t1/namespaces/payments/streams")
            .body(Body::empty())
            .unwrap(),
        json_request(
            "POST",
            "/v1/tenants/t1/namespaces/payments/caches",
            serde_json::json!({ "cache": "c", "display_name": "c" }),
        ),
        Request::builder()
            .uri("/v1/tenants/snapshot")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .uri("/v1/streams/changes?since=0")
            .body(Body::empty())
            .unwrap(),
    ];
    for request in cases {
        let uri = request.uri().to_string();
        let method = request.method().to_string();
        assert_eq!(
            status(&h, request).await,
            StatusCode::UNAUTHORIZED,
            "{method} {uri} answered without a credential",
        );
    }

    // And nothing changed.
    assert!(h.store.tenant_exists("t1").await.expect("exists"));
    assert!(!h.store.tenant_exists("t2").await.expect("exists"));
    assert!(
        h.store
            .list_streams("t1", "payments")
            .await
            .expect("streams")
            .is_empty()
    );
}

/// A tenant admin can manage everything inside the tenant, and nothing about
/// which tenants exist. The catalog is the operator's.
#[tokio::test]
async fn a_tenant_admin_manages_the_tenant_but_not_the_catalog() {
    let h = harness().await;
    let admin = h.credentials.tenant_admin("t1");

    assert_eq!(
        status(
            &h,
            json_request_as(
                "POST",
                "/v1/tenants/t1/namespaces/payments/streams",
                &admin,
                stream_body("orders"),
            ),
        )
        .await,
        StatusCode::CREATED,
    );
    assert_eq!(
        status(
            &h,
            json_request_as(
                "POST",
                "/v1/tenants/t1/namespaces/payments/caches",
                &admin,
                serde_json::json!({ "cache": "sessions", "display_name": "Sessions" }),
            ),
        )
        .await,
        StatusCode::CREATED,
    );
    assert_eq!(
        status(
            &h,
            request_as("DELETE", "/v1/tenants/t1/namespaces/billing", &admin)
        )
        .await,
        StatusCode::NO_CONTENT,
    );

    for request in [
        json_request_as("POST", "/v1/tenants", &admin, tenant_body("t2")),
        request_as("GET", "/v1/tenants", &admin),
        // Their own tenant included: deleting it takes the keys with it.
        request_as("DELETE", "/v1/tenants/t1", &admin),
        // The feeds are the brokers', not a tenant's.
        request_as("GET", "/v1/streams/snapshot", &admin),
    ] {
        let uri = request.uri().to_string();
        let method = request.method().to_string();
        assert_eq!(
            status(&h, request).await,
            StatusCode::FORBIDDEN,
            "{method} {uri} let a tenant admin through",
        );
    }
    assert!(h.store.tenant_exists("t1").await.expect("exists"));
}

/// An operator manages the catalog and reads the feeds, and holds nothing
/// inside a tenant: cluster scope is not a backdoor into tenant data.
#[tokio::test]
async fn an_operator_manages_the_catalog_and_nothing_inside_a_tenant() {
    let h = harness().await;
    let op = h.credentials.operator();

    assert_eq!(
        status(
            &h,
            json_request_as("POST", "/v1/tenants", &op, tenant_body("t2"))
        )
        .await,
        StatusCode::CREATED,
    );
    let listed = h
        .app
        .clone()
        .oneshot(request_as("GET", "/v1/tenants", &op))
        .await
        .expect("list");
    assert_eq!(listed.status(), StatusCode::OK);
    let payload = read_json(listed).await;
    let ids: Vec<&str> = payload["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tenant| tenant["tenant_id"].as_str())
        .collect();
    assert!(ids.contains(&"t1") && ids.contains(&"t2"), "{ids:?}");
    assert_eq!(
        status(&h, request_as("GET", "/v1/namespaces/snapshot", &op)).await,
        StatusCode::OK,
    );

    // The operator's token was minted by `ops`, so against `t1` it does not
    // even verify -- there is no scope check to fail.
    assert_eq!(
        status(&h, request_as("GET", "/v1/tenants/t1/namespaces", &op)).await,
        StatusCode::UNAUTHORIZED,
    );
    assert_eq!(
        status(
            &h,
            json_request_as(
                "POST",
                "/v1/tenants/t1/namespaces/payments/streams",
                &op,
                stream_body("orders"),
            ),
        )
        .await,
        StatusCode::UNAUTHORIZED,
    );

    assert_eq!(
        status(&h, request_as("DELETE", "/v1/tenants/t2", &op)).await,
        StatusCode::NO_CONTENT,
    );
}

/// A broker's credential reads the feeds and does nothing else.
#[tokio::test]
async fn a_broker_credential_reads_the_feeds_only() {
    let h = harness().await;
    let broker = h.credentials.broker();

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
        assert_eq!(
            status(&h, request_as("GET", path, &broker)).await,
            StatusCode::OK,
            "{path}",
        );
    }
    assert_eq!(
        status(
            &h,
            json_request_as("POST", "/v1/tenants", &broker, tenant_body("t2"))
        )
        .await,
        StatusCode::FORBIDDEN,
    );
    assert_eq!(
        status(&h, request_as("DELETE", "/v1/tenants/t1", &broker)).await,
        StatusCode::FORBIDDEN,
    );
}

/// A namespace admin is confined to their namespace: creating a stream in
/// another is refused, and the namespace listing shows only theirs.
#[tokio::test]
async fn a_namespace_admin_is_confined_to_the_namespace() {
    let h = harness().await;
    let payments = h.credentials.token(
        "t1",
        &[
            "ns.manage:namespace:t1/payments",
            "stream.manage:stream:t1/payments/*",
            "cache.manage:cache:t1/payments/*",
        ],
    );

    assert_eq!(
        status(
            &h,
            json_request_as(
                "POST",
                "/v1/tenants/t1/namespaces/payments/streams",
                &payments,
                stream_body("orders"),
            ),
        )
        .await,
        StatusCode::CREATED,
    );
    assert_eq!(
        status(
            &h,
            json_request_as(
                "POST",
                "/v1/tenants/t1/namespaces/billing/streams",
                &payments,
                stream_body("invoices"),
            ),
        )
        .await,
        StatusCode::FORBIDDEN,
    );
    assert_eq!(
        status(
            &h,
            request_as("DELETE", "/v1/tenants/t1/namespaces/billing", &payments)
        )
        .await,
        StatusCode::FORBIDDEN,
    );
    assert_eq!(
        status(
            &h,
            json_request_as(
                "POST",
                "/v1/tenants/t1/namespaces",
                &payments,
                serde_json::json!({ "namespace": "refunds", "display_name": "Refunds" }),
            ),
        )
        .await,
        StatusCode::FORBIDDEN,
    );

    let listed = h
        .app
        .clone()
        .oneshot(request_as("GET", "/v1/tenants/t1/namespaces", &payments))
        .await
        .expect("list");
    assert_eq!(listed.status(), StatusCode::OK);
    let payload = read_json(listed).await;
    let names: Vec<&str> = payload["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|ns| ns["namespace"].as_str())
        .collect();
    assert_eq!(
        names,
        vec!["payments"],
        "the listing leaked another namespace"
    );
}

/// A token minted for one tenant does nothing against another, and a tenant
/// that does not exist answers 401 rather than saying so.
#[tokio::test]
async fn a_token_is_bound_to_its_tenant() {
    let h = harness().await;
    // Real keys of its own, so this is a genuinely foreign token rather than
    // one that fails on `tid` alone.
    let other = Credentials {
        keys: felix_controlplane_service::auth::keys::generate_signing_keys().expect("keys"),
    };
    h.store
        .create_tenant(Tenant {
            tenant_id: "t2".to_string(),
            display_name: "Tenant Two".to_string(),
        })
        .await
        .expect("tenant");
    other.adopt(h.store.as_ref(), "t2").await;
    let t2_admin = other.tenant_admin("t2");

    assert_eq!(
        status(
            &h,
            request_as("GET", "/v1/tenants/t1/namespaces", &t2_admin)
        )
        .await,
        StatusCode::UNAUTHORIZED,
    );
    assert_eq!(
        status(
            &h,
            json_request_as(
                "POST",
                "/v1/tenants/t1/namespaces/payments/streams",
                &t2_admin,
                stream_body("orders"),
            ),
        )
        .await,
        StatusCode::UNAUTHORIZED,
    );
    assert_eq!(
        status(
            &h,
            request_as("GET", "/v1/tenants/nope/namespaces", &t2_admin)
        )
        .await,
        StatusCode::UNAUTHORIZED,
    );
    // A permission naming another tenant does not parse against this one and
    // is skipped, never trusted.
    let smuggled = h.credentials.token("t1", &["ns.manage:namespace:t2/*"]);
    assert_eq!(
        status(
            &h,
            request_as("GET", "/v1/tenants/t1/namespaces", &smuggled)
        )
        .await,
        StatusCode::FORBIDDEN,
    );
}

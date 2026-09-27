//! HTTP behaviour of the broker heartbeat endpoint.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use felix_controlplane_service::api::types::FeatureFlags;
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::config::NodeLivenessConfig;
use felix_controlplane_service::model::{Node, NodeCapacity, NodeLifecycle, NodeSpec, NodeStatus};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig};
use tower::ServiceExt;

use crate::common::json_request;
use crate::common::read_json;

const LIVENESS: NodeLivenessConfig = NodeLivenessConfig {
    heartbeat_interval_ms: 2_000,
    expiry_timeout_ms: 6_000,
    sweep_interval_ms: 500,
    shard_reconcile_interval_ms: 5_000,
};

fn node(node_id: &str) -> Node {
    Node {
        node_id: node_id.to_string(),
        spec: NodeSpec {
            advertise_addr: "10.0.0.4:7000".to_string(),
            client_addr: None,
            kafka_addr: None,
            region: "us-west-2".to_string(),
            labels: BTreeMap::new(),
            capacity: NodeCapacity::default(),
        },
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: 1,
            registered_at_millis: 1,
            incarnation: 0,
        },
    }
}

/// A credential covering every node. These tests are about heartbeat
/// behaviour, not authorisation -- `node_write_auth.rs` covers that.
async fn fleet_token(store: &InMemoryStore) -> String {
    let keys = felix_controlplane_service::auth::keys::generate_signing_keys().expect("keys");
    store
        .set_tenant_signing_keys("t1", keys.clone())
        .await
        .expect("keys");
    felix_controlplane_service::auth::felix_token::mint_token_for(
        &keys,
        "t1",
        "p:operator",
        vec!["node.manage:cluster:*".to_string()],
        Duration::from_secs(900),
        felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )
    .expect("token")
}

fn authed(bearer: &str, method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
    let mut request = json_request(method, uri, body);
    request.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {bearer}").parse().expect("header"),
    );
    request
}

async fn app_with(store: Arc<InMemoryStore>) -> axum::routing::RouterIntoService<Body, ()> {
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
        store,
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: LIVENESS,
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    build_router(state).into_service()
}

fn store() -> Arc<InMemoryStore> {
    Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(1_000),
    }))
}

#[tokio::test]
async fn a_heartbeat_returns_the_lifecycle_and_the_expected_cadence() {
    let store = store();
    store
        .register_node(node("broker-a"))
        .await
        .expect("register");
    let token = fleet_token(&store).await;
    let app = app_with(Arc::clone(&store)).await;

    let response = app
        .oneshot(authed(
            &token,
            "POST",
            "/v1/nodes/broker-a/heartbeat",
            serde_json::json!({ "incarnation": 0 }),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = read_json(response).await;
    assert_eq!(body["node_id"], "broker-a");
    assert_eq!(body["lifecycle"], "live");
    // The cadence comes from the control plane, so it is configured in one place.
    assert_eq!(
        body["heartbeat_interval_ms"],
        LIVENESS.heartbeat_interval_ms
    );
    assert_eq!(body["expiry_timeout_ms"], LIVENESS.expiry_timeout_ms);
}

/// The recorded time is the control plane's own. A broker that could supply it
/// could postpone its own expiry indefinitely.
#[tokio::test]
async fn a_heartbeat_records_the_control_planes_clock() {
    let store = store();
    store
        .register_node(node("broker-a"))
        .await
        .expect("register");
    let before = felix_controlplane_service::clock::now_millis();

    let token = fleet_token(&store).await;
    let app = app_with(Arc::clone(&store)).await;
    let response = app
        .oneshot(authed(
            &token,
            "POST",
            "/v1/nodes/broker-a/heartbeat",
            serde_json::json!({ "incarnation": 0, "last_heartbeat_at_millis": 99_999_999_999_999u64 }),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::OK);

    let recorded = store
        .get_node("broker-a")
        .await
        .expect("get")
        .status
        .last_heartbeat_at_millis;
    assert!(
        recorded >= before && recorded < 99_999_999_999_999,
        "recorded {recorded} should be the server clock, not the caller's",
    );
}

#[tokio::test]
async fn a_heartbeat_for_an_unregistered_node_is_not_found() {
    let store = store();
    let token = fleet_token(&store).await;
    let app = app_with(store).await;
    let response = app
        .oneshot(authed(
            &token,
            "POST",
            "/v1/nodes/absent/heartbeat",
            serde_json::json!({ "incarnation": 0 }),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_heartbeat_for_a_superseded_incarnation_conflicts() {
    let store = store();
    store
        .register_node(node("broker-a"))
        .await
        .expect("register");
    store
        .register_node(node("broker-a"))
        .await
        .expect("restart");

    let token = fleet_token(&store).await;
    let app = app_with(Arc::clone(&store)).await;
    let response = app
        .oneshot(authed(
            &token,
            "POST",
            "/v1/nodes/broker-a/heartbeat",
            serde_json::json!({ "incarnation": 0 }),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

/// A broker that reads `down` here knows it was expired and must register again
/// rather than carry on as if it were still a cluster member.
#[tokio::test]
async fn an_expired_broker_is_told_it_is_down() {
    let store = store();
    store
        .register_node(node("broker-a"))
        .await
        .expect("register");
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Down)
        .await
        .expect("down");

    let token = fleet_token(&store).await;
    let app = app_with(Arc::clone(&store)).await;
    let response = app
        .oneshot(authed(
            &token,
            "POST",
            "/v1/nodes/broker-a/heartbeat",
            serde_json::json!({ "incarnation": 0 }),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = read_json(response).await;
    assert_eq!(body["lifecycle"], "down");
}

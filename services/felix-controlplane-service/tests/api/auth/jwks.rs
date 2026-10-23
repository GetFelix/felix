//! Integration tests for the tenant JWKS endpoint.
//!
//! Verify that the control-plane exposes Ed25519 public keys in JWKS format and
//! that missing tenants return 404s.
//!
//! - JWKS entries must be OKP/Ed25519 with `alg = EdDSA`.
//! - No RSA components (`n`, `e`) are present for Felix keys.
//!
//! - JWKS is public by design; tests ensure no private material is exposed.
//! - Tokens/keys are treated as secrets and not logged.
//!
//! - Uses in-memory store for deterministic, race-free behavior.
//!
//! Run with `cargo test -p felix-controlplane-service auth_jwks` to execute these tests.
use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::auth::keys::generate_signing_keys;
use felix_controlplane_service::auth::oidc::UpstreamOidcValidator;
use felix_controlplane_service::store::{
    AuthStore, ControlPlaneStore, StoreConfig, memory::InMemoryStore,
};
use tower::ServiceExt;

use crate::common::read_json;

#[tokio::test]
async fn jwks_endpoint_returns_keys_for_tenant() {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(
            felix_controlplane_service::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS,
        ),
    });
    store
        .create_tenant(felix_controlplane_service::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    let keys = generate_signing_keys().expect("keys");
    store
        .set_tenant_signing_keys("t1", keys)
        .await
        .expect("set keys");

    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: std::sync::Arc::new(store),
        oidc_validator: UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
        new_cluster: Default::default(),
    };
    let app = build_router(state).into_service();

    let req = Request::builder()
        .uri("/v1/tenants/t1/.well-known/jwks.json")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(req).await.expect("jwks");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    let keys = payload["keys"].as_array().unwrap();
    assert!(!keys.is_empty());
    let key = &keys[0];
    assert_eq!(key["kty"], "OKP");
    assert_eq!(key["crv"], "Ed25519");
    assert_eq!(key["alg"], "EdDSA");
    assert_eq!(key["use"], "sig");
    let x = key["x"].as_str().expect("x");
    let decoded = URL_SAFE_NO_PAD.decode(x.as_bytes()).expect("decode x");
    assert_eq!(decoded.len(), 32);
    assert!(key.get("n").is_none());
    assert!(key.get("e").is_none());
}

#[tokio::test]
async fn jwks_endpoint_missing_tenant_returns_404() {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(
            felix_controlplane_service::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS,
        ),
    });

    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: std::sync::Arc::new(store),
        oidc_validator: UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
        new_cluster: Default::default(),
    };
    let app = build_router(state).into_service();

    let req = Request::builder()
        .uri("/v1/tenants/missing/.well-known/jwks.json")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(req).await.expect("jwks");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

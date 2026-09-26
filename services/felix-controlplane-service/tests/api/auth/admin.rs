use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::auth::felix_token::{TenantSigningKeys, mint_token_for};
use felix_controlplane_service::auth::keys::generate_signing_keys;
use felix_controlplane_service::auth::oidc::UpstreamOidcValidator;
use felix_controlplane_service::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use felix_controlplane_service::store::{
    AuthStore, ControlPlaneAuthStore, StoreConfig, memory::InMemoryStore,
};
use tower::ServiceExt;

use crate::common::json_request;
use crate::common::read_json;
use crate::common::{json_request_as, seed_credentials};

fn build_state(store: Arc<InMemoryStore>) -> AppState {
    let state_store: Arc<dyn ControlPlaneAuthStore + Send + Sync> = store;
    AppState {
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
        store: state_store,
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
    }
}

pub(super) async fn setup() -> (
    axum::routing::RouterIntoService<Body, ()>,
    Arc<InMemoryStore>,
    TenantSigningKeys,
) {
    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(
            felix_controlplane_service::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS,
        ),
    }));
    let operator = seed_credentials(store.as_ref()).await.operator();
    let app = build_router(build_state(store.clone())).into_service();

    let create_tenant = json_request_as(
        "POST",
        "/v1/tenants",
        &operator,
        serde_json::json!({
            "tenant_id": "t1",
            "display_name": "Tenant One"
        }),
    );
    let response = app.clone().oneshot(create_tenant).await.expect("tenant");
    assert_eq!(response.status(), StatusCode::CREATED);

    let keys = generate_signing_keys().expect("keys");
    store
        .set_tenant_signing_keys("t1", keys.clone())
        .await
        .expect("set keys");

    (app, store, keys)
}

pub(super) fn token(keys: &TenantSigningKeys, perms: Vec<&str>) -> String {
    token_for_tenant(keys, "t1", perms)
}

fn token_for_tenant(keys: &TenantSigningKeys, tenant_id: &str, perms: Vec<&str>) -> String {
    mint_token_for(
        keys,
        tenant_id,
        "p:admin",
        perms.into_iter().map(|value| value.to_string()).collect(),
        Duration::from_secs(900),
        felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )
    .expect("token")
}

#[tokio::test]
async fn ns_manage_cannot_modify_rbac() {
    let (app, _store, keys) = setup().await;
    let t = token(&keys, vec!["ns.manage:namespace:t1/payments"]);

    let add_policy = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:payments-publisher",
            "object": "stream:t1/payments/orders",
            "action": "stream.publish"
        }),
    );
    let response = app.oneshot(add_auth(add_policy, &t)).await.expect("policy");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn rbac_view_can_list_but_cannot_mutate() {
    let (app, store, keys) = setup().await;
    store
        .add_rbac_policy(
            "t1",
            PolicyRule {
                subject: "role:reader".to_string(),
                object: "stream:t1/payments/*".to_string(),
                action: "stream.subscribe".to_string(),
            },
        )
        .await
        .expect("policy");
    store
        .add_rbac_grouping(
            "t1",
            GroupingRule {
                user: "group:g1".to_string(),
                role: "role:reader".to_string(),
            },
        )
        .await
        .expect("grouping");

    let t = token(&keys, vec!["rbac.view:tenant:t1"]);

    let policies = Request::builder()
        .method("GET")
        .uri("/v1/tenants/t1/rbac/policies")
        .header("authorization", format!("Bearer {t}"))
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(policies).await.expect("policies");
    assert_eq!(response.status(), StatusCode::OK);

    let groupings = Request::builder()
        .method("GET")
        .uri("/v1/tenants/t1/rbac/groupings")
        .header("authorization", format!("Bearer {t}"))
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(groupings).await.expect("groupings");
    assert_eq!(response.status(), StatusCode::OK);

    let add_policy = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:writer",
            "object": "stream:t1/payments/orders",
            "action": "stream.publish"
        }),
    );
    let response = app.oneshot(add_auth(add_policy, &t)).await.expect("policy");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn namespace_rbac_admin_scope_is_enforced() {
    let (app, _store, keys) = setup().await;
    let t = token(&keys, vec!["rbac.policy.manage:namespace:t1/payments"]);

    let in_scope = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:payments-publisher",
            "object": "stream:t1/payments/*",
            "action": "stream.publish"
        }),
    );
    let response = app
        .clone()
        .oneshot(add_auth(in_scope, &t))
        .await
        .expect("policy");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let other_ns = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:orders-publisher",
            "object": "stream:t1/orders/*",
            "action": "stream.publish"
        }),
    );
    let response = app.oneshot(add_auth(other_ns, &t)).await.expect("policy");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn stream_rbac_admin_cannot_grant_namespace_wide_rules() {
    let (app, _store, keys) = setup().await;
    let t = token(&keys, vec!["rbac.policy.manage:stream:t1/payments/orders"]);

    let grant_namespace = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:payments-publisher",
            "object": "stream:t1/payments/*",
            "action": "stream.publish"
        }),
    );
    let response = app
        .oneshot(add_auth(grant_namespace, &t))
        .await
        .expect("policy");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn tenant_rbac_admin_can_grant_tenant_rules_but_tenant_star_is_rejected() {
    let (app, _store, keys) = setup().await;
    let t = token(&keys, vec!["rbac.policy.manage:tenant:t1"]);

    let valid = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:cache-reader",
            "object": "cache:t1/payments/*",
            "action": "cache.read"
        }),
    );
    let response = app
        .clone()
        .oneshot(add_auth(valid, &t))
        .await
        .expect("policy");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let invalid = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:admin",
            "object": "tenant:*",
            "action": "tenant.manage"
        }),
    );
    let response = app.oneshot(add_auth(invalid, &t)).await.expect("policy");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = read_json(response).await;
    assert_eq!(payload["code"], "validation_error");
}

#[tokio::test]
async fn tenant_manage_can_upsert_and_delete_idp_issuer() {
    let (app, store, keys) = setup().await;
    let t = token(&keys, vec!["tenant.manage:tenant:t1"]);

    let upsert = json_request(
        "POST",
        "/v1/tenants/t1/idp-issuers",
        serde_json::json!({
            "issuer": "https://issuer.example.com",
            "audiences": ["felix-controlplane"],
            "discovery_url": null,
            "jwks_url": "https://issuer.example.com/.well-known/jwks.json",
            "claim_mappings": {
                "subject_claim": "sub",
                "groups_claim": "groups"
            }
        }),
    );
    let response = app
        .clone()
        .oneshot(add_auth(upsert, &t))
        .await
        .expect("upsert issuer");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        store.list_idp_issuers("t1").await.expect("issuers").len(),
        1
    );

    let delete = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/idp-issuers/https:%2F%2Fissuer.example.com")
        .header("authorization", format!("Bearer {t}"))
        .body(Body::empty())
        .expect("delete issuer");
    let response = app.oneshot(delete).await.expect("delete issuer");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        store
            .list_idp_issuers("t1")
            .await
            .expect("issuers")
            .is_empty()
    );
}

#[tokio::test]
async fn idp_issuer_endpoints_require_correct_scope() {
    let (app, _store, keys) = setup().await;
    let ns_scope = token(&keys, vec!["tenant.manage:namespace:t1/payments"]);

    let upsert = json_request(
        "POST",
        "/v1/tenants/t1/idp-issuers",
        serde_json::json!({
            "issuer": "https://issuer.example.com",
            "audiences": ["felix-controlplane"],
            "discovery_url": null,
            "jwks_url": "https://issuer.example.com/.well-known/jwks.json",
            "claim_mappings": {
                "subject_claim": "sub",
                "groups_claim": "groups"
            }
        }),
    );
    let response = app
        .clone()
        .oneshot(add_auth(upsert, &ns_scope))
        .await
        .expect("upsert issuer");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let no_auth = json_request(
        "POST",
        "/v1/tenants/t1/idp-issuers",
        serde_json::json!({
            "issuer": "https://issuer.example.com",
            "audiences": ["felix-controlplane"],
            "discovery_url": null,
            "jwks_url": "https://issuer.example.com/.well-known/jwks.json",
            "claim_mappings": {
                "subject_claim": "sub",
                "groups_claim": "groups"
            }
        }),
    );
    let response = app.clone().oneshot(no_auth).await.expect("upsert no auth");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn add_policy_rejects_invalid_action_and_tenant_mismatch() {
    let (app, _store, keys) = setup().await;

    let t = token(&keys, vec!["rbac.policy.manage:tenant:t1"]);
    let invalid_action = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:writer",
            "object": "stream:t1/payments/orders",
            "action": "stream.fly"
        }),
    );
    let response = app
        .clone()
        .oneshot(add_auth(invalid_action, &t))
        .await
        .expect("invalid action");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let wrong_tid = token_for_tenant(&keys, "t2", vec!["rbac.policy.manage:tenant:t1"]);
    let valid_policy = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:writer",
            "object": "stream:t1/payments/orders",
            "action": "stream.publish"
        }),
    );
    let response = app
        .oneshot(add_auth(valid_policy, &wrong_tid))
        .await
        .expect("tenant mismatch");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn add_grouping_enforces_scope_and_role_policies() {
    let (app, store, keys) = setup().await;
    store
        .add_rbac_policy(
            "t1",
            PolicyRule {
                subject: "role:payments-reader".to_string(),
                object: "stream:t1/payments/orders".to_string(),
                action: "stream.subscribe".to_string(),
            },
        )
        .await
        .expect("seed role policy");

    let allowed = token(&keys, vec!["rbac.assignment.manage:namespace:t1/payments"]);
    let add = json_request(
        "POST",
        "/v1/tenants/t1/rbac/groupings",
        serde_json::json!({
            "user": "group:payments",
            "role": "role:payments-reader"
        }),
    );
    let response = app
        .clone()
        .oneshot(add_auth(add, &allowed))
        .await
        .expect("add grouping");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let out_of_scope = token(&keys, vec!["rbac.assignment.manage:namespace:t1/orders"]);
    let add_out_of_scope = json_request(
        "POST",
        "/v1/tenants/t1/rbac/groupings",
        serde_json::json!({
            "user": "group:payments",
            "role": "role:payments-reader"
        }),
    );
    let response = app
        .clone()
        .oneshot(add_auth(add_out_of_scope, &out_of_scope))
        .await
        .expect("add grouping out of scope");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let role_without_policies = json_request(
        "POST",
        "/v1/tenants/t1/rbac/groupings",
        serde_json::json!({
            "user": "group:payments",
            "role": "role:missing"
        }),
    );
    let response = app
        .oneshot(add_auth(role_without_policies, &allowed))
        .await
        .expect("add grouping missing role policies");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

pub(super) fn add_auth(mut request: Request<Body>, token: &str) -> Request<Body> {
    request.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {token}").parse().expect("auth header"),
    );
    request
}

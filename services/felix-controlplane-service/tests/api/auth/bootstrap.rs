use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use felix_controlplane_service::api::bootstrap::BootstrapInitializeRequest;
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{
    AppState, build_bootstrap_router, build_bootstrap_router_with_dev_tokens,
};
use felix_controlplane_service::auth::oidc::UpstreamOidcValidator;
use felix_controlplane_service::store::{
    AuthStore, ControlPlaneAuthStore, ControlPlaneStore, StoreConfig, memory::InMemoryStore,
};
use serde_json::json;
use tower::ServiceExt;

use crate::common::read_json;

fn bootstrap_state(enabled: bool, tokens: Vec<String>) -> (Arc<InMemoryStore>, AppState) {
    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(
            felix_controlplane_service::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS,
        ),
    }));
    let state_store: Arc<dyn ControlPlaneAuthStore + Send + Sync> = store.clone();
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
        store: state_store,
        oidc_validator: UpstreamOidcValidator::default(),
        bootstrap_enabled: enabled,
        bootstrap_tokens: tokens,
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    (store, state)
}

#[tokio::test]
async fn bootstrap_disabled_returns_404() {
    let (_store, state) = bootstrap_state(false, Vec::new());
    let app = build_bootstrap_router(state).into_service();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&BootstrapInitializeRequest {
                display_name: "Tenant One".to_string(),
                idp_issuers: Vec::new(),
                initial_admin_principals: vec!["p:admin".to_string()],
                policies: Vec::new(),
                groupings: Vec::new(),
            })
            .unwrap(),
        ))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload = read_json(response).await;
    assert_eq!(payload["code"], "not_enabled");
}

#[tokio::test]
async fn bootstrap_requires_token() {
    let (_store, state) = bootstrap_state(true, vec!["secret".to_string()]);
    let app = build_bootstrap_router(state).into_service();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&BootstrapInitializeRequest {
                display_name: "Tenant One".to_string(),
                idp_issuers: Vec::new(),
                initial_admin_principals: vec!["p:admin".to_string()],
                policies: Vec::new(),
                groupings: Vec::new(),
            })
            .unwrap(),
        ))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn bootstrap_initializes_tenant_and_auth() {
    let (store, state) = bootstrap_state(true, vec!["secret".to_string()]);
    let app = build_bootstrap_router(state).into_service();

    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Tenant One",
                "idp_issuers": [{
                    "issuer": "https://issuer.example.com",
                    "audiences": ["felix-controlplane"],
                    "discovery_url": null,
                    "jwks_url": "https://issuer.example.com/.well-known/jwks.json",
                    "claim_mappings": {
                        "subject_claim": "sub",
                        "groups_claim": "groups"
                    }
                }],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["status"], "initialized");

    let issuers = store.list_idp_issuers("t1").await.expect("issuers");
    assert_eq!(issuers.len(), 1);
    let policies = store.list_rbac_policies("t1").await.expect("policies");
    assert!(
        policies
            .iter()
            .any(|policy| policy.action == "tenant.manage")
    );
    assert!(
        policies
            .iter()
            .any(|policy| policy.action == "rbac.policy.manage")
    );
    let groupings = store.list_rbac_groupings("t1").await.expect("groupings");
    assert!(groupings.iter().any(|g| g.user == "p:admin"));
    let keys = store.get_tenant_signing_keys("t1").await.expect("keys");
    assert!(!keys.current.kid.is_empty());
    let bootstrapped = store
        .tenant_auth_is_bootstrapped("t1")
        .await
        .expect("bootstrapped");
    assert!(bootstrapped);

    let second = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Tenant One",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.oneshot(second).await.expect("response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload = read_json(response).await;
    assert_eq!(payload["code"], "already_initialized");
}

#[tokio::test]
async fn bootstrap_rejects_invalid_token_and_validation_errors() {
    let (_store, state) = bootstrap_state(true, vec!["secret".to_string()]);
    let app = build_bootstrap_router(state).into_service();

    let invalid_token = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "wrong")
        .body(Body::from(
            json!({
                "display_name": "Tenant One",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.clone().oneshot(invalid_token).await.expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let empty_name = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "   ",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.clone().oneshot(empty_name).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let empty_admins = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Tenant One",
                "idp_issuers": [],
                "initial_admin_principals": []
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.clone().oneshot(empty_admins).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let empty_issuer = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Tenant One",
                "idp_issuers": [{
                    "issuer": "   ",
                    "audiences": ["felix-controlplane"],
                    "discovery_url": null,
                    "jwks_url": "https://issuer.example.com/.well-known/jwks.json",
                    "claim_mappings": {
                        "subject_claim": "sub",
                        "groups_claim": "groups"
                    }
                }],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.oneshot(empty_issuer).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// Rotation is two rolling deploys, and this is the state between them: the
/// new token is `token`, the old one demoted to `previous_token`, and both
/// must work or some caller is locked out mid-rotation.
#[tokio::test]
async fn bootstrap_accepts_current_and_previous_token_during_rotation() {
    let (_store, state) = bootstrap_state(
        true,
        vec!["new-secret".to_string(), "old-secret".to_string()],
    );
    let app = build_bootstrap_router(state).into_service();

    let body = json!({
        "display_name": "Tenant One",
        "idp_issuers": [],
        "initial_admin_principals": ["p:admin"]
    });

    for (token, expected) in [
        ("old-secret", StatusCode::OK),
        // The tenant is initialized now, so the new token proves itself by
        // reaching the handler and being told 409 rather than 401.
        ("new-secret", StatusCode::CONFLICT),
        ("neither", StatusCode::UNAUTHORIZED),
    ] {
        let request = Request::builder()
            .method("POST")
            .uri("/internal/bootstrap/tenants/t1/initialize")
            .header("content-type", "application/json")
            .header("X-Felix-Bootstrap-Token", token)
            .body(Body::from(body.to_string()))
            .expect("request");
        let response = app.clone().oneshot(request).await.expect("response");
        assert_eq!(response.status(), expected, "token {token:?}");
    }
}

/// Two instances receiving the same initialize concurrently is the normal case
/// behind a load balancer, not an edge case. Exactly one may win; both callers
/// must end up agreeing on one set of signing keys.
#[tokio::test]
async fn concurrent_initializes_produce_one_winner_and_one_key_set() {
    let (store, state) = bootstrap_state(true, vec!["secret".to_string()]);
    let app = build_bootstrap_router(state).into_service();

    let request = |_: usize| {
        Request::builder()
            .method("POST")
            .uri("/internal/bootstrap/tenants/t1/initialize")
            .header("content-type", "application/json")
            .header("X-Felix-Bootstrap-Token", "secret")
            .body(Body::from(
                json!({
                    "display_name": "Tenant One",
                    "idp_issuers": [],
                    "initial_admin_principals": ["p:admin"]
                })
                .to_string(),
            ))
            .expect("request")
    };

    let mut handles = Vec::new();
    for i in 0..8 {
        let app = app.clone();
        let request = request(i);
        handles.push(tokio::spawn(async move {
            let response = app.oneshot(request).await.expect("response");
            let status = response.status();
            let payload = read_json(response).await;
            (status, payload)
        }));
    }

    let mut winners = Vec::new();
    let mut conflicts = 0;
    for handle in handles {
        let (status, payload) = handle.await.expect("join");
        match status {
            StatusCode::OK => winners.push(payload),
            StatusCode::CONFLICT => conflicts += 1,
            other => panic!("unexpected status {other}: {payload}"),
        }
    }
    assert_eq!(winners.len(), 1, "exactly one initialize may win");
    assert_eq!(conflicts, 7);

    // The winner's reported kid is the kid the tenant actually holds — the
    // failure mode this guards against is a second racer regenerating keys
    // after the winner read them.
    let keys = store.get_tenant_signing_keys("t1").await.expect("keys");
    assert_eq!(winners[0]["kid"], keys.current.kid.as_str());
}

#[tokio::test]
async fn bootstrap_missing_configured_token_returns_internal_error() {
    let (_store, state) = bootstrap_state(true, Vec::new());
    let app = build_bootstrap_router(state).into_service();

    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
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

#[tokio::test]
async fn bootstrap_existing_unbootstrapped_tenant_initializes_without_conflict() {
    let (store, state) = bootstrap_state(true, vec!["secret".to_string()]);
    store
        .create_tenant(felix_controlplane_service::model::Tenant {
            tenant_id: "t-existing".to_string(),
            display_name: "Existing".to_string(),
        })
        .await
        .expect("seed tenant");

    let app = build_bootstrap_router(state).into_service();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t-existing/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Existing",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn bootstrap_respects_preseeded_admin_policies_matching_required_scopes() {
    let (store, state) = bootstrap_state(true, vec!["secret".to_string()]);
    let app = build_bootstrap_router(state).into_service();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t2/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Tenant Two",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"],
                "policies": [
                    { "subject": "role:tenant-admin", "object": "tenant:t2", "action": "tenant.manage" },
                    { "subject": "role:tenant-admin", "object": "tenant:t2", "action": "rbac.view" },
                    { "subject": "role:tenant-admin", "object": "tenant:t2", "action": "rbac.policy.manage" },
                    { "subject": "role:tenant-admin", "object": "tenant:t2", "action": "rbac.assignment.manage" },
                    { "subject": "role:tenant-admin", "object": "namespace:t2/*", "action": "ns.manage" }
                ]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let policies = store.list_rbac_policies("t2").await.expect("policies");
    let tenant_manage = policies
        .iter()
        .filter(|policy| {
            policy.subject == "role:tenant-admin"
                && policy.object == "tenant:t2"
                && policy.action == "tenant.manage"
        })
        .count();
    assert_eq!(tenant_manage, 1);

    let ns_manage = policies
        .iter()
        .filter(|policy| {
            policy.subject == "role:tenant-admin"
                && policy.object == "namespace:t2/*"
                && policy.action == "ns.manage"
        })
        .count();
    assert_eq!(ns_manage, 1);
}

/// **A policy supplied at bootstrap cannot reach another tenant.**
///
/// The caller chooses `policies` outright, and nothing validates their scope —
/// a request initializing `t-victim-neighbour` may name `tenant:t-victim` in a
/// rule. What stops that being a cross-tenant escalation is not a check on the
/// way in but the domain on the way out: every rule is inserted into a
/// per-tenant enforcer under that tenant's domain, so a rule stored against one
/// tenant is not in the other's enforcer at all.
///
/// Asserted rather than assumed, because it is the whole answer to #127's
/// "bootstrap cannot be used to mint credentials outside its intended scope",
/// and it lives in a different file from the code that makes it true.
#[tokio::test]
async fn a_policy_naming_another_tenant_does_not_reach_it() {
    let (store, state) = bootstrap_state(true, vec!["secret".to_string()]);

    // The neighbour exists and is initialized first, so the attacker is aiming
    // at a live tenant rather than claiming an unused name.
    let app = build_bootstrap_router(state.clone()).into_service();
    let victim = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/victim/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Victim",
                "idp_issuers": [],
                "initial_admin_principals": ["p:victim-admin"]
            })
            .to_string(),
        ))
        .expect("request");
    assert_eq!(
        app.oneshot(victim).await.expect("response").status(),
        StatusCode::OK,
    );

    // Now initialize a different tenant, smuggling in a rule aimed at the
    // first one.
    let app = build_bootstrap_router(state).into_service();
    let attack = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/neighbour/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Neighbour",
                "idp_issuers": [],
                "initial_admin_principals": ["p:attacker"],
                "policies": [
                    { "subject": "p:attacker", "object": "tenant:victim", "action": "tenant.manage" },
                    { "subject": "p:attacker", "object": "tenant:*", "action": "tenant.manage" }
                ]
            })
            .to_string(),
        ))
        .expect("request");
    assert_eq!(
        app.oneshot(attack).await.expect("response").status(),
        StatusCode::OK,
    );

    // The rules were stored — nothing rejects them — but against the tenant
    // that was being initialized.
    let neighbour = store
        .list_rbac_policies("neighbour")
        .await
        .expect("policies");
    assert!(
        neighbour
            .iter()
            .any(|policy| policy.subject == "p:attacker" && policy.object == "tenant:victim"),
        "the test is not exercising anything if the rule was dropped on the way in",
    );

    // And the victim's own policy set is untouched, which is what the attacker
    // needed and did not get.
    let victim_policies = store.list_rbac_policies("victim").await.expect("policies");
    assert!(
        !victim_policies
            .iter()
            .any(|policy| policy.subject == "p:attacker"),
        "a policy supplied while initializing one tenant reached another: {victim_policies:?}",
    );

    // The enforcement side, which is where the containment actually lives: the
    // victim's enforcer is built from the victim's rules under the victim's
    // domain, so the attacker is not in it however the rule was worded.
    let enforcer = felix_controlplane_service::auth::rbac::enforcer::build_enforcer(
        &victim_policies,
        &store
            .list_rbac_groupings("victim")
            .await
            .expect("groupings"),
        "victim",
    )
    .await
    .expect("enforcer");
    let granted = felix_controlplane_service::auth::rbac::permissions::effective_permissions(
        &enforcer,
        "p:attacker",
        "victim",
    );
    assert!(
        granted.is_empty(),
        "the attacker was granted {granted:?} on a tenant they did not initialize",
    );
}

fn post(uri: &str, token: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", token)
        .body(Body::from(body.to_string()))
        .expect("request")
}

/// The `perms` claim of a Felix token, read without verifying it.
fn token_perms(token: &str) -> Vec<String> {
    use base64::Engine;
    let payload = token.split('.').nth(1).expect("payload");
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .expect("base64");
    let claims: serde_json::Value = serde_json::from_slice(&bytes).expect("claims");
    serde_json::from_value(claims["perms"].clone()).expect("perms")
}

/// **A development token needs no identity provider** and carries what RBAC
/// grants the principal it names, for a tenant bootstrap initialized.
#[tokio::test]
async fn a_dev_token_carries_what_rbac_grants_the_principal() {
    let (_store, state) = bootstrap_state(true, vec!["secret".to_string()]);
    let app = build_bootstrap_router_with_dev_tokens(state).into_service();
    let initialize = post(
        "/internal/bootstrap/tenants/t1/initialize",
        "secret",
        json!({
            "display_name": "Tenant One",
            "idp_issuers": [],
            "initial_admin_principals": ["p:admin"],
            "policies": [
                { "subject": "role:writer", "object": "stream:t1/default/*", "action": "stream.publish" }
            ],
            "groupings": [ { "user": "p:dev", "role": "role:writer" } ]
        }),
    );
    let response = app.clone().oneshot(initialize).await.expect("initialize");
    assert_eq!(response.status(), StatusCode::OK);

    let minted = post(
        "/internal/bootstrap/tenants/t1/dev-token",
        "secret",
        json!({ "principal": "p:dev" }),
    );
    let response = app.clone().oneshot(minted).await.expect("dev token");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    let perms = token_perms(payload["felix_token"].as_str().expect("token"));
    assert!(
        perms.iter().any(|perm| perm.starts_with("stream.publish:")),
        "{perms:?}"
    );
    assert!(payload["refresh_token"].as_str().is_some());

    // The bootstrap token is still required.
    let unauthorized = post(
        "/internal/bootstrap/tenants/t1/dev-token",
        "wrong",
        json!({ "principal": "p:dev" }),
    );
    let response = app.oneshot(unauthorized).await.expect("dev token");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Without the switch there is no such route.
#[tokio::test]
async fn dev_tokens_are_not_served_unless_enabled() {
    let (_store, state) = bootstrap_state(true, vec!["secret".to_string()]);
    let app = build_bootstrap_router(state).into_service();
    let minted = post(
        "/internal/bootstrap/tenants/t1/dev-token",
        "secret",
        json!({ "principal": "p:dev" }),
    );
    let response = app.oneshot(minted).await.expect("dev token");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

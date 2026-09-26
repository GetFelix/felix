//! Taking authorization back: removing RBAC rules, revoking a principal's
//! refresh tokens, and rotating a tenant's signing keys.
use axum::body::Body;
use axum::http::{Request, StatusCode};
use felix_controlplane_service::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use felix_controlplane_service::auth::refresh_token::RefreshTokenTake;
use felix_controlplane_service::store::AuthStore;
use tower::ServiceExt;

use super::admin::{add_auth, setup, token};
use crate::common::{json_request, read_json};

fn reader_policy(object: &str) -> PolicyRule {
    PolicyRule {
        subject: "role:reader".to_string(),
        object: object.to_string(),
        action: "stream.subscribe".to_string(),
    }
}

fn policy_body(rule: &PolicyRule) -> serde_json::Value {
    serde_json::json!({
        "subject": rule.subject,
        "object": rule.object,
        "action": rule.action,
    })
}

#[tokio::test]
async fn a_policy_can_be_removed_within_scope_only() {
    let (app, store, keys) = setup().await;
    let payments = reader_policy("stream:t1/payments/*");
    let orders = reader_policy("stream:t1/orders/*");
    store.add_rbac_policy("t1", payments.clone()).await.unwrap();
    store.add_rbac_policy("t1", orders.clone()).await.unwrap();
    let narrow = token(&keys, vec!["rbac.policy.manage:namespace:t1/payments"]);

    // Outside the caller's namespace: refused, and the rule stays.
    let response = app
        .clone()
        .oneshot(add_auth(
            json_request("DELETE", "/v1/tenants/t1/rbac/policies", policy_body(&orders)),
            &narrow,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = app
        .clone()
        .oneshot(add_auth(
            json_request("DELETE", "/v1/tenants/t1/rbac/policies", policy_body(&payments)),
            &narrow,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let remaining = store.list_rbac_policies("t1").await.unwrap();
    assert!(!remaining.contains(&payments));
    assert!(remaining.contains(&orders));

    // Already gone.
    let response = app
        .oneshot(add_auth(
            json_request("DELETE", "/v1/tenants/t1/rbac/policies", policy_body(&payments)),
            &narrow,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_grouping_can_be_removed_by_an_assignment_admin() {
    let (app, store, keys) = setup().await;
    store
        .add_rbac_policy("t1", reader_policy("stream:t1/payments/*"))
        .await
        .unwrap();
    let grouping = GroupingRule {
        user: "p:bob".to_string(),
        role: "role:reader".to_string(),
    };
    store.add_rbac_grouping("t1", grouping.clone()).await.unwrap();
    let body = serde_json::json!({"user": "p:bob", "role": "role:reader"});

    // Policy management is not assignment management.
    let wrong_action = token(&keys, vec!["rbac.policy.manage:tenant:t1"]);
    let response = app
        .clone()
        .oneshot(add_auth(
            json_request("DELETE", "/v1/tenants/t1/rbac/groupings", body.clone()),
            &wrong_action,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // A scope that does not cover the role's policies cannot take it back.
    let elsewhere = token(&keys, vec!["rbac.assignment.manage:namespace:t1/orders"]);
    let response = app
        .clone()
        .oneshot(add_auth(
            json_request("DELETE", "/v1/tenants/t1/rbac/groupings", body.clone()),
            &elsewhere,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let admin = token(&keys, vec!["rbac.assignment.manage:namespace:t1/payments"]);
    let response = app
        .oneshot(add_auth(
            json_request("DELETE", "/v1/tenants/t1/rbac/groupings", body),
            &admin,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        !store
            .list_rbac_groupings("t1")
            .await
            .unwrap()
            .contains(&grouping)
    );
}

#[tokio::test]
async fn revoking_a_principal_ends_its_refresh_tokens() {
    let (app, store, keys) = setup().await;
    let now = felix_controlplane_service::auth::refresh::now_secs();
    let ttl = std::time::Duration::from_secs(3600);
    let (bob, _) = felix_controlplane_service::auth::refresh::issue(
        "t1",
        "p:bob",
        Vec::new(),
        None,
        now,
        ttl,
    );
    let (carol, _) = felix_controlplane_service::auth::refresh::issue(
        "t1",
        "p:carol",
        Vec::new(),
        None,
        now,
        ttl,
    );
    store.insert_refresh_token(bob.clone()).await.unwrap();
    store.insert_refresh_token(carol.clone()).await.unwrap();
    let body = serde_json::json!({"principal_id": "p:bob"});

    let rbac_only = token(&keys, vec!["rbac.assignment.manage:tenant:t1"]);
    let response = app
        .clone()
        .oneshot(add_auth(
            json_request("POST", "/v1/tenants/t1/refresh-tokens/revoke", body.clone()),
            &rbac_only,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let admin = token(&keys, vec!["tenant.manage:tenant:t1"]);
    let response = app
        .oneshot(add_auth(
            json_request("POST", "/v1/tenants/t1/refresh-tokens/revoke", body),
            &admin,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let answer: serde_json::Value = read_json(response).await;
    assert_eq!(answer["revoked"], 1);

    assert_eq!(
        store
            .take_refresh_token("t1", &bob.token_id, now)
            .await
            .unwrap(),
        RefreshTokenTake::Unusable
    );
    assert!(matches!(
        store
            .take_refresh_token("t1", &carol.token_id, now)
            .await
            .unwrap(),
        RefreshTokenTake::Taken(_)
    ));
}

fn get(uri: &str, token: &str) -> Request<Body> {
    add_auth(
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
        token,
    )
}

fn empty(method: &str, uri: &str, token: &str) -> Request<Body> {
    add_auth(
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
        token,
    )
}

async fn jwks_kids(app: &axum::routing::RouterIntoService<Body, ()>) -> Vec<String> {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/tenants/t1/.well-known/jwks.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let jwks: serde_json::Value = read_json(response).await;
    jwks["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|key| key["kid"].as_str().unwrap().to_string())
        .collect()
}

/// Stage, activate, retire, seen from outside: the JWKS carries a key before
/// it signs, a token from the replaced key keeps working until that key is
/// retired, and the signing key itself cannot be retired.
#[tokio::test]
async fn a_signing_key_rotation_keeps_old_tokens_working_until_retired() {
    let (app, store, keys) = setup().await;
    let old_kid = keys.current.kid.clone();
    let old_token = token(&keys, vec!["tenant.manage:tenant:t1"]);

    let reader = token(&keys, vec!["rbac.view:tenant:t1"]);
    let response = app
        .clone()
        .oneshot(empty("POST", "/v1/tenants/t1/signing-keys", &reader))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = app
        .clone()
        .oneshot(empty("POST", "/v1/tenants/t1/signing-keys", &old_token))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let staged: serde_json::Value = read_json(response).await;
    assert_eq!(staged["current"], old_kid.as_str(), "staging does not sign");
    let new_kid = staged["verifying"][0].as_str().unwrap().to_string();
    let published = jwks_kids(&app).await;
    assert!(published.contains(&old_kid) && published.contains(&new_kid));

    let response = app
        .clone()
        .oneshot(empty(
            "POST",
            &format!("/v1/tenants/t1/signing-keys/{new_kid}/activate"),
            &old_token,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let rotated = store.get_tenant_signing_keys("t1").await.unwrap();
    assert_eq!(rotated.current.kid, new_kid);

    // The old key no longer signs but still verifies.
    let response = app
        .clone()
        .oneshot(get("/v1/tenants/t1/signing-keys", &old_token))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let new_token = token(&rotated, vec!["tenant.manage:tenant:t1"]);
    let response = app
        .clone()
        .oneshot(empty(
            "DELETE",
            &format!("/v1/tenants/t1/signing-keys/{new_kid}"),
            &new_token,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = app
        .clone()
        .oneshot(empty(
            "DELETE",
            &format!("/v1/tenants/t1/signing-keys/{old_kid}"),
            &new_token,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(jwks_kids(&app).await, vec![new_kid]);

    let response = app
        .oneshot(get("/v1/tenants/t1/signing-keys", &old_token))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "a retired key verifies nothing"
    );
}

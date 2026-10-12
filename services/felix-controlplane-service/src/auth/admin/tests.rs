use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::http::{HeaderMap, HeaderValue};

use super::*;
use crate::store::AuthStore;
use crate::store::memory::InMemoryStore;

async fn fixture() -> (AppState, Arc<InMemoryStore>) {
    let (store, _keys) = crate::test_support::one_shard_cluster().await;
    (crate::test_support::app_state_ready(store.clone()), store)
}

/// A tenant and cluster operator's credential.
async fn operator(store: &InMemoryStore) -> HeaderMap {
    let keys = store.get_tenant_signing_keys("t1").await.expect("keys");
    let token = crate::test_support::token(
        &keys,
        &[
            "tenant.manage:tenant:t1",
            "tenant.manage:cluster:*",
            "rbac.policy.manage:tenant:t1",
        ],
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
    );
    headers
}

fn issuer(issuer: &str, groups_claim: &str) -> IdpIssuerConfig {
    serde_json::from_value(serde_json::json!({
        "issuer": issuer,
        "audiences": ["felix"],
        "jwks_url": format!("{issuer}/jwks"),
        "claim_mappings": {"subject_claim": "sub", "groups_claim": groups_claim},
    }))
    .expect("issuer config")
}

/// Changing what an issuer's tokens are read as, or deleting the issuer,
/// takes back grants made through its groups, so it revokes the whole
/// tenant, as removing a rule that reached a group does.
#[tokio::test]
async fn changing_an_issuer_revokes_the_tenant() {
    let (state, store) = fixture().await;
    let headers = operator(&store).await;
    store
        .upsert_idp_issuer("t1", issuer("https://idp.example", "groups"))
        .await
        .expect("issuer");

    upsert(&state, &headers, issuer("https://idp.example", "roles")).await;
    let changed = store
        .principal_revoked_at("t1", "anyone")
        .await
        .expect("read");
    assert!(changed.is_some(), "a changed issuer revokes the tenant");
}

#[tokio::test]
async fn deleting_an_issuer_revokes_the_tenant() {
    let (state, store) = fixture().await;
    let headers = operator(&store).await;
    store
        .upsert_idp_issuer("t1", issuer("https://idp.example", "groups"))
        .await
        .expect("issuer");
    delete_idp_issuer(
        Path(("t1".to_string(), "https://idp.example".to_string())),
        State(state.clone()),
        headers,
    )
    .await
    .expect("deleted");
    let deleted = store.principal_revocations(0).await.expect("feed").items;
    assert!(
        deleted
            .iter()
            .any(|row| row.principal_id == crate::auth::revocation::ALL_PRINCIPALS),
        "{deleted:?}"
    );
}

/// A removal and its revocation are one write: when it fails, the rule is
/// still there and nobody was revoked, so the caller's retry does both.
#[tokio::test]
async fn a_removal_and_its_revocation_succeed_or_fail_together() {
    let (state, store) = fixture().await;
    let headers = operator(&store).await;
    let rule = PolicyRule {
        subject: "p:alice".to_string(),
        object: "stream:t1/ns/feed".to_string(),
        action: "stream.publish".to_string(),
    };
    let remove = || {
        remove_policy(
            Path("t1".to_string()),
            State(state.clone()),
            headers.clone(),
            Json(PolicyRequest {
                subject: rule.subject.clone(),
                object: rule.object.clone(),
                action: rule.action.clone(),
            }),
        )
    };
    store
        .add_rbac_policy("t1", rule.clone())
        .await
        .expect("policy");

    store.faults.revocation_writes.store(1, Ordering::SeqCst);
    let err = remove().await.expect_err("the write failed");
    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
    let policies = store.list_rbac_policies("t1").await.expect("policies");
    assert!(policies.contains(&rule), "removed without its revocation");
    assert_eq!(
        store
            .principal_revoked_at("t1", "p:alice")
            .await
            .expect("read"),
        None
    );

    remove().await.expect("the retry does both");
    assert!(
        store
            .principal_revoked_at("t1", "p:alice")
            .await
            .expect("read")
            .is_some()
    );
}

async fn upsert(state: &AppState, headers: &HeaderMap, config: IdpIssuerConfig) {
    upsert_idp_issuer(
        Path("t1".to_string()),
        State(state.clone()),
        headers.clone(),
        Json(config),
    )
    .await
    .expect("upserted");
}

/// Widening an issuer takes nothing back, so it revokes nobody. Dropping an
/// audience does.
#[tokio::test]
async fn only_a_narrowed_issuer_revokes_the_tenant() {
    let (state, store) = fixture().await;
    let headers = operator(&store).await;
    let mut config = issuer("https://idp.example", "groups");
    store
        .upsert_idp_issuer("t1", config.clone())
        .await
        .expect("issuer");

    config.audiences.push("another".to_string());
    upsert(&state, &headers, config.clone()).await;
    assert_eq!(
        store
            .principal_revoked_at("t1", "anyone")
            .await
            .expect("read"),
        None,
        "an added audience narrows nothing"
    );

    config.audiences.retain(|audience| audience == "another");
    upsert(&state, &headers, config).await;
    assert!(
        store
            .principal_revoked_at("t1", "anyone")
            .await
            .expect("read")
            .is_some(),
        "a dropped audience revokes the tenant"
    );
}

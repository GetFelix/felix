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

    upsert_idp_issuer(
        Path("t1".to_string()),
        State(state.clone()),
        headers.clone(),
        Json(issuer("https://idp.example", "roles")),
    )
    .await
    .expect("changed");
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

/// A removal whose revocation cannot be recorded is answered as an error
/// after the retries, so the caller knows the sessions kept their rights.
#[tokio::test]
async fn a_removal_whose_revocation_fails_is_an_error() {
    let (state, store) = fixture().await;
    let headers = operator(&store).await;
    let rule = || PolicyRule {
        subject: "p:alice".to_string(),
        object: "stream:t1/ns/feed".to_string(),
        action: "stream.publish".to_string(),
    };
    let remove = |state: AppState, headers: HeaderMap| {
        remove_policy(
            Path("t1".to_string()),
            State(state),
            headers,
            Json(PolicyRequest {
                subject: rule().subject,
                object: rule().object,
                action: rule().action,
            }),
        )
    };

    // One failure is retried.
    store.add_rbac_policy("t1", rule()).await.expect("policy");
    store.faults.revocation_writes.store(1, Ordering::SeqCst);
    remove(state.clone(), headers.clone())
        .await
        .expect("retried");
    assert!(
        store
            .principal_revoked_at("t1", "p:alice")
            .await
            .expect("read")
            .is_some()
    );

    // Failures past the retries are answered.
    store.add_rbac_policy("t1", rule()).await.expect("policy");
    store
        .faults
        .revocation_writes
        .store(u32::MAX, Ordering::SeqCst);
    let err = remove(state, headers).await.expect_err("not recorded");
    store.faults.revocation_writes.store(0, Ordering::SeqCst);
    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
}

use std::sync::Arc;

use anyhow::Result;
use axum::http::HeaderValue;
use axum::http::header::AUTHORIZATION;
use base64::Engine;
use serde_json::json;

use super::*;
use crate::api::AppState;
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::config::{DEFAULT_CHANGE_RETENTION_MAX_ROWS, DEFAULT_CHANGES_LIMIT};
use crate::store::memory::InMemoryStore;
use crate::store::{AuthStore, ControlPlaneStore, StoreConfig};

#[test]
fn filters_by_requested_actions() {
    let perms = vec![
        "stream.publish:stream:t1/payments/*".to_string(),
        "stream.subscribe:stream:t1/payments/*".to_string(),
    ];
    let request = TokenExchangeRequest {
        requested: Some(vec!["stream.publish".to_string()]),
        resources: None,
    };
    let filtered = filter_permissions(perms, &request, "t1");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0], "stream.publish:stream:t1/payments/*");
}

fn narrow(perms: &[&str], resources: &[&str]) -> Vec<String> {
    let request = TokenExchangeRequest {
        resources: Some(resources.iter().map(|value| value.to_string()).collect()),
        ..Default::default()
    };
    let mut narrowed = filter_permissions(
        perms.iter().map(|value| value.to_string()).collect(),
        &request,
        "t1",
    );
    narrowed.sort();
    narrowed
}

/// Asking for one stream out of a namespace grant yields that stream, not the
/// namespace grant unchanged.
#[test]
fn a_resource_hint_narrows_a_broader_grant() {
    assert_eq!(
        narrow(
            &["stream.publish:stream:t1/payments/*"],
            &["stream:t1/payments/orders"]
        ),
        vec!["stream.publish:stream:t1/payments/orders"]
    );
    assert_eq!(
        narrow(
            &["stream.subscribe:stream:t1/*/*"],
            &["namespace:t1/payments"]
        ),
        vec!["stream.subscribe:stream:t1/payments/*"]
    );
}

#[test]
fn a_broader_hint_keeps_a_narrower_grant_as_is() {
    assert_eq!(
        narrow(
            &["stream.publish:stream:t1/payments/orders"],
            &["namespace:t1/payments"]
        ),
        vec!["stream.publish:stream:t1/payments/orders"]
    );
    assert_eq!(
        narrow(&["tenant.manage:tenant:t1"], &["tenant:t1"]),
        vec!["tenant.manage:tenant:t1"]
    );
}

/// A hint never widens: outside every grant, unparseable, or for another
/// tenant, it yields nothing.
#[test]
fn a_resource_hint_never_widens() {
    assert!(narrow(&["stream.publish:stream:t1/payments/*"], &["stream:t1/orders/x"]).is_empty());
    assert!(narrow(&["stream.publish:stream:t1/payments/*"], &["stream:t2/payments/x"]).is_empty());
    assert!(narrow(&["stream.publish:stream:t1/payments/*"], &["not an object"]).is_empty());
    // A stream hint does not turn a tenant grant into a stream-shaped one.
    assert!(narrow(&["tenant.manage:tenant:t1"], &["stream:t1/payments/orders"]).is_empty());
}

#[test]
fn adds_group_claim_groupings_with_group_prefix() {
    let mut groupings = Vec::new();
    let principal = "p:user-1";
    let groups = vec!["g1".to_string(), "ops".to_string()];
    add_group_claim_groupings(&mut groupings, principal, &groups);

    assert!(groupings.contains(&GroupingRule {
        user: principal.to_string(),
        role: "group:g1".to_string(),
    }));
    assert!(groupings.contains(&GroupingRule {
        user: principal.to_string(),
        role: "group:ops".to_string(),
    }));
}

/// An IdP group literally named `group:operators` is not the `operators`
/// group. Collapsing them would let whoever can name a group at the IdP, but
/// not take an existing name, borrow that group's grants.
#[test]
fn a_group_named_like_a_subject_stays_distinct() {
    let principal = "p:user-1";
    let mut plain = Vec::new();
    add_group_claim_groupings(&mut plain, principal, &["operators".to_string()]);
    let mut lookalike = Vec::new();
    add_group_claim_groupings(&mut lookalike, principal, &["group:operators".to_string()]);

    assert_eq!(plain[0].role, "group:operators");
    assert_ne!(
        lookalike[0].role, plain[0].role,
        "`group:operators` from the IdP was mapped onto the `operators` group",
    );
}

#[test]
fn dedupes_group_claim_groupings_against_existing_and_duplicates() {
    let principal = "p:user-1";
    let mut groupings = vec![GroupingRule {
        user: principal.to_string(),
        role: "group:g1".to_string(),
    }];
    let groups = vec!["g1".to_string(), "g1".to_string()];

    add_group_claim_groupings(&mut groupings, principal, &groups);

    assert_eq!(
        groupings
            .iter()
            .filter(|grouping| grouping.user == principal && grouping.role == "group:g1")
            .count(),
        1
    );
}

fn test_state(store: Arc<InMemoryStore>) -> AppState {
    crate::test_support::app_state_ready(store)
}

fn store_config() -> StoreConfig {
    StoreConfig {
        changes_limit: DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    }
}

fn bearer_header(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let value = format!("Bearer {token}");
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&value).expect("auth header"),
    );
    headers
}

fn unsigned_es256_token(issuer: &str, kid: &str) -> String {
    let header = json!({
        "alg": "ES256",
        "kid": kid,
        "typ": "JWT"
    });
    let payload = json!({
        "iss": issuer
    });
    let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&header).expect("header json"));
    let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&payload).expect("payload json"));
    format!("{header_b64}.{payload_b64}.sig")
}

#[tokio::test]
async fn exchange_token_rejects_missing_bearer() {
    let store = Arc::new(InMemoryStore::new(store_config()));
    let state = test_state(store);
    let headers = HeaderMap::new();
    let err = exchange_token(Path("t1".to_string()), State(state), headers, None)
        .await
        .expect_err("missing bearer");
    assert_eq!(err.status, axum::http::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn exchange_token_rejects_unknown_tenant() {
    let store = Arc::new(InMemoryStore::new(store_config()));
    let state = test_state(store);
    let token = unsigned_es256_token("https://issuer.example", "kid1");
    let headers = bearer_header(&token);
    let err = exchange_token(Path("t1".to_string()), State(state), headers, None)
        .await
        .expect_err("unknown tenant");
    assert_eq!(err.status, axum::http::StatusCode::FORBIDDEN);
    assert!(err.body.message.contains("tenant not allowed"));
}

#[tokio::test]
async fn exchange_token_rejects_when_no_issuers_configured() -> Result<()> {
    let store = Arc::new(InMemoryStore::new(store_config()));
    store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant".to_string(),
        })
        .await?;
    let state = test_state(store);
    let token = unsigned_es256_token("https://issuer.example", "kid1");
    let headers = bearer_header(&token);
    let err = exchange_token(Path("t1".to_string()), State(state), headers, None)
        .await
        .expect_err("no issuers");
    assert_eq!(err.status, axum::http::StatusCode::FORBIDDEN);
    assert!(err.body.message.contains("no issuers configured"));
    Ok(())
}

#[tokio::test]
async fn exchange_token_rejects_issuer_not_allowed() -> Result<()> {
    let store = Arc::new(InMemoryStore::new(store_config()));
    store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant".to_string(),
        })
        .await?;
    store
        .upsert_idp_issuer(
            "t1",
            IdpIssuerConfig {
                issuer: "https://issuer.allowed".to_string(),
                audiences: vec!["aud".to_string()],
                discovery_url: None,
                jwks_url: None,
                claim_mappings: crate::auth::idp_registry::ClaimMappings::default(),
            },
        )
        .await?;
    let state = test_state(store);
    let token = unsigned_es256_token("https://issuer.denied", "kid1");
    let headers = bearer_header(&token);
    let err = exchange_token(Path("t1".to_string()), State(state), headers, None)
        .await
        .expect_err("issuer not allowed");
    assert_eq!(err.status, axum::http::StatusCode::FORBIDDEN);
    assert!(err.body.message.contains("issuer not allowed"));
    Ok(())
}

/// Exchange refusals land in the refused-credentials counter like every other
/// control-plane credential check does.
#[test]
fn exchange_refusals_are_counted() {
    let recorder = crate::test_support::CountingRecorder::default();
    recorder.run(async {
        let store = Arc::new(InMemoryStore::new(store_config()));
        let state = test_state(store.clone());
        let _ = exchange_token(
            Path("t1".to_string()),
            State(state.clone()),
            HeaderMap::new(),
            None,
        )
        .await;
        let token = unsigned_es256_token("https://issuer.example", "kid1");
        let _ = exchange_token(
            Path("t1".to_string()),
            State(state.clone()),
            bearer_header(&token),
            None,
        )
        .await;

        store
            .create_tenant(crate::model::Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant".to_string(),
            })
            .await
            .expect("tenant");
        store
            .upsert_idp_issuer(
                "t1",
                IdpIssuerConfig {
                    issuer: "https://issuer.allowed".to_string(),
                    audiences: vec!["aud".to_string()],
                    discovery_url: None,
                    jwks_url: None,
                    claim_mappings: crate::auth::idp_registry::ClaimMappings::default(),
                },
            )
            .await
            .expect("issuer");
        let err = exchange_token(
            Path("t1".to_string()),
            State(state),
            bearer_header("not-a-jwt"),
            None,
        )
        .await
        .expect_err("garbage token");
        assert_eq!(err.status, axum::http::StatusCode::UNAUTHORIZED);
    });

    let rejected = |reason: &str| {
        recorder.count(&format!(
            "felix_controlplane_auth_rejected_total{{reason={reason}}}"
        ))
    };
    assert_eq!(rejected("missing_token"), 1);
    assert_eq!(rejected("forbidden"), 1, "unknown tenant");
    assert_eq!(rejected("invalid_token"), 1);
}

use std::time::Duration;

use axum::http::{HeaderMap, HeaderValue, StatusCode};

use super::*;
use crate::auth::felix_token::{
    CONTROLPLANE_AUDIENCE, TenantSigningKeys, mint_token_for, verify_token,
};
use crate::store::{AuthStore, ControlPlaneStore};

const ALICE_FEED: &str = "stream.publish:stream:t1/ns/alice-feed";
const ALICE_READ: &str = "stream.subscribe:stream:t1/ns/alice-feed";

struct Fixture {
    state: AppState,
    keys: TenantSigningKeys,
    other_keys: TenantSigningKeys,
}

/// Tenants `t1` and `t2`, each with signing keys.
async fn fixture() -> Fixture {
    let (store, keys) = crate::test_support::one_shard_cluster().await;
    store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t2".to_string(),
            display_name: "T2".to_string(),
        })
        .await
        .expect("tenant");
    let other_keys = crate::auth::keys::generate_signing_keys().expect("keys");
    store
        .set_tenant_signing_keys("t2", other_keys.clone())
        .await
        .expect("keys");
    Fixture {
        state: crate::test_support::app_state_ready(store),
        keys,
        other_keys,
    }
}

/// The gateway's control-plane credential (`p:test`) carrying `perms`.
fn caller(keys: &TenantSigningKeys, perms: &[&str]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let token = crate::test_support::token(keys, perms);
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
    );
    headers
}

fn alice_token(keys: &TenantSigningKeys, tenant_id: &str, ttl: Duration) -> String {
    mint_token_for(
        keys,
        tenant_id,
        "p:alice",
        vec![ALICE_FEED.to_string(), ALICE_READ.to_string()],
        ttl,
        BROKER_AUDIENCE,
    )
    .expect("token")
}

fn request(subject_token: String) -> TokenDelegateRequest {
    TokenDelegateRequest {
        grant_type: TOKEN_EXCHANGE_GRANT.to_string(),
        subject_token,
        subject_token_type: Some(JWT_TOKEN_TYPE.to_string()),
        permissions: None,
    }
}

async fn delegate(
    fixture: &Fixture,
    tenant_id: &str,
    headers: HeaderMap,
    request: TokenDelegateRequest,
) -> Result<TokenDelegateResponse, ApiError> {
    delegate_token(
        Path(tenant_id.to_string()),
        State(fixture.state.clone()),
        headers,
        Json(request),
    )
    .await
    .map(|Json(response)| response)
}

fn status(result: Result<TokenDelegateResponse, ApiError>) -> StatusCode {
    match result {
        Ok(_) => StatusCode::OK,
        Err(err) => err.status,
    }
}

#[tokio::test]
async fn a_delegated_token_is_the_users_scope_with_the_caller_as_actor() {
    let fixture = fixture().await;
    let user_ttl = Duration::from_secs(120);
    let response = delegate(
        &fixture,
        "t1",
        caller(&fixture.keys, &["token.delegate:tenant:t1"]),
        request(alice_token(&fixture.keys, "t1", user_ttl)),
    )
    .await
    .expect("delegated");
    assert_eq!(response.issued_token_type, JWT_TOKEN_TYPE);
    assert!(response.expires_in <= user_ttl.as_secs());

    let claims = verify_token(&fixture.keys, "t1", &response.access_token, 0).expect("verify");
    assert_eq!(claims.sub, "p:alice");
    assert_eq!(claims.aud, BROKER_AUDIENCE);
    assert_eq!(
        claims.act.as_ref().map(|actor| actor.sub.as_str()),
        Some("p:test")
    );
    let mut perms = claims.perms.clone();
    perms.sort();
    assert_eq!(perms, vec![ALICE_FEED.to_string(), ALICE_READ.to_string()]);
    let user = verify_token(
        &fixture.keys,
        "t1",
        &alice_token(&fixture.keys, "t1", user_ttl),
        0,
    )
    .expect("verify");
    assert!(claims.exp <= user.exp, "outlives the user's token");
}

#[tokio::test]
async fn a_narrowing_never_widens_the_users_scope() {
    let fixture = fixture().await;
    let headers = || caller(&fixture.keys, &["token.delegate:tenant:t1"]);
    let mut narrowed = request(alice_token(&fixture.keys, "t1", Duration::from_secs(60)));
    narrowed.permissions = Some(vec!["stream.publish:stream:t1/*/*".to_string()]);
    let response = delegate(&fixture, "t1", headers(), narrowed)
        .await
        .expect("delegated");
    let claims = verify_token(&fixture.keys, "t1", &response.access_token, 0).expect("verify");
    assert_eq!(claims.perms, vec![ALICE_FEED.to_string()]);

    let mut wider = request(alice_token(&fixture.keys, "t1", Duration::from_secs(60)));
    wider.permissions = Some(vec!["cache.write:cache:t1/*/*".to_string()]);
    assert_eq!(
        status(delegate(&fixture, "t1", headers(), wider).await),
        StatusCode::FORBIDDEN,
        "a permission alice does not hold"
    );
}

#[tokio::test]
async fn a_caller_without_token_delegate_is_refused() {
    let fixture = fixture().await;
    for perms in [&[][..], &["tenant.manage:tenant:t1"][..], &[ALICE_FEED][..]] {
        let result = delegate(
            &fixture,
            "t1",
            caller(&fixture.keys, perms),
            request(alice_token(&fixture.keys, "t1", Duration::from_secs(60))),
        )
        .await;
        assert_eq!(status(result), StatusCode::FORBIDDEN, "{perms:?}");
    }
}

#[tokio::test]
async fn another_tenants_token_is_refused() {
    let fixture = fixture().await;
    // A t2 user's token, presented to t1.
    let result = delegate(
        &fixture,
        "t1",
        caller(&fixture.keys, &["token.delegate:tenant:t1"]),
        request(alice_token(
            &fixture.other_keys,
            "t2",
            Duration::from_secs(60),
        )),
    )
    .await;
    assert_eq!(status(result), StatusCode::FORBIDDEN);
    // A t1 caller acting on t2.
    let result = delegate(
        &fixture,
        "t2",
        caller(&fixture.keys, &["token.delegate:tenant:t1"]),
        request(alice_token(
            &fixture.other_keys,
            "t2",
            Duration::from_secs(60),
        )),
    )
    .await;
    assert!(status(result).is_client_error());
}

#[tokio::test]
async fn only_a_users_own_broker_token_can_be_delegated() {
    let fixture = fixture().await;
    let headers = || caller(&fixture.keys, &["token.delegate:tenant:t1"]);
    // Already delegated: one hop only.
    let delegated = delegate(
        &fixture,
        "t1",
        headers(),
        request(alice_token(&fixture.keys, "t1", Duration::from_secs(60))),
    )
    .await
    .expect("delegated");
    assert_eq!(
        status(delegate(&fixture, "t1", headers(), request(delegated.access_token)).await),
        StatusCode::FORBIDDEN
    );
    // A control-plane token is not a broker token.
    let control = crate::test_support::token(&fixture.keys, &[ALICE_FEED]);
    assert_eq!(
        status(delegate(&fixture, "t1", headers(), request(control)).await),
        StatusCode::FORBIDDEN
    );
    let mut wrong_grant = request(alice_token(&fixture.keys, "t1", Duration::from_secs(60)));
    wrong_grant.grant_type = "client_credentials".to_string();
    assert_eq!(
        status(delegate(&fixture, "t1", headers(), wrong_grant).await),
        StatusCode::BAD_REQUEST
    );
}

/// Brokers enforce nothing with `token.delegate`, and one that predates it
/// would refuse a token naming it.
#[test]
fn broker_tokens_never_carry_token_delegate() {
    let perms = vec![
        "token.delegate:tenant:t1".to_string(),
        ALICE_FEED.to_string(),
    ];
    assert_eq!(
        for_audience(perms.clone(), BROKER_AUDIENCE),
        vec![ALICE_FEED.to_string()]
    );
    assert_eq!(for_audience(perms.clone(), CONTROLPLANE_AUDIENCE), perms);
}

#[test]
fn the_response_never_shows_its_token() {
    let response = TokenDelegateResponse {
        access_token: "secret".to_string(),
        issued_token_type: JWT_TOKEN_TYPE.to_string(),
        token_type: "Bearer".to_string(),
        expires_in: 1,
    };
    assert!(!format!("{response:?}").contains("secret"));
}

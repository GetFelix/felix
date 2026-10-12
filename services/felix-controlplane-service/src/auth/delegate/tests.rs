use std::time::Duration;

use axum::http::{HeaderMap, HeaderValue, StatusCode};

use super::*;
use crate::auth::exchange::{TokenExchangeRequest, TokenExchangeResponse, mint_for_principal};
use crate::auth::felix_token::{
    CONTROLPLANE_AUDIENCE, TenantSigningKeys, mint_token_for, mint_token_may_act, verify_token,
};
use crate::auth::rbac::policy_store::PolicyRule;
use crate::auth::refresh::{TokenRefreshRequest, refresh_token_handler};
use crate::store::{AuthStore, ControlPlaneStore};

const ALICE_FEED: &str = "stream.publish:stream:t1/ns/alice-feed";
const ALICE_READ: &str = "stream.subscribe:stream:t1/ns/alice-feed";

struct Fixture {
    state: AppState,
    memory: std::sync::Arc<crate::store::memory::InMemoryStore>,
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
    // Both gateways the tests use hold `token.delegate` in current RBAC too.
    for gateway in ["p:test", "p:gateway-b"] {
        store
            .add_rbac_policy("t1", delegate_grant(gateway))
            .await
            .expect("policy");
    }
    Fixture {
        state: crate::test_support::app_state_ready(store.clone()),
        memory: store,
        keys,
        other_keys,
    }
}

fn delegate_grant(subject: &str) -> PolicyRule {
    PolicyRule {
        subject: subject.to_string(),
        object: "tenant:t1".to_string(),
        action: "token.delegate".to_string(),
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

/// Another gateway's control-plane credential, `p:gateway-b`, that may also
/// delegate.
fn gateway_b(keys: &TenantSigningKeys) -> HeaderMap {
    let token = mint_token_for(
        keys,
        "t1",
        "p:gateway-b",
        vec!["token.delegate:tenant:t1".to_string()],
        Duration::from_secs(900),
        CONTROLPLANE_AUDIENCE,
    )
    .expect("token");
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
    );
    headers
}

/// Alice's broker token, minted for the gateway `p:test` to delegate.
fn alice_token(keys: &TenantSigningKeys, tenant_id: &str, ttl: Duration) -> String {
    alice_token_for(keys, tenant_id, ttl, Some("p:test"))
}

fn alice_token_for(
    keys: &TenantSigningKeys,
    tenant_id: &str,
    ttl: Duration,
    may_act: Option<&str>,
) -> String {
    mint_token_may_act(
        keys,
        tenant_id,
        "p:alice",
        vec![ALICE_FEED.to_string(), ALICE_READ.to_string()],
        ttl,
        BROKER_AUDIENCE,
        may_act,
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

#[tokio::test]
async fn a_token_minted_for_one_gateway_cannot_be_delegated_by_another() {
    let fixture = fixture().await;
    // Minted for `p:test`, presented by `p:gateway-b`, which also holds
    // token.delegate: a token lifted from a browser or from gateway A.
    let stolen = alice_token(&fixture.keys, "t1", Duration::from_secs(60));
    let result = delegate(
        &fixture,
        "t1",
        gateway_b(&fixture.keys),
        request(stolen.clone()),
    )
    .await;
    assert_eq!(status(result), StatusCode::FORBIDDEN);
    // The gateway it was minted for can.
    delegate(
        &fixture,
        "t1",
        caller(&fixture.keys, &["token.delegate:tenant:t1"]),
        request(stolen),
    )
    .await
    .expect("delegated by its own gateway");
}

#[tokio::test]
async fn a_token_that_names_no_actor_is_refused_by_default() {
    let fixture = fixture().await;
    let unbound = alice_token_for(&fixture.keys, "t1", Duration::from_secs(60), None);
    let result = delegate(
        &fixture,
        "t1",
        caller(&fixture.keys, &["token.delegate:tenant:t1"]),
        request(unbound.clone()),
    )
    .await;
    assert_eq!(status(result), StatusCode::FORBIDDEN);

    // Only the explicit switch lets it through, and it never lets a token
    // bound to someone else through.
    let claims = verify_token(&fixture.keys, "t1", &unbound, 0).expect("verify");
    assert!(check_may_act(&claims, "p:test", true).is_ok());
    let bound = verify_token(
        &fixture.keys,
        "t1",
        &alice_token(&fixture.keys, "t1", Duration::from_secs(60)),
        0,
    )
    .expect("verify");
    assert!(check_may_act(&bound, "p:gateway-b", true).is_err());
}

/// The whole path: alice's sign-in exchanged with the gateway's credential as
/// `actor_token`, refreshed, then delegated.
#[tokio::test]
async fn an_exchange_with_an_actor_token_mints_a_token_only_that_actor_can_delegate() {
    let fixture = fixture().await;
    for action in ["stream.publish", "stream.subscribe"] {
        fixture
            .state
            .store
            .add_rbac_policy(
                "t1",
                PolicyRule {
                    subject: "p:alice".to_string(),
                    object: "stream:t1/ns/alice-feed".to_string(),
                    action: action.to_string(),
                },
            )
            .await
            .expect("policy");
    }
    let gateway = crate::test_support::token(&fixture.keys, &["token.delegate:tenant:t1"]);
    let exchanged = mint_for_principal(
        &fixture.state,
        "t1",
        "p:alice",
        &[],
        &TokenExchangeRequest {
            actor_token: Some(gateway.clone()),
            actor_token_type: Some(JWT_TOKEN_TYPE.to_string()),
            ..TokenExchangeRequest::default()
        },
        BROKER_AUDIENCE,
    )
    .await
    .expect("exchanged");
    let claims = verify_token(&fixture.keys, "t1", &exchanged.felix_token, 0).expect("verify");
    assert_eq!(
        claims.may_act.map(|actor| actor.sub),
        Some("p:test".to_string())
    );

    // A refreshed token keeps it, so the gateway can delegate again after
    // each refresh.
    let Json(refreshed) = refresh_token_handler(
        Path("t1".to_string()),
        State(fixture.state.clone()),
        Json(TokenRefreshRequest {
            refresh_token: exchanged.refresh_token,
            audience: None,
        }),
    )
    .await
    .expect("refreshed");
    for token in [exchanged.felix_token, refreshed.felix_token] {
        assert_eq!(
            status(
                delegate(
                    &fixture,
                    "t1",
                    gateway_b(&fixture.keys),
                    request(token.clone())
                )
                .await
            ),
            StatusCode::FORBIDDEN,
            "another gateway"
        );
        delegate(
            &fixture,
            "t1",
            caller(&fixture.keys, &["token.delegate:tenant:t1"]),
            request(token),
        )
        .await
        .expect("delegated by the actor it names");
    }
}

/// Every way the gateway's own `actor_token` can be refused answers
/// `actor_refused`, so the gateway can tell it from a user who lacks
/// permissions, which stays `forbidden`.
#[test]
fn a_refused_actor_token_has_its_own_code() {
    let recorder = crate::test_support::CountingRecorder::default();
    recorder.run(async {
        let fixture = fixture().await;
        fixture
            .state
            .store
            .add_rbac_policy(
                "t1",
                PolicyRule {
                    subject: "p:alice".to_string(),
                    object: "stream:t1/ns/alice-feed".to_string(),
                    action: "stream.publish".to_string(),
                },
            )
            .await
            .expect("policy");
        let exchange = |principal: &'static str, actor_token: String, audience: &'static str| {
            let state = fixture.state.clone();
            async move {
                mint_for_principal(
                    &state,
                    "t1",
                    principal,
                    &[],
                    &TokenExchangeRequest {
                        actor_token: Some(actor_token),
                        ..TokenExchangeRequest::default()
                    },
                    audience,
                )
                .await
                .map(|_| ())
                .map_err(|err| (err.status, err.body.code))
            }
        };
        let actor_refused = Err((StatusCode::FORBIDDEN, "actor_refused".to_string()));

        let without = crate::test_support::token(&fixture.keys, &[ALICE_FEED]);
        assert_eq!(
            exchange("p:alice", without, BROKER_AUDIENCE).await,
            actor_refused
        );
        // A broker token is not a control-plane credential.
        let broker = alice_token(&fixture.keys, "t1", Duration::from_secs(60));
        assert_eq!(
            exchange("p:alice", broker, BROKER_AUDIENCE).await,
            actor_refused
        );
        let wrong_key =
            crate::test_support::token(&fixture.other_keys, &["token.delegate:tenant:t1"]);
        assert_eq!(
            exchange("p:alice", wrong_key, BROKER_AUDIENCE).await,
            actor_refused
        );
        let other_tenant = mint_token_for(
            &fixture.other_keys,
            "t2",
            "p:test",
            vec!["token.delegate:tenant:t2".to_string()],
            Duration::from_secs(60),
            CONTROLPLANE_AUDIENCE,
        )
        .expect("token");
        assert_eq!(
            exchange("p:alice", other_tenant, BROKER_AUDIENCE).await,
            actor_refused
        );
        assert_eq!(
            exchange("p:alice", "not-a-jwt".to_string(), BROKER_AUDIENCE).await,
            actor_refused
        );

        let gateway = crate::test_support::token(&fixture.keys, &["token.delegate:tenant:t1"]);
        assert_eq!(
            exchange("p:bob", gateway.clone(), BROKER_AUDIENCE).await,
            Err((StatusCode::FORBIDDEN, "forbidden".to_string())),
            "a user without permissions is not the gateway's fault"
        );
        assert_eq!(
            exchange("p:alice", gateway.clone(), CONTROLPLANE_AUDIENCE)
                .await
                .map_err(|(status, _)| status),
            Err(StatusCode::BAD_REQUEST),
            "only broker tokens are delegated"
        );
        exchange("p:alice", gateway, BROKER_AUDIENCE)
            .await
            .expect("a gateway that may delegate");
    });
    assert_eq!(
        recorder.count("felix_controlplane_auth_rejected_total{reason=actor_refused}"),
        5
    );
    assert_eq!(
        recorder.count("felix_controlplane_auth_rejected_total{reason=forbidden}"),
        1
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

fn now_secs() -> i64 {
    crate::auth::refresh::now_secs()
}

/// `p:test`'s control-plane token holding `token.delegate`, issued at `iat`.
fn gateway_token_issued_at(keys: &TenantSigningKeys, iat: i64) -> String {
    crate::auth::felix_token::sign_claims(
        keys,
        "t1",
        &FelixClaims {
            iss: crate::auth::felix_token::ISSUER.to_string(),
            aud: CONTROLPLANE_AUDIENCE.to_string(),
            sub: "p:test".to_string(),
            tid: "t1".to_string(),
            exp: iat + 900,
            iat,
            jti: None,
            perms: vec!["token.delegate:tenant:t1".to_string()],
            act: None,
            may_act: None,
        },
    )
    .expect("token")
}

fn bearer(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
    );
    headers
}

async fn grant_alice(fixture: &Fixture) {
    for action in ["stream.publish", "stream.subscribe"] {
        fixture
            .state
            .store
            .add_rbac_policy(
                "t1",
                PolicyRule {
                    subject: "p:alice".to_string(),
                    object: "stream:t1/ns/alice-feed".to_string(),
                    action: action.to_string(),
                },
            )
            .await
            .expect("policy");
    }
}

async fn exchange_for_alice(
    fixture: &Fixture,
    actor_token: String,
) -> Result<TokenExchangeResponse, ApiError> {
    mint_for_principal(
        &fixture.state,
        "t1",
        "p:alice",
        &[],
        &TokenExchangeRequest {
            actor_token: Some(actor_token),
            actor_token_type: Some(JWT_TOKEN_TYPE.to_string()),
            ..TokenExchangeRequest::default()
        },
        BROKER_AUDIENCE,
    )
    .await
}

/// A gateway revoked after its control-plane token was issued cannot use that
/// token, still valid for up to its TTL, to bind or delegate a user token
/// issued after the revocation. A token issued after the revocation can.
#[tokio::test]
async fn a_revoked_gateways_token_cannot_bind_or_delegate_a_newer_user_token() {
    let fixture = fixture().await;
    grant_alice(&fixture).await;
    let now = now_secs();
    let stale = gateway_token_issued_at(&fixture.keys, now - 60);
    fixture
        .state
        .store
        .revoke_principals("t1", vec!["p:test".to_string()], now - 30)
        .await
        .expect("revoke");

    let err = exchange_for_alice(&fixture, stale.clone())
        .await
        .expect_err("a revoked actor token");
    assert_eq!(
        (err.status, err.body.code.as_str()),
        (StatusCode::FORBIDDEN, "actor_refused")
    );
    // Alice's token is newer than the revocation; the gateway's is not.
    let fresh_alice = alice_token(&fixture.keys, "t1", Duration::from_secs(120));
    let err = delegate(&fixture, "t1", bearer(&stale), request(fresh_alice))
        .await
        .expect_err("a revoked caller");
    assert_eq!(
        (err.status, err.body.code.as_str()),
        (StatusCode::FORBIDDEN, "actor_refused")
    );

    // Revocation ends sessions, not the right: a token issued since works.
    let current = gateway_token_issued_at(&fixture.keys, now);
    let exchanged = exchange_for_alice(&fixture, current.clone())
        .await
        .expect("a current actor token");
    delegate(
        &fixture,
        "t1",
        bearer(&current),
        request(exchanged.felix_token),
    )
    .await
    .expect("delegated");
}

/// `token.delegate` in the gateway's token is not enough on its own: current
/// RBAC has to grant it too.
#[tokio::test]
async fn a_gateway_whose_grant_is_gone_cannot_bind_or_delegate() {
    let fixture = fixture().await;
    grant_alice(&fixture).await;
    let gateway = crate::test_support::token(&fixture.keys, &["token.delegate:tenant:t1"]);
    let bound = exchange_for_alice(&fixture, gateway.clone())
        .await
        .expect("bound while granted");
    // Straight from the store, so no revocation is recorded and only the
    // live check can catch it.
    fixture
        .state
        .store
        .remove_rbac_policy("t1", delegate_grant("p:test"))
        .await
        .expect("remove");

    let err = exchange_for_alice(&fixture, gateway.clone())
        .await
        .expect_err("no longer granted");
    assert_eq!(
        (err.status, err.body.code.as_str()),
        (StatusCode::FORBIDDEN, "actor_refused")
    );
    assert_eq!(
        status(delegate(&fixture, "t1", bearer(&gateway), request(bound.felix_token)).await),
        StatusCode::FORBIDDEN
    );
    let err = refresh_token_handler(
        Path("t1".to_string()),
        State(fixture.state.clone()),
        Json(TokenRefreshRequest {
            refresh_token: bound.refresh_token,
            audience: None,
        }),
    )
    .await
    .expect_err("a chain bound to a gateway that may no longer delegate");
    assert_eq!(err.status, StatusCode::FORBIDDEN);
}

/// Revocations reaching a gateway follow routine RBAC edits, or cover the
/// whole tenant, so they must not log out the users behind it: a chain bound
/// to the gateway keeps refreshing. The gateway's token from before the
/// revocation still cannot delegate.
#[tokio::test]
async fn a_revocation_reaching_the_gateway_does_not_end_the_users_chains() {
    let fixture = fixture().await;
    grant_alice(&fixture).await;
    let gateway = crate::test_support::token(&fixture.keys, &["token.delegate:tenant:t1"]);
    let bound = exchange_for_alice(&fixture, gateway.clone())
        .await
        .expect("bound");
    // As an RBAC removal that reached the gateway, and one that reached an
    // IdP group, record.
    fixture
        .state
        .store
        .revoke_principals(
            "t1",
            vec![
                "p:test".to_string(),
                crate::auth::revocation::ALL_PRINCIPALS.to_string(),
            ],
            now_secs(),
        )
        .await
        .expect("revoke");
    let Json(refreshed) = refresh(&fixture, bound.refresh_token)
        .await
        .expect("still refreshes");
    let err = delegate(
        &fixture,
        "t1",
        bearer(&gateway),
        request(refreshed.felix_token),
    )
    .await
    .expect_err("the gateway's token predates its revocation");
    assert_eq!(
        (err.status, err.body.code.as_str()),
        (StatusCode::FORBIDDEN, "actor_refused")
    );
}

fn refresh(
    fixture: &Fixture,
    refresh_token: String,
) -> impl std::future::Future<Output = Result<Json<crate::auth::refresh::TokenRefreshResponse>, ApiError>>
{
    refresh_token_handler(
        Path("t1".to_string()),
        State(fixture.state.clone()),
        Json(TokenRefreshRequest {
            refresh_token,
            audience: None,
        }),
    )
}

/// A store read that fails during a refresh must leave the refresh token
/// usable: a retry that looked like a replay would end the whole chain.
#[tokio::test]
async fn a_failed_read_does_not_spend_the_refresh_token() {
    let fixture = fixture().await;
    grant_alice(&fixture).await;
    let gateway = crate::test_support::token(&fixture.keys, &["token.delegate:tenant:t1"]);
    let bound = exchange_for_alice(&fixture, gateway).await.expect("bound");
    fixture
        .memory
        .faults
        .rbac_reads
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let err = refresh(&fixture, bound.refresh_token.clone())
        .await
        .expect_err("the read failed");
    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
    let Json(_) = refresh(&fixture, bound.refresh_token)
        .await
        .expect("the retry is not a replay");
}

/// A grant of `token.delegate` through an IdP group counts only for a group
/// of an issuer the tenant trusts, and a bare group name only in legacy mode.
#[tokio::test]
async fn only_a_trusted_issuers_group_can_grant_token_delegate() {
    let fixture = fixture().await;
    grant_alice(&fixture).await;
    fixture
        .state
        .store
        .remove_rbac_policy("t1", delegate_grant("p:test"))
        .await
        .expect("remove direct grant");
    let gateway = crate::test_support::token(&fixture.keys, &["token.delegate:tenant:t1"]);
    for group in ["group:https://idp.example#gateways", "group:gateways"] {
        fixture
            .state
            .store
            .add_rbac_policy("t1", delegate_grant(group))
            .await
            .expect("group grant");
    }
    let err = exchange_for_alice(&fixture, gateway.clone())
        .await
        .expect_err("no trusted issuer names the group");
    assert_eq!(err.body.code, "actor_refused");

    fixture
        .state
        .store
        .upsert_idp_issuer("t1", issuer("https://idp.example"))
        .await
        .expect("issuer");
    exchange_for_alice(&fixture, gateway)
        .await
        .expect("a trusted issuer's group");
}

fn issuer(issuer: &str) -> crate::auth::idp_registry::IdpIssuerConfig {
    serde_json::from_value(serde_json::json!({
        "issuer": issuer,
        "audiences": ["felix"],
        "jwks_url": format!("{issuer}/jwks"),
        "claim_mappings": {"subject_claim": "sub", "groups_claim": "groups"},
    }))
    .expect("issuer config")
}

/// A gateway's own connection-only chain is not bound to an actor, so the
/// actor check leaves it alone.
#[tokio::test]
async fn a_gateways_connection_only_chain_still_refreshes() {
    let fixture = fixture().await;
    let minted = mint_for_principal(
        &fixture.state,
        "t1",
        "p:test",
        &[],
        &TokenExchangeRequest {
            requested: Some(Vec::new()),
            permissions: Some(Vec::new()),
            ..TokenExchangeRequest::default()
        },
        BROKER_AUDIENCE,
    )
    .await
    .expect("connection-only token");
    let Json(_) = refresh_token_handler(
        Path("t1".to_string()),
        State(fixture.state.clone()),
        Json(TokenRefreshRequest {
            refresh_token: minted.refresh_token,
            audience: None,
        }),
    )
    .await
    .expect("refreshed");
}

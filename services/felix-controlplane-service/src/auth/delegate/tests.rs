use std::time::Duration;

use axum::http::{HeaderMap, HeaderValue, StatusCode};

use super::*;
use crate::auth::exchange::{TokenExchangeRequest, mint_for_principal};
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

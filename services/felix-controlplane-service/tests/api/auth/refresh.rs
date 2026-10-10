//! The refresh endpoint, through the real router.
//!
//! These drive `/v1/tenants/{tenant}/token/refresh` rather than the store, so
//! what is under test is the *policy*: which refusals happen, what a replay
//! costs, and whether a refresh answers from RBAC as it stands rather than as
//! it stood when the token was minted.
//! The store's own guarantees have their own suite
//! (`src/store/contract/refresh_tokens.rs`, run against memory and Postgres alike).
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::auth::felix_token::{SigningKey, TenantSigningKeys};
use felix_controlplane_service::auth::oidc::UpstreamOidcValidator;
use felix_controlplane_service::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use felix_controlplane_service::auth::refresh;
use felix_controlplane_service::model::Tenant;
use felix_controlplane_service::store::{
    AuthStore, ControlPlaneStore, StoreConfig, memory::InMemoryStore,
};
use jsonwebtoken::Algorithm;
use tower::ServiceExt;

use crate::common::read_json;

const TENANT: &str = "t1";
const PRINCIPAL: &str = "oidc:https://idp.example#user-1";
const FELIX_PRIVATE_KEY: [u8; 32] = [9u8; 32];

/// A tenant with signing keys and one grant, plus the store behind it.
///
/// The store is returned alongside the state so a test can reach past the API
/// to revoke or inspect — an operator action and an assertion, neither of which
/// has an endpoint here yet.
async fn fixture() -> (AppState, Arc<InMemoryStore>) {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: 200,
        change_retention_max_rows: Some(200),
    });
    store
        .create_tenant(Tenant {
            tenant_id: TENANT.to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .add_rbac_policy(
            TENANT,
            PolicyRule {
                subject: "role:publisher".to_string(),
                object: "stream:t1/payments/*".to_string(),
                action: "stream.publish".to_string(),
            },
        )
        .await
        .expect("policy");
    store
        .add_rbac_grouping(
            TENANT,
            GroupingRule {
                user: PRINCIPAL.to_string(),
                role: "role:publisher".to_string(),
            },
        )
        .await
        .expect("grouping");
    store
        .set_tenant_signing_keys(
            TENANT,
            TenantSigningKeys {
                current: SigningKey {
                    kid: "k1".to_string(),
                    alg: Algorithm::EdDSA,
                    private_key: FELIX_PRIVATE_KEY,
                    public_key: Ed25519SigningKey::from_bytes(&FELIX_PRIVATE_KEY)
                        .verifying_key()
                        .to_bytes(),
                },
                previous: vec![],
            },
        )
        .await
        .expect("keys");

    let store = Arc::new(store);
    let state = AppState {
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
        store: store.clone(),
        oidc_validator: UpstreamOidcValidator::new_with_allowed_algorithms(
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            60,
            vec![Algorithm::ES256, Algorithm::RS256],
        ),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    (state, store)
}

/// Mint a refresh token straight into the store, the way exchange would.
///
/// Going through exchange would mean standing up a JWKS server and an upstream
/// IdP for every case here, and none of these are about the OIDC path — that is
/// `auth_exchange.rs`, which round-trips a real exchange into a real refresh.
async fn issue_into(store: &InMemoryStore, groups: Vec<String>) -> String {
    let (record, presented) = refresh::issue(
        TENANT,
        PRINCIPAL,
        groups,
        None,
        refresh::now_secs(),
        Duration::from_secs(3600),
    );
    store.insert_refresh_token(record).await.expect("insert");
    presented
}

async fn post_refresh(state: &AppState, token: &str) -> axum::response::Response {
    let body = serde_json::json!({ "refresh_token": token }).to_string();
    let request = Request::builder()
        .method("POST")
        .uri(format!("/v1/tenants/{TENANT}/token/refresh"))
        .header("content-type", "application/json")
        .body(Body::from(body))
        .expect("request");
    build_router(state.clone())
        .into_service::<Body>()
        .oneshot(request)
        .await
        .expect("refresh")
}

#[tokio::test]
async fn a_refresh_mints_an_access_token_and_its_replacement() {
    let (state, store) = fixture().await;
    let token = issue_into(&store, vec![]).await;

    let response = post_refresh(&state, &token).await;
    assert_eq!(response.status(), StatusCode::OK);

    let payload = read_json(response).await;
    assert!(
        payload["felix_token"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "no access token: {payload}",
    );
    assert!(payload["expires_in"].as_u64().unwrap_or(0) > 0);
    // The replacement is what makes single-use workable: spending a token has
    // to hand back the next one, or the caller is locked out by its own
    // refresh.
    let next = payload["refresh_token"].as_str().expect("replacement");
    assert_ne!(next, token, "the same refresh token was handed back");
    assert!(payload["refresh_expires_in"].as_u64().unwrap_or(0) > 0);
}

#[tokio::test]
async fn a_refresh_token_is_spent_by_using_it() {
    let (state, store) = fixture().await;
    let token = issue_into(&store, vec![]).await;

    assert_eq!(post_refresh(&state, &token).await.status(), StatusCode::OK);
    assert_eq!(
        post_refresh(&state, &token).await.status(),
        StatusCode::FORBIDDEN,
        "a refresh token worked twice, so a stolen copy is as good as the original",
    );
}

#[tokio::test]
async fn the_replacement_works_and_keeps_working() {
    let (state, store) = fixture().await;
    let mut token = issue_into(&store, vec![]).await;

    // Three rotations. One would not show that the chain continues past the
    // first replacement, which is the case a long-running process actually
    // lives in.
    for round in 0..3 {
        let response = post_refresh(&state, &token).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "rotation {round} was refused",
        );
        token = read_json(response).await["refresh_token"]
            .as_str()
            .expect("replacement")
            .to_string();
    }
}

#[tokio::test]
async fn replaying_a_spent_token_ends_the_whole_chain() {
    let (state, store) = fixture().await;
    let first = issue_into(&store, vec![]).await;

    let live = read_json(post_refresh(&state, &first).await).await["refresh_token"]
        .as_str()
        .expect("replacement")
        .to_string();

    // The attacker's copy of the spent token. It is refused, which on its own
    // would be enough to stop *them* — but it also means two parties held the
    // chain, and the server cannot tell which one is still using it.
    assert_eq!(
        post_refresh(&state, &first).await.status(),
        StatusCode::FORBIDDEN,
    );

    // So the legitimate holder's live token goes too. Losing a session beats
    // leaving an attacker inside one, and re-exchanging is available.
    assert_eq!(
        post_refresh(&state, &live).await.status(),
        StatusCode::FORBIDDEN,
        "a replay left the rest of the chain usable, so detecting the theft \
         changed nothing",
    );
}

/// Mint a refresh token for a named principal.
async fn issue_for(store: &InMemoryStore, principal: &str, groups: Vec<String>) -> String {
    let (record, presented) = refresh::issue(
        TENANT,
        principal,
        groups,
        None,
        refresh::now_secs(),
        Duration::from_secs(3600),
    );
    store.insert_refresh_token(record).await.expect("insert");
    presented
}

#[tokio::test]
async fn a_refresh_re_evaluates_rbac_rather_than_reusing_the_last_grant() {
    let (state, store) = fixture().await;
    let ungranted = "oidc:https://idp.example#user-2";

    // No grant yet. The record carries no permissions of its own, so the only
    // thing that can answer is the policy store — and right now it says no.
    let before = issue_for(&store, ungranted, vec![]).await;
    assert_eq!(
        post_refresh(&state, &before).await.status(),
        StatusCode::FORBIDDEN,
    );

    // Grant it, change nothing else, and a token issued under the same
    // conditions now refreshes. The policy store is the only variable, which is
    // what "re-evaluated" has to mean: a refresh reads RBAC as it stands, not
    // as it stood when the token was minted.
    store
        .add_rbac_grouping(
            TENANT,
            GroupingRule {
                user: ungranted.to_string(),
                role: "role:publisher".to_string(),
            },
        )
        .await
        .expect("grouping");

    let after = issue_for(&store, ungranted, vec![]).await;
    assert_eq!(
        post_refresh(&state, &after).await.status(),
        StatusCode::OK,
        "a refresh did not see a grant added after the token was issued, so it          is answering from something other than current RBAC",
    );
}

#[tokio::test]
async fn a_refresh_picks_up_group_claims_recorded_at_exchange() {
    let (state, store) = fixture().await;
    // A principal with no direct binding: its access exists only through a
    // group claim, which is exactly the grant that vanishes if the claims are
    // not carried on the record.
    let by_group = "oidc:https://idp.example#user-3";
    // Recorded claims are honoured only for an issuer the tenant still trusts.
    store
        .upsert_idp_issuer(
            TENANT,
            felix_controlplane_service::auth::idp_registry::IdpIssuerConfig {
                issuer: "https://idp.example".to_string(),
                audiences: vec!["felix".to_string()],
                discovery_url: None,
                jwks_url: None,
                claim_mappings: Default::default(),
            },
        )
        .await
        .expect("issuer");
    store
        .add_rbac_grouping(
            TENANT,
            GroupingRule {
                user: "group:https://idp.example#oncall".to_string(),
                role: "role:publisher".to_string(),
            },
        )
        .await
        .expect("grouping");

    let with_claim = issue_for(
        &store,
        by_group,
        vec!["https://idp.example#oncall".to_string()],
    )
    .await;
    assert_eq!(
        post_refresh(&state, &with_claim).await.status(),
        StatusCode::OK,
        "the group claims recorded at exchange were not re-evaluated, so a \
         principal granted access by group loses it at the first refresh",
    );

    // The same principal without the claim is refused, which is what pins the
    // claim as the cause rather than something else in the fixture.
    let without_claim = issue_for(&store, by_group, vec![]).await;
    assert_eq!(
        post_refresh(&state, &without_claim).await.status(),
        StatusCode::FORBIDDEN,
    );
}

#[tokio::test]
async fn revoking_a_principal_stops_its_refreshes() {
    let (state, store) = fixture().await;
    let token = issue_into(&store, vec![]).await;

    let revoked = store
        .revoke_refresh_tokens_for_principal(TENANT, PRINCIPAL)
        .await
        .expect("revoke");
    assert_eq!(revoked, 1);

    assert_eq!(
        post_refresh(&state, &token).await.status(),
        StatusCode::FORBIDDEN,
        "a revoked principal kept refreshing, so cutting one off means waiting \
         out every token it holds",
    );
}

#[tokio::test]
async fn a_wrong_secret_does_not_leave_the_token_usable() {
    let (state, store) = fixture().await;
    let token = issue_into(&store, vec![]).await;
    let (token_id, _) = token.split_once('.').expect("a well-formed token");

    assert_eq!(
        post_refresh(&state, &format!("{token_id}.wrong"))
            .await
            .status(),
        StatusCode::FORBIDDEN,
    );
    // Spent by the attempt. A wrong secret against a real token id means
    // someone holds half a credential; leaving the token live would make
    // guessing free, and one guess per token is the bound worth having.
    assert_eq!(
        post_refresh(&state, &token).await.status(),
        StatusCode::FORBIDDEN,
        "a failed guess left the token usable, so an attacker with the token id \
         can keep trying",
    );
}

#[tokio::test]
async fn a_malformed_token_is_refused_like_any_other() {
    let (state, _store) = fixture().await;
    // Each takes the same path a wrong token takes. A different status or
    // message for any of them tells a caller which token ids exist.
    for malformed in ["", "no-separator", "id.", ".secret"] {
        assert_eq!(
            post_refresh(&state, malformed).await.status(),
            StatusCode::FORBIDDEN,
            "{malformed:?} was answered differently",
        );
    }
}

/// A refresh re-runs RBAC, but must re-apply what the exchange narrowed it to;
/// otherwise a narrowed token plus its refresh token is the principal's full
/// rights, under whichever audience the refresher picks.
#[tokio::test]
async fn a_refresh_keeps_the_exchanges_narrowing_and_audience() {
    use base64::Engine;
    use felix_controlplane_service::auth::refresh_token::Narrowing;

    let (state, store) = fixture().await;
    let (mut record, presented) = refresh::issue(
        TENANT,
        PRINCIPAL,
        vec![],
        None,
        refresh::now_secs(),
        Duration::from_secs(3600),
    );
    record.narrowing = Some(Narrowing {
        requested: Some(vec!["stream.publish".to_string()]),
        resources: Some(vec!["stream:t1/payments/orders".to_string()]),
        permissions: None,
        audience: "felix-broker".to_string(),
        may_act: None,
    });
    store.insert_refresh_token(record).await.expect("insert");

    let post = |body: serde_json::Value| {
        let request = Request::builder()
            .method("POST")
            .uri(format!("/v1/tenants/{TENANT}/token/refresh"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request");
        build_router(state.clone())
            .into_service::<Body>()
            .oneshot(request)
    };

    let response = post(serde_json::json!({ "refresh_token": presented }))
        .await
        .expect("refresh");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    let claims = payload["felix_token"]
        .as_str()
        .and_then(|token| token.split('.').nth(1))
        .and_then(|part| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(part)
                .ok()
        })
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .expect("claims");
    assert_eq!(
        claims["perms"],
        serde_json::json!(["stream.publish:stream:t1/payments/orders"]),
        "the refresh widened back to the full grant",
    );
    assert_eq!(claims["aud"], "felix-broker");

    // The chain keeps its audience; asking for another is refused.
    let next = payload["refresh_token"].as_str().expect("next").to_string();
    let response = post(serde_json::json!({
        "refresh_token": next,
        "audience": "felix-controlplane",
    }))
    .await
    .expect("refresh");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

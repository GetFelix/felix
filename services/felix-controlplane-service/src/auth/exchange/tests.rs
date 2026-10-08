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
    let requested = vec!["stream.publish".to_string()];
    let filtered = filter_permissions(perms, Some(&requested), None, "t1");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0], "stream.publish:stream:t1/payments/*");
}

fn narrow(perms: &[&str], resources: &[&str]) -> Vec<String> {
    let resources: Vec<String> = resources.iter().map(|value| value.to_string()).collect();
    let mut narrowed = filter_permissions(
        perms.iter().map(|value| value.to_string()).collect(),
        None,
        Some(&resources),
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
    assert!(
        narrow(
            &["stream.publish:stream:t1/payments/*"],
            &["stream:t1/orders/x"]
        )
        .is_empty()
    );
    assert!(
        narrow(
            &["stream.publish:stream:t1/payments/*"],
            &["stream:t2/payments/x"]
        )
        .is_empty()
    );
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

/// Two IdPs asserting the same group name are two different groups, and
/// neither is the bare name an older grant was written against.
#[test]
fn group_claims_are_scoped_by_issuer() {
    let corp = scoped_group("https://corp.example", "ops");
    let other = scoped_group("https://tenant-idp.example", "ops");
    assert_ne!(corp, other);

    let mut groupings = Vec::new();
    add_group_claim_groupings(&mut groupings, "p1", &with_legacy_names(&[other], false));
    assert_eq!(
        groupings,
        vec![GroupingRule {
            user: "p1".to_string(),
            role: "group:https://tenant-idp.example#ops".to_string(),
        }]
    );
    assert!(!groupings.iter().any(|g| g.role == "group:ops"));
}

#[test]
fn the_legacy_switch_also_links_the_bare_name() {
    let names = with_legacy_names(&[scoped_group("https://corp.example", "ops")], true);
    assert_eq!(
        names,
        vec!["https://corp.example#ops".to_string(), "ops".to_string()]
    );
}

#[test]
fn the_requested_audience_defaults_to_brokers_and_is_closed() {
    assert_eq!(token_audience(None).unwrap(), BROKER_AUDIENCE);
    assert_eq!(
        token_audience(Some("felix-controlplane")).unwrap(),
        CONTROLPLANE_AUDIENCE
    );
    assert!(token_audience(Some("anything-else")).is_err());
}

fn pairs(perms: &[&str], pairs: &[&str]) -> Vec<String> {
    let pairs: Vec<String> = pairs.iter().map(|value| value.to_string()).collect();
    let mut narrowed = filter_pairs(
        perms.iter().map(|value| value.to_string()).collect(),
        &pairs,
        "t1",
    );
    narrowed.sort();
    narrowed
}

/// What the cross product of `requested` and `resources` cannot say: read one
/// stream and write another.
#[test]
fn each_pair_is_narrowed_on_its_own() {
    assert_eq!(
        pairs(
            &[
                "stream.subscribe:stream:t1/rooms/*",
                "stream.publish:stream:t1/rooms/*",
            ],
            &[
                "stream.subscribe:stream:t1/rooms/a",
                "stream.publish:stream:t1/rooms/b",
            ],
        ),
        vec![
            "stream.publish:stream:t1/rooms/b",
            "stream.subscribe:stream:t1/rooms/a",
        ]
    );
}

#[test]
fn a_pair_cannot_add_an_action_the_grant_lacks() {
    assert!(
        pairs(
            &["stream.subscribe:stream:t1/rooms/*"],
            &["stream.publish:stream:t1/rooms/a"],
        )
        .is_empty()
    );
    // Exact match: a manage grant does not answer for the actions it implies.
    assert!(
        pairs(
            &["stream.manage:stream:t1/rooms/*"],
            &["stream.publish:stream:t1/rooms/a"],
        )
        .is_empty()
    );
}

#[test]
fn a_broader_pair_keeps_the_grant_as_is() {
    for broader in [
        "stream.subscribe:stream:t1/rooms/*",
        "stream.subscribe:stream:t1/*/*",
        "stream.subscribe:namespace:t1/rooms",
        "stream.subscribe:tenant:t1",
    ] {
        assert_eq!(
            pairs(&["stream.subscribe:stream:t1/rooms/a"], &[broader]),
            vec!["stream.subscribe:stream:t1/rooms/a"],
            "{broader}"
        );
    }
}

/// Pairs do not borrow from one another: holding subscribe on `a` and
/// publish on `b` gives nothing when asking the other way round.
#[test]
fn pairs_do_not_mix_grants_across_resources() {
    let granted = [
        "stream.subscribe:stream:t1/rooms/a",
        "stream.publish:stream:t1/rooms/b",
    ];
    assert!(
        pairs(
            &granted,
            &[
                "stream.publish:stream:t1/rooms/a",
                "stream.subscribe:stream:t1/rooms/b",
            ],
        )
        .is_empty()
    );
    assert_eq!(
        pairs(
            &granted,
            &[
                "stream.subscribe:stream:t1/rooms/a",
                "stream.publish:stream:t1/rooms/a",
            ],
        ),
        vec!["stream.subscribe:stream:t1/rooms/a"]
    );
}

#[test]
fn a_pair_outside_every_grant_yields_nothing() {
    let granted = ["stream.publish:stream:t1/rooms/*"];
    for outside in [
        "stream.publish:stream:t1/other/x",
        "stream.publish:stream:t2/rooms/x",
        "stream.publish:not an object",
        "stream.publish",
        "stream.publish:tenant:t2",
        // A stream pair does not turn a tenant grant stream-shaped either.
        "tenant.manage:stream:t1/rooms/x",
    ] {
        assert!(pairs(&granted, &[outside]).is_empty(), "{outside}");
    }
    assert!(
        pairs(
            &["tenant.manage:tenant:t1"],
            &["tenant.manage:stream:t1/rooms/x"]
        )
        .is_empty()
    );
}

/// Over a grid of grants and pairs, every permission kept is inside a grant
/// with its action and inside a pair with its action.
#[test]
fn no_pair_ever_widens_a_grant() {
    use crate::auth::rbac::authorize::object_within_scope;

    let objects = [
        "tenant:t1",
        "namespace:t1/rooms",
        "namespace:t1/other",
        "stream:t1/*/*",
        "stream:t1/rooms/*",
        "stream:t1/rooms/a",
        "stream:t1/rooms/b",
        "stream:t1/other/a",
        "cache:t1/*/*",
        "cache:t1/rooms/c",
        "group:t1/rooms/a/g",
    ];
    let actions = [
        "stream.publish",
        "stream.subscribe",
        "cache.read",
        "tenant.manage",
    ];
    let all: Vec<String> = actions
        .iter()
        .flat_map(|action| {
            objects
                .iter()
                .map(move |object| format!("{action}:{object}"))
        })
        .collect();
    let within = |kept: &str, set: &[String]| {
        let (action, object) = kept.split_once(':').unwrap();
        let object = parse_object(object, "t1").unwrap();
        set.iter().any(|perm| {
            let (a, o) = perm.split_once(':').unwrap();
            a == action && object_within_scope(&parse_object(o, "t1").unwrap(), &object)
        })
    };
    for (i, first) in all.iter().enumerate() {
        for second in all.iter().skip(i) {
            let granted = vec![first.clone(), second.clone()];
            for asked_first in &all {
                for asked_second in [asked_first, &all[(i * 7) % all.len()]] {
                    let asked = vec![asked_first.clone(), asked_second.clone()];
                    for kept in filter_pairs(granted.clone(), &asked, "t1") {
                        assert!(within(&kept, &granted), "{kept} not granted by {granted:?}");
                        assert!(within(&kept, &asked), "{kept} not asked for in {asked:?}");
                    }
                }
            }
        }
    }
}

fn request(
    requested: Option<&[&str]>,
    resources: Option<&[&str]>,
    permissions: Option<&[&str]>,
) -> TokenExchangeRequest {
    let owned = |values: Option<&[&str]>| {
        values.map(|values| values.iter().map(|value| value.to_string()).collect())
    };
    TokenExchangeRequest {
        requested: owned(requested),
        resources: owned(resources),
        permissions: owned(permissions),
        audience: None,
    }
}

#[test]
fn permissions_cannot_be_mixed_with_the_older_fields() {
    let pair: &[&str] = &["stream.publish:stream:t1/rooms/a"];
    for refused in [
        // Without `requested: []` an older control plane would mint full rights.
        request(None, None, Some(pair)),
        request(Some(&["stream.publish"]), None, Some(pair)),
        request(Some(&[]), Some(&["stream:t1/rooms/a"]), Some(pair)),
        request(Some(&[]), None, Some(&["stream.publish:stream:t2/rooms/a"])),
        request(
            Some(&[]),
            None,
            Some(&["nonsense.action:stream:t1/rooms/a"]),
        ),
        request(Some(&[]), None, Some(&["stream.publish"])),
    ] {
        let err = narrowing_for(&refused, "t1", BROKER_AUDIENCE).expect_err("refused");
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
    }
    for accepted in [
        request(Some(&[]), None, Some(pair)),
        request(Some(&[]), Some(&[]), Some(pair)),
    ] {
        let narrowing = narrowing_for(&accepted, "t1", BROKER_AUDIENCE).expect("accepted");
        assert_eq!(narrowing.requested, Some(Vec::new()));
        assert_eq!(narrowing.resources, None);
        assert_eq!(
            narrowing.permissions,
            Some(vec!["stream.publish:stream:t1/rooms/a".to_string()])
        );
    }
}

/// A request without `permissions` records exactly what it did before, so an
/// older control plane reads the same record.
#[test]
fn the_older_fields_record_as_before() {
    let narrowing = narrowing_for(
        &request(
            Some(&["stream.publish"]),
            Some(&["stream:t1/rooms/a"]),
            None,
        ),
        "t1",
        BROKER_AUDIENCE,
    )
    .expect("accepted");
    assert_eq!(
        serde_json::to_value(&narrowing).unwrap(),
        json!({
            "requested": ["stream.publish"],
            "resources": ["stream:t1/rooms/a"],
            "audience": "felix-broker",
        })
    );
    let perms = vec!["stream.publish:stream:t1/rooms/*".to_string()];
    assert_eq!(
        narrow_permissions(perms.clone(), &narrowing, "t1"),
        filter_permissions(
            perms,
            Some(&["stream.publish".to_string()]),
            Some(&["stream:t1/rooms/a".to_string()]),
            "t1"
        )
    );
}

/// What a control plane that predates `permissions` does with a pair record:
/// it sees only `requested: []` and narrows to nothing.
#[test]
fn an_older_reader_of_a_pair_record_narrows_to_nothing() {
    let narrowing = narrowing_for(
        &request(Some(&[]), None, Some(&["stream.publish:stream:t1/rooms/a"])),
        "t1",
        BROKER_AUDIENCE,
    )
    .expect("accepted");
    let perms = vec!["stream.publish:stream:t1/rooms/*".to_string()];
    let older = Narrowing {
        permissions: None,
        ..narrowing.clone()
    };
    assert!(narrow_permissions(perms.clone(), &older, "t1").is_empty());
    assert_eq!(
        narrow_permissions(perms, &narrowing, "t1"),
        vec!["stream.publish:stream:t1/rooms/a"]
    );
}

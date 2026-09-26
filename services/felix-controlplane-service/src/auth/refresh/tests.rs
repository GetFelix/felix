//! What a minted refresh token looks like before it reaches a store.
use super::*;

#[test]
fn a_new_chain_gets_its_own_family() {
    let (first, _) = issue("acme", "p", vec![], None, 1_000, Duration::from_secs(60));
    let (second, _) = issue("acme", "p", vec![], None, 1_000, Duration::from_secs(60));

    assert_ne!(
        first.family_id, second.family_id,
        "two exchanges shared a rotation chain, so a replay on one would revoke \
         the other's tokens",
    );
    assert_ne!(first.token_id, second.token_id);
}

#[test]
fn a_rotation_stays_in_its_family() {
    let (first, _) = issue("acme", "p", vec![], None, 1_000, Duration::from_secs(60));
    let (next, _) = issue(
        "acme",
        "p",
        vec![],
        Some(first.family_id.clone()),
        1_100,
        Duration::from_secs(60),
    );

    // The chain is what makes a replay actionable: presenting any spent token
    // in it must be able to revoke every other.
    assert_eq!(next.family_id, first.family_id);
    assert_ne!(next.token_id, first.token_id);
}

#[test]
fn the_record_never_carries_the_secret() {
    let (record, presented) = issue("acme", "p", vec![], None, 1_000, Duration::from_secs(60));
    let (token_id, secret) = refresh_token::split(&presented).expect("a well-formed token");

    assert_eq!(token_id, record.token_id);
    assert_eq!(record.secret_hash, refresh_token::hash_secret(secret));
    // A database read must not yield anything presentable.
    assert!(!presented.contains(&record.secret_hash));
    assert_ne!(record.secret_hash, secret);
}

#[test]
fn every_token_is_distinct() {
    // Both halves come from the OS. A repeat in either would mean one caller's
    // token opening another's chain.
    let mut seen = std::collections::HashSet::new();
    for _ in 0..64 {
        let (_, presented) = issue("acme", "p", vec![], None, 0, Duration::from_secs(60));
        assert!(seen.insert(presented), "a refresh token repeated");
    }
}

#[test]
fn the_ttl_lands_on_the_record() {
    let (record, _) = issue("acme", "p", vec![], None, 1_000, Duration::from_secs(900));
    assert_eq!(record.issued_at_secs, 1_000);
    assert_eq!(record.expires_at_secs, 1_900);
    assert!(record.is_live(1_899));
    assert!(!record.is_live(1_900));
}

#[test]
fn the_claims_to_re_evaluate_are_kept() {
    let groups = vec!["eng".to_string(), "oncall".to_string()];
    let (record, _) = issue(
        "acme",
        "p",
        groups.clone(),
        None,
        0,
        Duration::from_secs(60),
    );
    // Claims, not permissions. Storing the granted permissions would freeze
    // them, which is the thing re-evaluation exists to avoid.
    assert_eq!(record.groups, groups);
}

/// Every refresh refusal is a refused credential, and the two an operator
/// alerts on -- a replay and a wrong secret -- are also counted on their own.
#[test]
fn refresh_refusals_are_counted() {
    use std::sync::Arc;

    use axum::extract::{Path, State};

    use crate::store::memory::InMemoryStore;
    use crate::store::{AuthStore, StoreConfig};

    let recorder = crate::test_support::CountingRecorder::default();
    recorder.run(async {
        let store = Arc::new(InMemoryStore::new(StoreConfig {
            changes_limit: crate::config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: Some(crate::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
        }));
        let state = crate::test_support::app_state_ready(store.clone());
        let refresh = |token: String| {
            let state = state.clone();
            async move {
                refresh_token_handler(
                    Path("t1".to_string()),
                    State(state),
                    Json(TokenRefreshRequest {
                        refresh_token: token,
                        audience: None,
                    }),
                )
                .await
                .expect_err("refused")
            }
        };

        let now = now_secs();
        let (wrong_secret, _) = issue("t1", "p", vec![], None, now, Duration::from_secs(60));
        let wrong_secret_id = wrong_secret.token_id.clone();
        store
            .insert_refresh_token(wrong_secret)
            .await
            .expect("insert");
        let (mut spent, spent_presented) =
            issue("t1", "p", vec![], None, now, Duration::from_secs(60));
        spent.used = true;
        store.insert_refresh_token(spent).await.expect("insert");

        refresh("not a refresh token".to_string()).await;
        refresh(refresh_token::join(&wrong_secret_id, "not-the-secret")).await;
        refresh(spent_presented).await;
    });

    assert_eq!(
        recorder.count("felix_controlplane_auth_rejected_total{reason=refresh_refused}"),
        3
    );
    assert_eq!(recorder.count("felix_refresh_token_bad_secret_total"), 1);
    assert_eq!(recorder.count("felix_refresh_token_replays_total"), 1);
}

#[test]
fn debug_never_shows_a_token() {
    let request = TokenRefreshRequest {
        refresh_token: "refresh-secret".to_string(),
        audience: None,
    };
    let response = TokenRefreshResponse {
        felix_token: "access-secret".to_string(),
        expires_in: 900,
        token_type: "Bearer".to_string(),
        refresh_token: "refresh-secret".to_string(),
        refresh_expires_in: 60,
    };
    for rendered in [format!("{request:?}"), format!("{response:?}")] {
        assert!(!rendered.contains("secret"), "{rendered}");
    }
}

fn issuer(url: &str) -> crate::auth::idp_registry::IdpIssuerConfig {
    crate::auth::idp_registry::IdpIssuerConfig {
        issuer: url.to_string(),
        audiences: vec!["felix".to_string()],
        discovery_url: None,
        jwks_url: None,
        claim_mappings: Default::default(),
    }
}

/// A refresh honours only groups from issuers the tenant still trusts, and
/// never a bare name unless the legacy switch says so.
#[test]
fn a_refresh_honours_only_groups_from_trusted_issuers() {
    let recorded = vec![
        "https://corp.example#ops".to_string(),
        "https://removed.example#ops".to_string(),
        // A prefix of a trusted issuer is not that issuer.
        "https://corp.example.evil#ops".to_string(),
        "ops".to_string(),
    ];
    let issuers = [issuer("https://corp.example")];
    assert_eq!(
        refreshable_groups(&recorded, &issuers, false),
        vec!["https://corp.example#ops".to_string()]
    );
    assert_eq!(
        refreshable_groups(&recorded, &issuers, true),
        vec![
            "https://corp.example#ops".to_string(),
            "ops".to_string(),
            "https://removed.example#ops".to_string(),
            "https://corp.example.evil#ops".to_string(),
            "ops".to_string(),
        ]
    );
}

//! Refresh-token behaviour every backend must satisfy.
//!
//! Parity matters more here than in the other suites, because these
//! rules are the security properties — single use, replay detection,
//! revocation — and a backend that implements them *almost* right is
//! indistinguishable from one that implements them until someone steals a
//! token.
use std::sync::Arc;

use crate::auth::refresh_token::{Narrowing, RefreshToken, RefreshTokenTake, hash_secret};
use crate::model::Tenant;
use crate::store::ControlPlaneStore;

const TENANT: &str = "refresh-t";
const PRINCIPAL: &str = "oidc:issuer#user-1";
const NOW: i64 = 1_000;

fn token(token_id: &str, family_id: &str) -> RefreshToken {
    RefreshToken {
        token_id: token_id.to_string(),
        tenant_id: TENANT.to_string(),
        principal_id: PRINCIPAL.to_string(),
        groups: vec!["group:engineering".to_string()],
        secret_hash: hash_secret("the-secret"),
        family_id: family_id.to_string(),
        issued_at_secs: NOW,
        expires_at_secs: NOW + 3_600,
        used: false,
        revoked: false,
        narrowing: Some(narrowing()),
    }
}

fn narrowing() -> Narrowing {
    Narrowing {
        requested: Some(vec!["stream.publish".to_string()]),
        resources: Some(vec!["stream:refresh-t/payments/orders".to_string()]),
        audience: "felix-controlplane".to_string(),
    }
}

async fn seed(store: &dyn ControlPlaneStore) {
    let _ = store
        .create_tenant(Tenant {
            tenant_id: TENANT.to_string(),
            display_name: "Refresh".to_string(),
        })
        .await;
}

pub(crate) async fn run_refresh_contract(store: Arc<dyn crate::store::ControlPlaneAuthStore>) {
    let store: &dyn crate::store::ControlPlaneAuthStore = store.as_ref();
    seed(store).await;

    a_stored_token_can_be_spent_once(store).await;
    a_spent_token_is_reported_as_a_replay(store).await;
    an_unknown_token_is_unusable(store).await;
    an_expired_token_cannot_be_spent(store).await;
    a_revoked_token_cannot_be_spent(store).await;
    revoking_a_family_leaves_other_families_alone(store).await;
    revoking_a_principal_leaves_other_principals_alone(store).await;
    the_stored_record_carries_the_claims_a_refresh_needs(store).await;
    purging_removes_only_what_has_expired(store).await;
}

async fn a_stored_token_can_be_spent_once(store: &dyn crate::store::ControlPlaneAuthStore) {
    store
        .insert_refresh_token(token("once", "family-once"))
        .await
        .expect("insert");

    let first = store
        .take_refresh_token(TENANT, "once", NOW)
        .await
        .expect("take");
    assert!(
        matches!(first, RefreshTokenTake::Taken(_)),
        "a live token was not spendable: {first:?}",
    );

    // The second attempt must not succeed. This is the property the whole
    // rotation scheme rests on: if a token can be spent twice, a stolen copy is
    // as good as the original for as long as it lives.
    let second = store
        .take_refresh_token(TENANT, "once", NOW)
        .await
        .expect("take");
    assert!(
        !matches!(second, RefreshTokenTake::Taken(_)),
        "a token was spent twice: {second:?}",
    );
}

async fn a_spent_token_is_reported_as_a_replay(store: &dyn crate::store::ControlPlaneAuthStore) {
    store
        .insert_refresh_token(token("replayed", "family-replay"))
        .await
        .expect("insert");
    store
        .take_refresh_token(TENANT, "replayed", NOW)
        .await
        .expect("take");

    // Not merely refused — reported as a replay, and carrying the record. A
    // backend that answered `Unusable` here would lose the only signal that
    // says someone else is holding the chain, and the family would never be
    // revoked.
    let again = store
        .take_refresh_token(TENANT, "replayed", NOW)
        .await
        .expect("take");
    let RefreshTokenTake::Replayed(record) = again else {
        panic!("a spent token was not reported as a replay: {again:?}");
    };
    assert_eq!(
        record.family_id, "family-replay",
        "a replay must name the family to revoke",
    );
}

async fn an_unknown_token_is_unusable(store: &dyn crate::store::ControlPlaneAuthStore) {
    let take = store
        .take_refresh_token(TENANT, "never-issued", NOW)
        .await
        .expect("take");
    assert!(matches!(take, RefreshTokenTake::Unusable), "{take:?}");
}

async fn an_expired_token_cannot_be_spent(store: &dyn crate::store::ControlPlaneAuthStore) {
    store
        .insert_refresh_token(token("expired", "family-expired"))
        .await
        .expect("insert");

    let take = store
        .take_refresh_token(TENANT, "expired", NOW + 7_200)
        .await
        .expect("take");
    assert!(
        matches!(take, RefreshTokenTake::Unusable),
        "an expired token was spendable: {take:?}",
    );

    // And it stays unusable rather than having been quietly marked spent — a
    // backend that consumed it would turn an expiry into a replay report the
    // next time the legitimate holder tried.
    let again = store
        .take_refresh_token(TENANT, "expired", NOW + 7_200)
        .await
        .expect("take");
    assert!(matches!(again, RefreshTokenTake::Unusable), "{again:?}");
}

async fn a_revoked_token_cannot_be_spent(store: &dyn crate::store::ControlPlaneAuthStore) {
    store
        .insert_refresh_token(token("revoked", "family-revoked"))
        .await
        .expect("insert");
    assert_eq!(
        store
            .revoke_refresh_family(TENANT, "family-revoked")
            .await
            .expect("revoke"),
        1,
    );

    let take = store
        .take_refresh_token(TENANT, "revoked", NOW)
        .await
        .expect("take");
    assert!(
        matches!(take, RefreshTokenTake::Unusable),
        "a revoked token was spendable: {take:?}",
    );

    // Revoking again reports nothing left to revoke, so a caller can tell a
    // real revocation from a repeat.
    assert_eq!(
        store
            .revoke_refresh_family(TENANT, "family-revoked")
            .await
            .expect("revoke"),
        0,
    );
}

async fn revoking_a_family_leaves_other_families_alone(
    store: &dyn crate::store::ControlPlaneAuthStore,
) {
    store
        .insert_refresh_token(token("chain-a1", "chain-a"))
        .await
        .expect("insert");
    store
        .insert_refresh_token(token("chain-a2", "chain-a"))
        .await
        .expect("insert");
    store
        .insert_refresh_token(token("chain-b1", "chain-b"))
        .await
        .expect("insert");

    // Both of one chain, and only that chain: a replay must not log out every
    // session the principal holds.
    assert_eq!(
        store
            .revoke_refresh_family(TENANT, "chain-a")
            .await
            .expect("revoke"),
        2,
    );
    let other = store
        .take_refresh_token(TENANT, "chain-b1", NOW)
        .await
        .expect("take");
    assert!(
        matches!(other, RefreshTokenTake::Taken(_)),
        "revoking one chain took another with it: {other:?}",
    );
}

async fn revoking_a_principal_leaves_other_principals_alone(
    store: &dyn crate::store::ControlPlaneAuthStore,
) {
    let mut other_principal = token("other-principal", "family-other");
    other_principal.principal_id = "oidc:issuer#user-2".to_string();
    store
        .insert_refresh_token(other_principal)
        .await
        .expect("insert");
    store
        .insert_refresh_token(token("mine-1", "family-mine"))
        .await
        .expect("insert");

    let revoked = store
        .revoke_refresh_tokens_for_principal(TENANT, PRINCIPAL)
        .await
        .expect("revoke");
    assert!(
        revoked >= 1,
        "revoking a principal revoked nothing, so a compromised principal keeps refreshing",
    );

    let theirs = store
        .take_refresh_token(TENANT, "other-principal", NOW)
        .await
        .expect("take");
    assert!(
        matches!(theirs, RefreshTokenTake::Taken(_)),
        "cutting off one principal cut off another: {theirs:?}",
    );
}

async fn the_stored_record_carries_the_claims_a_refresh_needs(
    store: &dyn crate::store::ControlPlaneAuthStore,
) {
    store
        .insert_refresh_token(token("claims", "family-claims"))
        .await
        .expect("insert");

    let take = store
        .take_refresh_token(TENANT, "claims", NOW)
        .await
        .expect("take");
    let RefreshTokenTake::Taken(record) = take else {
        panic!("{take:?}");
    };

    // Groups especially: a refresh re-runs RBAC, and group-derived grants
    // cannot be recomputed without the claims the principal presented. A
    // backend that dropped them would silently narrow every refreshed token to
    // its direct grants.
    assert_eq!(record.groups, vec!["group:engineering".to_string()]);
    assert_eq!(record.principal_id, PRINCIPAL);
    assert_eq!(record.secret_hash, hash_secret("the-secret"));
    assert_eq!(record.family_id, "family-claims");
    // Dropping it would let every refresh widen back to full RBAC rights.
    assert_eq!(record.narrowing, Some(narrowing()));
}

async fn purging_removes_only_what_has_expired(store: &dyn crate::store::ControlPlaneAuthStore) {
    store
        .insert_refresh_token(token("still-good", "family-purge"))
        .await
        .expect("insert");
    let mut old = token("long-gone", "family-purge");
    old.expires_at_secs = NOW - 1;
    store.insert_refresh_token(old).await.expect("insert");

    let purged = store
        .purge_expired_refresh_tokens(NOW)
        .await
        .expect("purge");
    assert!(purged >= 1, "the expired token was not purged");

    let live = store
        .take_refresh_token(TENANT, "still-good", NOW)
        .await
        .expect("take");
    assert!(
        matches!(live, RefreshTokenTake::Taken(_)),
        "the purge took a token that had not expired: {live:?}",
    );
}

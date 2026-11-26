//! Principal revocations every backend must satisfy.
use std::sync::Arc;

use crate::auth::rbac::policy_store::PolicyRule;
use crate::auth::revocation::{ALL_PRINCIPALS, PrincipalRevocation};
use crate::store::{AuthChange, ControlPlaneAuthStore, PrincipalCutoffs, Revocation, StoreError};

const TENANT: &str = "revocation-t";
/// Its own tenant, so the tenant-wide revocation above does not mask it.
const CHANGE_TENANT: &str = "revocation-change-t";

fn rows_of_tenant(items: &[PrincipalRevocation]) -> Vec<(&str, i64)> {
    items
        .iter()
        .filter(|row| row.tenant_id == TENANT)
        .map(|row| (row.principal_id.as_str(), row.revoked_at))
        .collect()
}

pub(crate) async fn run_revocation_contract(store: Arc<dyn ControlPlaneAuthStore>) {
    // Another suite may have written rows before this one on a shared store.
    let mut since = 0;
    loop {
        let page = store.principal_revocations(since).await.expect("feed");
        if page.items.is_empty() {
            break;
        }
        since = page.next_seq;
    }

    store
        .revoke_principals(TENANT, vec!["alice".into(), "bob".into()], 100)
        .await
        .expect("revoke");
    let changes = store.principal_revocations(since).await.expect("feed");
    assert_eq!(
        rows_of_tenant(&changes.items),
        vec![("alice", 100), ("bob", 100)]
    );
    assert!(changes.items[0].seq < changes.items[1].seq);
    assert_eq!(changes.next_seq, changes.items[1].seq + 1);
    let caught_up = store
        .principal_revocations(changes.next_seq)
        .await
        .expect("feed");
    assert!(caught_up.items.is_empty(), "nothing new past the end");

    assert_eq!(
        store
            .principal_revoked_at(TENANT, "alice")
            .await
            .expect("read"),
        Some(100)
    );
    assert_eq!(
        store
            .principal_revoked_at(TENANT, "carol")
            .await
            .expect("read"),
        None
    );
    assert_eq!(
        store
            .principal_revoked_at("other-t", "alice")
            .await
            .expect("read"),
        None
    );

    // An earlier time never moves a revocation back, but the row is written
    // again, so a broker past it still hears about it.
    store
        .revoke_principals(TENANT, vec!["alice".into()], 50)
        .await
        .expect("revoke");
    let again = store
        .principal_revocations(changes.next_seq)
        .await
        .expect("feed");
    assert_eq!(rows_of_tenant(&again.items), vec![("alice", 100)]);

    store
        .revoke_principals(TENANT, vec![ALL_PRINCIPALS.into()], 200)
        .await
        .expect("revoke");
    assert_eq!(
        store
            .principal_revoked_at(TENANT, "carol")
            .await
            .expect("read"),
        Some(200)
    );
    assert_eq!(
        store
            .principal_revoked_at(TENANT, "alice")
            .await
            .expect("read"),
        Some(200)
    );

    // A position this store never gave out is read from the start.
    let reset = store
        .principal_revocations(u64::MAX / 2)
        .await
        .expect("feed");
    assert!(
        reset
            .items
            .iter()
            .any(|row| row.tenant_id == TENANT && row.principal_id == ALL_PRINCIPALS),
        "a cursor from another store starts over"
    );

    a_change_and_its_revocation_commit_together(store.as_ref()).await;
    only_an_explicit_revocation_cuts_off_assertions(store.as_ref()).await;
}

/// Its own tenant, so the rows above do not mask it.
const ASSERTION_TENANT: &str = "revocation-assertion-t";

async fn only_an_explicit_revocation_cuts_off_assertions(store: &dyn ControlPlaneAuthStore) {
    let cutoffs = |principal: &'static str| async move {
        store
            .principal_cutoffs(ASSERTION_TENANT, principal)
            .await
            .expect("read")
    };
    let both = |tokens, assertions| PrincipalCutoffs { tokens, assertions };
    let mut since = 0;
    loop {
        let page = store.principal_revocations(since).await.expect("feed");
        if page.items.is_empty() {
            break;
        }
        since = page.next_seq;
    }

    store
        .revoke_principals(ASSERTION_TENANT, vec!["erin".into()], 100)
        .await
        .expect("revoke");
    assert_eq!(cutoffs("erin").await, both(Some(100), None));

    store
        .revoke_principals_explicitly(ASSERTION_TENANT, vec!["erin".into()], 90)
        .await
        .expect("revoke");
    assert_eq!(
        cutoffs("erin").await,
        both(Some(100), Some(90)),
        "an explicit revocation keeps a later token cutoff"
    );
    // A later RBAC cutoff leaves the assertion cutoff where it was, and an
    // earlier explicit one does not move it back.
    store
        .revoke_principals(ASSERTION_TENANT, vec!["erin".into()], 150)
        .await
        .expect("revoke");
    store
        .revoke_principals_explicitly(ASSERTION_TENANT, vec!["erin".into()], 80)
        .await
        .expect("revoke");
    assert_eq!(cutoffs("erin").await, both(Some(150), Some(90)));

    // The whole tenant's count for each principal, each kind on its own.
    store
        .revoke_principals_explicitly(ASSERTION_TENANT, vec![ALL_PRINCIPALS.into()], 120)
        .await
        .expect("revoke");
    assert_eq!(cutoffs("erin").await, both(Some(150), Some(120)));
    assert_eq!(cutoffs("frank").await, both(Some(120), Some(120)));

    // A change's revocation is an RBAC one.
    let _ = store
        .create_tenant(crate::model::Tenant {
            tenant_id: ASSERTION_TENANT.to_string(),
            display_name: "Revocation assertions".to_string(),
        })
        .await;
    let policy = PolicyRule {
        subject: "gina".to_string(),
        object: format!("tenant:{ASSERTION_TENANT}"),
        action: "stream.publish".to_string(),
    };
    store
        .add_rbac_policy(ASSERTION_TENANT, policy.clone())
        .await
        .expect("policy");
    store
        .change_auth(
            ASSERTION_TENANT,
            AuthChange::RemovePolicy(policy),
            Some(Revocation {
                principal_ids: vec!["gina".into()],
                revoked_at: 400,
            }),
        )
        .await
        .expect("change");
    assert_eq!(cutoffs("gina").await, both(Some(400), Some(120)));

    // The feed carries the assertion cutoff, so an export keeps it.
    let rows = store.principal_revocations(since).await.expect("feed");
    let erin = rows
        .items
        .iter()
        .find(|row| row.tenant_id == ASSERTION_TENANT && row.principal_id == "erin")
        .expect("erin's row");
    assert_eq!(erin.assertions_revoked_at, Some(90));
}

async fn a_change_and_its_revocation_commit_together(store: &dyn ControlPlaneAuthStore) {
    let policy = PolicyRule {
        subject: "dave".to_string(),
        object: format!("tenant:{CHANGE_TENANT}"),
        action: "stream.publish".to_string(),
    };
    let revoking = |principal: &str| {
        Some(Revocation {
            principal_ids: vec![principal.to_string()],
            revoked_at: 300,
        })
    };
    // Rules are held only for a tenant that exists.
    let _ = store
        .create_tenant(crate::model::Tenant {
            tenant_id: CHANGE_TENANT.to_string(),
            display_name: "Revocation change".to_string(),
        })
        .await;
    store
        .add_rbac_policy(CHANGE_TENANT, policy.clone())
        .await
        .expect("policy");
    store
        .change_auth(
            CHANGE_TENANT,
            AuthChange::RemovePolicy(policy.clone()),
            revoking("dave"),
        )
        .await
        .expect("change");
    assert!(
        !store
            .list_rbac_policies(CHANGE_TENANT)
            .await
            .expect("policies")
            .contains(&policy)
    );
    assert_eq!(
        store
            .principal_revoked_at(CHANGE_TENANT, "dave")
            .await
            .expect("read"),
        Some(300)
    );

    // A change that does not happen records no revocation either.
    let missing = store
        .change_auth(
            CHANGE_TENANT,
            AuthChange::RemovePolicy(policy),
            revoking("erin"),
        )
        .await;
    assert!(
        matches!(missing, Err(StoreError::NotFound(_))),
        "{missing:?}"
    );
    assert_eq!(
        store
            .principal_revoked_at(CHANGE_TENANT, "erin")
            .await
            .expect("read"),
        None
    );
}

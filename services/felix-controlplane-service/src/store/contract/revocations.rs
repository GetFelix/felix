//! Principal revocations every backend must satisfy.
use std::sync::Arc;

use crate::auth::revocation::{ALL_PRINCIPALS, PrincipalRevocation};
use crate::store::ControlPlaneAuthStore;

const TENANT: &str = "revocation-t";

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
}

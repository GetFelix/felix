//! Signing-key behaviour every backend must satisfy.
use std::sync::Arc;

use crate::model::Tenant;
use crate::store::{ControlPlaneAuthStore, StoreError};

const TENANT: &str = "signing-t";
/// Several rounds, because the race needs the callers to overlap between
/// the "no keys yet" read and the write, and one round may not.
const ROUNDS: usize = 5;
const CALLERS: usize = 24;

pub(crate) async fn run_signing_key_contract(store: Arc<dyn ControlPlaneAuthStore>) {
    concurrent_ensures_agree_on_one_stored_key(Arc::clone(&store)).await;
    a_rotation_stages_activates_and_retires(store.as_ref()).await;
}

/// Callers racing to ensure a tenant has keys must all get the key set that
/// ends up stored. A caller handed keys that a later write replaced would
/// sign tokens nothing can verify.
async fn concurrent_ensures_agree_on_one_stored_key(store: Arc<dyn ControlPlaneAuthStore>) {
    for round in 0..ROUNDS {
        let tenant_id = format!("{TENANT}-{round}");
        store
            .create_tenant(Tenant {
                tenant_id: tenant_id.clone(),
                display_name: "Signing".to_string(),
            })
            .await
            .expect("create tenant");

        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS));
        let callers: Vec<_> = (0..CALLERS)
            .map(|_| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                let tenant_id = tenant_id.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    store
                        .ensure_signing_key_current(&tenant_id)
                        .await
                        .expect("ensure signing keys")
                })
            })
            .collect();
        let mut kids = Vec::with_capacity(CALLERS);
        for caller in callers {
            kids.push(caller.await.expect("caller").current.kid);
        }

        let stored = store
            .get_tenant_signing_keys(&tenant_id)
            .await
            .expect("stored keys")
            .current
            .kid;
        let stale = kids.iter().filter(|kid| **kid != stored).count();
        assert_eq!(
            stale, 0,
            "round {round}: {stale} of {CALLERS} callers got keys that are not stored",
        );
    }
}

/// Stage, activate, retire: the staged key verifies before it signs, the old
/// key keeps verifying after it stops signing, and the signing key can never
/// be retired out from under the tenant.
async fn a_rotation_stages_activates_and_retires(store: &dyn ControlPlaneAuthStore) {
    let tenant_id = format!("{TENANT}-rotate");
    store
        .create_tenant(Tenant {
            tenant_id: tenant_id.clone(),
            display_name: "Rotate".to_string(),
        })
        .await
        .expect("create tenant");
    let original = store
        .ensure_signing_key_current(&tenant_id)
        .await
        .expect("keys")
        .current;
    let next = crate::auth::keys::generate_signing_keys()
        .expect("generate")
        .current;

    let staged = store
        .stage_signing_key(&tenant_id, next.clone())
        .await
        .expect("stage");
    assert_eq!(staged.current.kid, original.kid, "staging must not sign");
    assert!(staged.previous.iter().any(|key| key.kid == next.kid));
    assert!(matches!(
        store.stage_signing_key(&tenant_id, next.clone()).await,
        Err(StoreError::Conflict(_))
    ));

    let active = store
        .activate_signing_key(&tenant_id, &next.kid)
        .await
        .expect("activate");
    assert_eq!(active.current.kid, next.kid);
    assert_eq!(
        active
            .previous
            .iter()
            .map(|key| key.kid.as_str())
            .collect::<Vec<_>>(),
        vec![original.kid.as_str()],
        "the replaced key keeps verifying"
    );
    // Idempotent, so a retried activate is not an error.
    let again = store
        .activate_signing_key(&tenant_id, &next.kid)
        .await
        .expect("activate again");
    assert_eq!(again.current.kid, next.kid);

    assert!(matches!(
        store.retire_signing_key(&tenant_id, &next.kid).await,
        Err(StoreError::Conflict(_))
    ));
    let retired = store
        .retire_signing_key(&tenant_id, &original.kid)
        .await
        .expect("retire");
    assert!(retired.previous.is_empty());
    assert_eq!(
        store
            .get_tenant_signing_keys(&tenant_id)
            .await
            .expect("stored")
            .current
            .kid,
        next.kid
    );
    assert!(matches!(
        store.retire_signing_key(&tenant_id, &original.kid).await,
        Err(StoreError::NotFound(_))
    ));
    assert!(matches!(
        store.activate_signing_key(&tenant_id, "no-such-kid").await,
        Err(StoreError::NotFound(_))
    ));
}

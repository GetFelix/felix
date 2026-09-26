//! RBAC rule removal every backend must satisfy.
use std::sync::Arc;

use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::Tenant;
use crate::store::{ControlPlaneAuthStore, StoreError};

const TENANT: &str = "rbac-t";

pub(crate) async fn run_rbac_contract(store: Arc<dyn ControlPlaneAuthStore>) {
    store
        .create_tenant(Tenant {
            tenant_id: TENANT.to_string(),
            display_name: "Rbac".to_string(),
        })
        .await
        .expect("create tenant");
    removing_a_policy_leaves_the_others(store.as_ref()).await;
    removing_a_grouping_leaves_the_others(store.as_ref()).await;
}

fn policy(object: &str) -> PolicyRule {
    PolicyRule {
        subject: "role:reader".to_string(),
        object: object.to_string(),
        action: "stream.subscribe".to_string(),
    }
}

async fn removing_a_policy_leaves_the_others(store: &dyn ControlPlaneAuthStore) {
    let kept = policy("stream:rbac-t/ns/kept");
    let removed = policy("stream:rbac-t/ns/removed");
    store.add_rbac_policy(TENANT, kept.clone()).await.unwrap();
    store
        .add_rbac_policy(TENANT, removed.clone())
        .await
        .unwrap();

    store
        .remove_rbac_policy(TENANT, removed.clone())
        .await
        .expect("remove policy");
    assert_eq!(store.list_rbac_policies(TENANT).await.unwrap(), vec![kept]);

    // Gone is gone: a second removal, and a rule that never existed, are both
    // not-found rather than a silent success the caller cannot tell apart.
    assert!(matches!(
        store.remove_rbac_policy(TENANT, removed).await,
        Err(StoreError::NotFound(_))
    ));
    assert!(matches!(
        store
            .remove_rbac_policy("no-such-tenant", policy("stream:x/y/z"))
            .await,
        Err(StoreError::NotFound(_))
    ));
}

async fn removing_a_grouping_leaves_the_others(store: &dyn ControlPlaneAuthStore) {
    let kept = GroupingRule {
        user: "p:kept".to_string(),
        role: "role:reader".to_string(),
    };
    let removed = GroupingRule {
        user: "p:removed".to_string(),
        role: "role:reader".to_string(),
    };
    store.add_rbac_grouping(TENANT, kept.clone()).await.unwrap();
    store
        .add_rbac_grouping(TENANT, removed.clone())
        .await
        .unwrap();

    store
        .remove_rbac_grouping(TENANT, removed.clone())
        .await
        .expect("remove grouping");
    assert_eq!(store.list_rbac_groupings(TENANT).await.unwrap(), vec![kept]);
    assert!(matches!(
        store.remove_rbac_grouping(TENANT, removed).await,
        Err(StoreError::NotFound(_))
    ));
}

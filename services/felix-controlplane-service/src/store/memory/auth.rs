//! Per-tenant auth: IdP issuers, RBAC rules, signing keys and bootstrap.
use super::InMemoryStore;
use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::store::{AuthStore, StoreError, StoreResult};

pub(super) async fn list_idp_issuers(
    store: &InMemoryStore,
    tenant_id: &str,
) -> StoreResult<Vec<IdpIssuerConfig>> {
    Ok(store
        .idp_issuers
        .read()
        .await
        .get(tenant_id)
        .cloned()
        .unwrap_or_default())
}

pub(super) async fn upsert_idp_issuer(
    store: &InMemoryStore,
    tenant_id: &str,
    issuer: IdpIssuerConfig,
) -> StoreResult<()> {
    let mut issuers = store.idp_issuers.write().await;
    let entries = issuers.entry(tenant_id.to_string()).or_default();
    if let Some(existing) = entries.iter_mut().find(|item| item.issuer == issuer.issuer) {
        *existing = issuer;
    } else {
        entries.push(issuer);
    }
    Ok(())
}

pub(super) async fn delete_idp_issuer(
    store: &InMemoryStore,
    tenant_id: &str,
    issuer: &str,
) -> StoreResult<()> {
    let mut issuers = store.idp_issuers.write().await;
    if let Some(entries) = issuers.get_mut(tenant_id) {
        entries.retain(|item| item.issuer != issuer);
    }
    Ok(())
}

pub(super) async fn list_rbac_policies(
    store: &InMemoryStore,
    tenant_id: &str,
) -> StoreResult<Vec<PolicyRule>> {
    Ok(store
        .rbac_policies
        .read()
        .await
        .get(tenant_id)
        .cloned()
        .unwrap_or_default())
}

pub(super) async fn list_rbac_groupings(
    store: &InMemoryStore,
    tenant_id: &str,
) -> StoreResult<Vec<GroupingRule>> {
    Ok(store
        .rbac_groupings
        .read()
        .await
        .get(tenant_id)
        .cloned()
        .unwrap_or_default())
}

pub(super) async fn add_rbac_policy(
    store: &InMemoryStore,
    tenant_id: &str,
    policy: PolicyRule,
) -> StoreResult<()> {
    store
        .rbac_policies
        .write()
        .await
        .entry(tenant_id.to_string())
        .or_default()
        .push(policy);
    Ok(())
}

pub(super) async fn add_rbac_grouping(
    store: &InMemoryStore,
    tenant_id: &str,
    grouping: GroupingRule,
) -> StoreResult<()> {
    store
        .rbac_groupings
        .write()
        .await
        .entry(tenant_id.to_string())
        .or_default()
        .push(grouping);
    Ok(())
}

pub(super) async fn remove_rbac_policy(
    store: &InMemoryStore,
    tenant_id: &str,
    policy: &PolicyRule,
) -> StoreResult<()> {
    let mut all = store.rbac_policies.write().await;
    let entries = all.get_mut(tenant_id);
    remove_all(entries, policy, "policy")
}

pub(super) async fn remove_rbac_grouping(
    store: &InMemoryStore,
    tenant_id: &str,
    grouping: &GroupingRule,
) -> StoreResult<()> {
    let mut all = store.rbac_groupings.write().await;
    let entries = all.get_mut(tenant_id);
    remove_all(entries, grouping, "grouping")
}

pub(super) async fn get_tenant_signing_keys(
    store: &InMemoryStore,
    tenant_id: &str,
) -> StoreResult<TenantSigningKeys> {
    store
        .tenant_signing_keys
        .read()
        .await
        .get(tenant_id)
        .cloned()
        .ok_or_else(|| StoreError::NotFound("signing keys".into()))
}

pub(super) async fn set_tenant_signing_keys(
    store: &InMemoryStore,
    tenant_id: &str,
    keys: TenantSigningKeys,
) -> StoreResult<()> {
    store
        .tenant_signing_keys
        .write()
        .await
        .insert(tenant_id.to_string(), keys);
    crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
    Ok(())
}

pub(super) async fn stage_signing_key(
    store: &InMemoryStore,
    tenant_id: &str,
    key: SigningKey,
) -> StoreResult<TenantSigningKeys> {
    let mut all = store.tenant_signing_keys.write().await;
    let keys = all
        .get_mut(tenant_id)
        .ok_or_else(|| StoreError::NotFound("signing keys".into()))?;
    if keys.all_keys().any(|existing| existing.kid == key.kid) {
        return Err(StoreError::Conflict("kid already in use".into()));
    }
    keys.previous.push(key);
    let staged = keys.clone();
    drop(all);
    crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
    Ok(staged)
}

pub(super) async fn activate_signing_key(
    store: &InMemoryStore,
    tenant_id: &str,
    kid: &str,
) -> StoreResult<TenantSigningKeys> {
    let mut all = store.tenant_signing_keys.write().await;
    let keys = all
        .get_mut(tenant_id)
        .ok_or_else(|| StoreError::NotFound("signing keys".into()))?;
    if keys.current.kid != kid {
        let position = keys
            .previous
            .iter()
            .position(|key| key.kid == kid)
            .ok_or_else(|| StoreError::NotFound("signing key".into()))?;
        std::mem::swap(&mut keys.current, &mut keys.previous[position]);
    }
    let activated = keys.clone();
    drop(all);
    crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
    Ok(activated)
}

pub(super) async fn retire_signing_key(
    store: &InMemoryStore,
    tenant_id: &str,
    kid: &str,
) -> StoreResult<TenantSigningKeys> {
    let mut all = store.tenant_signing_keys.write().await;
    let keys = all
        .get_mut(tenant_id)
        .ok_or_else(|| StoreError::NotFound("signing keys".into()))?;
    if keys.current.kid == kid {
        return Err(StoreError::Conflict(
            "the current signing key cannot be retired".into(),
        ));
    }
    let before = keys.previous.len();
    keys.previous.retain(|key| key.kid != kid);
    if keys.previous.len() == before {
        return Err(StoreError::NotFound("signing key".into()));
    }
    let retired = keys.clone();
    drop(all);
    crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
    Ok(retired)
}

pub(super) async fn tenant_auth_is_bootstrapped(
    store: &InMemoryStore,
    tenant_id: &str,
) -> StoreResult<bool> {
    Ok(store
        .auth_bootstrapped
        .read()
        .await
        .get(tenant_id)
        .copied()
        .unwrap_or(false))
}

pub(super) async fn set_tenant_auth_bootstrapped(
    store: &InMemoryStore,
    tenant_id: &str,
    bootstrapped: bool,
) -> StoreResult<()> {
    store
        .auth_bootstrapped
        .write()
        .await
        .insert(tenant_id.to_string(), bootstrapped);
    Ok(())
}

pub(super) async fn ensure_signing_key_current(
    store: &InMemoryStore,
    tenant_id: &str,
) -> StoreResult<TenantSigningKeys> {
    let candidate = crate::auth::keys::generate_signing_keys().map_err(StoreError::Unexpected)?;
    Ok(install_signing_keys_if_absent(store, tenant_id, candidate).await)
}

pub(super) async fn seed_rbac_policies_and_groupings(
    store: &InMemoryStore,
    tenant_id: &str,
    policies: Vec<PolicyRule>,
    groupings: Vec<GroupingRule>,
) -> StoreResult<()> {
    {
        let mut existing = store.rbac_policies.write().await;
        let entry = existing.entry(tenant_id.to_string()).or_default();
        for policy in policies {
            if !entry.contains(&policy) {
                entry.push(policy);
            }
        }
    }
    {
        let mut existing = store.rbac_groupings.write().await;
        let entry = existing.entry(tenant_id.to_string()).or_default();
        for grouping in groupings {
            if !entry.contains(&grouping) {
                entry.push(grouping);
            }
        }
    }
    Ok(())
}

pub(super) async fn bootstrap_tenant_auth(
    store: &InMemoryStore,
    tenant_id: &str,
    seed: crate::store::TenantAuthSeed,
) -> StoreResult<TenantSigningKeys> {
    let _serial = store.bootstrap_serial.lock().await;

    if !store.tenants.read().await.contains_key(tenant_id) {
        return Err(StoreError::NotFound("tenant".into()));
    }
    if store
        .auth_bootstrapped
        .read()
        .await
        .get(tenant_id)
        .copied()
        .unwrap_or(false)
    {
        return Err(StoreError::Conflict("tenant already initialized".into()));
    }

    // Install the caller's keys only when none exist: the seed's keys are
    // the propose-time randomness, and existing keys always win so a
    // replayed or raced bootstrap cannot rotate a tenant's keys.
    let keys = install_signing_keys_if_absent(store, tenant_id, seed.signing_keys).await;
    for issuer in seed.issuers {
        store.upsert_idp_issuer(tenant_id, issuer).await?;
    }
    store
        .seed_rbac_policies_and_groupings(tenant_id, seed.policies, seed.groupings)
        .await?;
    // Last, so a failure above leaves the tenant retryable rather than
    // half-initialized and claimed.
    store
        .auth_bootstrapped
        .write()
        .await
        .insert(tenant_id.to_string(), true);
    Ok(keys)
}

/// Store `candidate` unless the tenant already has keys, and return whichever
/// set is stored. Check and insert share one write lock, or racing callers
/// each insert their own and all but the last hand out keys that are gone.
async fn install_signing_keys_if_absent(
    store: &InMemoryStore,
    tenant_id: &str,
    candidate: TenantSigningKeys,
) -> TenantSigningKeys {
    let mut all = store.tenant_signing_keys.write().await;
    if let Some(existing) = all.get(tenant_id) {
        return existing.clone();
    }
    all.insert(tenant_id.to_string(), candidate.clone());
    drop(all);
    crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
    candidate
}

/// Remove every copy of `rule`; `NotFound` when there was none. Every copy,
/// because a removed grant that survives as a duplicate is still a grant.
fn remove_all<T: PartialEq>(entries: Option<&mut Vec<T>>, rule: &T, what: &str) -> StoreResult<()> {
    let Some(entries) = entries else {
        return Err(StoreError::NotFound(what.to_string()));
    };
    let before = entries.len();
    entries.retain(|entry| entry != rule);
    if entries.len() == before {
        return Err(StoreError::NotFound(what.to_string()));
    }
    Ok(())
}

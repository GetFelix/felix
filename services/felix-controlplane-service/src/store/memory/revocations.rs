//! Principal revocations: one row per principal, at its latest revocation.
use std::collections::BTreeMap;

use super::InMemoryStore;
use crate::auth::revocation::{ALL_PRINCIPALS, PrincipalRevocation};
use crate::store::{ChangeSet, StoreResult};

/// The rows, keyed by tenant and principal, and the next position to give
/// one. Ordered so an export is the same on every Raft member.
#[derive(Debug, Default)]
pub(super) struct Revocations {
    pub(super) rows: BTreeMap<(String, String), PrincipalRevocation>,
    pub(super) next_seq: u64,
}

impl Revocations {
    pub(super) fn from_rows(rows: Vec<PrincipalRevocation>) -> Self {
        let next_seq = rows.iter().map(|row| row.seq + 1).max().unwrap_or(1);
        Self {
            rows: rows
                .into_iter()
                .map(|row| ((row.tenant_id.clone(), row.principal_id.clone()), row))
                .collect(),
            next_seq,
        }
    }

    pub(super) fn sorted_rows(&self) -> Vec<PrincipalRevocation> {
        self.rows.values().cloned().collect()
    }
}

pub(super) async fn revoke_principals(
    store: &InMemoryStore,
    tenant_id: &str,
    principal_ids: Vec<String>,
    revoked_at: i64,
) -> StoreResult<()> {
    let mut revocations = store.revocations.write().await;
    // Positions start at 1, so a `since` of 0 always means "from the start".
    revocations.next_seq = revocations.next_seq.max(1);
    for principal_id in principal_ids {
        let seq = revocations.next_seq;
        revocations.next_seq += 1;
        let key = (tenant_id.to_string(), principal_id);
        let revoked_at = revocations
            .rows
            .get(&key)
            .map_or(revoked_at, |row| row.revoked_at.max(revoked_at));
        revocations.rows.insert(
            key.clone(),
            PrincipalRevocation {
                seq,
                tenant_id: key.0,
                principal_id: key.1,
                revoked_at,
            },
        );
    }
    Ok(())
}

pub(super) async fn principal_revocations(
    store: &InMemoryStore,
    since: u64,
) -> StoreResult<ChangeSet<PrincipalRevocation>> {
    let revocations = store.revocations.read().await;
    let since = if since > revocations.next_seq {
        0
    } else {
        since
    };
    let mut items: Vec<PrincipalRevocation> = revocations
        .rows
        .values()
        .filter(|row| row.seq >= since)
        .cloned()
        .collect();
    items.sort_by_key(|row| row.seq);
    items.truncate(store.limit());
    let next_seq = items.last().map_or(since, |row| row.seq + 1);
    Ok(ChangeSet { items, next_seq })
}

pub(super) async fn principal_revoked_at(
    store: &InMemoryStore,
    tenant_id: &str,
    principal_id: &str,
) -> StoreResult<Option<i64>> {
    let revocations = store.revocations.read().await;
    let at = |principal: &str| {
        revocations
            .rows
            .get(&(tenant_id.to_string(), principal.to_string()))
            .map(|row| row.revoked_at)
    };
    Ok(at(principal_id).max(at(ALL_PRINCIPALS)))
}

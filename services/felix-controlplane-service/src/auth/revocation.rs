//! Ending a principal's broker sessions before its tokens expire.
//!
//! A revocation is a cutoff time: every broker token the principal was issued
//! at or before it is refused, and brokers end the streams those tokens
//! opened. Brokers poll [`principal_revocation_changes`] the way they poll the
//! catalog feeds. `docs/auth.md` ("Revoking a principal") has the rest.
use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::AppState;
use crate::api::error::{ApiError, api_internal};
use crate::auth::bearer::require_cluster_action;
use crate::auth::rbac::authorize::ACTION_NODE_VIEW;
use crate::auth::rbac::policy_store::GroupingRule;
use crate::store::StoreResult;

/// The principal id that stands for every principal of a tenant.
pub const ALL_PRINCIPALS: &str = "*";

/// The longest a mint waits for the second of a revocation to pass. More than
/// this is clock skew between members, and waiting longer would not fix it.
const MAX_MINT_WAIT: Duration = Duration::from_secs(2);

/// One principal's latest revocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PrincipalRevocation {
    /// Position in the feed. Rewritten each time the principal is revoked again.
    pub seq: u64,
    pub tenant_id: String,
    /// A token's `sub`, or `*` for every principal of the tenant.
    pub principal_id: String,
    /// Seconds since the epoch. Tokens issued at or before it are revoked.
    pub revoked_at: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PrincipalRevocationChangesResponse {
    pub items: Vec<PrincipalRevocation>,
    pub next_seq: u64,
}

#[derive(Debug, Deserialize)]
pub struct ChangesQuery {
    #[serde(default)]
    since: u64,
}

/// Who loses rights when `subject` does: `subject`, and every subject that
/// reaches it through role assignments. An IdP group among them has members
/// only the IdP knows, so the whole tenant is named instead.
pub(crate) fn reached_by(groupings: &[GroupingRule], subject: &str) -> Vec<String> {
    let mut members: HashMap<&str, Vec<&str>> = HashMap::new();
    for grouping in groupings {
        members
            .entry(grouping.role.as_str())
            .or_default()
            .push(grouping.user.as_str());
    }
    let mut reached = BTreeSet::from([subject]);
    let mut pending = vec![subject];
    while let Some(next) = pending.pop() {
        for member in members.get(next).into_iter().flatten() {
            if reached.insert(member) {
                pending.push(member);
            }
        }
    }
    if reached.iter().any(|subject| subject.starts_with("group:")) {
        return vec![ALL_PRINCIPALS.to_string()];
    }
    reached.into_iter().map(str::to_string).collect()
}

/// Revoke `principal_ids` of `tenant_id` as of now.
pub(crate) async fn revoke(
    state: &AppState,
    tenant_id: &str,
    principal_ids: Vec<String>,
) -> StoreResult<()> {
    let count = principal_ids.len() as u64;
    state
        .store
        .revoke_principals(tenant_id, principal_ids, crate::auth::refresh::now_secs())
        .await?;
    metrics::counter!("felix_principal_revocations_total").increment(count);
    Ok(())
}

/// Hold a mint for `principal_id` until a revocation made this second is in
/// the past. Token times are whole seconds, so a token minted in the same
/// second as a revocation would be refused with the ones it replaced.
pub(crate) async fn wait_out_revocation(
    state: &AppState,
    tenant_id: &str,
    principal_id: &str,
) -> Result<(), ApiError> {
    let revoked_at = state
        .store
        .principal_revoked_at(tenant_id, principal_id)
        .await
        .map_err(|err| api_internal("failed to load revocations", &err))?;
    wait_past(revoked_at).await;
    Ok(())
}

/// [`wait_out_revocation`] for a cutoff already read.
pub(crate) async fn wait_past(revoked_at: Option<i64>) {
    let Some(revoked_at) = revoked_at else {
        return;
    };
    let now_millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as i64)
        .unwrap_or(0);
    let wait_millis = (revoked_at.saturating_add(1)).saturating_mul(1000) - now_millis;
    if wait_millis > 0 {
        tokio::time::sleep(Duration::from_millis(wait_millis as u64).min(MAX_MINT_WAIT)).await;
    }
}

/// Principal revocations since a position, for brokers.
#[utoipa::path(
    get,
    path = "/v1/principal-revocations/changes",
    tag = "auth",
    params(("since" = Option<u64>, Query, description = "Last next_seq seen; 0 for all")),
    responses(
        (status = 200, body = PrincipalRevocationChangesResponse),
        (status = 401, description = "Missing or invalid bearer token", body = crate::api::types::ErrorResponse),
        (status = 403, description = "Missing node.view:cluster:*", body = crate::api::types::ErrorResponse)
    )
)]
pub async fn principal_revocation_changes(
    Query(query): Query<ChangesQuery>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<PrincipalRevocationChangesResponse>, ApiError> {
    require_cluster_action(&state, &headers, ACTION_NODE_VIEW).await?;
    let changes = state
        .store
        .principal_revocations(query.since)
        .await
        .map_err(|err| api_internal("failed to load revocations", &err))?;
    Ok(Json(PrincipalRevocationChangesResponse {
        items: changes.items,
        next_seq: changes.next_seq,
    }))
}

#[cfg(test)]
mod tests;

//! Bearer-token checks shared by every authenticated control-plane handler.
//!
//! Two shapes of check, one verification path:
//!
//! - **Cluster-scoped.** The object is `cluster:*`, which no tenant owns, so
//!   the token's own `tid` picks the signing keys. Membership and the metadata
//!   feeds the brokers consume live here, and so does the tenant catalog.
//! - **Tenant-scoped.** The path names the tenant; its keys verify the token
//!   and its `tid` has to match. Everything under `/v1/tenants/{tenant_id}`.
//!
//! In both, a permission that does not parse is skipped rather than trusted,
//! so a malformed entry can never widen what a token allows.
//!
//! Every refusal goes through [`refused`], so a credential that was presented
//! and turned away is counted and logged: an unauthorized attempt is something
//! an operator gets to see, not only something the caller gets told.
use axum::http::{HeaderMap, StatusCode};

use crate::api::AppState;
use crate::api::error::{ApiError, api_error, api_forbidden, api_internal, api_unauthorized};
use crate::auth::felix_token::{
    BROKER_AUDIENCE, CONTROLPLANE_AUDIENCE, FelixClaims, verify_token_for,
};
use crate::auth::rbac::authorize::{
    ParsedObject, ParsedPermission, object_within_scope, parse_permission,
};
use crate::store::StoreError;

/// Clock skew tolerated when checking `exp`, in seconds.
const LEEWAY_SECS: u64 = 5;

/// Set to `true` to keep accepting broker-audience tokens on the API while
/// callers move to `felix-controlplane` tokens. It lets a broker replay a
/// client's token against the API, so it is for the migration only.
pub(crate) const ACCEPT_BROKER_AUDIENCE_ENV: &str = "FELIX_CONTROLPLANE_ACCEPT_BROKER_AUDIENCE";

/// Counts every credential refusal. `reason` is one of a closed set, so the
/// label stays bounded.
pub(crate) const AUTH_REJECTED_TOTAL: &str = "felix_controlplane_auth_rejected_total";

/// Why a credential was turned away.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Refusal {
    /// No `Authorization: Bearer` at all.
    MissingToken,
    /// Not a JWT, or one without a tenant claim.
    MalformedToken,
    /// Did not verify against the tenant's keys, or the tenant has none.
    InvalidToken,
    /// Verified, but for a different tenant than the one addressed.
    TenantMismatch,
    /// Verified, but without the permission the request needs.
    Forbidden,
    /// A refresh token that cannot be used, for any reason. One label, like
    /// the one answer the caller gets, so the counter is no probing aid either.
    RefreshRefused,
    /// The `actor_token` of an exchange, the gateway's own credential, did not
    /// verify for the tenant or does not hold `token.delegate`. Its own code so
    /// a gateway can tell its token was refused, not the user.
    ActorRefused,
}

impl Refusal {
    fn label(self) -> &'static str {
        match self {
            Self::MissingToken => "missing_token",
            Self::MalformedToken => "malformed_token",
            Self::InvalidToken => "invalid_token",
            Self::TenantMismatch => "tenant_mismatch",
            Self::Forbidden => "forbidden",
            Self::RefreshRefused => "refresh_refused",
            Self::ActorRefused => "actor_refused",
        }
    }
}

/// Turn a credential away: count it, log it, and answer.
///
/// Never the token, never the key. What is logged is the reason and the
/// message the caller also sees.
pub(crate) fn refused(reason: Refusal, message: &str) -> ApiError {
    metrics::counter!(AUTH_REJECTED_TOTAL, "reason" => reason.label()).increment(1);
    tracing::info!(
        reason = reason.label(),
        message,
        "refused a control-plane credential"
    );
    match reason {
        Refusal::MissingToken | Refusal::MalformedToken | Refusal::InvalidToken => {
            api_unauthorized(message)
        }
        Refusal::TenantMismatch | Refusal::Forbidden | Refusal::RefreshRefused => {
            api_forbidden(message)
        }
        Refusal::ActorRefused => api_error(StatusCode::FORBIDDEN, reason.label(), message),
    }
}

pub(crate) fn extract_bearer(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| refused(Refusal::MissingToken, "missing bearer token"))
}

/// Verify a token against the keys of the tenant it names, and return both.
///
/// For endpoints outside the tenant hierarchy. The `tid` claim only selects
/// which keys to check against -- the signature is what grants trust, exactly
/// as `kid` selects a key without conferring one.
pub(crate) async fn verified_claims(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(String, FelixClaims), ApiError> {
    let bearer = extract_bearer(headers)?;
    let tenant_id = unverified_tenant(bearer)?;
    let claims = verify_against(state, &tenant_id, bearer).await?;
    Ok((tenant_id, claims))
}

/// Verify a token against `tenant_id`'s keys and require that it was minted
/// for that tenant.
///
/// A tenant with no keys answers 401, not 404: whether a tenant exists is not
/// something an unauthenticated caller gets to learn by asking.
pub(crate) async fn tenant_claims(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
) -> Result<FelixClaims, ApiError> {
    let bearer = extract_bearer(headers)?;
    let claims = verify_against(state, tenant_id, bearer).await?;
    if claims.tid != tenant_id {
        return Err(refused(Refusal::TenantMismatch, "tenant mismatch"));
    }
    Ok(claims)
}

/// The caller's parsed permissions within `tenant_id`.
pub(crate) async fn tenant_permissions(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
) -> Result<Vec<ParsedPermission>, ApiError> {
    let claims = tenant_claims(state, tenant_id, headers).await?;
    Ok(claims
        .perms
        .iter()
        .filter_map(|perm| parse_permission(perm, tenant_id).ok())
        .collect())
}

/// The objects the caller holds `action` over within `tenant_id`.
///
/// 403 when there are none, so a listing endpoint can filter to these without
/// a separate "may list at all" check.
pub(crate) async fn tenant_scopes_for(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
    action: &str,
) -> Result<Vec<ParsedObject>, ApiError> {
    let scopes: Vec<ParsedObject> = tenant_permissions(state, tenant_id, headers)
        .await?
        .into_iter()
        .filter(|perm| perm.action == action)
        .map(|perm| perm.object)
        .collect();
    if scopes.is_empty() {
        return Err(refused(Refusal::Forbidden, "missing required permission"));
    }
    Ok(scopes)
}

/// Require `action` over one object inside `tenant_id`.
pub(crate) async fn require_tenant_action(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
    action: &str,
    target: &ParsedObject,
) -> Result<(), ApiError> {
    let scopes = tenant_scopes_for(state, tenant_id, headers, action).await?;
    if scopes
        .iter()
        .any(|scope| object_within_scope(scope, target))
    {
        Ok(())
    } else {
        Err(refused(Refusal::Forbidden, "insufficient scope"))
    }
}

/// Require `action` over `cluster:*`.
///
/// No tenant scope contains the cluster, so a tenant admin cannot write this
/// rule for themselves; it reaches a token only when an operator who already
/// holds cluster scope grants it.
pub(crate) async fn require_cluster_action(
    state: &AppState,
    headers: &HeaderMap,
    action: &str,
) -> Result<(), ApiError> {
    let (tenant_id, claims) = verified_claims(state, headers).await?;
    let allowed = claims.perms.iter().any(|perm| {
        // Parsed against the token's own tenant; a cluster object ignores it.
        matches!(
            parse_permission(perm, &tenant_id),
            Ok(parsed) if parsed.action == action && parsed.object == ParsedObject::Cluster
        )
    });
    if allowed {
        Ok(())
    } else {
        Err(refused(
            Refusal::Forbidden,
            &format!("missing {action}:cluster:* permission"),
        ))
    }
}

/// This instance cannot say whether the token is good.
///
/// Distinct from `401 invalid token` on purpose. A 401 is a statement about
/// the caller's credential and sends whoever reads it to re-mint, re-auth, or
/// suspect their identity provider. This says the *server* cannot verify right
/// now, which is a different remedy: try another instance, or try again.
///
/// 503 rather than 500 for the same reason `/v1/system/ready` answers 503 --
/// it is a statement about this instance at this moment, and a load balancer
/// taking it out of rotation is the correct response rather than something to
/// alert on.
pub(crate) fn cannot_verify(detail: &str) -> ApiError {
    metrics::counter!(AUTH_REJECTED_TOTAL, "reason" => "cannot_verify").increment(1);
    tracing::warn!(
        detail,
        "cannot verify a credential: this instance does not hold the tenant's \
         signing keys and is not ready to serve",
    );
    crate::api::error::api_error(
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "cannot_verify",
        &format!("this instance cannot verify credentials right now: {detail}"),
    )
}

async fn verify_against(
    state: &AppState,
    tenant_id: &str,
    bearer: &str,
) -> Result<FelixClaims, ApiError> {
    let keys = match state.store.get_tenant_signing_keys(tenant_id).await {
        Ok(keys) => keys,
        // No keys means nothing could have signed this token -- *if* this
        // instance holds the cluster's state. On one that does not, it means
        // "I do not know yet", and the token may be perfectly good: a member
        // whose volume was replaced holds no keys until they replicate in,
        // and answering 401 there tells a caller to fix a credential that is
        // not broken (#601).
        //
        // So the answer depends on whether this instance can serve at all.
        // Asked only here, on a path that is already failing, and the check is
        // cached and bounded -- a probe must not cost a round trip.
        Err(StoreError::NotFound(_)) => {
            return Err(match state.readiness.check().await {
                Ok(()) => refused(Refusal::InvalidToken, "invalid token"),
                Err(reason) => cannot_verify(&reason.to_string()),
            });
        }
        Err(ref err) => return Err(api_internal("failed to load signing keys", err)),
    };
    verify_token_for(&keys, tenant_id, bearer, LEEWAY_SECS, &api_audiences())
        .map_err(|_| refused(Refusal::InvalidToken, "invalid token"))
}

/// The audiences the API accepts: its own, and the broker's only while
/// [`ACCEPT_BROKER_AUDIENCE_ENV`] is set.
fn api_audiences() -> Vec<&'static str> {
    let accept_broker = std::env::var(ACCEPT_BROKER_AUDIENCE_ENV)
        .map(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false);
    if accept_broker {
        vec![CONTROLPLANE_AUDIENCE, BROKER_AUDIENCE]
    } else {
        vec![CONTROLPLANE_AUDIENCE]
    }
}

/// Read `tid` from an unverified token, only to choose a verification key.
fn unverified_tenant(token: &str) -> Result<String, ApiError> {
    use base64::Engine as _;

    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| refused(Refusal::MalformedToken, "malformed token"))?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| refused(Refusal::MalformedToken, "malformed token"))?;
    let claims: serde_json::Value = serde_json::from_slice(&decoded)
        .map_err(|_| refused(Refusal::MalformedToken, "malformed token"))?;

    claims
        .get("tid")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| refused(Refusal::MalformedToken, "token has no tenant claim"))
}

#[cfg(test)]
mod tests;

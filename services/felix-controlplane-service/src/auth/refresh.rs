//! Token refresh: `POST /v1/tenants/{tenant_id}/token/refresh`.
//!
//! Exchange is the expensive half — an upstream IdP round trip, issuer
//! validation, the whole OIDC path. Refresh is how a long-running caller stays
//! authenticated without repeating it, and without being handed a long-lived
//! bearer token instead.
//!
//! What it deliberately does *not* do is reuse the previous token's
//! permissions. RBAC is re-evaluated on every refresh against the principal and
//! the group claims recorded at exchange, so a revoked grant takes effect at
//! the next refresh rather than whenever the caller happens to re-exchange. A
//! refresh that froze its grants would turn a short access TTL into a long one
//! for authorization purposes, which is most of what the short TTL was for.
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, State};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::AppState;
use crate::api::error::{ApiError, api_internal, api_internal_message};
use crate::auth::bearer::{Refusal, refused};
use crate::auth::felix_token::mint_token_may_act;
use crate::auth::rbac::enforcer::build_enforcer;
use crate::auth::rbac::permissions::effective_permissions;
use crate::auth::refresh_token::{self, RefreshToken, RefreshTokenTake};
use crate::store::StoreError;

/// How long a refresh token is good for.
///
/// Long enough that a process can stay up across a deployment's usual quiet
/// periods, short enough to bound a leak that rotation did not catch — a stolen
/// token that is never used produces no replay to detect, so this is the only
/// thing that ends it.
const DEFAULT_REFRESH_TTL_SECONDS: u64 = 30 * 24 * 60 * 60;

pub fn refresh_ttl() -> Duration {
    Duration::from_secs(
        std::env::var("FELIX_REFRESH_TOKEN_TTL_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|&value| value > 0)
            .unwrap_or(DEFAULT_REFRESH_TTL_SECONDS),
    )
}

/// How long after a refresh token is rotated a retry with it still gets a
/// working token rather than ending the chain. 0 turns it off.
pub(crate) const REUSE_GRACE_ENV: &str = "FELIX_REFRESH_REUSE_GRACE_SECONDS";
const DEFAULT_REUSE_GRACE_SECONDS: i64 = 30;

fn reuse_grace_secs() -> i64 {
    std::env::var(REUSE_GRACE_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|&value| value >= 0)
        .unwrap_or(DEFAULT_REUSE_GRACE_SECONDS)
}

#[derive(Deserialize, ToSchema, Clone)]
pub struct TokenRefreshRequest {
    /// The refresh token handed out by exchange or by a previous refresh.
    pub refresh_token: String,
    /// As for exchange: `felix-broker` or `felix-controlplane`. A token keeps
    /// the audience its exchange chose; naming a different one is a `400`.
    #[serde(default)]
    pub audience: Option<String>,
}

#[derive(Serialize, ToSchema, Clone)]
pub struct TokenRefreshResponse {
    pub felix_token: String,
    pub expires_in: u64,
    pub token_type: String,
    /// The replacement refresh token. The one just presented is spent.
    pub refresh_token: String,
    pub refresh_expires_in: u64,
}

impl std::fmt::Debug for TokenRefreshRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenRefreshRequest")
            .field("refresh_token", &"<redacted>")
            .field("audience", &self.audience)
            .finish()
    }
}

impl std::fmt::Debug for TokenRefreshResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenRefreshResponse")
            .field("felix_token", &"<redacted>")
            .field("expires_in", &self.expires_in)
            .field("token_type", &self.token_type)
            .field("refresh_token", &"<redacted>")
            .field("refresh_expires_in", &self.refresh_expires_in)
            .finish()
    }
}

/// Mint a refresh token for a principal, returning the record to store and the
/// secret to hand back exactly once.
///
/// `family_id` continues an existing rotation chain, or starts one when `None`.
pub fn issue(
    tenant_id: &str,
    principal_id: &str,
    groups: Vec<String>,
    family_id: Option<String>,
    now_secs: i64,
    ttl: Duration,
) -> (RefreshToken, String) {
    let token_id = random_token();
    let secret = random_token();
    let record = RefreshToken {
        token_id: token_id.clone(),
        tenant_id: tenant_id.to_string(),
        principal_id: principal_id.to_string(),
        groups,
        secret_hash: refresh_token::hash_secret(&secret),
        family_id: family_id.unwrap_or_else(random_token),
        issued_at_secs: now_secs,
        expires_at_secs: now_secs.saturating_add(ttl.as_secs() as i64),
        used: false,
        revoked: false,
        narrowing: None,
        rotated: None,
    };
    (record, refresh_token::join(&token_id, &secret))
}

/// 256 bits from the OS, hex encoded.
fn random_token() -> String {
    use rand::Rng;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0)
}

/// The recorded group claims a refresh still honours.
///
/// Only names scoped by an issuer the tenant still trusts: removing an IdP
/// takes its groups with it at the next refresh, and a record from before
/// groups were scoped carries bare names, which are dropped unless the
/// legacy switch is on.
pub(crate) fn refreshable_groups(
    recorded: &[String],
    issuers: &[crate::auth::idp_registry::IdpIssuerConfig],
    legacy: bool,
) -> Vec<String> {
    let mut honoured = Vec::new();
    for group in recorded {
        let trusted = issuers.iter().any(|issuer| {
            group
                .strip_prefix(issuer.issuer.as_str())
                .is_some_and(|rest| rest.starts_with('#'))
        });
        if trusted {
            honoured.extend(crate::auth::exchange::with_legacy_names(
                std::slice::from_ref(group),
                legacy,
            ));
        } else if legacy {
            honoured.push(group.clone());
        }
    }
    honoured
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/token/refresh",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant")),
    request_body = TokenRefreshRequest,
    responses(
        (status = 200, description = "A new access token and its replacement refresh token", body = TokenRefreshResponse),
        (status = 403, description = "The refresh token is not usable"),
    )
)]
pub async fn refresh_token_handler(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    Json(request): Json<TokenRefreshRequest>,
) -> Result<Json<TokenRefreshResponse>, ApiError> {
    // One refusal for every way a token can be unusable — unparseable, unknown,
    // expired, revoked, wrong secret. Distinguishing them would let a caller
    // probe which token ids exist and which secrets are close.
    let unusable = || refused(Refusal::RefreshRefused, "refresh token is not usable");
    // Before the token is spent, so a bad request does not cost the caller
    // its chain.
    let requested_audience = crate::auth::exchange::token_audience(request.audience.as_deref())?;

    let Some((token_id, secret)) = refresh_token::split(&request.refresh_token) else {
        return Err(unusable());
    };

    // Every store read happens before the token is spent. A read that fails
    // after it would make the client's retry look like a replay and end the
    // whole chain.
    let rules = crate::auth::delegate::DelegateRules::load(&state, &tenant_id).await?;
    let keys = match state.store.get_tenant_signing_keys(&tenant_id).await {
        Ok(keys) => keys,
        // A tenant without keys holds no refresh token worth spending, and
        // answering otherwise would say which tenants exist.
        Err(StoreError::NotFound(_)) => return Err(unusable()),
        Err(err) => return Err(api_internal("failed to load signing keys", &err)),
    };

    let now = now_secs();
    let Some(found) = state
        .store
        .peek_refresh_token(&tenant_id, token_id)
        .await
        .map_err(|err| api_internal("failed to read refresh token", &err))?
    else {
        return Err(unusable());
    };
    let cutoff = state
        .store
        .principal_revoked_at(&tenant_id, &found.principal_id)
        .await
        .map_err(|err| api_internal("failed to load revocations", &err))?;

    // A wrong secret against a real token id means someone has half a
    // credential, so it spends the token: leaving it usable would make
    // guessing free.
    if refresh_token::hash_secret(secret) != found.secret_hash {
        let take = state
            .store
            .take_refresh_token(&tenant_id, token_id, now)
            .await
            .map_err(|err| api_internal("failed to read refresh token", &err))?;
        return Err(match take {
            RefreshTokenTake::Replayed(record) => replayed(&state, &tenant_id, &record).await?,
            RefreshTokenTake::Taken(_) => {
                metrics::counter!("felix_refresh_token_bad_secret_total").increment(1);
                unusable()
            }
            RefreshTokenTake::Unusable => unusable(),
        });
    }

    // The chain keeps its exchange's audience.
    let audience = match &found.narrowing {
        Some(narrowing) => {
            let stored = crate::auth::exchange::token_audience(Some(&narrowing.audience))?;
            if request.audience.is_some() && requested_audience != stored {
                return Err(crate::api::error::api_validation_error(
                    "audience differs from the one this refresh token was issued for",
                ));
            }
            stored
        }
        None => requested_audience,
    };

    // The replacement stays in the same family, so a replay of any token in
    // the chain can end the whole chain. It is stored in the same write that
    // spends the token presented: a write that failed in between would leave
    // the chain spent, and the client's retry would look like a replay.
    let ttl = refresh_ttl();
    let (mut next, next_secret) = issue(
        &tenant_id,
        &found.principal_id,
        found.groups.clone(),
        Some(found.family_id.clone()),
        now,
        ttl,
    );
    next.narrowing = found.narrowing.clone();

    // Everything that can fail runs before the spend, so an error here costs
    // the caller nothing. Re-evaluated, never carried over: a grant removed
    // since the last exchange has to stop working at the next refresh.
    let mut groupings = rules.groupings.clone();
    crate::auth::exchange::add_group_claim_groupings(
        &mut groupings,
        &found.principal_id,
        &refreshable_groups(
            &found.groups,
            &rules.issuers,
            crate::auth::exchange::legacy_unscoped_groups(),
        ),
    );
    let enforcer = build_enforcer(&rules.policies, &groupings, &tenant_id)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to build rbac enforcer");
            api_internal_message("failed to build enforcer")
        })?;
    let granted = effective_permissions(&enforcer, &found.principal_id, &tenant_id);
    // Current RBAC, narrowed the way the exchange narrowed it, so a refresh
    // never hands back more than the exchange did.
    let mut perms = granted.clone();
    if let Some(narrowing) = &found.narrowing {
        perms = crate::auth::exchange::narrow_permissions(perms, narrowing, &tenant_id);
    }
    let perms = crate::auth::exchange::for_audience(perms, audience);
    let connect_only = found.narrowing.as_ref().is_some_and(|narrowing| {
        crate::auth::exchange::may_connect_only(&granted, narrowing, &tenant_id)
    });
    let may_act = found
        .narrowing
        .as_ref()
        .and_then(|narrowing| narrowing.may_act.as_deref());
    let refusal = if perms.is_empty() && !connect_only {
        // Every grant is gone since the token was issued, or for a
        // connection-only chain, `token.delegate` is.
        Some("no permissions")
    } else if let Some(actor) = may_act
        && !rules.grant(&tenant_id, actor).await?
    {
        // A chain minted for a gateway to delegate ends once the gateway may
        // no longer delegate. Revocations of the gateway are not checked
        // here: they follow every RBAC edit that reaches it, or the whole
        // tenant, and ending the users' chains for those would be a logout.
        // `/token/delegate` checks them against the gateway's own token.
        Some(crate::auth::delegate::NO_LONGER_GRANTED)
    } else {
        None
    };
    crate::auth::revocation::wait_past(cutoff).await;
    let access_ttl = crate::auth::exchange::access_token_ttl();
    let felix_token = mint_token_may_act(
        &keys,
        &tenant_id,
        &found.principal_id,
        perms,
        access_ttl,
        audience,
        may_act,
    )
    .map_err(|_| api_internal_message("failed to mint token"))?;

    let record = match rotate(&state, &tenant_id, token_id, now, next).await? {
        RefreshTokenTake::Taken(record) => *record,
        RefreshTokenTake::Replayed(record) => {
            return Err(replayed(&state, &tenant_id, &record).await?);
        }
        RefreshTokenTake::Unusable => return Err(unusable()),
    };
    if let Some(reason) = refusal {
        // Refusing is the point of re-evaluating, and the chain ends too, so
        // a principal whose access was removed cannot keep rotating.
        state
            .store
            .revoke_refresh_family(&tenant_id, &record.family_id)
            .await
            .map_err(|err| api_internal("failed to revoke refresh family", &err))?;
        return Err(refused(Refusal::Forbidden, reason));
    }
    metrics::counter!("felix_refresh_tokens_issued_total", "via" => "refresh").increment(1);

    Ok(Json(TokenRefreshResponse {
        felix_token,
        expires_in: access_ttl.as_secs(),
        token_type: "Bearer".to_string(),
        refresh_token: next_secret,
        refresh_expires_in: ttl.as_secs(),
    }))
}

/// Spend `token_id` for `next`. A token rotated within the reuse grace
/// window, whose replacement is still unspent, is a retry of a refresh whose
/// answer was lost (or a concurrent refresh), not a replay: its replacement
/// is spent for `next` instead. Of two concurrent refreshes, each spends the
/// other's result, so the older one is a replay once the window ends.
async fn rotate(
    state: &AppState,
    tenant_id: &str,
    token_id: &str,
    now: i64,
    next: RefreshToken,
) -> Result<RefreshTokenTake, ApiError> {
    let failed = |err: StoreError| api_internal("failed to rotate refresh token", &err);
    let take = state
        .store
        .rotate_refresh_token(tenant_id, token_id, now, next.clone())
        .await
        .map_err(failed)?;
    let RefreshTokenTake::Replayed(spent) = &take else {
        return Ok(take);
    };
    let grace = reuse_grace_secs();
    let Some(rotation) = spent
        .rotated
        .as_ref()
        .filter(|rotation| grace > 0 && now.saturating_sub(rotation.at_secs) <= grace)
    else {
        return Ok(take);
    };
    match state
        .store
        .rotate_refresh_token(tenant_id, &rotation.successor_id, now, next)
        .await
        .map_err(failed)?
    {
        RefreshTokenTake::Taken(successor) => {
            metrics::counter!("felix_refresh_token_grace_reuses_total").increment(1);
            Ok(RefreshTokenTake::Taken(successor))
        }
        // The replacement was used already: two holders, so a replay after all.
        _ => Ok(take),
    }
}

/// Answer a spent refresh token presented again. Nobody legitimately does
/// that: either it was stolen and used, or used and stolen, and the store
/// cannot tell which holder is which, so the chain ends for both and whoever
/// is genuine re-exchanges.
async fn replayed(
    state: &AppState,
    tenant_id: &str,
    record: &RefreshToken,
) -> Result<ApiError, ApiError> {
    tracing::warn!(
        tenant_id = %tenant_id,
        principal_id = %record.principal_id,
        family_id = %record.family_id,
        "a spent refresh token was presented again; revoking the rotation chain",
    );
    metrics::counter!("felix_refresh_token_replays_total").increment(1);
    let revoked = state
        .store
        .revoke_refresh_family(tenant_id, &record.family_id)
        .await
        .map_err(|err| api_internal("failed to revoke refresh family", &err))?;
    metrics::counter!("felix_refresh_tokens_revoked_total").increment(revoked);
    Ok(refused(
        Refusal::RefreshRefused,
        "refresh token is not usable",
    ))
}

#[cfg(test)]
mod tests;

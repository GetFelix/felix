//! Delegation: `POST /v1/tenants/{tenant_id}/token/delegate`.
//!
//! A gateway acting for many users presents each user's broker token over its
//! own connections. With subject binding on, the broker only accepts that if
//! the token says the gateway may present it. This endpoint is where the
//! token comes from: an RFC 8693 token exchange where the caller (the actor)
//! authenticates with its own control-plane token and hands over the user's
//! broker token (the subject token). The result is a broker token for the
//! user's subject with `act: {sub: <caller>}`, scoped no wider than the
//! user's token: same tenant, a subset of its permissions, and an expiry no
//! later than its own.
//!
//! The subject token must name the caller in `may_act`, which the exchange
//! sets when the gateway sends its own token as `actor_token`. Without that, a
//! holder of `token.delegate` could bind any broker token of the tenant it
//! got hold of, a stolen one included, to its own certificate.
use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::AppState;
use crate::api::error::{ApiError, api_internal, api_internal_message, api_validation_error};
use crate::auth::bearer::{Refusal, refused, tenant_claims};
use crate::auth::exchange::{access_token_ttl, for_audience, narrow_permissions};
use crate::auth::felix_token::{
    BROKER_AUDIENCE, CONTROLPLANE_AUDIENCE, FelixClaims, TenantSigningKeys, mint_delegated_token,
    verify_token_for,
};
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::authorize::{
    ACTION_TOKEN_DELEGATE, ParsedObject, object_within_scope, parse_permission,
};
use crate::auth::rbac::enforcer::build_enforcer;
use crate::auth::rbac::permissions::effective_permissions;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::auth::refresh_token::Narrowing;

/// RFC 8693's `grant_type` for a token exchange.
pub const TOKEN_EXCHANGE_GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
/// RFC 8693's token type for a JWT, the only kind taken and issued here.
pub const JWT_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:jwt";

/// Set to `true` to also delegate a subject token that names no actor in
/// `may_act`. Any holder of `token.delegate` can then delegate any of the
/// tenant's broker tokens it has, so it is off by default.
pub const DELEGATE_UNBOUND_TOKENS_ENV: &str = "FELIX_CONTROLPLANE_DELEGATE_UNBOUND_TOKENS";

/// Clock skew tolerated on the subject token's `exp`, as on the caller's.
const LEEWAY_SECS: u64 = 5;

/// An RFC 8693 token exchange, as JSON. The caller is the actor and
/// authenticates with `Authorization: Bearer`.
#[derive(Debug, Deserialize, ToSchema, Clone)]
pub struct TokenDelegateRequest {
    /// Must be `urn:ietf:params:oauth:grant-type:token-exchange`.
    pub grant_type: String,
    /// The user's Felix broker token.
    pub subject_token: String,
    /// `urn:ietf:params:oauth:token-type:jwt` when sent.
    #[serde(default)]
    pub subject_token_type: Option<String>,
    /// Keep only these `action:object` pairs of the subject token's
    /// permissions, each narrowed separately. Never widens.
    #[serde(default)]
    pub permissions: Option<Vec<String>>,
}

/// The delegated broker token. Treat `access_token` as a secret.
#[derive(Serialize, ToSchema, Clone)]
pub struct TokenDelegateResponse {
    pub access_token: String,
    /// Always `urn:ietf:params:oauth:token-type:jwt`.
    pub issued_token_type: String,
    pub token_type: String,
    pub expires_in: u64,
}

impl std::fmt::Debug for TokenDelegateResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenDelegateResponse")
            .field("access_token", &"<redacted>")
            .field("issued_token_type", &self.issued_token_type)
            .field("token_type", &self.token_type)
            .field("expires_in", &self.expires_in)
            .finish()
    }
}

/// Exchange a user's broker token for one the caller may present for them.
///
/// # Errors
/// `400` for a malformed request, `401` for a missing or invalid caller
/// token, `403` when the caller lacks `token.delegate` on the tenant (in its
/// token and in current RBAC), its token predates a revocation of it, or the
/// subject token is invalid, for another tenant, already delegated, not
/// issued for the caller (`may_act`), or left with no permissions, `500` for
/// store failures.
#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/token/delegate",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = TokenDelegateRequest,
    responses(
        (status = 200, description = "Delegated token", body = TokenDelegateResponse),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden")
    )
)]
pub async fn delegate_token(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<TokenDelegateRequest>,
) -> Result<Json<TokenDelegateResponse>, ApiError> {
    if request.grant_type != TOKEN_EXCHANGE_GRANT {
        return Err(api_validation_error(&format!(
            "grant_type must be {TOKEN_EXCHANGE_GRANT}"
        )));
    }
    if request
        .subject_token_type
        .as_deref()
        .is_some_and(|kind| kind != JWT_TOKEN_TYPE)
    {
        return Err(api_validation_error(&format!(
            "subject_token_type must be {JWT_TOKEN_TYPE}"
        )));
    }
    if let Some(pairs) = &request.permissions {
        for pair in pairs {
            parse_permission(pair, &tenant_id).map_err(|err| {
                api_validation_error(&format!("permission {pair:?} is invalid: {err}"))
            })?;
        }
    }

    // The caller: a control-plane token for this tenant holding
    // `token.delegate` over it.
    let actor = tenant_claims(&state, &tenant_id, &headers).await?;
    if !may_delegate(&actor, &tenant_id) {
        return Err(refused(
            Refusal::Forbidden,
            "missing token.delegate permission on the tenant",
        ));
    }

    // The user: a broker token of the same tenant, verified against its keys.
    let keys = state
        .store
        .get_tenant_signing_keys(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load signing keys", &err))?;
    let subject = verify_token_for(
        &keys,
        &tenant_id,
        &request.subject_token,
        LEEWAY_SECS,
        &[BROKER_AUDIENCE],
    )
    .map_err(|_| refused(Refusal::Forbidden, "invalid subject token"))?;
    // One hop only: a delegated token is not handed on again.
    if subject.act.is_some() {
        return Err(refused(
            Refusal::Forbidden,
            "the subject token is already delegated",
        ));
    }
    check_may_act(&subject, &actor.sub, delegate_unbound_tokens())?;
    // Both cutoffs in one read; each already counts the whole tenant's.
    let cutoffs = state
        .store
        .principals_revoked_at(&tenant_id, &[subject.sub.clone(), actor.sub.clone()])
        .await
        .map_err(|err| api_internal("failed to load revocations", &err))?;
    let (subject_cutoff, actor_cutoff) = (cutoffs[0], cutoffs[1]);
    // The caller's own token proves nothing about now: one issued before its
    // principal was revoked stays valid here until it expires.
    if let Some(reason) =
        actor_refusal(&state, &tenant_id, &actor.sub, actor.iat, actor_cutoff).await?
    {
        return Err(refused(Refusal::ActorRefused, reason));
    }
    // A revoked token stays revoked: delegating it would mint one newer than
    // the revocation.
    let latest_cutoff = subject_cutoff.max(actor_cutoff);
    if latest_cutoff.is_some_and(|revoked_at| subject.iat <= revoked_at) {
        return Err(refused(Refusal::Forbidden, "the subject token was revoked"));
    }

    let narrowing = Narrowing {
        requested: None,
        resources: None,
        permissions: request.permissions.clone(),
        audience: BROKER_AUDIENCE.to_string(),
        may_act: None,
    };
    let perms = for_audience(
        narrow_permissions(subject.perms, &narrowing, &tenant_id),
        BROKER_AUDIENCE,
    );
    if perms.is_empty() {
        return Err(refused(Refusal::Forbidden, "no permissions"));
    }

    crate::auth::revocation::wait_past(latest_cutoff).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let exp = subject
        .exp
        .min(now.saturating_add(access_token_ttl().as_secs() as i64));
    if exp <= now {
        return Err(refused(Refusal::Forbidden, "invalid subject token"));
    }
    let access_token =
        mint_delegated_token(&keys, &tenant_id, &subject.sub, &actor.sub, perms, exp)
            .map_err(|_| api_internal_message("failed to mint token"))?;
    metrics::counter!("felix_delegated_tokens_issued_total").increment(1);
    Ok(Json(TokenDelegateResponse {
        access_token,
        issued_token_type: JWT_TOKEN_TYPE.to_string(),
        token_type: "Bearer".to_string(),
        expires_in: (exp - now) as u64,
    }))
}

/// The `sub` of `actor_token`, a `felix-controlplane` token of `tenant_id`
/// that may delegate. This is how an exchange learns which gateway the
/// token it mints is for.
///
/// # Errors
/// `403` with code `actor_refused` when the token is invalid, expired, for
/// another audience or tenant, does not hold `token.delegate` on the tenant,
/// was issued at or before a revocation of its principal, or current RBAC no
/// longer grants that principal `token.delegate`.
pub(crate) async fn verified_actor(
    state: &AppState,
    keys: &TenantSigningKeys,
    tenant_id: &str,
    actor_token: &str,
) -> Result<String, ApiError> {
    let claims = verify_token_for(
        keys,
        tenant_id,
        actor_token,
        LEEWAY_SECS,
        &[CONTROLPLANE_AUDIENCE],
    )
    .map_err(|_| refused(Refusal::ActorRefused, "invalid actor token"))?;
    if !may_delegate(&claims, tenant_id) {
        return Err(refused(
            Refusal::ActorRefused,
            "the actor token lacks token.delegate on the tenant",
        ));
    }
    if let Some(reason) = actor_cut_off(state, tenant_id, &claims.sub, claims.iat).await? {
        return Err(refused(Refusal::ActorRefused, reason));
    }
    Ok(claims.sub)
}

/// Why `actor` may no longer delegate on the strength of a token issued at
/// `issued_at`, if it may not: its principal was revoked since, or current
/// RBAC no longer grants it `token.delegate`.
pub(crate) async fn actor_cut_off(
    state: &AppState,
    tenant_id: &str,
    actor: &str,
    issued_at: i64,
) -> Result<Option<&'static str>, ApiError> {
    let revoked_at = state
        .store
        .principal_revoked_at(tenant_id, actor)
        .await
        .map_err(|err| api_internal("failed to load revocations", &err))?;
    actor_refusal(state, tenant_id, actor, issued_at, revoked_at).await
}

/// [`actor_cut_off`] with the actor's cutoff already read.
async fn actor_refusal(
    state: &AppState,
    tenant_id: &str,
    actor: &str,
    issued_at: i64,
    revoked_at: Option<i64>,
) -> Result<Option<&'static str>, ApiError> {
    if revoked_at.is_some_and(|revoked_at| issued_at <= revoked_at) {
        return Ok(Some("the actor was revoked"));
    }
    let rules = DelegateRules::load(state, tenant_id).await?;
    if !rules.grant(tenant_id, actor).await? {
        return Ok(Some(NO_LONGER_GRANTED));
    }
    Ok(None)
}

/// The refusal for an actor current RBAC no longer grants `token.delegate`.
pub(crate) const NO_LONGER_GRANTED: &str = "the actor no longer holds token.delegate on the tenant";

/// The tenant's RBAC rules and trusted issuers, loaded up front so a caller
/// can do its reads before it commits to anything.
pub(crate) struct DelegateRules {
    pub(crate) policies: Vec<PolicyRule>,
    pub(crate) groupings: Vec<GroupingRule>,
    pub(crate) issuers: Vec<IdpIssuerConfig>,
}

impl DelegateRules {
    pub(crate) async fn load(state: &AppState, tenant_id: &str) -> Result<Self, ApiError> {
        Ok(Self {
            policies: state
                .store
                .list_rbac_policies(tenant_id)
                .await
                .map_err(|err| api_internal("failed to load policies", &err))?,
            groupings: state
                .store
                .list_rbac_groupings(tenant_id)
                .await
                .map_err(|err| api_internal("failed to load groupings", &err))?,
            issuers: state
                .store
                .list_idp_issuers(tenant_id)
                .await
                .map_err(|err| api_internal("failed to load issuers", &err))?,
        })
    }

    /// Whether these rules grant `principal` `token.delegate` on the tenant.
    ///
    /// The principal's IdP groups are not known here, so it counts as a
    /// member of every group the rules name from an issuer the tenant still
    /// trusts, and of bare legacy names only while those are honoured.
    /// Removing a rule that reaches a group, or an issuer, revokes the whole
    /// tenant, which the cutoff check catches.
    pub(crate) async fn grant(&self, tenant_id: &str, principal: &str) -> Result<bool, ApiError> {
        let legacy = crate::auth::exchange::legacy_unscoped_groups();
        let trusted = |group: &str| match group.split_once('#') {
            Some((issuer, _)) => self.issuers.iter().any(|item| item.issuer == issuer),
            None => legacy,
        };
        let mut groups: Vec<&String> = self
            .policies
            .iter()
            .map(|policy| &policy.subject)
            .chain(
                self.groupings
                    .iter()
                    .flat_map(|rule| [&rule.user, &rule.role]),
            )
            .filter(|subject| subject.strip_prefix("group:").is_some_and(trusted))
            .collect();
        groups.sort();
        groups.dedup();
        let mut groupings = self.groupings.clone();
        groupings.extend(groups.into_iter().map(|role| GroupingRule {
            user: principal.to_string(),
            role: role.clone(),
        }));
        let enforcer = build_enforcer(&self.policies, &groupings, tenant_id)
            .await
            .map_err(|err| {
                tracing::error!(error = ?err, "failed to build rbac enforcer");
                api_internal_message("failed to build enforcer")
            })?;
        Ok(holds_token_delegate(
            &effective_permissions(&enforcer, principal, tenant_id),
            tenant_id,
        ))
    }
}

/// Whether `claims` hold `token.delegate` over `tenant_id` in their own
/// right. A delegated token never does.
fn may_delegate(claims: &FelixClaims, tenant_id: &str) -> bool {
    claims.act.is_none() && holds_token_delegate(&claims.perms, tenant_id)
}

/// Whether `perms` include `token.delegate` over all of `tenant_id`.
pub(crate) fn holds_token_delegate(perms: &[String], tenant_id: &str) -> bool {
    let tenant = ParsedObject::Tenant {
        tenant_id: tenant_id.to_string(),
    };
    perms
        .iter()
        .filter_map(|perm| parse_permission(perm, tenant_id).ok())
        .any(|perm| {
            perm.action == ACTION_TOKEN_DELEGATE && object_within_scope(&perm.object, &tenant)
        })
}

/// Refuse a subject token whose `may_act` names someone other than
/// `caller`, or names no one unless `allow_unbound`.
fn check_may_act(subject: &FelixClaims, caller: &str, allow_unbound: bool) -> Result<(), ApiError> {
    match &subject.may_act {
        Some(bound) if bound.sub == caller => Ok(()),
        Some(_) => Err(refused(
            Refusal::Forbidden,
            "the subject token was issued for another actor",
        )),
        None if allow_unbound => Ok(()),
        None => Err(refused(
            Refusal::Forbidden,
            "the subject token names no actor (may_act)",
        )),
    }
}

fn delegate_unbound_tokens() -> bool {
    std::env::var(DELEGATE_UNBOUND_TOKENS_ENV)
        .map(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests;

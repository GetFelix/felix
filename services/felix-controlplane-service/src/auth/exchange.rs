//! Token exchange: `POST /v1/tenants/{tenant_id}/token/exchange`.
//!
//! This is the boundary between external IdP identity and Felix authorization.
//! An upstream OIDC token is validated against the tenant's configured
//! issuers, RBAC decides the effective permissions, and the result is a Felix
//! EdDSA token for the broker. The request body can narrow those permissions
//! but can never widen them.
use std::collections::HashSet;
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::AppState;
use crate::api::error::{
    ApiError, api_conflict, api_internal, api_internal_message, api_validation_error,
};
use crate::auth::bearer::{Refusal, extract_bearer, refused};
use crate::auth::felix_token::{BROKER_AUDIENCE, CONTROLPLANE_AUDIENCE, mint_token_for};
use crate::auth::oidc::OidcError;
use crate::auth::principal;
use crate::auth::rbac::authorize::{
    ACTION_TOKEN_DELEGATE, ParsedObject, format_object, narrow_object, parse_object,
    parse_permission,
};
use crate::auth::rbac::enforcer::build_enforcer;
use crate::auth::rbac::permissions::effective_permissions;
use crate::auth::rbac::policy_store::GroupingRule;
use crate::auth::refresh_token::Narrowing;
use crate::store::StoreError;

/// Optional narrowing filter: keep only these actions and/or resources out of
/// what RBAC already granted. Never widens scope.
#[derive(Debug, Deserialize, ToSchema, Clone, Default)]
pub struct TokenExchangeRequest {
    /// Keep only these actions, on every resource the token keeps.
    pub requested: Option<Vec<String>>,
    /// Keep only these resources, for every action the token keeps.
    pub resources: Option<Vec<String>>,
    /// Keep only these `action:object` pairs, each narrowed separately, so a
    /// token can read one resource and write another. Requires `requested`
    /// to be sent as `[]` and `resources` to be absent or empty: an older
    /// control plane ignores `permissions`, and an empty `requested` makes it
    /// refuse instead of minting full rights.
    #[serde(default)]
    pub permissions: Option<Vec<String>>,
    /// Who the token is for: `felix-broker` (the default) to connect to
    /// brokers, or `felix-controlplane` for this API. A token is accepted by
    /// one of the two, never both.
    #[serde(default)]
    pub audience: Option<String>,
}

/// The minted Felix bearer token plus expiry. Treat `felix_token` as a secret.
#[derive(Serialize, ToSchema, Clone)]
pub struct TokenExchangeResponse {
    pub felix_token: String,
    pub expires_in: u64,
    pub token_type: String,
    /// Presented to `/token/refresh` for a new access token, without another
    /// IdP round trip. Single-use: refreshing mints its replacement.
    pub refresh_token: String,
    pub refresh_expires_in: u64,
}

impl std::fmt::Debug for TokenExchangeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenExchangeResponse")
            .field("felix_token", &"<redacted>")
            .field("expires_in", &self.expires_in)
            .field("token_type", &self.token_type)
            .field("refresh_token", &"<redacted>")
            .field("refresh_expires_in", &self.refresh_expires_in)
            .finish()
    }
}

/// Exchange an upstream IdP token for a Felix EdDSA token.
///
/// # Errors
/// `400` for a malformed narrowing, `401` for a missing or invalid bearer
/// token, `403` when the issuer is not allowed or no permissions remain, `409`
/// for `permissions` before every Raft member supports it, `500` for store
/// failures.
#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/token/exchange",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = TokenExchangeRequest,
    responses(
        (status = 200, description = "Exchange token", body = TokenExchangeResponse),
        (status = 400, description = "Invalid narrowing"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 409, description = "Pair narrowing needs every control-plane member upgraded")
    )
)]
pub async fn exchange_token(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Option<Json<TokenExchangeRequest>>,
) -> Result<Json<TokenExchangeResponse>, ApiError> {
    let bearer = extract_bearer(&headers)?;
    let request = body.map(|Json(value)| value).unwrap_or_default();
    let audience = token_audience(request.audience.as_deref())?;

    // Forbidden (not 404) for unknown tenants, so callers can't probe which
    // tenants exist.
    let tenant_exists = state
        .store
        .tenant_exists(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to check tenant", &err))?;
    if !tenant_exists {
        return Err(refused(Refusal::Forbidden, "tenant not allowed"));
    }

    let issuers = state
        .store
        .list_idp_issuers(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load issuers", &err))?;
    if issuers.is_empty() {
        // Same ambiguity as a missing signing key on the bearer path: a tenant
        // with no issuers and a tenant this instance has not learned about yet
        // look identical from here. "No issuers configured" sends an operator
        // to the tenant's IdP settings, which are fine (#601).
        return Err(match state.readiness.check().await {
            Ok(()) => refused(Refusal::Forbidden, "no issuers configured"),
            Err(reason) => crate::auth::bearer::cannot_verify(&reason.to_string()),
        });
    }

    let validated = match state.oidc_validator.validate(bearer, &issuers).await {
        Ok(token) => token,
        Err(OidcError::IssuerNotAllowed) => {
            return Err(refused(Refusal::Forbidden, "issuer not allowed"));
        }
        Err(_) => return Err(refused(Refusal::InvalidToken, "invalid token")),
    };

    // Scoped by issuer before anything else sees them: a tenant can trust
    // several IdPs, and a group name is only as trustworthy as the IdP that
    // asserted it.
    let scoped = validated
        .groups
        .iter()
        .map(|group| scoped_group(&validated.issuer, group))
        .collect();
    let principal = principal::from_claims(&validated.issuer, &validated.subject, scoped);
    mint_for_principal(
        &state,
        &tenant_id,
        &principal.principal_id,
        &principal.groups,
        &request,
        audience,
    )
    .await
    .map(Json)
}

/// The Felix token and refresh token for `principal_id` in `tenant_id`, with
/// the permissions RBAC grants it, narrowed by `request`. `groups` are the
/// identity provider's group claims, already scoped by issuer.
pub(crate) async fn mint_for_principal(
    state: &AppState,
    tenant_id: &str,
    principal_id: &str,
    groups: &[String],
    request: &TokenExchangeRequest,
    audience: &str,
) -> Result<TokenExchangeResponse, ApiError> {
    let tenant_id = tenant_id.to_string();
    let narrowing = narrowing_for(request, &tenant_id, audience)?;
    let policies = state
        .store
        .list_rbac_policies(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load policies", &err))?;
    let mut groupings = state
        .store
        .list_rbac_groupings(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load groupings", &err))?;
    add_group_claim_groupings(
        &mut groupings,
        principal_id,
        &with_legacy_names(groups, legacy_unscoped_groups()),
    );

    let enforcer = build_enforcer(&policies, &groupings, &tenant_id)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to build rbac enforcer");
            api_internal_message("failed to build enforcer")
        })?;

    let perms = for_audience(
        narrow_permissions(
            effective_permissions(&enforcer, principal_id, &tenant_id),
            &narrowing,
            &tenant_id,
        ),
        audience,
    );

    // A token with no permissions is useless and usually masks a
    // misconfiguration; reject instead.
    if perms.is_empty() {
        return Err(refused(Refusal::Forbidden, "no permissions"));
    }

    let keys = state
        .store
        .get_tenant_signing_keys(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load signing keys", &err))?;

    let ttl = access_token_ttl();
    let felix_token = mint_token_for(&keys, &tenant_id, principal_id, perms, ttl, audience)
        .map_err(|_| api_internal_message("failed to mint token"))?;

    // The refresh token is what makes the short access TTL above workable for
    // anything long-running. Its group claims are recorded rather than its
    // permissions: a refresh re-runs RBAC, so a grant removed later stops
    // working without waiting for a re-exchange. The narrowing is recorded so
    // refresh re-applies it rather than handing back full rights.
    let refresh_ttl = crate::auth::refresh::refresh_ttl();
    let (mut record, refresh_secret) = crate::auth::refresh::issue(
        &tenant_id,
        principal_id,
        groups.to_vec(),
        None,
        crate::auth::refresh::now_secs(),
        refresh_ttl,
    );
    record.narrowing = Some(narrowing);
    state
        .store
        .insert_refresh_token(record)
        .await
        .map_err(|err| match err {
            // A pair narrowing during a rolling upgrade, before every Raft
            // member can keep it; the message says so.
            StoreError::Conflict(message) => api_conflict("conflict", &message),
            err => api_internal("failed to store refresh token", &err),
        })?;
    metrics::counter!("felix_refresh_tokens_issued_total", "via" => "exchange").increment(1);

    Ok(TokenExchangeResponse {
        felix_token,
        expires_in: ttl.as_secs(),
        token_type: "Bearer".to_string(),
        refresh_token: refresh_secret,
        refresh_expires_in: refresh_ttl.as_secs(),
    })
}

/// The audience a caller asked for, `felix-broker` when it did not say.
pub(crate) fn token_audience(requested: Option<&str>) -> Result<&'static str, ApiError> {
    match requested {
        None | Some(BROKER_AUDIENCE) => Ok(BROKER_AUDIENCE),
        Some(CONTROLPLANE_AUDIENCE) => Ok(CONTROLPLANE_AUDIENCE),
        Some(_) => Err(api_validation_error(
            "audience must be felix-broker or felix-controlplane",
        )),
    }
}

/// How long a minted access token is good for.
///
/// Short by default (900s) to limit blast radius if one leaks. Refresh is what
/// keeps a long-running caller authenticated, so raising this is a tuning knob
/// rather than the way to stay up — see `FELIX_REFRESH_TOKEN_TTL_SECONDS`.
pub fn access_token_ttl() -> Duration {
    Duration::from_secs(
        std::env::var("FELIX_EXCHANGE_TOKEN_TTL_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|&value| value > 0)
            .unwrap_or(900),
    )
}

/// The narrowing `request` asks for, as the refresh record keeps it.
///
/// With `permissions`, `requested` must be `[]` and is recorded that way, so a
/// control plane that predates `permissions` refreshes the chain to nothing
/// rather than to full rights.
fn narrowing_for(
    request: &TokenExchangeRequest,
    tenant_id: &str,
    audience: &str,
) -> Result<Narrowing, ApiError> {
    if let Some(pairs) = &request.permissions {
        if request
            .requested
            .as_ref()
            .is_none_or(|actions| !actions.is_empty())
            || request
                .resources
                .as_ref()
                .is_some_and(|hints| !hints.is_empty())
        {
            return Err(api_validation_error(
                "permissions needs requested: [] and no resources",
            ));
        }
        for pair in pairs {
            parse_permission(pair, tenant_id).map_err(|err| {
                api_validation_error(&format!("permission {pair:?} is invalid: {err}"))
            })?;
        }
    }
    Ok(Narrowing {
        requested: request.requested.clone(),
        resources: request
            .resources
            .clone()
            .filter(|_| request.permissions.is_none()),
        permissions: request.permissions.clone(),
        audience: audience.to_string(),
    })
}

/// `perms` without what only the control plane enforces, on a broker token.
///
/// `token.delegate` is checked here and nowhere else, and a broker that
/// predates it would refuse a token naming an action it cannot parse.
pub(crate) fn for_audience(mut perms: Vec<String>, audience: &str) -> Vec<String> {
    if audience == BROKER_AUDIENCE {
        perms.retain(|perm| {
            perm.split_once(':')
                .is_none_or(|(action, _)| action != ACTION_TOKEN_DELEGATE)
        });
    }
    perms
}

/// `perms` narrowed as `narrowing` says. `permissions`, when present, is the
/// whole narrowing; the empty `requested` beside it is only there for older
/// control planes.
pub(crate) fn narrow_permissions(
    perms: Vec<String>,
    narrowing: &Narrowing,
    tenant_id: &str,
) -> Vec<String> {
    match &narrowing.permissions {
        Some(pairs) => filter_pairs(perms, pairs, tenant_id),
        None => filter_permissions(
            perms,
            narrowing.requested.as_deref(),
            narrowing.resources.as_deref(),
            tenant_id,
        ),
    }
}

/// Keep each granted permission narrowed by every `action:object` pair with
/// the same action, the rule [`filter_permissions`] applies, but per pair
/// rather than across every requested action and resource. Never widens: the
/// action must match a grant exactly, the object is cut down to what the
/// grant covers, and a pair that does not parse matches nothing.
fn filter_pairs(perms: Vec<String>, pairs: &[String], tenant_id: &str) -> Vec<String> {
    let wanted: Vec<(&str, ParsedObject)> = pairs
        .iter()
        .filter_map(|pair| {
            let (action, object) = pair.split_once(':')?;
            Some((action, parse_object(object, tenant_id).ok()?))
        })
        .collect();

    let mut kept = Vec::new();
    let mut seen = HashSet::new();
    for perm in perms {
        let Some((action, object)) = perm.split_once(':') else {
            continue;
        };
        let Ok(granted) = parse_object(object, tenant_id) else {
            continue;
        };
        for (wanted_action, wanted_object) in &wanted {
            if *wanted_action != action {
                continue;
            }
            if let Some(narrowed) = narrow_object(&granted, wanted_object) {
                let narrowed = format!("{action}:{}", format_object(&narrowed));
                if seen.insert(narrowed.clone()) {
                    kept.push(narrowed);
                }
            }
        }
    }
    kept
}

/// Keep what was granted AND requested. `requested` filters by action;
/// `resources` narrows each grant to the part a resource hint covers, so
/// asking for one stream out of a namespace grant yields that stream, not the
/// namespace. Never widens: a hint outside every grant yields nothing, and a
/// hint that does not parse matches nothing.
pub(crate) fn filter_permissions(
    perms: Vec<String>,
    requested: Option<&[String]>,
    resources: Option<&[String]>,
    tenant_id: &str,
) -> Vec<String> {
    let requested_actions =
        requested.map(|actions| actions.iter().map(String::as_str).collect::<HashSet<_>>());
    let hints = resources.map(|resources| {
        resources
            .iter()
            .filter_map(|hint| parse_object(hint, tenant_id).ok())
            .collect::<Vec<_>>()
    });

    let mut kept = Vec::new();
    let mut seen = HashSet::new();
    for perm in perms {
        let Some((action, object)) = perm.split_once(':') else {
            continue;
        };
        if let Some(actions) = &requested_actions
            && !actions.contains(action)
        {
            continue;
        }
        let Some(hints) = &hints else {
            if seen.insert(perm.clone()) {
                kept.push(perm);
            }
            continue;
        };
        let Ok(granted) = parse_object(object, tenant_id) else {
            continue;
        };
        for hint in hints {
            if let Some(narrowed) = narrow_object(&granted, hint) {
                let narrowed = format!("{action}:{}", format_object(&narrowed));
                if seen.insert(narrowed.clone()) {
                    kept.push(narrowed);
                }
            }
        }
    }
    kept
}

/// Set to `true` to also link each IdP group under its bare, unscoped name
/// (`group:{name}`), for the migration from before groups were scoped. It
/// reopens the hole scoping closes, so turn it off once groupings are moved.
pub const LEGACY_UNSCOPED_GROUPS_ENV: &str = "FELIX_CONTROLPLANE_LEGACY_UNSCOPED_GROUPS";

pub(crate) fn legacy_unscoped_groups() -> bool {
    std::env::var(LEGACY_UNSCOPED_GROUPS_ENV)
        .map(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false)
}

/// An IdP group's name as RBAC sees it: `{issuer}#{group}`, so the RBAC
/// subject is `group:{issuer}#{group}`.
///
/// Without the issuer, any IdP a tenant admin registers could assert
/// `groups: ["ops"]` and inherit every grant made to `group:ops`, including
/// cluster scope granted to operators in the same tenant. The issuer comes
/// from the validated token, and issuers are refused with a `#`, so the
/// split is unambiguous.
pub(crate) fn scoped_group(issuer: &str, group: &str) -> String {
    format!("{issuer}#{group}")
}

/// `scoped` plus, when `legacy` is set, each one's bare name.
pub(crate) fn with_legacy_names(scoped: &[String], legacy: bool) -> Vec<String> {
    let mut names = scoped.to_vec();
    if legacy {
        names.extend(
            scoped
                .iter()
                .filter_map(|group| group.split_once('#').map(|(_, name)| name.to_string())),
        );
    }
    names
}

// Group claims from the IdP become ephemeral Casbin groupings for this
// request only; they are never persisted.
pub(crate) fn add_group_claim_groupings(
    groupings: &mut Vec<GroupingRule>,
    principal_id: &str,
    groups: &[String],
) {
    for group in groups {
        let membership = GroupingRule {
            user: principal_id.to_string(),
            role: group_subject(group),
        };
        if !groupings.contains(&membership) {
            groupings.push(membership);
        }
    }
}

// Always prefixed, even when the value already starts with `group:`: an IdP
// group named `group:ops` is a different group from `ops`.
fn group_subject(group: &str) -> String {
    format!("group:{group}")
}

#[cfg(test)]
mod tests;

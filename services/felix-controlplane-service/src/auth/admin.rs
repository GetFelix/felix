//! Admin endpoints for IdP issuer configuration and RBAC management.
//!
//! # Security model
//! - RBAC reads require `rbac.view`.
//! - Policy writes require `rbac.policy.manage`.
//! - Assignment writes require `rbac.assignment.manage`.
//! - Non-RBAC tenant settings (IdP issuer config, refresh-token revocation)
//!   require `tenant.manage`. Changing where an existing issuer's keys come
//!   from, or deleting an issuer, also requires `tenant.manage:cluster:*`, as
//!   creating a tenant does.
//!
//! # Delegation model
//! Callers can only read/mutate rules within the scope encoded in their token
//! permissions. Scope checks happen server-side before store writes, and the
//! credential is checked before the tenant's existence, so an unauthenticated
//! caller cannot probe for tenants.
use std::collections::HashSet;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::AppState;
use crate::api::ensure_tenant_exists;
use crate::api::error::{
    ApiError, api_conflict, api_forbidden, api_internal, api_not_found, api_validation_error,
};
use crate::api::pagination::{PageParams, list_visible};
use crate::api::types::{GroupingListResponse, GroupingListing, PolicyListResponse, PolicyListing};
use crate::auth::bearer::{Refusal, refused, require_cluster_action, tenant_permissions};
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::authorize::{
    ACTION_RBAC_ASSIGNMENT_MANAGE, ACTION_RBAC_POLICY_MANAGE, ACTION_RBAC_VIEW,
    ACTION_TENANT_MANAGE, ParsedObject, canonical_action, object_within_scope, parse_object,
    validate_assignment_allowed, validate_new_rule_allowed,
};
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::store::StoreError;

#[derive(Debug, Deserialize, ToSchema, Clone)]
pub struct PolicyRequest {
    /// RBAC subject, typically a role such as `role:namespace-admin`.
    pub subject: String,
    /// Canonical object string (for example `stream:t1/payments/orders`).
    pub object: String,
    /// Canonical action string such as `stream.publish`.
    pub action: String,
}

#[derive(Debug, Deserialize, ToSchema, Clone)]
pub struct GroupingRequest {
    /// Principal identifier (user or group subject).
    pub user: String,
    /// Role identifier (for example `role:stream-reader`).
    pub role: String,
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/idp-issuers",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = IdpIssuerConfig,
    responses((status = 204), (status = 404))
)]
pub async fn upsert_idp_issuer(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<IdpIssuerConfig>,
) -> Result<StatusCode, ApiError> {
    require_action_for_object(
        &state,
        &tenant_id,
        &headers,
        ACTION_TENANT_MANAGE,
        &format!("tenant:{tenant_id}"),
    )
    .await?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    body.validate(
        state.oidc_validator.allow_insecure_http(),
        state.oidc_validator.allow_private(),
    )
    .map_err(|err| api_validation_error(&err))?;
    // Principal ids are derived from (issuer, subject), and grants, cluster
    // ones included, hang off them. Whoever re-points an existing issuer's keys
    // can mint tokens as any of its subjects, so that takes cluster rights.
    let issuers = state
        .store
        .list_idp_issuers(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load issuers", &err))?;
    if let Some(current) = issuers.iter().find(|item| item.issuer == body.issuer)
        && (current.jwks_url != body.jwks_url || current.discovery_url() != body.discovery_url())
    {
        require_cluster_action(&state, &headers, ACTION_TENANT_MANAGE).await?;
    }
    state
        .store
        .upsert_idp_issuer(&tenant_id, body)
        .await
        .map_err(|err| api_internal("failed to upsert issuer", &err))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/idp-issuers/{issuer}",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier"), ("issuer" = String, Path, description = "Issuer")),
    responses((status = 204), (status = 404))
)]
pub async fn delete_idp_issuer(
    Path((tenant_id, issuer)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    require_action_for_object(
        &state,
        &tenant_id,
        &headers,
        ACTION_TENANT_MANAGE,
        &format!("tenant:{tenant_id}"),
    )
    .await?;
    // Deleting and re-creating would re-point the issuer without the check
    // `upsert_idp_issuer` makes, so deletion takes the same cluster rights.
    require_cluster_action(&state, &headers, ACTION_TENANT_MANAGE).await?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    state
        .store
        .delete_idp_issuer(&tenant_id, &issuer)
        .await
        .map_err(|err| api_internal("failed to delete issuer", &err))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/rbac/policies",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier"), PageParams),
    responses(
        (status = 200, description = "The rules the caller may see: every one as a bare array \
            when neither `limit` nor `cursor` is given, otherwise a page ordered by subject, \
            object and action", body = PolicyListing),
        (status = 400, description = "Invalid limit or cursor", body = crate::api::types::ErrorResponse),
        (status = 404)
    )
)]
pub(crate) async fn list_policies(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(page): Query<PageParams>,
) -> Result<Json<PolicyListing>, ApiError> {
    let scope = require_action_scope(&state, &tenant_id, &headers, ACTION_RBAC_VIEW).await?;
    // Unpaged unless asked: this listing answered with a bare array before
    // paging existed, and a caller that knows no cursor must still get it.
    let request = (!page.is_absent()).then(|| page.request()).transpose()?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    let Some(request) = request else {
        let policies = state
            .store
            .list_rbac_policies(&tenant_id)
            .await
            .map_err(|err| api_internal("failed to list policies", &err))?;
        return Ok(Json(PolicyListing::All(filter_policies_by_scope(
            &policies,
            &scope.scopes,
            &tenant_id,
        ))));
    };
    let listed = list_visible(
        request,
        |page| state.store.list_rbac_policies_page(&tenant_id, page),
        |policy| policy_in_scope(policy, &scope.scopes, &tenant_id),
        PolicyRule::clone,
    )
    .await
    .map_err(|err| api_internal("failed to list policies", &err))?;
    Ok(Json(PolicyListing::Page(PolicyListResponse {
        items: listed.items,
        next_cursor: listed.next_cursor,
    })))
}

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/rbac/groupings",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier"), PageParams),
    responses(
        (status = 200, description = "The role assignments the caller may see: every one as a \
            bare array when neither `limit` nor `cursor` is given, otherwise a page ordered by \
            user and role", body = GroupingListing),
        (status = 400, description = "Invalid limit or cursor", body = crate::api::types::ErrorResponse),
        (status = 404)
    )
)]
pub(crate) async fn list_groupings(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(page): Query<PageParams>,
) -> Result<Json<GroupingListing>, ApiError> {
    let scope = require_action_scope(&state, &tenant_id, &headers, ACTION_RBAC_VIEW).await?;
    // Unpaged unless asked, as for policies.
    let request = (!page.is_absent()).then(|| page.request()).transpose()?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    let policies = state
        .store
        .list_rbac_policies(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to list policies", &err))?;
    // Only reveal assignments for roles visible in the caller's RBAC scope.
    let visible_roles: HashSet<String> =
        filter_policies_by_scope(&policies, &scope.scopes, &tenant_id)
            .into_iter()
            .map(|policy| policy.subject)
            .collect();
    if let Some(request) = request {
        let listed = list_visible(
            request,
            |page| state.store.list_rbac_groupings_page(&tenant_id, page),
            |grouping| visible_roles.contains(&grouping.role),
            GroupingRule::clone,
        )
        .await
        .map_err(|err| api_internal("failed to list groupings", &err))?;
        return Ok(Json(GroupingListing::Page(GroupingListResponse {
            items: listed.items,
            next_cursor: listed.next_cursor,
        })));
    }
    let groupings = state
        .store
        .list_rbac_groupings(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to list groupings", &err))?;
    Ok(Json(GroupingListing::All(
        groupings
            .into_iter()
            .filter(|grouping| visible_roles.contains(&grouping.role))
            .collect(),
    )))
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/rbac/policies",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = PolicyRequest,
    responses((status = 204), (status = 404))
)]
pub async fn add_policy(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<PolicyRequest>,
) -> Result<StatusCode, ApiError> {
    let scope =
        require_action_scope(&state, &tenant_id, &headers, ACTION_RBAC_POLICY_MANAGE).await?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    let rule = PolicyRule {
        subject: body.subject,
        object: body.object,
        action: body.action,
    };
    if canonical_action(&rule.action).is_none() {
        return Err(api_validation_error("invalid action"));
    }
    if let Err(err) = validate_new_rule_allowed(&scope.scopes, &tenant_id, &rule) {
        if err.contains("scope") {
            return Err(api_forbidden(&err));
        }
        return Err(api_validation_error(&err));
    }
    state
        .store
        .add_rbac_policy(&tenant_id, rule)
        .await
        .map_err(|err| api_internal("failed to add policy", &err))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/rbac/groupings",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = GroupingRequest,
    responses((status = 204), (status = 404))
)]
pub async fn add_grouping(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<GroupingRequest>,
) -> Result<StatusCode, ApiError> {
    let scope =
        require_action_scope(&state, &tenant_id, &headers, ACTION_RBAC_ASSIGNMENT_MANAGE).await?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    let grouping = GroupingRule {
        user: body.user,
        role: body.role,
    };
    // Enforce safe delegation by validating every policy attached to this role.
    let role_policies = state
        .store
        .list_rbac_policies(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load policies", &err))?
        .into_iter()
        .filter(|policy| policy.subject == grouping.role)
        .collect::<Vec<PolicyRule>>();
    validate_assignment_allowed(&scope.scopes, &tenant_id, &grouping, &role_policies)
        .map_err(|err| api_forbidden(&err))?;
    state
        .store
        .add_rbac_grouping(&tenant_id, grouping)
        .await
        .map_err(|err| api_internal("failed to add grouping", &err))?;
    Ok(StatusCode::NO_CONTENT)
}

/// Remove one policy rule.
///
/// Held to the same scope as adding it: a delegated admin can take back only
/// what they could have granted.
#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/rbac/policies",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = PolicyRequest,
    responses((status = 204), (status = 403), (status = 404, description = "No such rule"))
)]
pub async fn remove_policy(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<PolicyRequest>,
) -> Result<StatusCode, ApiError> {
    let scope =
        require_action_scope(&state, &tenant_id, &headers, ACTION_RBAC_POLICY_MANAGE).await?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    let rule = PolicyRule {
        subject: body.subject,
        object: body.object,
        action: body.action,
    };
    if let Err(err) = validate_new_rule_allowed(&scope.scopes, &tenant_id, &rule) {
        if err.contains("scope") {
            return Err(api_forbidden(&err));
        }
        return Err(api_validation_error(&err));
    }
    match state.store.remove_rbac_policy(&tenant_id, rule).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound(_)) => Err(api_not_found("policy not found")),
        Err(StoreError::Conflict(what)) => Err(api_conflict("conflict", &what)),
        Err(err) => Err(api_internal("failed to remove policy", &err)),
    }
}

/// Remove one role assignment.
///
/// The caller must be able to make the assignment in the first place: every
/// policy on the role within their scope. A role with no policies grants
/// nothing any narrower scope can vouch for, so only a tenant-wide caller
/// may remove assignments to it.
#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/rbac/groupings",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = GroupingRequest,
    responses((status = 204), (status = 403), (status = 404, description = "No such assignment"))
)]
pub async fn remove_grouping(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<GroupingRequest>,
) -> Result<StatusCode, ApiError> {
    let scope =
        require_action_scope(&state, &tenant_id, &headers, ACTION_RBAC_ASSIGNMENT_MANAGE).await?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    let grouping = GroupingRule {
        user: body.user,
        role: body.role,
    };
    let role_policies = state
        .store
        .list_rbac_policies(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to load policies", &err))?
        .into_iter()
        .filter(|policy| policy.subject == grouping.role)
        .collect::<Vec<PolicyRule>>();
    if role_policies.is_empty() {
        let tenant = ParsedObject::Tenant {
            tenant_id: tenant_id.clone(),
        };
        if !scope
            .scopes
            .iter()
            .any(|candidate| object_within_scope(candidate, &tenant))
        {
            return Err(refused(Refusal::Forbidden, "insufficient scope"));
        }
    } else {
        validate_assignment_allowed(&scope.scopes, &tenant_id, &grouping, &role_policies)
            .map_err(|err| api_forbidden(&err))?;
    }
    match state.store.remove_rbac_grouping(&tenant_id, grouping).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound(_)) => Err(api_not_found("grouping not found")),
        Err(StoreError::Conflict(what)) => Err(api_conflict("conflict", &what)),
        Err(err) => Err(api_internal("failed to remove grouping", &err)),
    }
}

/// Which principal's refresh tokens to revoke.
#[derive(Debug, Deserialize, ToSchema, Clone)]
pub struct RevokeRefreshTokensRequest {
    /// The principal id, as it appears in a Felix token's `sub`.
    pub principal_id: String,
}

/// How many live refresh tokens a revocation ended.
#[derive(Debug, Serialize, ToSchema, Clone)]
pub struct RevokeRefreshTokensResponse {
    pub revoked: u64,
}

/// Revoke every refresh token a principal holds in this tenant.
///
/// Access tokens already minted stay valid until they expire (900 s by
/// default); this stops the principal getting new ones without the IdP.
/// Requires `tenant.manage` over the tenant: cutting off a principal is a
/// tenant-wide act, whatever scope granted them their roles.
#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/refresh-tokens/revoke",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = RevokeRefreshTokensRequest,
    responses((status = 200, body = RevokeRefreshTokensResponse), (status = 403), (status = 404))
)]
pub async fn revoke_principal_refresh_tokens(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<RevokeRefreshTokensRequest>,
) -> Result<Json<RevokeRefreshTokensResponse>, ApiError> {
    require_action_for_object(
        &state,
        &tenant_id,
        &headers,
        ACTION_TENANT_MANAGE,
        &format!("tenant:{tenant_id}"),
    )
    .await?;
    ensure_tenant_exists(&state, &tenant_id).await?;
    if body.principal_id.trim().is_empty() {
        return Err(api_validation_error("principal_id must not be empty"));
    }
    let revoked = state
        .store
        .revoke_refresh_tokens_for_principal(&tenant_id, &body.principal_id)
        .await
        .map_err(|err| api_internal("failed to revoke refresh tokens", &err))?;
    metrics::counter!("felix_refresh_tokens_revoked_total").increment(revoked);
    tracing::info!(
        tenant_id = %tenant_id,
        principal_id = %body.principal_id,
        revoked,
        "revoked a principal's refresh tokens",
    );
    Ok(Json(RevokeRefreshTokensResponse { revoked }))
}

struct ActionScope {
    /// Parsed RBAC objects this caller can manage/view for one action.
    scopes: Vec<ParsedObject>,
}

/// Require that caller has at least one scope for the requested action.
async fn require_action_scope(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
    action: &str,
) -> Result<ActionScope, ApiError> {
    let scopes = tenant_permissions(state, tenant_id, headers)
        .await?
        .into_iter()
        .filter(|perm| perm.action == action)
        .map(|perm| perm.object)
        .collect::<Vec<ParsedObject>>();
    if scopes.is_empty() {
        return Err(refused(Refusal::Forbidden, "missing required permission"));
    }
    Ok(ActionScope { scopes })
}

/// Require `action` over a specific target object.
///
/// This is used for non-RBAC mutation endpoints such as IdP issuer config.
async fn require_action_for_object(
    state: &AppState,
    tenant_id: &str,
    headers: &HeaderMap,
    action: &str,
    object: &str,
) -> Result<(), ApiError> {
    let scope = require_action_scope(state, tenant_id, headers, action).await?;
    let parsed = parse_object(object, tenant_id).map_err(|err| api_validation_error(&err))?;
    if scope
        .scopes
        .iter()
        .any(|candidate| object_within_scope(candidate, &parsed))
    {
        return Ok(());
    }
    Err(refused(Refusal::Forbidden, "insufficient scope"))
}

/// Return only policies that fall within one of the caller's scopes.
fn filter_policies_by_scope(
    policies: &[PolicyRule],
    scopes: &[ParsedObject],
    tenant_id: &str,
) -> Vec<PolicyRule> {
    policies
        .iter()
        .filter(|policy| policy_in_scope(policy, scopes, tenant_id))
        .cloned()
        .collect()
}

fn policy_in_scope(policy: &PolicyRule, scopes: &[ParsedObject], tenant_id: &str) -> bool {
    parse_object(&policy.object, tenant_id)
        .ok()
        .map(|target| {
            scopes
                .iter()
                .any(|scope| object_within_scope(scope, &target))
        })
        .unwrap_or(false)
}

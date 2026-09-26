//! Bootstrap API handlers.
//!
//! Implements the initial tenant bootstrap flow that seeds auth configuration,
//! RBAC policies, and signing keys for a newly created tenant.
use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use casbin::function_map::key_match2;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::AppState;
use crate::api::error::{
    ApiError, api_conflict, api_internal, api_internal_message, api_not_enabled, api_unauthorized,
    api_validation_error,
};
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::Tenant;

#[derive(Debug, Deserialize, Serialize, ToSchema, Clone)]
pub struct BootstrapInitializeRequest {
    pub display_name: String,
    pub idp_issuers: Vec<IdpIssuerConfig>,
    pub initial_admin_principals: Vec<String>,
    #[serde(default)]
    pub policies: Vec<PolicyRule>,
    #[serde(default)]
    pub groupings: Vec<GroupingRule>,
}

#[derive(Debug, Serialize, ToSchema, Clone)]
pub struct BootstrapInitializeResponse {
    pub tenant_id: String,
    pub kid: String,
    pub jwks_url: String,
    pub status: String,
}

#[utoipa::path(
    post,
    path = "/internal/bootstrap/tenants/{tenant_id}/initialize",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    request_body = BootstrapInitializeRequest,
    responses(
        (status = 200, description = "Bootstrap initialized", body = BootstrapInitializeResponse),
        (status = 400, description = "Validation error"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not enabled"),
        (status = 409, description = "Already initialized")
    )
)]
pub async fn initialize(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<BootstrapInitializeRequest>,
) -> Result<Json<BootstrapInitializeResponse>, ApiError> {
    if !state.bootstrap_enabled {
        return Err(api_not_enabled("bootstrap not enabled"));
    }

    ensure_bootstrap_authorized(&state, &headers, &tenant_id)?;

    crate::model::validate_identifier("tenant_id", &tenant_id)
        .map_err(|err| api_validation_error(&err))?;
    if body.display_name.trim().is_empty() {
        return Err(api_validation_error("display_name is required"));
    }
    if body.initial_admin_principals.is_empty() {
        return Err(api_validation_error(
            "initial_admin_principals must not be empty",
        ));
    }
    for issuer in &body.idp_issuers {
        issuer
            .validate(state.oidc_validator.allow_insecure_http())
            .map_err(|err| api_validation_error(&err))?;
    }

    let tenant_exists = state
        .store
        .tenant_exists(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to check tenant", &err))?;

    if !tenant_exists {
        match state
            .store
            .create_tenant(Tenant {
                tenant_id: tenant_id.clone(),
                display_name: body.display_name.clone(),
            })
            .await
        {
            Ok(_) => {}
            // Another instance created it between our check and this write;
            // the atomic bootstrap below decides who actually initializes it.
            Err(crate::store::StoreError::Conflict(_)) => {}
            Err(err) => return Err(api_internal("failed to create tenant", &err)),
        }
    }

    let mut policies = body.policies.clone();
    let required = format!("tenant:{tenant_id}");
    // Seed a tenant-scoped management role; never grant tenant:* during bootstrap.
    if !policies.iter().any(|policy| {
        policy.subject == "role:tenant-admin"
            && policy.action == "tenant.manage"
            && key_match2(&policy.object, &required)
    }) {
        policies.push(PolicyRule {
            subject: "role:tenant-admin".to_string(),
            object: required.clone(),
            action: "tenant.manage".to_string(),
        });
    }
    // Seed explicit RBAC admin/read actions so bootstrap admins can manage RBAC
    // without relying on non-RBAC permissions.
    for action in ["rbac.view", "rbac.policy.manage", "rbac.assignment.manage"] {
        if !policies.iter().any(|policy| {
            policy.subject == "role:tenant-admin"
                && policy.action == action
                && key_match2(&policy.object, &required)
        }) {
            policies.push(PolicyRule {
                subject: "role:tenant-admin".to_string(),
                object: required.clone(),
                action: action.to_string(),
            });
        }
    }
    // Keep namespace management explicit for tenant-admin bootstrap parity.
    if !policies.iter().any(|policy| {
        policy.subject == "role:tenant-admin"
            && policy.action == "ns.manage"
            && key_match2(&policy.object, &format!("namespace:{tenant_id}/*"))
    }) {
        policies.push(PolicyRule {
            subject: "role:tenant-admin".to_string(),
            object: format!("namespace:{tenant_id}/*"),
            action: "ns.manage".to_string(),
        });
    }

    let mut groupings = body.groupings.clone();
    for principal in &body.initial_admin_principals {
        let grouping = GroupingRule {
            user: principal.clone(),
            role: "role:tenant-admin".to_string(),
        };
        if !groupings.contains(&grouping) {
            groupings.push(grouping);
        }
    }

    // Key material is generated here, at the API layer, and carried in the
    // seed: the store applies it only if the tenant has none, and stays free
    // of randomness — which is what lets the Raft state machine (#338) apply
    // the same operation identically on every replica.
    let candidate_keys = crate::auth::keys::generate_signing_keys()
        .map_err(|err| api_internal("failed to generate signing keys", &err.into()))?;

    // One store operation, so N instances racing on the same tenant produce
    // exactly one winner and everyone else a clean conflict — never two key
    // sets where the reported `kid` belongs to the overwritten one.
    let keys = match state
        .store
        .bootstrap_tenant_auth(
            &tenant_id,
            crate::store::TenantAuthSeed {
                issuers: body.idp_issuers.clone(),
                policies,
                groupings,
                signing_keys: candidate_keys,
            },
        )
        .await
    {
        Ok(keys) => keys,
        Err(crate::store::StoreError::Conflict(_)) => {
            // Not a failure: an already-initialized tenant refusing a second
            // bootstrap is the control working. Counted so a burst of them is
            // visible, since repeated attempts against live tenants is what a
            // leaked token looks like.
            tracing::info!(
                tenant_id = %tenant_id,
                "bootstrap refused: tenant already initialized",
            );
            metrics::counter!(ATTEMPTS, "outcome" => "refused", "reason" => "already_initialized")
                .increment(1);
            return Err(api_conflict(
                "already_initialized",
                "tenant already initialized",
            ));
        }
        Err(err) => return Err(api_internal("failed to bootstrap tenant auth", &err)),
    };

    // A tenant is initialized exactly once, so this line is the record that it
    // happened, who it granted, and when — the audit question an operator asks
    // months later is "who has admin on this tenant and how did they get it".
    tracing::info!(
        tenant_id = %tenant_id,
        kid = %keys.current.kid,
        admin_principals = body.initial_admin_principals.len(),
        issuers = body.idp_issuers.len(),
        "tenant initialized by bootstrap",
    );
    metrics::counter!(ATTEMPTS, "outcome" => "initialized", "reason" => "ok").increment(1);

    Ok(Json(BootstrapInitializeResponse {
        tenant_id: tenant_id.clone(),
        kid: keys.current.kid,
        jwks_url: format!("/v1/tenants/{tenant_id}/.well-known/jwks.json"),
        status: "initialized".to_string(),
    }))
}

/// The bootstrap credential check: a shared token, with room for two.
///
/// Two accepted tokens is what makes rotation a rolling deploy instead of an
/// outage: the new token ships as `token` with the old one demoted to
/// `previous_token`, both generations of instance accept both, and the old one
/// is dropped once the deploy settles. Transport-level client authentication is
/// enforced *before* this runs when bootstrap mTLS is configured — see
/// [`crate::server::tls`] — which is the second factor the token alone does not give.
fn ensure_bootstrap_authorized(
    state: &AppState,
    headers: &HeaderMap,
    tenant_id: &str,
) -> Result<(), ApiError> {
    let token = match headers.get("X-Felix-Bootstrap-Token") {
        Some(value) => match value.to_str() {
            Ok(token) => token,
            Err(_) => {
                audit_rejected(tenant_id, "malformed_token");
                return Err(api_unauthorized("invalid bootstrap token"));
            }
        },
        None => {
            audit_rejected(tenant_id, "missing_token");
            return Err(api_unauthorized("missing bootstrap token"));
        }
    };

    if state.bootstrap_tokens.is_empty() {
        audit_rejected(tenant_id, "no_token_configured");
        return Err(api_internal_message("bootstrap token missing"));
    }

    // Every candidate is compared, in constant time each, so neither the match
    // nor which token matched shows up as a timing difference.
    let mut matched = false;
    for expected in &state.bootstrap_tokens {
        matched |= constant_time_eq(token.as_bytes(), expected.as_bytes());
    }
    if !matched {
        audit_rejected(tenant_id, "invalid_token");
        return Err(api_unauthorized("invalid bootstrap token"));
    }

    Ok(())
}

/// The counter to alert on.
///
/// Bootstrap is the day-0 credential: it is presented once per tenant, by an
/// operator, and never again. A rejected attempt is therefore either a
/// misconfigured deploy or someone guessing, and both are worth waking for —
/// which nothing could do before, because this endpoint logged nothing at all
/// and emitted no metric.
const ATTEMPTS: &str = "felix_bootstrap_attempts_total";

/// Rejected, with why — never with the token that was offered.
///
/// The reason is a small closed set rather than the error text so a dashboard
/// can group it, and a near-miss token is not written to a log that is shipped
/// somewhere less protected than the token is.
fn audit_rejected(tenant_id: &str, reason: &'static str) {
    tracing::warn!(
        tenant_id = %tenant_id,
        reason,
        "bootstrap attempt rejected",
    );
    metrics::counter!(ATTEMPTS, "outcome" => "rejected", "reason" => reason).increment(1);
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (left, right) in a.iter().zip(b.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

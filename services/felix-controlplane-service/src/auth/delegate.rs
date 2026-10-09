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
use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::AppState;
use crate::api::error::{ApiError, api_internal, api_internal_message, api_validation_error};
use crate::auth::bearer::{Refusal, refused, tenant_claims};
use crate::auth::exchange::{access_token_ttl, for_audience, narrow_permissions};
use crate::auth::felix_token::{BROKER_AUDIENCE, mint_delegated_token, verify_token_for};
use crate::auth::rbac::authorize::{
    ACTION_TOKEN_DELEGATE, ParsedObject, object_within_scope, parse_permission,
};
use crate::auth::refresh_token::Narrowing;

/// RFC 8693's `grant_type` for a token exchange.
pub const TOKEN_EXCHANGE_GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
/// RFC 8693's token type for a JWT, the only kind taken and issued here.
pub const JWT_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:jwt";

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
/// token, `403` when the caller lacks `token.delegate` on the tenant, or the
/// subject token is invalid, for another tenant, already delegated, or left
/// with no permissions, `500` for store failures.
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
    let tenant = ParsedObject::Tenant {
        tenant_id: tenant_id.clone(),
    };
    let may_delegate = actor
        .perms
        .iter()
        .filter_map(|perm| parse_permission(perm, &tenant_id).ok())
        .any(|perm| {
            perm.action == ACTION_TOKEN_DELEGATE && object_within_scope(&perm.object, &tenant)
        });
    if !may_delegate || actor.act.is_some() {
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

    let narrowing = Narrowing {
        requested: None,
        resources: None,
        permissions: request.permissions.clone(),
        audience: BROKER_AUDIENCE.to_string(),
    };
    let perms = for_audience(
        narrow_permissions(subject.perms, &narrowing, &tenant_id),
        BROKER_AUDIENCE,
    );
    if perms.is_empty() {
        return Err(refused(Refusal::Forbidden, "no permissions"));
    }

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

#[cfg(test)]
mod tests;

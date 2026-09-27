//! Tenant signing-key rotation: `/v1/tenants/{tenant_id}/signing-keys`.
//!
//! Rotation is three calls rather than one because brokers cache a tenant's
//! JWKS (for up to an hour) and do not refetch it on an unknown `kid`. A key
//! that signed the moment it was created would be refused by every broker
//! until its cache expired. So:
//!
//! 1. **stage** a new key: published in the JWKS and accepted, but not used
//!    to sign;
//! 2. once brokers have picked it up, **activate** it: it signs from then on,
//!    and the key it replaces keeps verifying;
//! 3. once every token the old key signed has expired, **retire** the old key.
//!
//! The waits are the operator's; `docs/auth.md` says how long. Every call
//! requires `tenant.manage` over the tenant. Private key material never
//! leaves the store: responses carry `kid`s only.
use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::AppState;
use crate::api::ensure_tenant_exists;
use crate::api::error::{
    ApiError, api_conflict, api_internal, api_internal_message, api_not_found,
};
use crate::auth::bearer::require_tenant_action;
use crate::auth::felix_token::TenantSigningKeys;
use crate::auth::rbac::authorize::{ACTION_TENANT_MANAGE, ParsedObject};
use crate::store::{StoreError, StoreResult};

/// A tenant's signing keys, by `kid`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SigningKeysResponse {
    /// The key new tokens are signed with.
    pub current: String,
    /// Keys that verify but do not sign: staged ones waiting to be activated,
    /// and replaced ones waiting to be retired.
    pub verifying: Vec<String>,
}

impl From<TenantSigningKeys> for SigningKeysResponse {
    fn from(keys: TenantSigningKeys) -> Self {
        Self {
            current: keys.current.kid,
            verifying: keys.previous.into_iter().map(|key| key.kid).collect(),
        }
    }
}

/// List the tenant's signing keys.
#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/signing-keys",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses((status = 200, body = SigningKeysResponse), (status = 403), (status = 404))
)]
pub async fn list_signing_keys(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<SigningKeysResponse>, ApiError> {
    authorize(&state, &tenant_id, &headers).await?;
    answer(state.store.get_tenant_signing_keys(&tenant_id).await)
}

/// Stage a freshly generated key. It is published and accepted at once, but
/// signs nothing until it is activated.
#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/signing-keys",
    tag = "auth",
    params(("tenant_id" = String, Path, description = "Tenant identifier")),
    responses((status = 201, body = SigningKeysResponse), (status = 403), (status = 404))
)]
pub async fn stage_signing_key(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<SigningKeysResponse>), ApiError> {
    authorize(&state, &tenant_id, &headers).await?;
    // Generated here, not in the store: under Raft every replica applies the
    // command, and they must all install the same key.
    let key = crate::auth::keys::generate_signing_keys()
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to generate a signing key");
            api_internal_message("failed to generate a signing key")
        })?
        .current;
    let Json(keys) = answer(state.store.stage_signing_key(&tenant_id, key).await)?;
    Ok((StatusCode::CREATED, Json(keys)))
}

/// Make a staged key the signing key.
#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/signing-keys/{kid}/activate",
    tag = "auth",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("kid" = String, Path, description = "Key id")
    ),
    responses((status = 200, body = SigningKeysResponse), (status = 403), (status = 404))
)]
pub async fn activate_signing_key(
    Path((tenant_id, kid)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<SigningKeysResponse>, ApiError> {
    authorize(&state, &tenant_id, &headers).await?;
    answer(state.store.activate_signing_key(&tenant_id, &kid).await)
}

/// Retire a key that no longer signs. Tokens it signed stop verifying.
#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/signing-keys/{kid}",
    tag = "auth",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("kid" = String, Path, description = "Key id")
    ),
    responses(
        (status = 200, body = SigningKeysResponse),
        (status = 403),
        (status = 404),
        (status = 409, description = "The key is the current signing key")
    )
)]
pub async fn retire_signing_key(
    Path((tenant_id, kid)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<SigningKeysResponse>, ApiError> {
    authorize(&state, &tenant_id, &headers).await?;
    answer(state.store.retire_signing_key(&tenant_id, &kid).await)
}

/// `tenant.manage` over the tenant, checked before existence so the route is
/// no probe for which tenants exist.
async fn authorize(state: &AppState, tenant_id: &str, headers: &HeaderMap) -> Result<(), ApiError> {
    let target = ParsedObject::Tenant {
        tenant_id: tenant_id.to_string(),
    };
    require_tenant_action(state, tenant_id, headers, ACTION_TENANT_MANAGE, &target).await?;
    ensure_tenant_exists(state, tenant_id).await
}

fn answer(result: StoreResult<TenantSigningKeys>) -> Result<Json<SigningKeysResponse>, ApiError> {
    match result {
        Ok(keys) => Ok(Json(keys.into())),
        Err(StoreError::NotFound(what)) => Err(api_not_found(&format!("{what} not found"))),
        Err(StoreError::Conflict(what)) => Err(api_conflict("conflict", &what)),
        Err(err) => Err(api_internal("failed to change signing keys", &err)),
    }
}

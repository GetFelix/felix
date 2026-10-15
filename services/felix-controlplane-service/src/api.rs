//! Control-plane HTTP handlers plus the shared existence checks that run
//! before any store mutation.
pub mod bootstrap;
pub mod caches;
pub mod error;
pub mod namespaces;
pub mod nodes;
pub mod openapi;
pub(crate) mod pagination;
pub mod readiness;
pub mod regions;
pub(crate) mod resources;
mod router;
pub mod shard_assignments;
pub mod shard_moves;
mod state;
pub mod streams;
pub mod system;
pub mod tenants;
mod trace_context;
pub mod types;

pub use router::{build_bootstrap_router, build_bootstrap_router_with_dev_tokens, build_router};
pub use state::AppState;

use crate::api::error::{ApiError, api_internal, api_not_found};
use crate::model::NamespaceKey;

/// 404 unless both the tenant and the namespace exist. The tenant is checked
/// first so a caller can't learn about namespaces in tenants it can't see.
pub(crate) async fn ensure_tenant_namespace(
    state: &AppState,
    tenant_id: &str,
    namespace: &str,
) -> Result<(), ApiError> {
    ensure_tenant_exists(state, tenant_id).await?;
    let namespace_key = NamespaceKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
    };
    let exists = state
        .store
        .namespace_exists(&namespace_key)
        .await
        .map_err(|err| api_internal("failed to check namespace existence", &err))?;
    if !exists {
        return Err(api_not_found("namespace not found"));
    }
    Ok(())
}

/// 404 unless the tenant exists.
pub(crate) async fn ensure_tenant_exists(
    state: &AppState,
    tenant_id: &str,
) -> Result<(), ApiError> {
    let exists = state
        .store
        .tenant_exists(tenant_id)
        .await
        .map_err(|err| api_internal("failed to check tenant existence", &err))?;
    if !exists {
        return Err(api_not_found("tenant not found"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

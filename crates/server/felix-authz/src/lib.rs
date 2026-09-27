//! Authn/authz primitives shared by the control plane and the broker:
//! permission matching and token/JWKS helpers. The Casbin model itself lives
//! with the control plane, the only place policy is evaluated.
//!
//! The control plane mints tokens and publishes JWKS; brokers verify and
//! enforce. Both sides must agree on issuer and audience, tokens are
//! EdDSA/Ed25519 only, and permission strings follow `action:resource` with
//! wildcards.

// Authorization: what a permission grants and whether a request matches it.
mod action;
mod matcher;
mod permission;
mod resource;

// Authentication: who is asking, and proving it with a signed token.
mod ids;
mod jwks;
mod token;

mod error;

pub use action::Action;
pub use error::{AuthzError, AuthzResult};
pub use ids::{CacheScope, GroupName, Namespace, StreamName, TenantId};
pub use jwks::{Jwk, Jwks, KeyUse};
pub use matcher::{PermissionMatcher, wildcard_match};
pub use permission::{Permission, PermissionPattern};
pub use resource::{
    cache_resource, group_resource, namespace_resource, stream_resource, tenant_resource,
};
pub use token::{
    FelixClaims, FelixTokenIssuer, FelixTokenVerifier, TenantKeyCache, TenantKeyMaterial,
    TenantKeyStore, TenantSigningKey, TenantVerificationKey,
};

//! Control-plane authn/authz: OIDC validation, Felix token minting, JWKS
//! publishing, and tenant-scoped RBAC. Private signing keys stay inside this
//! boundary; only public keys leave, via JWKS.
pub mod admin;
pub(crate) mod bearer;
pub mod exchange;
pub mod felix_token;
pub mod idp_registry;
pub mod jwks;
pub mod keys;
pub mod oidc;
pub mod principal;
pub mod rbac;
pub mod refresh;
pub mod refresh_token;
pub mod signing_keys;

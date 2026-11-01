//! Error variants for token, key, and permission handling. Callers match on
//! these, so treat the set as a stable surface; JWT failures keep the
//! underlying `jsonwebtoken` error for the root cause.
use thiserror::Error;

/// Errors emitted by Felix authorization helpers.
#[derive(Debug, Error)]
pub enum AuthzError {
    #[error("invalid action: {0}")]
    InvalidAction(String),
    #[error("invalid permission: {0}")]
    InvalidPermission(String),
    #[error("missing signing key for tenant {0}")]
    MissingSigningKey(String),
    #[error("missing verification keys for tenant {0}")]
    MissingVerificationKeys(String),
    #[error("jwt error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("key error: {0}")]
    Key(String),
    #[error("claims tenant mismatch: expected {expected}, got {actual}")]
    TenantMismatch { expected: String, actual: String },
    #[error("jwks not available for tenant {0}")]
    MissingJwks(String),
    #[error("invalid lease template: {0}")]
    InvalidLeaseTemplate(String),
    #[error("lease key cannot name a resource: {0}")]
    InvalidLeaseKey(String),
}

pub type AuthzResult<T> = Result<T, AuthzError>;

#[cfg(test)]
mod tests;

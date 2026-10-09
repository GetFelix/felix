//! Minting tokens.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::Header;

use super::FelixClaims;
use super::key_cache::TenantKeyCache;
use super::keys::TenantKeyStore;
use crate::{AuthzResult, TenantId};

/// Mints Felix JWTs. Issuer/audience/TTL are fixed at construction and must
/// match the verifier's configuration.
pub struct FelixTokenIssuer {
    issuer: String,
    audience: String,
    ttl: Duration,
    key_store: Arc<dyn TenantKeyStore>,
    cache: Arc<TenantKeyCache>,
}

impl FelixTokenIssuer {
    pub fn new(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        ttl: Duration,
        key_store: Arc<dyn TenantKeyStore>,
    ) -> Self {
        Self {
            issuer: issuer.into(),
            audience: audience.into(),
            ttl,
            key_store,
            cache: Arc::new(TenantKeyCache::default()),
        }
    }

    /// Share a key cache with other components. The cache must be invalidated
    /// on key rotation.
    pub fn with_cache(mut self, cache: Arc<TenantKeyCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Mint a token for a tenant/principal with the given permissions.
    /// Treat the result as a secret; don't log it.
    ///
    /// # Errors
    /// Key-store lookup failures and JWT encoding errors.
    pub fn mint(
        &self,
        tenant_id: &TenantId,
        principal_id: &str,
        perms: Vec<String>,
    ) -> AuthzResult<String> {
        self.mint_with_actor(tenant_id, principal_id, perms, None)
    }

    /// [`Self::mint`] with an `act` claim naming who may present the token
    /// for `principal_id`.
    ///
    /// # Errors
    /// As [`Self::mint`].
    pub fn mint_with_actor(
        &self,
        tenant_id: &TenantId,
        principal_id: &str,
        perms: Vec<String>,
        actor: Option<&str>,
    ) -> AuthzResult<String> {
        let now = now_epoch_seconds();
        let exp = now + self.ttl.as_secs() as i64;
        let claims = FelixClaims {
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            sub: principal_id.to_string(),
            tid: tenant_id.to_string(),
            exp,
            iat: now,
            jti: None,
            perms,
            act: actor.map(|sub| super::Actor {
                sub: sub.to_string(),
            }),
        };
        let signing_key = self.key_store.current_signing_key(tenant_id)?;
        let encoding_key = self.cache.encoding_key(tenant_id, &signing_key)?;
        let mut header = Header::new(signing_key.alg);
        header.kid = Some(signing_key.kid);
        let token = jsonwebtoken::encode(&header, &claims, &encoding_key)?;
        Ok(token)
    }
}

pub(super) fn now_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}

#[cfg(test)]
mod tests;

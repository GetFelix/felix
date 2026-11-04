//! Who a connection is, as the broker service decides it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use felix_authz::{Action, Namespace, PermissionMatcher, StreamName, TenantId, stream_resource};

/// Why a `CONNECT` was not accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The token or tenant is wrong. The text is safe to log, not to send.
    Rejected(String),
    /// The broker could not check right now, for example because the control
    /// plane is unreachable. The client may retry.
    Unavailable,
}

/// Verifies the tenant and token a `CONNECT` carries.
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// `peer_certs` is the client's certificate chain (DER, leaf first), empty
    /// without mutual TLS. A broker that binds tokens to certificates checks
    /// them here.
    async fn authenticate(
        &self,
        tenant_id: &str,
        token: &str,
        peer_certs: &[Vec<u8>],
    ) -> Result<Principal, AuthError>;
}

type RevokedFn = dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;

/// An authenticated connection's tenant, what it may do, and how long the
/// credential behind it lasts.
#[derive(Clone)]
pub struct Principal {
    tenant_id: String,
    access: Access,
    expires_at: Option<i64>,
    revoked: Option<Arc<RevokedFn>>,
}

#[derive(Clone)]
enum Access {
    /// The development switch: every stream of the tenant.
    Anonymous,
    Token(Option<PermissionMatcher>),
}

impl Principal {
    /// A tenant's connection with no permissions yet; give it some with
    /// [`Self::with_permissions`]. Until then it may publish nowhere.
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            access: Access::Token(None),
            expires_at: None,
            revoked: None,
        }
    }

    /// A connection that sent no credentials, allowed every stream of
    /// `tenant_id`. Only for the anonymous development switch.
    pub fn anonymous(tenant_id: impl Into<String>) -> Self {
        Self {
            access: Access::Anonymous,
            ..Self::new(tenant_id)
        }
    }

    /// What the verified token allows, as a QUIC publish checks it.
    pub fn with_permissions(mut self, matcher: PermissionMatcher) -> Self {
        self.access = Access::Token(Some(matcher));
        self
    }

    /// The credential stops being valid at `expires_at` (seconds since the
    /// epoch); the connection is closed then.
    pub fn with_expiry(mut self, expires_at: i64) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Close the connection once the future `revoked` returns resolves, which
    /// it does when the credential is revoked.
    pub fn with_revocation<F>(mut self, revoked: F) -> Self
    where
        F: Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    {
        self.revoked = Some(Arc::new(revoked));
        self
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Whether this principal may publish to a stream: `stream.publish` on the
    /// stream's resource, the check a QUIC publish makes.
    pub(crate) fn may_publish(&self, namespace: &str, stream: &str) -> bool {
        match &self.access {
            Access::Anonymous => true,
            Access::Token(None) => false,
            Access::Token(Some(matcher)) => matcher.allows(
                Action::StreamPublish,
                &stream_resource(
                    &TenantId::new(&self.tenant_id),
                    &Namespace::new(namespace),
                    &StreamName::new(stream),
                ),
            ),
        }
    }

    pub(crate) fn expires_at(&self) -> Option<i64> {
        self.expires_at
    }

    /// Resolves when the credential is revoked; never, if it has no
    /// revocation to watch.
    pub(crate) fn revoked(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        match &self.revoked {
            Some(revoked) => revoked(),
            None => Box::pin(std::future::pending()),
        }
    }
}

impl std::fmt::Debug for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Principal")
            .field("tenant_id", &self.tenant_id)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

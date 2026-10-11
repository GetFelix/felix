//! Principal revocations, as learned from the control plane.
//!
//! A revocation is a cutoff: a token issued at or before it is refused, and
//! every stream such a token opened is ended. Long-lived work holds a
//! [`Revocable`] for the token it authenticated with and waits on
//! [`Revocable::revoked`] beside its own work. `docs/auth.md` ("Revoking a
//! principal") describes the whole path.
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::watch;

/// The principal id that stands for every principal of a tenant.
pub const ALL_PRINCIPALS: &str = "*";

/// Every revocation this broker knows, by tenant and principal.
pub struct Revocations {
    cutoffs: RwLock<HashMap<(String, String), i64>>,
    /// Bumped whenever a cutoff moves, waking every waiter to check its own.
    /// Revocations are rare, so a broadcast that most waiters ignore is
    /// cheaper than keeping an index of who holds which token.
    changed: watch::Sender<u64>,
}

impl Default for Revocations {
    fn default() -> Self {
        Self {
            cutoffs: RwLock::new(HashMap::new()),
            changed: watch::channel(0).0,
        }
    }
}

impl Revocations {
    /// Record that `principal_id` of `tenant_id` was revoked at `revoked_at`
    /// (seconds since the epoch). A later cutoff already known is kept.
    pub fn revoke(&self, tenant_id: &str, principal_id: &str, revoked_at: i64) {
        let moved = {
            let mut cutoffs = self.cutoffs.write();
            let cutoff = cutoffs
                .entry((tenant_id.to_string(), principal_id.to_string()))
                .or_insert(i64::MIN);
            let moved = revoked_at > *cutoff;
            *cutoff = (*cutoff).max(revoked_at);
            moved
        };
        if moved {
            metrics::counter!("felix_broker_principal_revocations_total").increment(1);
            self.changed.send_modify(|generation| *generation += 1);
        }
    }

    /// Whether a token of `subject` (presented by `actor`, if delegated)
    /// issued at `issued_at` is revoked.
    pub fn revokes(
        &self,
        tenant_id: &str,
        subject: &str,
        actor: Option<&str>,
        issued_at: i64,
    ) -> bool {
        let cutoffs = self.cutoffs.read();
        let cutoff = |principal: &str| {
            cutoffs
                .get(&(tenant_id.to_string(), principal.to_string()))
                .copied()
        };
        [Some(subject), actor, Some(ALL_PRINCIPALS)]
            .into_iter()
            .flatten()
            .filter_map(cutoff)
            .any(|cutoff| issued_at <= cutoff)
    }
}

/// One verified token, as revocations see it.
#[derive(Clone)]
pub struct Revocable {
    revocations: Arc<Revocations>,
    tenant_id: String,
    subject: String,
    actor: Option<String>,
    issued_at: i64,
}

impl Revocable {
    pub(crate) fn new(
        revocations: Arc<Revocations>,
        tenant_id: String,
        subject: String,
        actor: Option<String>,
        issued_at: i64,
    ) -> Self {
        Self {
            revocations,
            tenant_id,
            subject,
            actor,
            issued_at,
        }
    }

    pub fn is_revoked(&self) -> bool {
        self.revocations.revokes(
            &self.tenant_id,
            &self.subject,
            self.actor.as_deref(),
            self.issued_at,
        )
    }

    /// Resolves once this token is revoked; never, if it is not.
    pub async fn revoked(&self) {
        // Subscribed before the first check, so a revocation landing between
        // the check and the wait still wakes it.
        let mut changed = self.revocations.changed.subscribe();
        loop {
            if self.is_revoked() {
                return;
            }
            if changed.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// The refusal for a token issued before its principal was revoked.
#[derive(Debug)]
pub struct Revoked;

impl std::fmt::Display for Revoked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("credential revoked")
    }
}

impl std::error::Error for Revoked {}

#[cfg(test)]
mod tests;

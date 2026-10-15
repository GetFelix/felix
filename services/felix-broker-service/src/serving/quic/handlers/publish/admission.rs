//! Admission control: the byte budgets (global, per connection, per identity) and the
//! subscription caps.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use tokio::sync::Semaphore;

/// The byte-budget permits a job holds while queued/processing: its identity's share, the
/// connection's ceiling, and the shared process-wide budget. All are released together when
/// the job finishes (or is dropped before ever being enqueued).
pub(crate) struct AdmissionPermit {
    pub(super) _identity: tokio::sync::OwnedSemaphorePermit,
    pub(super) _conn: tokio::sync::OwnedSemaphorePermit,
    pub(super) _global: tokio::sync::OwnedSemaphorePermit,
    /// Keeps the identity's entry alive while its bytes are in flight, so a
    /// stream reopened under the same identity finds the same budget.
    pub(super) _share: Arc<IdentityLimits>,
}

/// Bounds total bytes queued-or-processing across all publishes, independent of the
/// publish queue's item count (`pub_queue_depth`).
///
/// `pub_queue_depth` alone caps how many *jobs* can be queued, but a job's payload can be as
/// large as `max_frame_bytes`; a handful of large batches can still blow past the intended
/// ingress memory budget even with a small queue depth. This mirrors the client's publish-side
/// `PublishAdmission` (see `felix-client`), applying the same in-flight-byte budget on ingest.
///
/// The permit is attached to the `PublishJob` and released only once the job has actually been
/// claimed (or dropped before ever being enqueued), not merely once it is hand
/// off to the channel — this is what makes the bound reflect real resident bytes rather than
/// just admission-time bytes.
pub(crate) struct PublishAdmission {
    pub(super) semaphore: Arc<Semaphore>,
}

/// Who a stream authenticated as. Streams of one connection that authenticate
/// as the same tenant and token subject share one [`IdentityLimits`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct IdentityKey {
    tenant_id: String,
    subject: String,
}

impl IdentityKey {
    pub(crate) fn new(tenant_id: &str, subject: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            subject: subject.to_string(),
        }
    }
}

/// One connection's limits across every identity its streams authenticate as.
///
/// Constructed fresh per connection in `handle_connection` rather than keyed off any
/// shared/cached lookup: a cap must be scoped to one real connection, and a cache keyed by a
/// value that isn't globally unique would let unrelated connections share a limit.
pub(crate) struct ConnLimits {
    subscriptions: AtomicUsize,
    max_subscriptions: usize,
    identity_max_subscriptions: usize,
    identity_inflight_bytes: usize,
    /// Weak, so an identity's entry goes when the last stream, subscription
    /// or in-flight publish holding it does. A client cycling through
    /// identities cannot grow this.
    identities: Mutex<HashMap<IdentityKey, Weak<IdentityLimits>>>,
}

impl ConnLimits {
    pub(crate) fn new(config: &crate::config::BrokerConfig) -> Arc<Self> {
        Arc::new(Self {
            subscriptions: AtomicUsize::new(0),
            max_subscriptions: config.conn_subscription_ceiling(),
            identity_max_subscriptions: config.max_subscriptions_per_conn,
            identity_inflight_bytes: config.pub_conn_inflight_bytes,
            identities: Mutex::new(HashMap::new()),
        })
    }

    /// The limits `key` shares on this connection, created on first use.
    pub(crate) fn share(self: &Arc<Self>, key: IdentityKey) -> Arc<IdentityLimits> {
        let mut identities = self.identities.lock();
        if let Some(share) = identities.get(&key).and_then(Weak::upgrade) {
            return share;
        }
        let share = Arc::new(IdentityLimits {
            subscriptions: AtomicUsize::new(0),
            max_subscriptions: self.identity_max_subscriptions,
            admission: PublishAdmission::new(self.identity_inflight_bytes),
            conn: Some((Arc::clone(self), key.clone())),
        });
        identities.insert(key, Arc::downgrade(&share));
        share
    }

    /// Identities with live state on this connection.
    #[cfg(test)]
    pub(crate) fn identities(&self) -> usize {
        self.identities.lock().len()
    }
}

/// Which cap refused a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriptionCap {
    /// The identity's own share (`max_subscriptions_per_conn`).
    Identity,
    /// The connection's ceiling across identities.
    Connection,
}

impl SubscriptionCap {
    /// The refusal's text. A plain client only ever meets `Identity`, whose
    /// text is what it always was.
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::Identity => "max subscriptions per connection exceeded",
            Self::Connection => "max subscriptions per connection exceeded across identities",
        }
    }
}

/// One identity's share of a connection's limits: its subscription cap and its publish byte
/// budget. Every stream that authenticates as the identity uses the same share, so a client
/// acting for many users over one connection cannot let one user starve the rest.
pub(crate) struct IdentityLimits {
    subscriptions: AtomicUsize,
    max_subscriptions: usize,
    pub(crate) admission: PublishAdmission,
    /// The connection this is a share of, and the key it is filed under.
    /// `None` for a standalone share (tests, templates).
    conn: Option<(Arc<ConnLimits>, IdentityKey)>,
}

impl IdentityLimits {
    /// A share belonging to no connection, with no limits of its own.
    #[cfg(test)]
    pub(crate) fn unlimited() -> Arc<Self> {
        Arc::new(Self {
            subscriptions: AtomicUsize::new(0),
            max_subscriptions: usize::MAX,
            admission: PublishAdmission::new(u32::MAX as usize),
            conn: None,
        })
    }

    /// The share `key` holds on the same connection. A standalone share has
    /// no connection to look in and stays itself.
    pub(crate) fn rebind(self: &Arc<Self>, key: IdentityKey) -> Arc<Self> {
        match &self.conn {
            Some((conn, current)) if *current != key => conn.share(key),
            _ => Arc::clone(self),
        }
    }

    /// Reserve one subscription slot under this identity's cap and the connection's ceiling.
    /// Must be paired with exactly one `release()` call (on any exit path, success or failure)
    /// once reserved.
    pub(crate) fn try_reserve(&self) -> Result<(), SubscriptionCap> {
        if !reserve(&self.subscriptions, self.max_subscriptions) {
            return Err(SubscriptionCap::Identity);
        }
        if let Some((conn, _)) = &self.conn
            && !reserve(&conn.subscriptions, conn.max_subscriptions)
        {
            release(&self.subscriptions);
            return Err(SubscriptionCap::Connection);
        }
        Ok(())
    }

    /// Saturating: must never underflow even if called without a matching reserve, since
    /// wrapping to `usize::MAX` would wedge the cap permanently closed.
    pub(crate) fn release(&self) {
        release(&self.subscriptions);
        if let Some((conn, _)) = &self.conn {
            release(&conn.subscriptions);
        }
    }
}

impl Drop for IdentityLimits {
    fn drop(&mut self) {
        let Some((conn, key)) = &self.conn else {
            return;
        };
        let this: *const Self = self;
        let mut identities = conn.identities.lock();
        // A share for the same key may have replaced this one between the
        // last strong reference going and this lock.
        if identities
            .get(key)
            .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), this))
        {
            identities.remove(key);
        }
    }
}

fn reserve(count: &AtomicUsize, max: usize) -> bool {
    count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            (count < max).then_some(count + 1)
        })
        .is_ok()
}

fn release(count: &AtomicUsize) {
    let _ = count.fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
        Some(count.saturating_sub(1))
    });
}

fn publish_admission_permits(bytes: usize) -> u32 {
    bytes.clamp(1, u32::MAX as usize) as u32
}

impl PublishAdmission {
    pub(crate) fn new(limit_bytes: usize) -> Self {
        let limit = limit_bytes.clamp(1, u32::MAX as usize);
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
        }
    }

    #[cfg(test)]
    pub(crate) fn unlimited() -> Self {
        Self::new(u32::MAX as usize)
    }

    pub(super) async fn acquire(
        &self,
        bytes: usize,
    ) -> std::result::Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
        Arc::clone(&self.semaphore)
            .acquire_many_owned(publish_admission_permits(bytes))
            .await
    }

    pub(super) fn try_acquire(
        &self,
        bytes: usize,
    ) -> std::result::Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::TryAcquireError> {
        Arc::clone(&self.semaphore).try_acquire_many_owned(publish_admission_permits(bytes))
    }
}

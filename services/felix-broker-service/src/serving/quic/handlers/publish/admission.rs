//! Admission control: the global/per-connection byte budget and the subscription cap.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Semaphore;

/// Bundles the two byte-budget permits a job holds while queued/processing: its slice of the
/// per-connection budget and its slice of the shared process-wide budget. Both are released
/// together when the job finishes (or is dropped before ever being enqueued).
pub(crate) struct AdmissionPermit {
    pub(super) _conn: tokio::sync::OwnedSemaphorePermit,
    pub(super) _global: tokio::sync::OwnedSemaphorePermit,
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

/// Bounds concurrent subscriptions on a single connection.
///
/// Constructed fresh per connection in `handle_connection` (same pattern as
/// `PublishContext::conn_admission`) rather than keyed off any shared/cached lookup — this is
/// deliberate: a subscription cap must be scoped to one real connection, and any cache keyed by
/// a value that isn't guaranteed globally unique (e.g. `WriterLaneManager`'s cache, keyed loosely
/// enough that unrelated connections can collide) would let unrelated connections share a limit.
pub(crate) struct SubscriptionLimiter {
    pub(super) count: AtomicUsize,
}

impl SubscriptionLimiter {
    pub(crate) fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
        }
    }

    /// Reserve one subscription slot if room remains under `max`. Must be paired with exactly
    /// one `release()` call (on any exit path, success or failure) once reserved.
    pub(crate) fn try_reserve(&self, max: usize) -> bool {
        self.count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < max).then_some(count + 1)
            })
            .is_ok()
    }

    /// Saturating: must never underflow even if called without a matching reserve, since
    /// wrapping to `usize::MAX` would wedge the cap permanently closed.
    pub(crate) fn release(&self) {
        let _ = self
            .count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                Some(count.saturating_sub(1))
            });
    }
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

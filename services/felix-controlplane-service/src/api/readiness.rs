//! Whether this instance can serve metadata right now.
//!
//! Split from liveness deliberately. A load balancer needs to know "should I
//! send this instance traffic"; an orchestrator needs to know "should I restart
//! this process". They are different questions and the wrong answer to the
//! second is expensive: an external database outage that fails a *liveness*
//! probe restarts every instance, repeatedly, for a fault none of them caused
//! and a restart cannot fix.
//!
//! So liveness here is process-local and never touches the database, and
//! readiness is the one that can fail.
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::store::StoreResult;

/// The one thing readiness asks of a store.
///
/// A trait of its own rather than the whole store: readiness needs to know
/// whether the backend answers, and nothing else. Narrow enough that a test can
/// supply a backend that fails on command, which the full store trait is not.
#[async_trait::async_trait]
pub trait HealthProbe: Send + Sync {
    async fn health_check(&self) -> StoreResult<()>;
}

/// A probe with nothing behind it.
///
/// For a state whose backend cannot be unready: the in-memory store's health
/// check has no failure mode, so probing it is the same answer with an extra
/// indirection. Not a stand-in for a real backend — [`StoreProbe`] is that.
pub struct AlwaysReady;

#[async_trait::async_trait]
impl HealthProbe for AlwaysReady {
    async fn health_check(&self) -> StoreResult<()> {
        Ok(())
    }
}

/// The real store, seen through that one method.
pub struct StoreProbe(pub Arc<dyn crate::store::ControlPlaneAuthStore + Send + Sync>);

#[async_trait::async_trait]
impl HealthProbe for StoreProbe {
    async fn health_check(&self) -> StoreResult<()> {
        crate::store::ControlPlaneStore::health_check(self.0.as_ref()).await
    }
}

/// Longest a readiness check may take before it is treated as a failure.
///
/// A probe that hangs is worse than one that fails: the load balancer keeps
/// sending traffic to an instance nobody has heard from. Well under a typical
/// probe timeout, so this answers before the prober gives up and the answer is
/// this service's rather than the network's.
pub const DEFAULT_CHECK_TIMEOUT: Duration = Duration::from_secs(2);

/// How long an answer is reused before the database is asked again.
///
/// Several load balancers polling every second each would otherwise be a query
/// each, on the connection pool that real work needs. One second keeps the cost
/// flat no matter how many probers there are, and bounds how stale an answer
/// can be — including how long recovery takes to show, since a failure is
/// cached for the same window.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(1);

/// Why an instance is not ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotReady {
    /// The process is shutting down and should be taken out of rotation.
    ///
    /// Checked before the store and never cached: draining is a local fact, it
    /// only ever moves one way, and an instance that kept answering ready for
    /// up to a cache window would keep receiving traffic it is about to stop
    /// serving.
    Draining,
    /// Startup has not finished: the API listener is not bound yet.
    Starting,
    /// The check did not finish in time.
    Timeout { after_ms: u64 },
    /// The store answered, and the answer was no.
    Store { detail: String },
}

impl std::fmt::Display for NotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout { after_ms } => {
                write!(f, "the store did not answer within {after_ms}ms")
            }
            Self::Draining => write!(f, "this instance is shutting down"),
            Self::Starting => write!(f, "this instance is still starting"),
            Self::Store { detail } => write!(f, "{detail}"),
        }
    }
}

struct Cached {
    at: Instant,
    outcome: Result<(), NotReady>,
}

/// Answers readiness, bounded and at a fixed cost.
pub struct Readiness {
    /// The process-wide lifecycle flag, shared with the metrics endpoint's
    /// `/ready`. One flag, so the two cannot disagree about whether this
    /// instance is in rotation.
    lifecycle: felix_common::lifecycle::Readiness,
    store: Arc<dyn HealthProbe>,
    timeout: Duration,
    ttl: Duration,
    /// Held across the check, so probes arriving together produce one query
    /// rather than one each — the second waits and reads what the first found.
    cached: Mutex<Option<Cached>>,
}

impl std::fmt::Debug for Readiness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Readiness")
            .field("timeout", &self.timeout)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl Readiness {
    pub fn new(store: Arc<dyn HealthProbe>) -> Self {
        Self::with_limits(store, DEFAULT_CHECK_TIMEOUT, DEFAULT_CACHE_TTL)
    }

    pub fn with_limits(store: Arc<dyn HealthProbe>, timeout: Duration, ttl: Duration) -> Self {
        Self::with_lifecycle(
            felix_common::lifecycle::Readiness::ready(),
            store,
            timeout,
            ttl,
        )
    }

    /// Share the process's lifecycle flag, so `/v1/system/ready` and the metrics
    /// endpoint's `/ready` answer the same question during a shutdown.
    pub fn with_lifecycle(
        lifecycle: felix_common::lifecycle::Readiness,
        store: Arc<dyn HealthProbe>,
        timeout: Duration,
        ttl: Duration,
    ) -> Self {
        Self {
            lifecycle,
            store,
            timeout,
            ttl,
            cached: Mutex::new(None),
        }
    }

    /// Whether this instance can serve metadata, as of at most `ttl` ago.
    pub async fn check(&self) -> Result<(), NotReady> {
        self.check_at(Instant::now()).await
    }

    /// [`Readiness::check`], with the clock supplied so the cache is testable
    /// without sleeping.
    pub async fn check_at(&self, now: Instant) -> Result<(), NotReady> {
        // Before the cache, and before the store. A draining instance must fall
        // out of rotation on the very next probe, not after the cache window,
        // and asking a database whether we are shutting down is pointless.
        if !self.lifecycle.is_ready() {
            return Err(if self.lifecycle.is_draining() {
                NotReady::Draining
            } else {
                NotReady::Starting
            });
        }
        let mut cached = self.cached.lock().await;
        if let Some(held) = cached.as_ref()
            && now.duration_since(held.at) < self.ttl
        {
            return held.outcome.clone();
        }

        let outcome = match tokio::time::timeout(self.timeout, self.store.health_check()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(NotReady::Store {
                detail: err.to_string(),
            }),
            Err(_) => Err(NotReady::Timeout {
                after_ms: self.timeout.as_millis() as u64,
            }),
        };
        *cached = Some(Cached {
            at: now,
            outcome: outcome.clone(),
        });
        outcome
    }
}

#[cfg(test)]
mod tests;

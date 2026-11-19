//! Process lifecycle: termination signals, readiness gating, and bounded drain.
//!
//! # Why this exists
//! Kubernetes, systemd, and `docker stop` all terminate a process with **SIGTERM**.
//! Waiting only on `ctrl_c()` (SIGINT) means those signals fall through to the
//! default handler, which kills the process immediately and drops every in-flight
//! publish, acknowledgement, and subscription write. Every rolling update would
//! abort in-flight work.
//!
//! # Shutdown order
//! The order matters more than the individual steps:
//!
//! 1. **Readiness goes false.** Load balancers and the Kubernetes endpoints
//!    controller observe `/ready` and stop routing new traffic here. This happens
//!    before anything stops working, so clients are steered away from a healthy
//!    instance rather than discovering a broken one.
//! 2. **Stop accepting new connections.** Already-accepted work is untouched.
//! 3. **Drain, bounded by a deadline.** In-flight connections finish on their own.
//! 4. **Force-cancel whatever is left, and say what it was.** A drain that silently
//!    hangs until SIGKILL is indistinguishable from the bug it was meant to fix, so
//!    the subsystems that missed the deadline are named in the logs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
// The budget must measure against the same clock that `tokio::time::timeout` uses,
// or the remaining-time arithmetic and the timeouts it drives disagree. This is
// identical to `std::time::Instant` at runtime and additionally tracks tokio's
// paused clock under test.
use tokio::time::Instant;

/// Readiness flag shared between the health endpoints and the shutdown path.
///
/// Cloning shares the underlying flag; every clone observes the same state.
#[derive(Clone, Debug)]
pub struct Readiness {
    ready: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
}

impl Readiness {
    /// Create a flag that already reports ready.
    pub fn ready() -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(true)),
            draining: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create a flag that reports *not* ready until [`Readiness::mark_ready`].
    ///
    /// For work that must finish before traffic arrives. Reporting ready first
    /// and initialising afterwards is worse than a slow start: an orchestrator
    /// routes to the instance immediately, and requests land on state that does
    /// not exist yet.
    pub fn starting() -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(false)),
            draining: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether this process is serving, as a gauge.
    ///
    /// `1` while ready, `0` once draining. A log line saying a drain began is
    /// gone with the pod; this is the thing a dashboard can show and an alert
    /// can fire on, and it is what distinguishes "the instance left rotation
    /// deliberately" from "the instance vanished".
    pub const READY_STATE: &str = "felix_ready_state";

    /// Flip to ready once startup work has finished.
    ///
    /// Returns the previous state. Deliberately not the inverse of
    /// [`Readiness::begin_draining`]: a draining instance must never be brought
    /// back, so callers only use this during startup.
    pub fn mark_ready(&self) -> bool {
        metrics::gauge!(Self::READY_STATE).set(1.0);
        self.ready.swap(true, Ordering::Release)
    }

    /// Whether `/ready` should report success.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Whether a drain has begun, as opposed to startup not having finished.
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    /// Flip to not-ready.
    ///
    /// Call this *before* the listener stops, so traffic is steered away while the
    /// broker can still serve it. Returns the previous state so callers can tell a
    /// first shutdown from a repeated signal.
    pub fn begin_draining(&self) -> bool {
        metrics::gauge!(Self::READY_STATE).set(0.0);
        self.draining.store(true, Ordering::Release);
        self.ready.swap(false, Ordering::Release)
    }
}

impl Default for Readiness {
    fn default() -> Self {
        Self::ready()
    }
}

/// How many requests are being served right now.
///
/// A drain waits for the *server* to finish, which is the right thing to wait
/// on — but it says nothing about how much was in flight when the signal
/// arrived, or whether the count reached zero before the deadline. That is the
/// difference between "drained cleanly" and "the deadline expired and we called
/// it done", and it is invisible without counting.
#[derive(Debug, Clone, Default)]
pub struct InFlight {
    count: Arc<std::sync::atomic::AtomicI64>,
}

impl InFlight {
    /// Requests currently being served.
    pub const GAUGE: &str = "felix_inflight_requests";

    pub fn new() -> Self {
        Self::default()
    }

    /// Mark a request started. The returned guard decrements on drop, so a
    /// handler that panics or returns early cannot leak the count.
    pub fn enter(&self) -> InFlightGuard {
        let now = self.count.fetch_add(1, Ordering::AcqRel) + 1;
        metrics::gauge!(Self::GAUGE).set(now as f64);
        InFlightGuard {
            count: Arc::clone(&self.count),
        }
    }

    /// How many requests are outstanding.
    pub fn current(&self) -> i64 {
        self.count.load(Ordering::Acquire)
    }
}

/// Decrements the in-flight count when dropped.
#[derive(Debug)]
pub struct InFlightGuard {
    count: Arc<std::sync::atomic::AtomicI64>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let now = self.count.fetch_sub(1, Ordering::AcqRel) - 1;
        metrics::gauge!(InFlight::GAUGE).set(now as f64);
    }
}

/// Tracks a drain against a total deadline shared by every subsystem.
///
/// Each subsystem gets whatever is left of the budget rather than its own full
/// copy, so N subsystems cannot stretch a 25s deadline into 25N seconds. Anything
/// that misses the deadline is recorded and reported together at the end.
pub struct DrainBudget {
    deadline: Duration,
    started: Instant,
    unfinished: Vec<&'static str>,
}

impl DrainBudget {
    pub fn new(deadline: Duration) -> Self {
        Self {
            deadline,
            started: Instant::now(),
            unfinished: Vec::new(),
        }
    }

    /// Time left in the overall budget; zero once the deadline has passed.
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_sub(self.started.elapsed())
    }

    /// Await `task` within the remaining budget, recording `name` if it does not
    /// finish in time. Returns whether it finished.
    pub async fn drain<F>(&mut self, name: &'static str, task: F) -> bool
    where
        F: Future<Output = ()>,
    {
        let remaining = self.remaining();
        if remaining.is_zero() {
            self.unfinished.push(name);
            return false;
        }
        match tokio::time::timeout(remaining, task).await {
            Ok(()) => {
                tracing::debug!(subsystem = name, "drained");
                true
            }
            Err(_) => {
                self.unfinished.push(name);
                false
            }
        }
    }

    /// Subsystems that did not finish within the deadline.
    pub fn unfinished(&self) -> &[&'static str] {
        &self.unfinished
    }

    /// How long the last drain took, in milliseconds.
    pub const DRAIN_DURATION_MS: &str = "felix_drain_duration_ms";
    /// Subsystems cancelled because the drain deadline expired.
    ///
    /// Counted per subsystem, and deliberately *not* folded into the duration
    /// gauge: a drain that finished in time and a drain that was cut off both
    /// take about the deadline to report, and only this tells them apart. A
    /// non-zero value means work was dropped.
    pub const DRAIN_FORCED_TOTAL: &str = "felix_drain_forced_total";

    /// Report the outcome, as metrics and as a log line.
    ///
    /// Forced cancellation is a warning rather than an info line: it means work
    /// was dropped and the operator needs to see it. The counter exists because
    /// the log line does not survive the pod.
    pub fn report(&self) {
        metrics::gauge!(Self::DRAIN_DURATION_MS).set(self.started.elapsed().as_millis() as f64);
        for subsystem in &self.unfinished {
            metrics::counter!(Self::DRAIN_FORCED_TOTAL, "subsystem" => *subsystem).increment(1);
        }
        let elapsed_ms = self.started.elapsed().as_millis();
        if self.unfinished.is_empty() {
            tracing::info!(elapsed_ms, "drain complete");
        } else {
            tracing::warn!(
                elapsed_ms,
                deadline_ms = self.deadline.as_millis(),
                unfinished = ?self.unfinished,
                "drain deadline expired; forcing cancellation"
            );
        }
    }
}

/// Installs the termination handlers and returns a future resolving when the
/// process is asked to terminate.
///
/// On Unix this is SIGTERM (Kubernetes, systemd, `docker stop`) or SIGINT (Ctrl-C).
/// On other platforms only Ctrl-C is available. The signal that fired is logged,
/// because "which signal did we get" is the first question when a pod is being
/// killed unexpectedly.
///
/// **Handlers are installed when this is called, not when the returned future is
/// awaited**, so call it before binding any listener. A signal arriving before
/// the handlers exist terminates the process with the default disposition, with
/// no drain and no readiness flip.
///
/// Must be called from within a Tokio runtime.
#[cfg(unix)]
pub fn termination_signal() -> impl std::future::Future<Output = ()> {
    use tokio::signal::unix::{SignalKind, signal};

    // Handlers are installed here, when this is *called*, and not when the
    // returned future is first polled. A caller binds its listeners and only
    // then reaches the `select!` that awaits this; anything registered lazily
    // would leave the process reachable but with the default disposition still
    // in force, and a signal arriving in that window kills it outright instead
    // of draining. Callers should call this before they bind anything.
    //
    // Registering either one can fail, and one working signal is much better
    // than no shutdown path at all, so each is kept independently.
    let sigterm = signal(SignalKind::terminate())
        .inspect_err(|err| {
            tracing::error!(error = %err, "failed to install SIGTERM handler");
        })
        .ok();
    let sigint = signal(SignalKind::interrupt())
        .inspect_err(|err| {
            tracing::error!(error = %err, "failed to install SIGINT handler");
        })
        .ok();
    if sigterm.is_none() && sigint.is_none() {
        tracing::error!("no termination handler could be installed; shutdown will not be graceful");
    }

    async move {
        // `pending()` stands in for a signal that could not be registered, so
        // the surviving one still resolves the select rather than racing an
        // arm that would fire immediately.
        let mut sigterm = sigterm;
        let mut sigint = sigint;
        let terminate = async {
            match sigterm.as_mut() {
                Some(stream) => stream.recv().await,
                None => std::future::pending().await,
            }
        };
        let interrupt = async {
            match sigint.as_mut() {
                Some(stream) => stream.recv().await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            _ = interrupt => {
                tracing::info!(signal = "SIGINT", "termination signal received");
            }
            _ = terminate => {
                tracing::info!(signal = "SIGTERM", "termination signal received");
            }
        }
    }
}

/// Resolves when the process is asked to terminate.
///
/// Non-Unix targets have no SIGTERM; Ctrl-C is the only portable trigger. Unlike
/// the Unix version this cannot install its handler eagerly — `ctrl_c` registers
/// on first poll and exposes no way to separate the two — so a Ctrl-C between
/// binding and awaiting this is still lost.
#[cfg(not(unix))]
pub fn termination_signal() -> impl std::future::Future<Output = ()> {
    async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!(signal = "CTRL_C", "termination signal received");
    }
}

#[cfg(test)]
mod tests;

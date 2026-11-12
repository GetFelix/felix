//! Tokio's blocking pool, through one seam a simulator can take over.
//!
//! A simulation runs every broker on one current-thread runtime with a paused
//! clock. Work on a real thread finishes in real time, so where it lands among
//! the simulated tasks changes from run to run and a seed stops being
//! repeatable. Once a simulator has installed its clock ([`crate::clock::sim`])
//! the work here runs as a task on the caller's runtime instead. Builds
//! without the simulator (a release build without the `sim` feature) always
//! use the blocking pool.

use tokio::task::JoinHandle;

/// Whether blocking work runs on the caller's runtime instead of a thread:
/// only while a simulator drives this process.
#[inline]
pub fn on_runtime() -> bool {
    #[cfg(any(debug_assertions, test, feature = "sim"))]
    {
        crate::clock::sim::installed()
    }
    #[cfg(not(any(debug_assertions, test, feature = "sim")))]
    {
        false
    }
}

/// [`tokio::task::spawn_blocking`], or under a simulator a task on the
/// current runtime that runs `work` when first polled. Either way a panic in
/// `work` comes back as a panicked [`tokio::task::JoinError`].
pub fn spawn<F, R>(work: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    if on_runtime() {
        return tokio::spawn(async move { work() });
    }
    tokio::task::spawn_blocking(work)
}

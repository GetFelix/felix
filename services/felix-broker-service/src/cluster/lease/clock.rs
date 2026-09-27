//! The clock the lease is measured on.
//!
//! `felix_common::clock::boottime`: `CLOCK_BOOTTIME` on Linux, so a broker
//! suspended through its expiry wakes with the lease already spent, as the
//! control plane's clock says it is. The module doc there has the detail, and
//! the test-only fault seam that lets the cluster harness skew it.
//!
//! Scheduling (sleeps, tickers) stays on tokio: a late tick costs latency, not
//! safety. Only validity is judged on this clock.

use std::time::Duration;

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

/// A reading of a [`LeaseClock`]. Comparable only with readings of the same
/// clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LeaseInstant(Duration);

impl LeaseInstant {
    pub(crate) fn saturating_duration_since(self, earlier: LeaseInstant) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}

/// Where lease time comes from.
#[derive(Clone, Debug)]
pub(crate) enum LeaseClock {
    /// `felix_common::clock::boottime`.
    System,
    /// Tokio's clock, so a paused-time test can drive the lease with
    /// `tokio::time::advance`.
    #[cfg(test)]
    Tokio(tokio::time::Instant),
    /// Moved only by the test holding it, in millis.
    #[cfg(test)]
    Manual(Arc<AtomicU64>),
}

impl LeaseClock {
    pub(crate) fn now(&self) -> LeaseInstant {
        match self {
            LeaseClock::System => LeaseInstant(felix_common::clock::boottime()),
            #[cfg(test)]
            LeaseClock::Tokio(origin) => LeaseInstant(origin.elapsed()),
            #[cfg(test)]
            LeaseClock::Manual(millis) => {
                LeaseInstant(Duration::from_millis(millis.load(Ordering::Acquire)))
            }
        }
    }
}

#[cfg(test)]
mod tests;

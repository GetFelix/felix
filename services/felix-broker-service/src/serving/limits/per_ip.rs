//! A cap on the connections one source address may hold.
//!
//! Without it, one host can open connections until the broker runs out of
//! memory or file descriptors, and every other client is refused with it.
//! The count is by IP, not by port: a client's ports are free for it to pick.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use parking_lot::Mutex;

/// Counts connections per source address against a fixed cap.
pub(crate) struct PerIpLimiter {
    /// `0` is unlimited.
    max: usize,
    counts: Mutex<HashMap<IpAddr, usize>>,
}

impl PerIpLimiter {
    pub(crate) fn new(max: usize) -> Arc<Self> {
        Arc::new(Self {
            max,
            counts: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn max(&self) -> usize {
        self.max
    }

    /// Take a place for `source`, or `None` when it already holds `max`.
    pub(crate) fn try_acquire(self: &Arc<Self>, source: IpAddr) -> Option<PerIpPermit> {
        // A dual-stack socket reports IPv4 peers as `::ffff:a.b.c.d`; counting
        // those apart from the plain IPv4 form would give one host two caps.
        let source = source.to_canonical();
        if self.max == 0 {
            return Some(PerIpPermit {
                limiter: None,
                source,
            });
        }
        let mut counts = self.counts.lock();
        let held = counts.entry(source).or_insert(0);
        if *held >= self.max {
            return None;
        }
        *held += 1;
        Some(PerIpPermit {
            limiter: Some(Arc::clone(self)),
            source,
        })
    }

    #[cfg(test)]
    pub(crate) fn held(&self, source: IpAddr) -> usize {
        self.counts
            .lock()
            .get(&source.to_canonical())
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn addresses(&self) -> usize {
        self.counts.lock().len()
    }
}

/// A connection's place in the count, given back on drop.
///
/// A guard rather than a decrement at the end of the connection task: that
/// task has several exits and can be cancelled between any of them, and a
/// leaked count is an address locked out for the life of the process.
pub(crate) struct PerIpPermit {
    limiter: Option<Arc<PerIpLimiter>>,
    source: IpAddr,
}

impl Drop for PerIpPermit {
    fn drop(&mut self) {
        let Some(limiter) = &self.limiter else {
            return;
        };
        let mut counts = limiter.counts.lock();
        if let Some(held) = counts.get_mut(&self.source) {
            *held = held.saturating_sub(1);
            if *held == 0 {
                // Otherwise the map keeps an entry for every address ever seen.
                counts.remove(&self.source);
            }
        }
    }
}

#[cfg(test)]
mod tests;

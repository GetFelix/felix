//! Store liveness in a simulated cluster: reading what each broker would
//! decide, and the slow-sync fault on a broker's probe.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use felix_replication::liveness::Liveness;

use super::SimCluster;

impl SimCluster {
    /// The store liveness of the running broker at `index`. `None` while it
    /// is down, or in a run without [`SimConfig::store_liveness`].
    ///
    /// [`SimConfig::store_liveness`]: super::SimConfig::store_liveness
    pub fn store_liveness(&self, index: usize) -> Option<Arc<Liveness>> {
        self.nodes[index].store_liveness()
    }

    /// Make every later probe sync on the broker at `index` take `delay` of
    /// virtual time. Only the probe: the logs keep syncing as before, so
    /// this is a disk that stalls under store liveness and nothing else.
    /// A sync already running finishes on the delay it started with.
    pub fn slow_probe_sync(&self, index: usize, delay: Duration) {
        self.nodes[index].probe_delay_ms.store(
            u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

#[cfg(test)]
mod tests;

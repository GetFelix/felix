//! Severing two brokers while both stay alive.
//!
//! Test-only, and off unless `FELIX_PEER_PARTITION_FILE` names a file. A
//! partition is the one fault the cluster harness cannot inject from outside
//! the process: killing, stopping and freezing a broker are each a signal away,
//! but cutting the path between two brokers while both keep running -- and keep
//! heartbeating to the control plane -- needs the broker's cooperation.
//!
//! That combination is worth reaching. A leader that looks healthy to the
//! control plane and cannot reach a single follower is where a replication
//! design is most likely to be wrong, and no other fault produces it: a frozen
//! broker stops heartbeating, so the cluster notices it by other means.
//!
//! The file holds one node id per line. Empty or missing means no partition,
//! so healing is a delete and injecting is a write.
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// How long a reading of the file is reused.
///
/// Short enough that a test injecting a partition sees it take effect without
/// coordinating, long enough that a broker under load is not stat-ing a file
/// once per forwarded publish.
const REREAD_AFTER: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub struct PartitionInjector {
    path: PathBuf,
    cached: Mutex<Cached>,
}

#[derive(Debug)]
struct Cached {
    nodes: HashSet<String>,
    read_at: Option<Instant>,
}

impl PartitionInjector {
    pub fn new(path: PathBuf) -> Self {
        tracing::warn!(
            path = %path.display(),
            "peer partition injection is ENABLED; this is a test-only facility",
        );
        Self {
            path,
            cached: Mutex::new(Cached {
                nodes: HashSet::new(),
                read_at: None,
            }),
        }
    }

    /// Whether this broker is currently cut off from `node_id`.
    pub fn blocks(&self, node_id: &str) -> bool {
        let mut cached = self.cached.lock();
        let stale = cached
            .read_at
            .is_none_or(|read_at| read_at.elapsed() >= REREAD_AFTER);
        if stale {
            // A missing file is the common case once a test heals a partition,
            // and it is not an error: it means nothing is severed.
            cached.nodes = std::fs::read_to_string(&self.path)
                .map(|body| {
                    body.lines()
                        .map(str::trim)
                        .filter(|line| !line.is_empty())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            cached.read_at = Some(Instant::now());
        }
        cached.nodes.contains(node_id)
    }
}

#[cfg(test)]
mod tests;

//! Checks on the cluster itself, beside the history checker.
//!
//! The history checker reads what clients saw. A [`Checker`] reads the
//! cluster: it can ask any broker or the control plane what it believes and
//! say whether that is allowed. A run holds a list of them and calls each at
//! two points:
//!
//! - [`Point::Healed`]: the moment the nemesis has healed everything, on a
//!   task of its own while the clients keep running. A checker may wait
//!   (virtual time) for the cluster to reach a state, as [`ElectionSettles`]
//!   does.
//! - [`Point::End`]: after the clients stopped, in-flight work drained and the
//!   final reads were taken, with the whole [`History`] to look at.
//!
//! # Adding a checker
//!
//! Implement [`Checker`] and put it in the list [`run_seed_with`] takes:
//!
//! ```ignore
//! struct NoTwoLeaders;
//!
//! #[async_trait::async_trait]
//! impl Checker for NoTwoLeaders {
//!     fn name(&self) -> &'static str {
//!         "no-two-leaders"
//!     }
//!
//!     async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
//!         if ctx.point != Point::End {
//!             return Vec::new();
//!         }
//!         let mut findings = Vec::new();
//!         for key in ctx.cluster.shard_keys() {
//!             if ctx.cluster.servers_of(&key).await.len() > 1 {
//!                 findings.push(Finding::new(self.name(), "two leaders").with(ctx.cluster.census(&key).await));
//!             }
//!         }
//!         findings
//!     }
//! }
//! ```
//!
//! A finding fails the run. Do not weaken a checker to make a seed pass: file
//! the seed as an issue and pin it with `#[ignore = "#<issue>"]`.
//!
//! [`run_seed_with`]: crate::run::run_seed_with

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use felix_cluster::history::model::History;

use crate::cluster::{SimCluster, describe};

/// When a checker is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Point {
    /// Right after every fault is healed.
    Healed,
    /// After the clients stopped and the final reads were taken.
    End,
}

/// What a checker can look at.
pub struct CheckContext {
    pub cluster: Arc<SimCluster>,
    pub point: Point,
    /// When the faults healed, on the cluster's clock ([`SimCluster::elapsed`]).
    pub healed_at: Duration,
    /// The history so far; complete at [`Point::End`], empty at
    /// [`Point::Healed`].
    pub history: History,
}

/// One thing a checker found wrong.
#[derive(Debug, Clone)]
pub struct Finding {
    pub checker: &'static str,
    pub summary: String,
    /// What to read to see why: a census, a timeline.
    pub detail: String,
}

impl Finding {
    pub fn new(checker: &'static str, summary: impl Into<String>) -> Self {
        Self {
            checker,
            summary: summary.into(),
            detail: String::new(),
        }
    }

    /// Add text to the detail.
    pub fn with(mut self, detail: impl fmt::Display) -> Self {
        use std::fmt::Write;
        let _ = writeln!(self.detail, "{detail}");
        self
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}] {}", self.checker, self.summary)?;
        write!(f, "{}", self.detail)
    }
}

/// A check on the live cluster.
#[async_trait::async_trait]
pub trait Checker: Send {
    fn name(&self) -> &'static str;

    /// Look at the cluster at `ctx.point` and return what is wrong, nothing if
    /// all is well.
    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding>;
}

/// After the faults heal, every shard is served by exactly one broker within
/// [`SimConfig::settle_within`](crate::cluster::SimConfig::settle_within).
///
/// "Served" is a broker that would take a write for the shard now: it routes
/// the shard to itself, its fence is open at that generation and its lease is
/// valid. A shard that has none, or more than one, for the whole window fails.
/// The finding carries a census of the first few stuck shards.
pub struct ElectionSettles {
    /// How many stuck shards to print a census for.
    pub show: usize,
}

impl Default for ElectionSettles {
    fn default() -> Self {
        Self { show: 3 }
    }
}

#[async_trait::async_trait]
impl Checker for ElectionSettles {
    fn name(&self) -> &'static str {
        "election-settles"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
        if ctx.point != Point::Healed {
            return Vec::new();
        }
        let cluster = &ctx.cluster;
        let deadline = ctx.healed_at + cluster.config.settle_within;
        loop {
            let stuck = cluster.unserved().await;
            if stuck.is_empty() {
                return Vec::new();
            }
            if cluster.elapsed() >= deadline {
                let names: Vec<String> = stuck
                    .iter()
                    .take(12)
                    .map(|(key, serving)| format!("{} ({} serving)", describe(key), serving.len()))
                    .collect();
                let mut finding = Finding::new(
                    self.name(),
                    format!(
                        "{} of {} shards are not served by exactly one broker {:?} after the faults healed: {}{}",
                        stuck.len(),
                        cluster.shard_keys().count(),
                        cluster.config.settle_within,
                        names.join(", "),
                        if stuck.len() > names.len() {
                            ", ..."
                        } else {
                            ""
                        },
                    ),
                );
                for (key, _) in stuck.iter().take(self.show) {
                    finding = finding.with(cluster.census(key).await);
                }
                return vec![finding];
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

#[cfg(test)]
mod tests;

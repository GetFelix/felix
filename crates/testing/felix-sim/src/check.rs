//! Checks on the cluster itself, beside the history checker.
//!
//! The history checker reads what clients saw. A [`Checker`] reads the
//! cluster: it can ask any broker or the control plane what it believes and
//! say whether that is allowed. A run holds a list of them and calls each at
//! three kinds of point:
//!
//! - [`Point::Quiet`]: about once a virtual second while the run goes. A broker
//!   that is starting has no view of its shards and is left out of the
//!   sample. It happens during the faults too, which is the only way to see a state that lasts seconds, such
//!   as two brokers leading one shard. A checker that has nothing to look at
//!   then returns at once. A call is skipped if the checker is still busy with
//!   an earlier one.
//! - [`Point::Healed`]: the moment the nemesis has healed everything, on a
//!   task of its own while the clients keep running. A checker may wait
//!   (virtual time) for the cluster to reach a state, as [`ElectionSettles`]
//!   does. The history holds what the clients had recorded by then.
//! - [`Point::End`]: after the clients stopped, in-flight work drained and the
//!   final reads were taken, with the whole [`History`] to look at.
//!
//! The checkers a run has by default are the ones named after the invariants
//! of `docs/formal/FelixShard.tla`; see [`default_checkers`].
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
//! A finding fails the run, unless it is a [`Severity::Warning`]. Do not weaken a checker to make a seed pass: file
//! the seed as an issue and pin it with `#[ignore = "#<issue>"]`.
//!
//! [`run_seed_with`]: crate::run::run_seed_with

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use felix_broker_service::sim::{ShardKey, ShardStand};
use felix_cluster::history::model::History;

use crate::client::Recorder;
use crate::cluster::{SimCluster, describe};
use crate::nemesis::Plan;

mod acked;
mod burn;
mod disks;
mod generations;
mod leadership;
mod power;

pub use self::acked::AckedHeldByLeader;
pub use self::burn::GenerationBurn;
pub use self::disks::AckedOnSomeDisk;
pub use self::generations::{RecordGenerationsAgree, Tally};
pub use self::leadership::{AdoptedMatchesOpened, AtMostOneServing, OneLeaderPerGeneration};
pub use self::power::TornTailRepaired;

/// When a checker is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Point {
    /// Between faults and during them, about once a virtual second, without
    /// the brokers that are starting. The history holds what the clients have recorded so
    /// far and `healed_at` is zero.
    Quiet,
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
    /// What the clients recorded: complete at [`Point::End`], what had been
    /// recorded by the heal at [`Point::Healed`], so far at [`Point::Quiet`].
    pub history: History,
    /// The recorder behind `history`, for what lands after the snapshot (an
    /// operator abandoning a log while a checker is still polling).
    pub recorder: Arc<Recorder>,
    /// The faults the nemesis was given, empty for a run without faults. Some
    /// may have been skipped (a crash that would take down a majority).
    pub plan: Plan,
}

/// Whether a finding fails the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// The cluster did something it must not. Fails the run.
    Failure,
    /// Worth reading, but not wrong by itself.
    Warning,
}

/// One thing a checker found wrong.
#[derive(Debug, Clone)]
pub struct Finding {
    pub checker: &'static str,
    pub severity: Severity,
    pub summary: String,
    /// What to read to see why: a census, a timeline.
    pub detail: String,
}

impl Finding {
    pub fn new(checker: &'static str, summary: impl Into<String>) -> Self {
        Self {
            checker,
            severity: Severity::Failure,
            summary: summary.into(),
            detail: String::new(),
        }
    }

    /// A finding that does not fail the run.
    pub fn warning(checker: &'static str, summary: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            ..Self::new(checker, summary)
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
        let warning = match self.severity {
            Severity::Failure => "",
            Severity::Warning => "warning ",
        };
        writeln!(f, "[{warning}{}] {}", self.checker, self.summary)?;
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

/// The checkers a run has when it is given no others.
pub fn default_checkers() -> Vec<Box<dyn Checker>> {
    vec![
        Box::new(ElectionSettles::default()),
        Box::new(OneLeaderPerGeneration::default()),
        Box::new(AtMostOneServing::default()),
        Box::new(AckedHeldByLeader::default()),
        Box::new(AdoptedMatchesOpened::default()),
        Box::new(GenerationBurn::default()),
        Box::new(TornTailRepaired),
        Box::new(AckedOnSomeDisk),
        Box::new(RecordGenerationsAgree::default()),
    ]
}

/// What one running broker believed about a shard at one instant.
#[derive(Debug, Clone)]
pub struct Seen {
    pub node: usize,
    /// The broker's id, `b1` and so on.
    pub name: String,
    pub stand: ShardStand,
}

/// What every running broker believes about `key`, all read at one instant.
pub fn seen_by_all(cluster: &SimCluster, key: &ShardKey) -> Vec<Seen> {
    cluster
        .stands(key)
        .into_iter()
        .map(|(node, stand)| Seen {
            node,
            name: cluster.node_id(node).to_string(),
            stand,
        })
        .collect()
}

/// Keeps a checker that sees the same fault every second from printing it every
/// second, and one systematic fault from printing a census for every shard.
#[derive(Debug)]
pub(crate) struct Digest {
    show: usize,
    seen: BTreeSet<String>,
    shown: usize,
    hidden: usize,
}

impl Digest {
    pub(crate) fn new(show: usize) -> Self {
        Self {
            show,
            seen: BTreeSet::new(),
            shown: 0,
            hidden: 0,
        }
    }

    /// Whether `id` should be reported now: it is new, and fewer than `show`
    /// others have been.
    pub(crate) fn admit(&mut self, id: &str) -> bool {
        if !self.seen.insert(id.to_string()) {
            return false;
        }
        if self.shown >= self.show {
            self.hidden += 1;
            return false;
        }
        self.shown += 1;
        true
    }

    /// A finding for what `admit` held back, once the run is over.
    pub(crate) fn rest(&mut self, checker: &'static str) -> Option<Finding> {
        let hidden = std::mem::take(&mut self.hidden);
        (hidden > 0).then(|| Finding::new(checker, format!("{hidden} more, not shown")))
    }
}

/// After the faults heal, every shard is served by exactly one broker within
/// [`SimConfig::settle_within`](crate::cluster::SimConfig::settle_within).
///
/// "Served" is a broker that would take a write for the shard now: it routes
/// the shard to itself, its fence is open at that generation and its lease is
/// valid. A shard that has none, or more than one, for the whole window fails.
/// The finding carries a census of the first few stuck shards.
///
/// A shard held after unclean kills is left out:
/// the broker holds it on purpose until an operator abandons a member (see
/// [`SimCluster::held_shards`]). One with a copy still untrusted although a
/// leader served beside it is not held, and is marked `vouchable`. Only this check skips it; the history checks
/// and the other cluster checks still cover it.
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
        let report = |holds: &crate::cluster::Holds| {
            println!(
                "election-settles: {} of {} shards exempt, held on purpose after unclean kills",
                holds.held.len(),
                cluster.shard_keys().count(),
            );
            for (shard, why) in &holds.reasons {
                println!(
                    "election-settles: warning: {shard} held on purpose, no attestation majority: {}",
                    why.join("; "),
                );
            }
            if !holds.vouchable.is_empty() {
                println!(
                    "election-settles: {} unserved shards have a copy still untrusted after a leader served beside it: {}",
                    holds.vouchable.len(),
                    holds
                        .vouchable
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
        };
        loop {
            // Read again each pass: a copy a leader vouches meanwhile is
            // trusted again, and its shard stops being exempt.
            let holds = cluster.holds().await;
            let stuck = without_held(cluster.unserved().await, &holds.held);
            if stuck.is_empty() {
                report(&holds);
                return Vec::new();
            }
            if cluster.elapsed() >= deadline {
                report(&holds);
                let names: Vec<String> = stuck
                    .iter()
                    .take(12)
                    .map(|(key, serving)| {
                        let shard = describe(key);
                        let vouchable = if holds.vouchable.contains(&shard) {
                            ", vouchable"
                        } else {
                            ""
                        };
                        format!("{shard} ({} serving{vouchable})", serving.len())
                    })
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

/// `stuck` without the shards the broker holds on purpose.
fn without_held(
    mut stuck: Vec<(ShardKey, Vec<usize>)>,
    held: &BTreeSet<String>,
) -> Vec<(ShardKey, Vec<usize>)> {
    stuck.retain(|(key, _)| !held.contains(&describe(key)));
    stuck
}

#[cfg(test)]
mod tests;

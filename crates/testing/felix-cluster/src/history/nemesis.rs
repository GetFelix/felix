//! The nemesis: which fault to inject next, and how to inject and heal it.
//!
//! [`Fault`] is a fault in effect and knows how to undo itself; [`Nemesis`]
//! chooses the next one. A new kind of fault is a [`FaultKind`] variant, a
//! [`Fault`] variant, and its arms in `inject`/`heal`. Nothing else in the
//! campaign needs to know about it.
//!
//! Faults are injected one at a time and each is healed before the next, so a
//! three-node cluster always has a majority that is only ever one fault away
//! from whole. Overlapping faults would mostly measure unavailability.

use std::fmt;

use anyhow::Result;

use super::rng::Rng;
use crate::Cluster;

/// Chooses the next fault.
///
/// A trait so a campaign can be driven by something other than a random
/// schedule, such as a fixed replay of the faults a failing run injected.
pub trait Nemesis {
    /// The next fault to inject, or `None` to leave the cluster alone for
    /// this round.
    fn next_fault(&mut self, rng: &mut Rng, view: &ClusterView) -> Option<Fault>;
}

/// What a nemesis may target.
#[derive(Debug, Clone, Default)]
pub struct ClusterView {
    pub nodes: Vec<String>,
    /// The brokers leading at least one of the workload's lists. Faulting a
    /// leader is what forces a failover, so a nemesis should favour these.
    pub leaders: Vec<String>,
}

/// A kind of fault a [`RandomNemesis`] may pick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// `SIGKILL`, then a restart on the same data directory when healed.
    Kill,
    /// `SIGSTOP`, then `SIGCONT` when healed. Leases lapse while it sleeps.
    Pause,
    /// Cut one broker off from its peers through the partition file. The
    /// broker keeps heartbeating, so the control plane still believes in it.
    Partition,
}

/// One fault in effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    Kill { node: String },
    Pause { node: String },
    Partition { node: String },
}

impl Fault {
    /// Put the cluster into this fault.
    pub async fn inject(&self, cluster: &mut Cluster) -> Result<()> {
        match self {
            Fault::Kill { node } => cluster.kill_node(node),
            Fault::Pause { node } => pause(cluster, node, true),
            Fault::Partition { node } => cluster.partition_node(node),
        }
    }

    /// Take the cluster out of it again.
    pub async fn heal(&self, cluster: &mut Cluster) -> Result<()> {
        match self {
            Fault::Kill { node } => cluster.restart_node(node).await,
            Fault::Pause { node } => pause(cluster, node, false),
            Fault::Partition { .. } => cluster.heal_partitions(),
        }
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fault::Kill { node } => write!(f, "kill {node}"),
            Fault::Pause { node } => write!(f, "pause {node}"),
            Fault::Partition { node } => write!(f, "partition {node}"),
        }
    }
}

/// Picks a fault kind uniformly from the enabled ones, aimed at a list leader
/// most of the time and at any broker otherwise.
#[derive(Debug, Clone)]
pub struct RandomNemesis {
    kinds: Vec<FaultKind>,
    /// Chance, in percent, of aiming at a leader rather than any broker.
    leader_bias: u64,
}

impl RandomNemesis {
    pub fn new(kinds: Vec<FaultKind>) -> Self {
        Self {
            kinds,
            leader_bias: 75,
        }
    }

    /// Every fault felix-cluster can inject today.
    pub fn process_faults() -> Self {
        Self::new(vec![
            FaultKind::Kill,
            FaultKind::Pause,
            FaultKind::Partition,
        ])
    }
}

impl Nemesis for RandomNemesis {
    fn next_fault(&mut self, rng: &mut Rng, view: &ClusterView) -> Option<Fault> {
        if self.kinds.is_empty() || view.nodes.is_empty() {
            return None;
        }
        let kind = *rng.pick(&self.kinds);
        let targets = if !view.leaders.is_empty() && rng.percent(self.leader_bias) {
            &view.leaders
        } else {
            &view.nodes
        };
        let node = rng.pick(targets).clone();
        Some(match kind {
            FaultKind::Kill => Fault::Kill { node },
            FaultKind::Pause => Fault::Pause { node },
            FaultKind::Partition => Fault::Partition { node },
        })
    }
}

#[cfg(unix)]
fn pause(cluster: &Cluster, node: &str, stop: bool) -> Result<()> {
    if stop {
        cluster.pause_node(node)
    } else {
        cluster.resume_node(node)
    }
}

#[cfg(not(unix))]
fn pause(_cluster: &Cluster, node: &str, _stop: bool) -> Result<()> {
    anyhow::bail!("cannot pause {node}: suspending a broker needs SIGSTOP")
}

//! The nemesis: which fault to inject next, and how to inject and heal it.
//!
//! [`Fault`] is a fault in effect and knows how to undo itself; [`Nemesis`]
//! chooses the next one. A new kind of fault is a [`FaultKind`] variant, a
//! [`Fault`] variant, and its arms in `inject`/`heal`/`Display`. Nothing else
//! in the campaign needs to know about it.
//!
//! This [`Fault`] is the campaign's: a fault with its target already chosen.
//! Most of them are built from the harness's own [`crate::Fault`] values and
//! injected through [`Cluster::inject`]; kill, pause and partition use the
//! older primitives directly.
//!
//! Faults are injected one at a time and each is healed before the next, so a
//! three-node cluster always has a majority that is only ever one fault away
//! from whole. Overlapping faults would mostly measure unavailability.

use std::fmt;
use std::time::Duration;

use anyhow::Result;

use super::rng::Rng;
use crate::Cluster;
use crate::Fault as HarnessFault;
use crate::fault::{ClockFault, Endpoint, FsyncFault};

/// How late a delayed link delivers.
const LINK_DELAY: Duration = Duration::from_millis(250);

/// The broker clock rates a [`FaultKind::ClockRate`] picks from, in permille:
/// half speed, and twenty times.
const CLOCK_RATES_PERMILLE: [u32; 2] = [500, 20_000];

/// How far a [`FaultKind::ControlPlaneClockStep`] moves the control plane's
/// wall clock forward.
const CONTROL_PLANE_STEP: Duration = Duration::from_secs(15);

/// How long each flush waits under [`FaultKind::SlowFsync`].
const FSYNC_DELAY: Duration = Duration::from_millis(200);

/// Chooses the next fault.
///
/// A trait so a campaign can be driven by something other than a random
/// schedule, such as a fixed replay of the faults a failing run injected.
pub trait Nemesis {
    /// The next fault to inject, or `None` to leave the cluster alone for
    /// this round.
    fn next_fault(&mut self, rng: &mut Rng, view: &ClusterView) -> Option<Fault>;

    /// Whether any fault this nemesis may pick acts on a proxied link, so
    /// the cluster must be started with
    /// [`ClusterConfig::proxy_links`](crate::ClusterConfig::proxy_links).
    fn needs_proxy_links(&self) -> bool {
        false
    }

    /// Whether any fault this nemesis may pick acts on fsync. Brokers then
    /// flush on every commit and acknowledge only after it; otherwise a failed
    /// flush happens behind acknowledgements that never waited for it.
    fn needs_fsync_on_commit(&self) -> bool {
        false
    }
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
    /// Everything one broker sends its peers is lost; what they send it
    /// still arrives. Needs proxied links.
    DropOutbound,
    /// Everything one broker sends its peers arrives late. Needs proxied
    /// links.
    DelayOutbound,
    /// The control plane's replies to one broker are lost, so its heartbeats
    /// land but it never hears that they did. Needs proxied links.
    DropControlPlaneReplies,
    /// One broker's lease clock runs at half speed or twenty times. Healing
    /// keeps the drift: boottime never runs back.
    ClockRate,
    /// The control plane's wall clock jumps forward. Healing steps it back.
    ControlPlaneClockStep,
    /// One broker's flushes each wait a while first.
    SlowFsync,
    /// One broker's next flush fails with `EIO`. That log is poisoned for
    /// good, so healing restarts the broker.
    FsyncFailOnce,
}

impl FaultKind {
    /// The family this kind belongs to.
    pub fn family(self) -> FaultFamily {
        match self {
            FaultKind::Kill | FaultKind::Pause | FaultKind::Partition => FaultFamily::Process,
            FaultKind::DropOutbound
            | FaultKind::DelayOutbound
            | FaultKind::DropControlPlaneReplies => FaultFamily::Link,
            FaultKind::ClockRate | FaultKind::ControlPlaneClockStep => FaultFamily::Clock,
            FaultKind::SlowFsync | FaultKind::FsyncFailOnce => FaultFamily::Disk,
        }
    }
}

/// What a fault acts on. A campaign over several families should see at
/// least one fault of each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FaultFamily {
    /// Processes and the partition file: kill, pause, partition.
    Process,
    /// The proxied network.
    Link,
    Clock,
    Disk,
}

/// One fault in effect.
///
/// A clock rate is kept in permille rather than as the harness's `f64`, so
/// this stays `Eq`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    Kill {
        node: String,
    },
    Pause {
        node: String,
    },
    Partition {
        node: String,
    },
    DropOutbound {
        node: String,
        peers: Vec<String>,
    },
    DelayOutbound {
        node: String,
        peers: Vec<String>,
        by: Duration,
    },
    DropControlPlaneReplies {
        node: String,
    },
    ClockRate {
        node: String,
        permille: u32,
    },
    /// Forwards only; healing it is the step back.
    ControlPlaneClockStep {
        by: Duration,
    },
    SlowFsync {
        node: String,
        by: Duration,
    },
    FsyncFailOnce {
        node: String,
    },
}

impl Fault {
    /// The family this fault belongs to.
    pub fn family(&self) -> FaultFamily {
        self.kind().family()
    }

    /// The kind of fault this is.
    pub fn kind(&self) -> FaultKind {
        match self {
            Fault::Kill { .. } => FaultKind::Kill,
            Fault::Pause { .. } => FaultKind::Pause,
            Fault::Partition { .. } => FaultKind::Partition,
            Fault::DropOutbound { .. } => FaultKind::DropOutbound,
            Fault::DelayOutbound { .. } => FaultKind::DelayOutbound,
            Fault::DropControlPlaneReplies { .. } => FaultKind::DropControlPlaneReplies,
            Fault::ClockRate { .. } => FaultKind::ClockRate,
            Fault::ControlPlaneClockStep { .. } => FaultKind::ControlPlaneClockStep,
            Fault::SlowFsync { .. } => FaultKind::SlowFsync,
            Fault::FsyncFailOnce { .. } => FaultKind::FsyncFailOnce,
        }
    }

    /// Put the cluster into this fault.
    pub async fn inject(&self, cluster: &mut Cluster) -> Result<()> {
        match self {
            Fault::Kill { node } => cluster.kill_node(node),
            Fault::Pause { node } => pause(cluster, node, true),
            Fault::Partition { node } => cluster.partition_node(node),
            _ => {
                for fault in self.harness_faults() {
                    cluster.inject(&fault).await?;
                }
                Ok(())
            }
        }
    }

    /// Take the cluster out of it again.
    pub async fn heal(&self, cluster: &mut Cluster) -> Result<()> {
        match self {
            Fault::Kill { node } => cluster.restart_node(node).await,
            Fault::Pause { node } => pause(cluster, node, false),
            Fault::Partition { .. } => cluster.heal_partitions(),
            _ => {
                for fault in self.harness_faults() {
                    cluster.heal(&fault).await?;
                }
                if let Fault::FsyncFailOnce { node } = self {
                    // A failed fsync poisons the log until the process
                    // restarts; healing the disk alone leaves it refusing.
                    cluster.kill_node(node)?;
                    cluster.restart_node(node).await?;
                }
                Ok(())
            }
        }
    }

    /// The harness faults this one is made of. Empty for kill, pause and
    /// partition, which use the cluster's own primitives.
    pub(crate) fn harness_faults(&self) -> Vec<HarnessFault> {
        match self {
            Fault::Kill { .. } | Fault::Pause { .. } | Fault::Partition { .. } => Vec::new(),
            Fault::DropOutbound { node, peers } => peers
                .iter()
                .map(|peer| HarnessFault::Drop {
                    from: Endpoint::node(node),
                    to: Endpoint::node(peer),
                })
                .collect(),
            Fault::DelayOutbound { node, peers, by } => peers
                .iter()
                .map(|peer| HarnessFault::Delay {
                    from: Endpoint::node(node),
                    to: Endpoint::node(peer),
                    by: *by,
                })
                .collect(),
            Fault::DropControlPlaneReplies { node } => vec![HarnessFault::Drop {
                from: Endpoint::ControlPlane,
                to: Endpoint::node(node),
            }],
            Fault::ClockRate { node, permille } => vec![HarnessFault::Clock {
                process: Endpoint::node(node),
                fault: ClockFault::Rate(f64::from(*permille) / 1000.0),
            }],
            Fault::ControlPlaneClockStep { by } => vec![HarnessFault::Clock {
                process: Endpoint::ControlPlane,
                fault: ClockFault::forward(*by),
            }],
            Fault::SlowFsync { node, by } => vec![HarnessFault::Fsync {
                node: node.clone(),
                fault: FsyncFault::Delay(*by),
            }],
            Fault::FsyncFailOnce { node } => vec![HarnessFault::Fsync {
                node: node.clone(),
                fault: FsyncFault::FailOnce,
            }],
        }
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fault::Kill { node } => write!(f, "kill {node}"),
            Fault::Pause { node } => write!(f, "pause {node}"),
            Fault::Partition { node } => write!(f, "partition {node}"),
            Fault::DropOutbound { node, peers } => {
                write!(f, "drop {node} -> {}", peers.join(","))
            }
            Fault::DelayOutbound { node, peers, by } => {
                write!(f, "delay {node} -> {} by {by:?}", peers.join(","))
            }
            Fault::DropControlPlaneReplies { node } => write!(f, "drop control plane -> {node}"),
            Fault::ClockRate { node, permille } => {
                write!(
                    f,
                    "run {node}'s clock at {}x",
                    f64::from(*permille) / 1000.0
                )
            }
            Fault::ControlPlaneClockStep { by } => {
                write!(f, "step the control plane's clock forward {by:?}")
            }
            Fault::SlowFsync { node, by } => write!(f, "slow {node}'s fsync by {by:?}"),
            Fault::FsyncFailOnce { node } => write!(f, "fail {node}'s next fsync"),
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

    /// Kill, pause and partition: the faults that need nothing from the
    /// cluster's configuration. The per-PR campaign's schedule depends on
    /// this list, so a seed keeps replaying the same faults.
    pub fn process_faults() -> Self {
        Self::new(vec![
            FaultKind::Kill,
            FaultKind::Pause,
            FaultKind::Partition,
        ])
    }

    /// Every fault kind: processes, links, clocks and disks. Start the
    /// cluster from [`Campaign::cluster_config`](super::Campaign::cluster_config)
    /// with this nemesis, so links are proxied and brokers flush on commit.
    pub fn all_faults() -> Self {
        Self::new(vec![
            FaultKind::Kill,
            FaultKind::Pause,
            FaultKind::Partition,
            FaultKind::DropOutbound,
            FaultKind::DelayOutbound,
            FaultKind::DropControlPlaneReplies,
            FaultKind::ClockRate,
            FaultKind::ControlPlaneClockStep,
            FaultKind::SlowFsync,
            FaultKind::FsyncFailOnce,
        ])
    }
}

impl Nemesis for RandomNemesis {
    fn next_fault(&mut self, rng: &mut Rng, view: &ClusterView) -> Option<Fault> {
        if self.kinds.is_empty() || view.nodes.is_empty() {
            return None;
        }
        // Kind, then target, then anything kind-specific: the draws for the
        // process faults must stay as they were, or old seeds replay
        // different schedules.
        let kind = *rng.pick(&self.kinds);
        let targets = if !view.leaders.is_empty() && rng.percent(self.leader_bias) {
            &view.leaders
        } else {
            &view.nodes
        };
        let node = rng.pick(targets).clone();
        let peers: Vec<String> = view
            .nodes
            .iter()
            .filter(|peer| **peer != node)
            .cloned()
            .collect();
        Some(match kind {
            FaultKind::Kill => Fault::Kill { node },
            FaultKind::Pause => Fault::Pause { node },
            FaultKind::Partition => Fault::Partition { node },
            FaultKind::DropOutbound => Fault::DropOutbound { node, peers },
            FaultKind::DelayOutbound => Fault::DelayOutbound {
                node,
                peers,
                by: LINK_DELAY,
            },
            FaultKind::DropControlPlaneReplies => Fault::DropControlPlaneReplies { node },
            FaultKind::ClockRate => Fault::ClockRate {
                node,
                permille: *rng.pick(&CLOCK_RATES_PERMILLE),
            },
            FaultKind::ControlPlaneClockStep => Fault::ControlPlaneClockStep {
                by: CONTROL_PLANE_STEP,
            },
            FaultKind::SlowFsync => Fault::SlowFsync {
                node,
                by: FSYNC_DELAY,
            },
            FaultKind::FsyncFailOnce => Fault::FsyncFailOnce { node },
        })
    }

    fn needs_proxy_links(&self) -> bool {
        self.kinds
            .iter()
            .any(|kind| kind.family() == FaultFamily::Link)
    }

    fn needs_fsync_on_commit(&self) -> bool {
        self.kinds
            .iter()
            .any(|kind| kind.family() == FaultFamily::Disk)
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

#[cfg(test)]
mod tests;

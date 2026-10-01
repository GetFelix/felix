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
//! older primitives directly. Moves and drains go through the control
//! plane's operator API, as an operator would.
//!
//! The single faults are injected one at a time and each is healed before the
//! next. The compound kinds are the exception on purpose: two
//! faults at once, a fault in the middle of a move, one broker restarted over
//! and over, the power cut to every broker, or the control plane crashing
//! with work in flight. They are what [`RandomNemesis::adversarial`] picks
//! from.

use std::fmt;
use std::time::Duration;

use anyhow::{Context, Result};

mod compound;

use super::rng::Rng;
use crate::Fault as HarnessFault;
use crate::fault::{ClockFault, Endpoint, FsyncFault, WriteFault};
use crate::{Cluster, wait};

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

/// How long the moves a [`FaultKind::MoveShard`] or [`FaultKind::Drain`]
/// started may take to finish once it is healed. A move still running after
/// this is a stall, and fails the campaign.
const MOVE_SETTLE: Duration = Duration::from_secs(60);

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

    /// Whether any fault this nemesis may pick cuts the power, so brokers
    /// must run under the power-loss model
    /// ([`ClusterConfig::power_loss`](crate::ClusterConfig::power_loss)).
    fn needs_power_loss(&self) -> bool {
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
    /// The workload's shards and who leads each, for the faults that move one.
    pub shards: Vec<ShardView>,
}

/// One of the workload's shards, as the control plane assigns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardView {
    /// `stream` or `cache`.
    pub kind: &'static str,
    pub name: String,
    pub shard: u32,
    pub leader: String,
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
    /// An operator moves one of the workload's shards to another of its
    /// replicas, online. Healing waits for the move to finish.
    MoveShard,
    /// An operator drains one broker, so placement moves its leaderships
    /// off. Healing puts it back and waits for the moves in flight. With a
    /// broker to spare, its follower copies are replaced as well, which
    /// changes the shards' replica sets.
    Drain,
    /// One broker's next segment write lands half its batch and fails.
    /// Healing restarts it, so recovery finds the torn record.
    TornWrite,
    /// Cut one leader off from its peers and from the control plane, so the
    /// control plane stops hearing from it and fails its shards over while
    /// the clients keep writing. Needs proxied links.
    Isolate,
    /// Kill two brokers at once. On three brokers that is a majority, and
    /// every shard stops until they are back.
    KillTwo,
    /// Partition one broker and delay everything another one sends. Needs
    /// proxied links.
    PartitionAndDelay,
    /// Two faults at once on different brokers: any two of kill, pause,
    /// partition, a dropped or delayed link, a slow fsync, a move and a
    /// drain.
    Overlap,
    /// Start a move of one of the workload's shards, then kill its source or
    /// its destination before the move can finish.
    InterruptedMove,
    /// Kill one broker, then restart it and kill it again before it has
    /// caught up, a few times, before letting it stay up.
    RestartLoop,
    /// Every broker loses power at once: what no flush covered is lost, torn
    /// or zeroed, and they stay down for the hold. Healing starts them all.
    /// Linux and debug brokers only.
    PowerLoss,
    /// Start a move, a drain or the kill of a leader, then crash the control
    /// plane before it can finish. Healing brings the control plane back over
    /// the same state and heals what was in flight.
    ControlPlaneCrash,
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
            FaultKind::SlowFsync | FaultKind::FsyncFailOnce | FaultKind::TornWrite => {
                FaultFamily::Disk
            }
            FaultKind::MoveShard | FaultKind::Drain => FaultFamily::Assignment,
            FaultKind::Isolate
            | FaultKind::KillTwo
            | FaultKind::PartitionAndDelay
            | FaultKind::Overlap
            | FaultKind::InterruptedMove
            | FaultKind::RestartLoop
            | FaultKind::PowerLoss
            | FaultKind::ControlPlaneCrash => FaultFamily::Compound,
        }
    }

    /// Whether a fault of this kind may act on a proxied link.
    pub fn needs_proxy_links(self) -> bool {
        self.family() == FaultFamily::Link
            || matches!(
                self,
                FaultKind::Isolate | FaultKind::PartitionAndDelay | FaultKind::Overlap
            )
    }

    /// Whether a fault of this kind may act on fsync. A power loss does: it
    /// keeps only what was flushed, so only acknowledging after the flush
    /// makes losing an acknowledged write a bug.
    pub fn needs_fsync_on_commit(self) -> bool {
        matches!(
            self,
            FaultKind::SlowFsync
                | FaultKind::FsyncFailOnce
                | FaultKind::Overlap
                | FaultKind::PowerLoss
        )
    }

    /// Whether a fault of this kind cuts the power.
    pub fn needs_power_loss(self) -> bool {
        self == FaultKind::PowerLoss
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
    /// Who leads what: operator moves and drains.
    Assignment,
    /// Several faults at once, a fault in the middle of a move, one broker
    /// restarted over and over, a power loss, or a control plane crash.
    Compound,
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
    MoveShard {
        kind: &'static str,
        name: String,
        shard: u32,
        from: String,
        to: String,
    },
    Drain {
        node: String,
    },
    TornWrite {
        node: String,
    },
    Isolate {
        node: String,
    },
    /// Faults in effect together. `kind` is the kind that chose them.
    Several {
        kind: FaultKind,
        faults: Vec<Fault>,
    },
    InterruptedMove {
        kind: &'static str,
        name: String,
        shard: u32,
        from: String,
        to: String,
        /// `from` or `to`: the broker killed once the move has started.
        victim: String,
    },
    RestartLoop {
        node: String,
        /// How many times it is started again before it is left up.
        restarts: u32,
    },
    PowerLoss {
        /// Decides which unflushed writes each broker keeps.
        seed: u64,
    },
    ControlPlaneCrash {
        /// What is in flight when the control plane goes down.
        during: Box<Fault>,
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
            Fault::MoveShard { .. } => FaultKind::MoveShard,
            Fault::Drain { .. } => FaultKind::Drain,
            Fault::TornWrite { .. } => FaultKind::TornWrite,
            Fault::Isolate { .. } => FaultKind::Isolate,
            Fault::Several { kind, .. } => *kind,
            Fault::InterruptedMove { .. } => FaultKind::InterruptedMove,
            Fault::RestartLoop { .. } => FaultKind::RestartLoop,
            Fault::PowerLoss { .. } => FaultKind::PowerLoss,
            Fault::ControlPlaneCrash { .. } => FaultKind::ControlPlaneCrash,
        }
    }

    /// The brokers this fault is aimed at, so a second fault in effect at
    /// the same time can be aimed elsewhere.
    pub fn targets(&self) -> Vec<String> {
        match self {
            Fault::Kill { node }
            | Fault::Pause { node }
            | Fault::Partition { node }
            | Fault::DropOutbound { node, .. }
            | Fault::DelayOutbound { node, .. }
            | Fault::DropControlPlaneReplies { node }
            | Fault::ClockRate { node, .. }
            | Fault::SlowFsync { node, .. }
            | Fault::FsyncFailOnce { node }
            | Fault::Drain { node }
            | Fault::TornWrite { node }
            | Fault::Isolate { node }
            | Fault::RestartLoop { node, .. } => vec![node.clone()],
            // Every broker, so nothing else can be aimed away from it.
            Fault::ControlPlaneClockStep { .. } | Fault::PowerLoss { .. } => Vec::new(),
            Fault::ControlPlaneCrash { during } => during.targets(),
            Fault::MoveShard { from, to, .. } | Fault::InterruptedMove { from, to, .. } => {
                vec![from.clone(), to.clone()]
            }
            Fault::Several { faults, .. } => faults.iter().flat_map(Fault::targets).collect(),
        }
    }

    /// Put the cluster into this fault.
    pub async fn inject(&self, cluster: &mut Cluster) -> Result<()> {
        match self {
            Fault::Kill { node } => cluster.kill_node(node),
            Fault::Pause { node } => pause(cluster, node, true),
            Fault::Partition { node } => cluster.partition_node(node),
            Fault::MoveShard {
                kind,
                name,
                shard,
                to,
                ..
            } => cluster
                .start_move_of(kind, name, *shard, to)
                .await
                .map(drop),
            Fault::Drain { node } => cluster.drain_node(node).await,
            Fault::Isolate { node } => compound::isolate(cluster, node).await,
            Fault::Several { faults, .. } => compound::inject_all(cluster, faults).await,
            Fault::InterruptedMove {
                kind,
                name,
                shard,
                to,
                victim,
                ..
            } => compound::interrupt_move(cluster, kind, name, *shard, to, victim).await,
            Fault::RestartLoop { node, .. } => cluster.kill_node(node),
            Fault::PowerLoss { seed } => cluster.power_off(*seed).await,
            Fault::ControlPlaneCrash { during } => {
                compound::crash_control_plane(cluster, during).await
            }
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
            Fault::Kill { node } => restart_if_down(cluster, node).await,
            Fault::Pause { node } => pause(cluster, node, false),
            Fault::Partition { .. } => cluster.heal_partitions(),
            Fault::MoveShard { .. } => settle_moves(cluster).await,
            Fault::Drain { node } => {
                cluster.undrain_node(node).await?;
                settle_moves(cluster).await
            }
            Fault::Isolate { node } => compound::rejoin(cluster, node).await,
            Fault::Several { faults, .. } => compound::heal_all(cluster, faults).await,
            Fault::InterruptedMove { victim, .. } => {
                restart_if_down(cluster, victim).await?;
                settle_moves(cluster).await
            }
            Fault::RestartLoop { node, restarts } => {
                compound::restart_loop(cluster, node, *restarts).await
            }
            Fault::PowerLoss { .. } => cluster.restart_stopped_nodes().await,
            Fault::ControlPlaneCrash { during } => {
                compound::recover_control_plane(cluster, during).await
            }
            _ => {
                for fault in self.harness_faults() {
                    cluster.heal(&fault).await?;
                }
                if let Fault::FsyncFailOnce { node } | Fault::TornWrite { node } = self {
                    // A failed fsync poisons the log until the process
                    // restarts, and a torn write is only repaired by the
                    // recovery a restart runs.
                    cluster.kill_node(node)?;
                    cluster.restart_node(node).await?;
                }
                Ok(())
            }
        }
    }

    /// The harness faults this one is made of. Empty for the ones that use
    /// the cluster's own primitives or its operator API.
    pub(crate) fn harness_faults(&self) -> Vec<HarnessFault> {
        match self {
            Fault::Kill { .. }
            | Fault::Pause { .. }
            | Fault::Partition { .. }
            | Fault::MoveShard { .. }
            | Fault::Drain { .. }
            | Fault::Isolate { .. }
            | Fault::InterruptedMove { .. }
            | Fault::RestartLoop { .. }
            | Fault::PowerLoss { .. } => Vec::new(),
            Fault::ControlPlaneCrash { during } => during.harness_faults(),
            Fault::Several { faults, .. } => {
                faults.iter().flat_map(Fault::harness_faults).collect()
            }
            Fault::TornWrite { node } => vec![HarnessFault::Write {
                node: node.clone(),
                fault: WriteFault::IoOnce,
            }],
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
            Fault::MoveShard {
                kind,
                name,
                shard,
                from,
                to,
            } => write!(f, "move {kind} {name}/{shard} from {from} to {to}"),
            Fault::Drain { node } => write!(f, "drain {node}"),
            Fault::TornWrite { node } => write!(f, "tear {node}'s next segment write"),
            Fault::Isolate { node } => {
                write!(f, "isolate {node} from its peers and the control plane")
            }
            Fault::Several { faults, .. } => {
                let each: Vec<String> = faults.iter().map(Fault::to_string).collect();
                f.write_str(&each.join(" and "))
            }
            Fault::InterruptedMove {
                kind,
                name,
                shard,
                from,
                to,
                victim,
            } => write!(
                f,
                "move {kind} {name}/{shard} from {from} to {to} and kill {victim} mid-move"
            ),
            Fault::RestartLoop { node, restarts } => {
                write!(f, "kill {node} and restart it {restarts} times in a row")
            }
            Fault::PowerLoss { seed } => {
                write!(f, "cut the power to every broker (image seed {seed})")
            }
            Fault::ControlPlaneCrash { during } => {
                write!(f, "crash the control plane during: {during}")
            }
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

    /// The kinds this nemesis picks from.
    pub fn kinds(&self) -> &[FaultKind] {
        &self.kinds
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

    /// Every fault kind: processes, links, clocks, disks and assignments.
    /// Start the
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
            FaultKind::MoveShard,
            FaultKind::Drain,
        ])
    }

    /// The compound faults: failures that overlap, a partition that forces a
    /// failover, moves cut short, replica sets changed by drains, torn
    /// writes, restart loops, a whole-cluster power loss and a control plane
    /// crash with work in flight. Run it on four brokers
    /// ([`Campaign::adversarial`](super::Campaign::adversarial)), so a drain
    /// has somewhere to move a follower copy to.
    pub fn adversarial() -> Self {
        Self::new(vec![
            FaultKind::Isolate,
            FaultKind::KillTwo,
            FaultKind::PartitionAndDelay,
            FaultKind::Overlap,
            FaultKind::InterruptedMove,
            FaultKind::RestartLoop,
            FaultKind::TornWrite,
            FaultKind::Drain,
            FaultKind::PowerLoss,
            FaultKind::ControlPlaneCrash,
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
            FaultKind::MoveShard => {
                // A shard the target leads if it leads any, so the move is
                // off the broker the other faults favour.
                let led: Vec<&ShardView> =
                    view.shards.iter().filter(|s| s.leader == node).collect();
                let shard = match led.as_slice() {
                    [] if view.shards.is_empty() => return None,
                    [] => rng.pick(&view.shards),
                    led => *rng.pick(led),
                };
                let others: Vec<&String> = view
                    .nodes
                    .iter()
                    .filter(|peer| **peer != shard.leader)
                    .collect();
                if others.is_empty() {
                    return None;
                }
                Fault::MoveShard {
                    kind: shard.kind,
                    name: shard.name.clone(),
                    shard: shard.shard,
                    from: shard.leader.clone(),
                    to: (*rng.pick(&others)).clone(),
                }
            }
            FaultKind::Drain => Fault::Drain { node },
            FaultKind::TornWrite => Fault::TornWrite { node },
            FaultKind::Isolate => compound::pick_isolate(rng, view, node),
            FaultKind::KillTwo => compound::pick_kill_two(rng, node, &peers),
            FaultKind::PartitionAndDelay => {
                compound::pick_partition_and_delay(rng, view, node, &peers)
            }
            FaultKind::Overlap => compound::pick_overlap(rng, view)?,
            FaultKind::InterruptedMove => compound::pick_interrupted_move(rng, view)?,
            FaultKind::RestartLoop => Fault::RestartLoop {
                node,
                restarts: compound::RESTARTS,
            },
            FaultKind::PowerLoss => Fault::PowerLoss {
                seed: rng.next_u64(),
            },
            FaultKind::ControlPlaneCrash => compound::pick_control_plane_crash(rng, view, node)?,
        })
    }

    fn needs_proxy_links(&self) -> bool {
        self.kinds.iter().any(|kind| kind.needs_proxy_links())
    }

    fn needs_fsync_on_commit(&self) -> bool {
        self.kinds.iter().any(|kind| kind.needs_fsync_on_commit())
    }

    fn needs_power_loss(&self) -> bool {
        self.kinds.iter().any(|kind| kind.needs_power_loss())
    }
}

/// Wait until no shard is moving: every move the fault started, and any
/// placement started on its own meanwhile, has cut over or been dropped.
async fn settle_moves(cluster: &Cluster) -> Result<()> {
    let last = std::sync::Mutex::new(Vec::new());
    wait::until(MOVE_SETTLE, "the shard moves to finish", || async {
        match cluster.moving_shards().await {
            Ok(moving) => {
                let done = moving.is_empty();
                *last.lock().expect("moving lock") = moving;
                done
            }
            Err(_) => false,
        }
    })
    .await
    .with_context(|| {
        format!(
            "still moving: {}",
            last.lock().expect("moving lock").join(", ")
        )
    })
}

/// Start `node` again unless it is already running: a compound fault that
/// failed half way may never have stopped it.
async fn restart_if_down(cluster: &mut Cluster, node: &str) -> Result<()> {
    if cluster
        .node(node)
        .is_some_and(crate::BrokerNode::is_running)
    {
        return Ok(());
    }
    cluster.restart_node(node).await
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

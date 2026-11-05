//! The faults a run injects, drawn from the seed.
//!
//! [`Plan::generate`] lays the whole schedule out before the run starts, so
//! the seed alone fixes which fault lands when. [`Nemesis::run`] then plays it
//! against a [`SimCluster`]. Faults may overlap. At the end of the fault
//! window [`Nemesis::heal`] undoes everything: links, control plane, and any
//! broker still crashed.
//!
//! | Fault | What it does |
//! |---|---|
//! | `Crash` | Stops a broker as a power cut does not: a process crash, data kept. Restarts it after a while. |
//! | `Freeze` | Cuts a broker off from every peer and from the control plane, without stopping it. |
//! | `Isolate` | Cuts a broker off from its peers only; its heartbeats still reach the control plane. |
//! | `Split` | Cuts a minority off from the rest. The control plane sits with the majority or with everyone. |
//! | `OneWay` | Drops one direction of one link. |
//! | `ControlPlane` | Slows, pauses or takes down the control plane. |
//! | `PausedThenCrash` | Pauses the control plane, then crashes a broker that leads shards while it is paused. |
//!
//! # Why `Freeze` is approximate
//!
//! A paused process (SIGSTOP, a long garbage collection, a VM freeze) runs
//! nothing: its timers do not fire and it learns nothing until it resumes,
//! when it acts on state that went stale while it slept. The simulator cannot
//! stop one broker's tasks on a runtime they share. A frozen broker here keeps
//! running, so it notices its lease running out as soon as it does. The
//! network and heartbeat half of a pause is real; the zombie that wakes up
//! believing it still leads is not reproduced.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use felix_cluster::history::rng::Rng;
use parking_lot::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::client::Recorder;
use crate::cluster::SimCluster;
use crate::controlplane::CpFault;
use crate::net::Link;

/// One fault, with how long it lasts.
#[derive(Debug, Clone, PartialEq)]
pub enum Fault {
    Crash {
        node: usize,
        down_for: Duration,
    },
    Freeze {
        node: usize,
        lasts: Duration,
    },
    Isolate {
        node: usize,
        lasts: Duration,
    },
    Split {
        minority: Vec<usize>,
        /// Whether the minority can still reach the control plane.
        minority_reaches_controlplane: bool,
        lasts: Duration,
    },
    OneWay {
        from: usize,
        to: usize,
        lasts: Duration,
    },
    ControlPlane {
        fault: CpFault,
        lasts: Duration,
    },
    /// The control plane pauses, then `after` later a broker that leads
    /// shards crashes. The control plane stays paused `cp_lasts` in all.
    PausedThenCrash {
        cp_lasts: Duration,
        after: Duration,
        down_for: Duration,
    },
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fault::Crash { node, down_for } => write!(f, "crash b{} for {down_for:?}", node + 1),
            Fault::Freeze { node, lasts } => write!(f, "freeze b{} for {lasts:?}", node + 1),
            Fault::Isolate { node, lasts } => write!(f, "isolate b{} for {lasts:?}", node + 1),
            Fault::Split {
                minority,
                minority_reaches_controlplane,
                lasts,
            } => {
                let names: Vec<_> = minority.iter().map(|n| format!("b{}", n + 1)).collect();
                write!(
                    f,
                    "split {{{}}} from the rest for {lasts:?} (minority {} the control plane)",
                    names.join(","),
                    if *minority_reaches_controlplane {
                        "reaches"
                    } else {
                        "cut from"
                    }
                )
            }
            Fault::OneWay { from, to, lasts } => {
                write!(f, "drop b{} -> b{} for {lasts:?}", from + 1, to + 1)
            }
            Fault::ControlPlane { fault, lasts } => {
                write!(f, "control plane {fault:?} for {lasts:?}")
            }
            Fault::PausedThenCrash {
                cp_lasts,
                after,
                down_for,
            } => write!(
                f,
                "control plane paused for {cp_lasts:?}, a shard leader crashes {after:?} in for {down_for:?}"
            ),
        }
    }
}

/// A fault and when it starts, counted from the start of the run.
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    pub at: Duration,
    pub fault: Fault,
}

/// The whole schedule of one run.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub steps: Vec<Step>,
}

impl Plan {
    /// A schedule for `brokers` brokers that starts nothing after `window`.
    pub fn generate(seed: u64, brokers: usize, window: Duration) -> Self {
        let mut rng = Rng::new(seed ^ 0x4e45_4d45_5349_5300);
        let secs = |rng: &mut Rng, low: u64, high: u64| {
            rng.between(Duration::from_secs(low), Duration::from_secs(high))
        };
        let mut steps = Vec::new();
        let mut at = secs(&mut rng, 3, 8);
        while at < window {
            let node = rng.below(brokers as u64) as usize;
            let fault = match rng.below(100) {
                0..=24 => Fault::Crash {
                    node,
                    down_for: secs(&mut rng, 3, 20),
                },
                25..=34 => Fault::Freeze {
                    node,
                    lasts: secs(&mut rng, 5, 25),
                },
                35..=49 => Fault::Isolate {
                    node,
                    lasts: secs(&mut rng, 5, 20),
                },
                50..=59 => {
                    let size = 1 + rng.below(((brokers as u64 - 1) / 2).max(1)) as usize;
                    let mut order: Vec<usize> = (0..brokers).collect();
                    for i in (1..order.len()).rev() {
                        order.swap(i, rng.below(i as u64 + 1) as usize);
                    }
                    order.truncate(size);
                    order.sort_unstable();
                    Fault::Split {
                        minority: order,
                        minority_reaches_controlplane: rng.percent(30),
                        lasts: secs(&mut rng, 5, 25),
                    }
                }
                60..=69 => {
                    let to = (node + 1 + rng.below(brokers as u64 - 1) as usize) % brokers;
                    Fault::OneWay {
                        from: node,
                        to,
                        lasts: secs(&mut rng, 5, 20),
                    }
                }
                70..=77 => Fault::ControlPlane {
                    fault: CpFault::Slow(
                        rng.between(Duration::from_millis(500), Duration::from_secs(4)),
                    ),
                    lasts: secs(&mut rng, 5, 20),
                },
                78..=85 => Fault::ControlPlane {
                    fault: CpFault::Paused,
                    lasts: secs(&mut rng, 5, 30),
                },
                86..=91 => Fault::ControlPlane {
                    fault: CpFault::Down,
                    lasts: secs(&mut rng, 5, 30),
                },
                _ => {
                    let cp_lasts = secs(&mut rng, 10, 35);
                    Fault::PausedThenCrash {
                        cp_lasts,
                        after: secs(&mut rng, 1, 3),
                        down_for: secs(&mut rng, 5, 20),
                    }
                }
            };
            steps.push(Step { at, fault });
            at += secs(&mut rng, 3, 10);
        }
        Self { steps }
    }
}

/// How many faults hold each link or switch, so one ending does not undo
/// another that overlaps it.
#[derive(Default)]
struct Holds {
    /// Directed `(from, to)` links that drop everything.
    links: HashMap<(usize, usize), u32>,
    /// Brokers cut off from the control plane.
    controlplane: HashMap<usize, u32>,
    /// Which control plane fault is current, so only its own end clears it.
    cp_epoch: u64,
}

/// Plays a [`Plan`] against a cluster.
pub struct Nemesis {
    cluster: Arc<SimCluster>,
    recorder: Arc<Recorder>,
    holds: Mutex<Holds>,
    /// Cancelled by [`Nemesis::heal`]: pending ends of faults stand down.
    stopped: CancellationToken,
}

impl Nemesis {
    pub fn new(cluster: Arc<SimCluster>, recorder: Arc<Recorder>) -> Arc<Self> {
        Arc::new(Self {
            cluster,
            recorder,
            holds: Mutex::new(Holds::default()),
            stopped: CancellationToken::new(),
        })
    }

    /// Start each step of `plan` at its time, counted from now. Returns once
    /// the last has started; the faults themselves end on their own tasks.
    pub async fn run(self: &Arc<Self>, plan: &Plan) {
        let begun = Instant::now();
        for step in &plan.steps {
            tokio::select! {
                () = self.stopped.cancelled() => return,
                () = tokio::time::sleep_until(begun + step.at) => {}
            }
            let this = Arc::clone(self);
            let fault = step.fault.clone();
            tokio::spawn(async move { this.inject(fault).await });
        }
    }

    /// Undo every fault: links healed, control plane healthy, every broker
    /// running again.
    pub async fn heal(self: &Arc<Self>) {
        self.stopped.cancel();
        {
            let mut holds = self.holds.lock();
            holds.links.clear();
            holds.controlplane.clear();
        }
        self.sync_links();
        self.cluster.cp().set_fault(CpFault::Healthy);
        for index in 0..self.cluster.broker_count() {
            self.cluster.restart(index);
        }
        self.recorder
            .fault("healed: links, control plane and brokers restored");
    }

    async fn inject(self: Arc<Self>, fault: Fault) {
        if self.stopped.is_cancelled() {
            return;
        }
        match &fault {
            Fault::Crash { node, down_for } => {
                if !self.may_crash(*node) {
                    return;
                }
                self.recorder.fault(format!("{fault}"));
                self.crash_for(*node, *down_for).await;
            }
            Fault::Freeze { node, lasts } => {
                self.recorder.fault(format!("{fault}"));
                let links = self.links_around(*node, true);
                self.hold(&links, &[*node]);
                self.lapse(*lasts).await;
                self.release(&links, &[*node]);
                self.recorder.fault(format!("end: {fault}"));
            }
            Fault::Isolate { node, lasts } => {
                self.recorder.fault(format!("{fault}"));
                let links = self.links_around(*node, true);
                self.hold(&links, &[]);
                self.lapse(*lasts).await;
                self.release(&links, &[]);
                self.recorder.fault(format!("end: {fault}"));
            }
            Fault::Split {
                minority,
                minority_reaches_controlplane,
                lasts,
            } => {
                self.recorder.fault(format!("{fault}"));
                let n = self.cluster.broker_count();
                let majority: Vec<usize> = (0..n).filter(|i| !minority.contains(i)).collect();
                let mut links = Vec::new();
                for &a in minority {
                    for &b in &majority {
                        links.push((a, b));
                        links.push((b, a));
                    }
                }
                let cut: Vec<usize> = if *minority_reaches_controlplane {
                    Vec::new()
                } else {
                    minority.clone()
                };
                self.hold(&links, &cut);
                self.lapse(*lasts).await;
                self.release(&links, &cut);
                self.recorder.fault(format!("end: {fault}"));
            }
            Fault::OneWay { from, to, lasts } => {
                self.recorder.fault(format!("{fault}"));
                let links = [(*from, *to)];
                self.hold(&links, &[]);
                self.lapse(*lasts).await;
                self.release(&links, &[]);
                self.recorder.fault(format!("end: {fault}"));
            }
            Fault::ControlPlane { fault: cp, lasts } => {
                self.recorder.fault(format!("{fault}"));
                let epoch = self.set_controlplane(*cp);
                self.lapse(*lasts).await;
                self.clear_controlplane(epoch);
                self.recorder.fault(format!("end: {fault}"));
            }
            Fault::PausedThenCrash {
                cp_lasts,
                after,
                down_for,
            } => {
                self.recorder.fault(format!("{fault}"));
                let epoch = self.set_controlplane(CpFault::Paused);
                let ends = Instant::now() + *cp_lasts;
                self.lapse(*after).await;
                if let Some(node) = self.busiest_leader().await
                    && self.may_crash(node)
                {
                    self.recorder.fault(format!(
                        "crash b{} (a shard leader) while the control plane is paused",
                        node + 1
                    ));
                    let this = Arc::clone(&self);
                    let down_for = *down_for;
                    tokio::spawn(async move { this.crash_for(node, down_for).await });
                }
                tokio::select! {
                    () = self.stopped.cancelled() => return,
                    () = tokio::time::sleep_until(ends) => {}
                }
                self.clear_controlplane(epoch);
                self.recorder.fault(format!("end: {fault}"));
            }
        }
    }

    /// Never take down more than a minority, so some runs keep a quorum.
    fn may_crash(&self, node: usize) -> bool {
        let n = self.cluster.broker_count();
        let down = (0..n).filter(|&i| !self.cluster.is_running(i)).count();
        self.cluster.is_running(node) && down < n / 2
    }

    async fn crash_for(&self, node: usize, down_for: Duration) {
        self.cluster.crash(node).await;
        self.lapse(down_for).await;
        if !self.stopped.is_cancelled() {
            self.recorder.fault(format!("restart b{}", node + 1));
            self.cluster.restart(node);
        }
    }

    /// The running broker serving the most of the first shards.
    async fn busiest_leader(&self) -> Option<usize> {
        let mut led = vec![0usize; self.cluster.broker_count()];
        for key in self.cluster.shard_keys().take(24) {
            for index in self.cluster.servers_of(&key).await {
                led[index] += 1;
            }
        }
        led.iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .max_by_key(|(_, count)| **count)
            .map(|(index, _)| index)
    }

    /// Wait out a fault, or return early if the run is healing.
    async fn lapse(&self, lasts: Duration) {
        tokio::select! {
            () = self.stopped.cancelled() => {}
            () = tokio::time::sleep(lasts) => {}
        }
    }

    /// Every directed link into and out of `node`.
    fn links_around(&self, node: usize, both: bool) -> Vec<(usize, usize)> {
        let n = self.cluster.broker_count();
        let mut links = Vec::new();
        for other in (0..n).filter(|&i| i != node) {
            links.push((node, other));
            if both {
                links.push((other, node));
            }
        }
        links
    }

    fn hold(&self, links: &[(usize, usize)], cut: &[usize]) {
        {
            let mut holds = self.holds.lock();
            for link in links {
                *holds.links.entry(*link).or_default() += 1;
            }
            for node in cut {
                *holds.controlplane.entry(*node).or_default() += 1;
            }
        }
        self.sync_links();
    }

    fn release(&self, links: &[(usize, usize)], cut: &[usize]) {
        if self.stopped.is_cancelled() {
            return;
        }
        {
            let mut holds = self.holds.lock();
            for link in links {
                if let Some(count) = holds.links.get_mut(link) {
                    *count = count.saturating_sub(1);
                }
            }
            for node in cut {
                if let Some(count) = holds.controlplane.get_mut(node) {
                    *count = count.saturating_sub(1);
                }
            }
        }
        self.sync_links();
    }

    /// Make the network and the control plane switches match the holds.
    fn sync_links(&self) {
        let holds = self.holds.lock();
        let net = self.cluster.net();
        self.cluster.reset_links();
        for (&(from, to), &count) in &holds.links {
            if count > 0 {
                net.set_link(
                    self.cluster.node_id(from),
                    self.cluster.node_id(to),
                    Link {
                        partitioned: true,
                        ..net.link(self.cluster.node_id(from), self.cluster.node_id(to))
                    },
                );
            }
        }
        for index in 0..self.cluster.broker_count() {
            let cut = holds.controlplane.get(&index).copied().unwrap_or(0) > 0;
            self.cluster.cut_controlplane(index, cut);
        }
    }

    fn set_controlplane(&self, fault: CpFault) -> u64 {
        let mut holds = self.holds.lock();
        holds.cp_epoch += 1;
        self.cluster.cp().set_fault(fault);
        holds.cp_epoch
    }

    fn clear_controlplane(&self, epoch: u64) {
        if self.stopped.is_cancelled() {
            return;
        }
        let holds = self.holds.lock();
        if holds.cp_epoch == epoch {
            self.cluster.cp().set_fault(CpFault::Healthy);
        }
    }
}

#[cfg(test)]
mod tests;

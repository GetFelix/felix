//! Faults that change devices: a cut of several brokers at once, and wipes.
//!
//! Each one runs under the nemesis's disk lock. It reads every broker's
//! stream shards before and after, so [`judge`](crate::cluster::disks::judge)
//! can say which acknowledged records no disk holds, and whether this fault
//! may lose them. A loss it may cause is not held against the cluster: those
//! records become unknown in the history, and the run reports the loss.
//! Anything else is a finding.

use std::sync::Arc;
use std::time::Duration;

use felix_cluster::history::rng::Rng;
use felix_storage::log::FsyncMode;

use super::{Fault, Nemesis, Plan, Step};
use crate::client::Recorder;
use crate::cluster::disks::{Observed, acked_values, judge, thin};
use crate::cluster::{DiskEvent, WipeScope};

impl Plan {
    /// The same schedule with the disk faults the config asks for. They draw
    /// from streams of their own, so every other fault of a seed stays.
    pub fn with_disk_faults(
        mut self,
        seed: u64,
        brokers: usize,
        window: Duration,
        majority_cut: bool,
        wipes: bool,
    ) -> Self {
        if majority_cut {
            self.steps
                .extend(majority_cut_steps(seed, brokers, window).steps);
        }
        if wipes {
            self.steps.extend(wipe_steps(seed, brokers, window).steps);
        }
        self.steps.sort_by_key(|step| step.at);
        self
    }
}

fn secs(rng: &mut Rng, low: u64, high: u64) -> Duration {
    rng.between(Duration::from_secs(low), Duration::from_secs(high))
}

/// `count` brokers out of `brokers`, in order.
fn some_of(rng: &mut Rng, brokers: usize, count: usize) -> Vec<usize> {
    let mut order: Vec<usize> = (0..brokers).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, rng.below(i as u64 + 1) as usize);
    }
    order.truncate(count);
    order.sort_unstable();
    order
}

/// One to two cuts of several brokers a run: two brokers, or a majority of
/// the cluster.
fn majority_cut_steps(seed: u64, brokers: usize, window: Duration) -> Plan {
    let mut rng = Rng::new(seed ^ 0x4d41_4a43_5554_0001);
    let mut steps = Vec::new();
    let mut at = secs(&mut rng, 10, 25);
    for _ in 0..1 + rng.below(2) {
        if at >= window {
            break;
        }
        let count = if rng.percent(50) { 2 } else { brokers / 2 + 1 };
        steps.push(Step {
            at,
            fault: Fault::MajorityCut {
                nodes: some_of(&mut rng, brokers, count),
                down_for: secs(&mut rng, 5, 20),
            },
        });
        at += secs(&mut rng, 25, 40);
    }
    Plan { steps }
}

/// A wipe every 20 to 40 seconds: one shard directory or a whole data
/// directory, on one broker or on two (a majority of a group of three).
fn wipe_steps(seed: u64, brokers: usize, window: Duration) -> Plan {
    let mut rng = Rng::new(seed ^ 0x5749_5045_0000_0001);
    let mut steps = Vec::new();
    let mut at = secs(&mut rng, 10, 30);
    while at < window {
        let scope = if rng.percent(55) {
            WipeScope::Shard {
                pick: rng.next_u64(),
            }
        } else {
            WipeScope::Root
        };
        let count = if rng.percent(70) { 1 } else { 2 };
        steps.push(Step {
            at,
            fault: Fault::Wipe {
                nodes: some_of(&mut rng, brokers, count),
                scope,
                down_for: secs(&mut rng, 5, 20),
            },
        });
        at += secs(&mut rng, 20, 40);
    }
    Plan { steps }
}

impl Nemesis {
    pub(super) async fn inject_disks(self: Arc<Self>, fault: &Fault) {
        let (nodes, down_for) = match fault {
            Fault::MajorityCut { nodes, down_for }
            | Fault::Wipe {
                nodes, down_for, ..
            } => (nodes.clone(), *down_for),
            _ => return,
        };
        // Faults never take more than they say, and a broker already down
        // has nothing to lose.
        let nodes: Vec<usize> = nodes
            .into_iter()
            .filter(|&node| self.cluster.is_running(node))
            .collect();
        if nodes.is_empty() {
            return;
        }
        self.recorder.fault(format!("{fault}"));
        let relaxed = self.cluster.config.fsync_mode != FsyncMode::OnCommit;
        match fault {
            Fault::MajorityCut { .. } => {
                let cut = self.cluster.power_loss_together(&nodes);
                self.disk_fault(format!("{fault}"), &nodes, relaxed, cut)
                    .await;
            }
            Fault::Wipe { scope, .. } => {
                // Stopped first, so nothing is acknowledged between reading
                // the disks and wiping them.
                for &node in &nodes {
                    self.cluster.crash(node).await;
                }
                let scope = *scope;
                let cluster = &self.cluster;
                let wiped = async { cluster.wipe_stopped(&nodes, scope) };
                self.disk_fault(format!("{fault}"), &nodes, true, wiped)
                    .await;
            }
            _ => return,
        }
        self.lapse(down_for).await;
        for node in nodes {
            self.recorder.fault(format!("restart b{}", node + 1));
            self.cluster.restart(node);
        }
    }

    /// Run `act`, which changes the devices of `nodes`, and account for what
    /// it cost. A multi-broker `act` must change the devices before its first
    /// await: a record acknowledged after the disks were read is not judged
    /// here, only at the end of the run.
    pub(super) async fn disk_fault<R>(
        &self,
        what: String,
        nodes: &[usize],
        loses_synced: bool,
        act: impl std::future::Future<Output = R>,
    ) -> R {
        let _guard = self.disks.lock().await;
        // Which copies a leader could vouch, for the hold rule.
        self.cluster.note_state().await;
        let at = self.cluster.elapsed();
        // Acknowledged before the disks are read, so a quorum's disks hold it.
        let acked = acked_values(&self.recorder.history());
        let before = self.cluster.disk_values();
        let out = act.await;
        let after = self.cluster.disk_values();
        let judged: std::collections::BTreeSet<u64> = acked.values().flatten().copied().collect();
        let late: Vec<u64> = acked_values(&self.recorder.history())
            .into_values()
            .flatten()
            .filter(|value| !judged.contains(value))
            .collect();
        let event = judge(Observed {
            at,
            what,
            faulted: nodes,
            loses_synced,
            before: &before,
            after: &after,
            acked: &acked,
            gone: &self.gone.lock(),
            thinned: &self.thinned.lock(),
        });
        thin(
            &mut self.thinned.lock(),
            nodes,
            &acked,
            &late,
            &before,
            &after,
        );
        self.gone.lock().extend(
            event
                .excused
                .values()
                .flatten()
                .copied()
                .chain(event.unexcused.iter().map(|(_, value, _)| *value)),
        );
        settle(&self.recorder, &event, nodes.len(), loses_synced);
        self.cluster.record_disk_event(event);
        out
    }
}

/// Forget what a fault was allowed to lose, and say so in the timeline.
fn settle(recorder: &Recorder, event: &DiskEvent, faulted: usize, loses_synced: bool) {
    for (shard, values) in &event.excused {
        recorder.fault(format!(
            "{} acknowledged records of stream shard {shard} are on no disk, which this fault may lose",
            values.len()
        ));
        recorder.lost(values.iter().copied());
    }
    // The cache's disks are not read, so a fault that may have taken the last
    // copy of a cache write is assumed to have.
    if loses_synced && (faulted >= 2 || !event.excused.is_empty()) {
        recorder.fault(
            "acknowledged cache writes may be on no disk either; cache writes before now are not judged"
                .to_string(),
        );
        recorder.cache_lost();
    }
    if !event.unexcused.is_empty() {
        recorder.fault(format!(
            "{} acknowledged records are on no disk and this fault may not lose them",
            event.unexcused.len()
        ));
    }
}

#[cfg(test)]
mod tests;

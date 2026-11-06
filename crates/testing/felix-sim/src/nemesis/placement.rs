//! Placement faults: the nemesis acting as an operator.
//!
//! Every action is a request to the control plane's operator API, the one
//! `felixctl` uses, so placement runs its real move, drain, replacement and
//! abandon paths. Nothing here writes an assignment or a node record itself.
//! The only direct read is the store's assignment change feed, which the
//! promotion watch polls so that a fault on the control plane does not hide a
//! promotion from it.
//!
//! The schedule is drawn from a stream of its own, so turning placement
//! faults on adds steps to a seed's plan without moving any it already had.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use felix_broker_service::sim::{ShardKey, ShardKind};
use felix_cluster::history::rng::Rng;
use felix_controlplane_service::model::ShardKind as CpShardKind;
use felix_controlplane_service::store::ControlPlaneStore;
use tokio::time::Instant;

use super::{Fault, Nemesis, Plan, Step};
use crate::cluster::describe;

/// How often the promotion watch looks. Opening a shard takes a fence round
/// trip to a majority, several link delays, so most promoted brokers are seen
/// before they open.
const PROMOTION_POLL: Duration = Duration::from_millis(2);
/// Watching one shard costs little, so it looks every millisecond.
const ONE_SHARD_POLL: Duration = Duration::from_millis(1);
/// How often a seat race looks for a replacement in flight, and for how long.
const SEAT_POLL: Duration = Duration::from_millis(100);
const SEAT_WITHIN: Duration = Duration::from_secs(20);
/// How long a seat race waits for the second promotion.
const SECOND_PROMOTION_WITHIN: Duration = Duration::from_secs(20);
/// How often a removal retries deleting the node's record, and an abandon
/// looks for held shards.
const OPERATOR_RETRY: Duration = Duration::from_secs(1);

impl Plan {
    /// This plan with placement faults for `brokers` brokers added, none
    /// starting after `window`. The steps stay in time order; a placement
    /// step that starts at the same instant as another goes after it.
    pub fn with_placement(mut self, seed: u64, brokers: usize, window: Duration) -> Self {
        self.steps
            .extend(placement_steps(seed, brokers, window).steps);
        self.steps.sort_by_key(|step| step.at);
        self
    }
}

/// The placement faults alone.
pub(super) fn placement_steps(seed: u64, brokers: usize, window: Duration) -> Plan {
    let mut rng = Rng::new(seed ^ 0x504c_4143_454d_4e54);
    let secs = |rng: &mut Rng, low: u64, high: u64| {
        rng.between(Duration::from_secs(low), Duration::from_secs(high))
    };
    // A seat is replaced only when a broker outside the set can take it.
    let spare = brokers > 3;
    let mut steps = Vec::new();
    let mut at = secs(&mut rng, 5, 12);
    while at < window {
        let node = rng.below(brokers as u64) as usize;
        let fault = match rng.below(100) {
            0..=19 => Fault::Move {
                shard: rng.next_u64(),
                to: node,
                cancel_after: rng.percent(30).then(|| secs(&mut rng, 1, 6)),
            },
            20..=34 => Fault::Drain {
                node,
                lasts: secs(&mut rng, 10, 30),
            },
            35..=44 => Fault::Remove {
                node,
                drain_for: secs(&mut rng, 5, 15),
                down_for: secs(&mut rng, 10, 25),
            },
            45..=54 => Fault::Abandon {
                within: secs(&mut rng, 20, 40),
            },
            55..=74 => Fault::KillBeforeOpen {
                watch_for: secs(&mut rng, 5, 20),
                down_for: secs(&mut rng, 5, 20),
                kill: rng.percent(50),
            },
            _ if spare => Fault::SeatRace {
                node,
                down_for: secs(&mut rng, 5, 20),
                kill: rng.percent(50),
            },
            _ => Fault::KillBeforeOpen {
                watch_for: secs(&mut rng, 5, 20),
                down_for: secs(&mut rng, 5, 20),
                kill: rng.percent(50),
            },
        };
        steps.push(Step { at, fault });
        at += secs(&mut rng, 8, 20);
    }
    Plan { steps }
}

impl Nemesis {
    pub(super) async fn inject_placement(self: Arc<Self>, fault: &Fault) {
        match fault {
            Fault::Move {
                shard,
                to,
                cancel_after,
            } => self.move_shard(*shard, *to, *cancel_after).await,
            Fault::Drain { node, lasts } => {
                self.recorder.fault(format!("{fault}"));
                self.hold_drain(*node).await;
                self.lapse(*lasts).await;
                self.release_drain(*node).await;
                self.recorder.fault(format!("end: {fault}"));
            }
            Fault::Remove {
                node,
                drain_for,
                down_for,
            } => {
                if !self.may_crash(*node) {
                    return;
                }
                self.recorder.fault(format!("{fault}"));
                self.remove(*node, *drain_for, *down_for).await;
            }
            Fault::Abandon { within } => {
                self.recorder.fault(format!("{fault}"));
                self.abandon_held(*within).await;
            }
            Fault::KillBeforeOpen {
                watch_for,
                down_for,
                kill,
            } => {
                self.recorder.fault(format!("{fault}"));
                if !self
                    .kill_next_promoted(None, *watch_for, *down_for, *kill)
                    .await
                {
                    self.recorder
                        .fault("kill before open: no promotion to a broker that may stop");
                }
            }
            Fault::SeatRace {
                node,
                down_for,
                kill,
            } => {
                self.recorder.fault(format!("{fault}"));
                self.hold_drain(*node).await;
                self.seat_race(*node, *down_for, *kill).await;
                self.lapse(*down_for).await;
                self.release_drain(*node).await;
                self.recorder.fault(format!("end: {fault}"));
            }
            _ => unreachable!("not a placement fault: {fault}"),
        }
    }

    async fn move_shard(&self, shard: u64, to: usize, cancel_after: Option<Duration>) {
        let keys: Vec<ShardKey> = self.cluster.shard_keys().collect();
        let key = &keys[(shard % keys.len() as u64) as usize];
        let views = self.cluster.cp().shard_views().await.unwrap_or_default();
        let leader = views
            .get(&describe(key))
            .map(|view| view.leader.clone())
            .unwrap_or_default();
        let n = self.cluster.broker_count();
        let to = if self.cluster.node_id(to) == leader {
            (to + 1) % n
        } else {
            to
        };
        let destination = self.cluster.node_id(to).to_string();
        let answer = match self.cluster.cp().move_shard(key, &destination).await {
            Ok(answer) => answer,
            Err(err) => {
                tracing::error!(error = %format!("{err:#}"), "build a move request");
                return;
            }
        };
        self.recorder.fault(format!(
            "move {} from {leader} to {destination}: {}",
            describe(key),
            answer.text
        ));
        if let (true, Some(after)) = (answer.ok, cancel_after) {
            self.lapse(after).await;
            if self.stopped.is_cancelled() {
                return;
            }
            let answer = self.cluster.cp().cancel_move(key).await;
            self.recorder.fault(format!(
                "cancel the move of {}: {}",
                describe(key),
                answer.text
            ));
        }
    }

    /// Drain, stop, deregister, then delete the record once no shard names
    /// the broker. It comes back after `down_for` and registers afresh.
    async fn remove(&self, node: usize, drain_for: Duration, down_for: Duration) {
        let id = self.cluster.node_id(node).to_string();
        self.hold_drain(node).await;
        self.lapse(drain_for).await;
        if self.stopped.is_cancelled() || !self.may_crash(node) {
            self.release_drain(node).await;
            return;
        }
        self.cluster.crash(node).await;
        self.recorder.fault(format!("stop {id}"));
        let back = Instant::now() + down_for;
        let (mut deregistered, mut last) = (false, None);
        while Instant::now() < back && !self.stopped.is_cancelled() {
            let answer = if deregistered {
                self.cluster.cp().remove_node(&id).await
            } else {
                self.cluster.cp().deregister(&id).await
            };
            if answer.ok && deregistered {
                self.recorder.fault(format!("removed {id}"));
                last = None;
                break;
            }
            if answer.ok {
                self.recorder.fault(format!("deregistered {id}"));
                deregistered = true;
                continue;
            }
            last = Some(answer.text);
            self.lapse(OPERATOR_RETRY.min(back.saturating_duration_since(Instant::now())))
                .await;
        }
        if let Some(refused) = last {
            self.recorder
                .fault(format!("{id} not removed, last refused: {refused}"));
        }
        tokio::select! {
            () = self.stopped.cancelled() => return,
            () = tokio::time::sleep_until(back) => {}
        }
        self.recorder.fault(format!("restart b{}", node + 1));
        self.cluster.restart(node);
        self.release_drain(node).await;
    }

    /// Abandon each held stream shard, as an operator reading the held list
    /// would. The control plane refuses until it, too, sees the shard waiting
    /// on a lost log, so a refused shard is asked again each second and only
    /// its first refusal is logged. A cache shard is left alone: its keys would
    /// be lost too, and the cache check has no way to forget them.
    async fn abandon_held(&self, within: Duration) {
        let until = Instant::now() + within;
        let mut refused = BTreeSet::new();
        while Instant::now() < until && !self.stopped.is_cancelled() {
            let held = self.cluster.held_shards().await;
            for key in self.cluster.shard_keys() {
                let shard = describe(&key);
                if key.kind != ShardKind::Stream || !held.contains(&shard) {
                    continue;
                }
                let answer = self.cluster.abandon(&key).await;
                if answer.ok || refused.insert(shard.clone()) {
                    self.recorder
                        .fault(format!("abandon the log of {shard}: {}", answer.text));
                }
            }
            self.lapse(OPERATOR_RETRY).await;
        }
    }

    /// Wait up to `within` for a seat being replaced on a shard `node`
    /// follows, kill that shard's leader, then kill the next leader named for
    /// it before it opens.
    async fn seat_race(self: &Arc<Self>, node: usize, down_for: Duration, kill: bool) {
        let id = self.cluster.node_id(node).to_string();
        let until = Instant::now() + SEAT_WITHIN;
        let found = loop {
            if self.stopped.is_cancelled() {
                return;
            }
            if let Some(found) = self.replacing(&id).await {
                break found;
            }
            if Instant::now() >= until {
                self.recorder
                    .fault(format!("seat race: no seat of {id} was replaced in time"));
                return;
            }
            tokio::time::sleep(SEAT_POLL).await;
        };
        let (key, leader, newcomer) = found;
        self.recorder.fault(format!(
            "seat race: {} replaces {id} with {newcomer}; stop its leader b{}",
            describe(&key),
            leader + 1
        ));
        let this = Arc::clone(self);
        tokio::spawn(async move { this.crash_for(leader, down_for, kill).await });
        if !self
            .kill_next_promoted(Some(&key), SECOND_PROMOTION_WITHIN, down_for, kill)
            .await
        {
            self.recorder.fault(format!(
                "seat race: no second promotion of {} to stop",
                describe(&key)
            ));
        }
    }

    /// A shard whose follower seat on `departing` is being replaced, with a
    /// leader that may be stopped: `(shard, leader, newcomer)`. The newcomer
    /// is `joining` while it copies in, then named in the pending set that
    /// leaves `departing` out until the leader reports the change.
    async fn replacing(&self, departing: &str) -> Option<(ShardKey, usize, String)> {
        let snapshot = self
            .cluster
            .cp()
            .store()
            .shard_assignment_snapshot()
            .await
            .ok()?;
        snapshot.items.into_iter().find_map(|a| {
            if a.key.kind != CpShardKind::Stream
                || a.leader == departing
                || !a.replicas.iter().any(|r| r == departing)
            {
                return None;
            }
            let pending = a
                .pending_set
                .as_ref()
                .filter(|set| !set.replicas.iter().any(|r| r == departing))
                .and_then(|set| {
                    set.replicas
                        .iter()
                        .find(|r| !a.replicas.contains(r))
                        .cloned()
                });
            let newcomer = a.joining.clone().or(pending)?;
            let leader = self.cluster.index_of(&a.leader)?;
            self.may_crash(leader)
                .then(|| (broker_key(&a.key), leader, newcomer))
        })
    }

    /// Watch for up to `within` for a new leader of a shard (only `only`, if
    /// given) and stop that broker before it opens. A new leader is one
    /// placement names, seen in the change feed, or one that stood for
    /// election and routes the shard to itself above every generation the
    /// control plane has written. Returns whether it stopped one.
    async fn kill_next_promoted(
        self: &Arc<Self>,
        only: Option<&ShardKey>,
        within: Duration,
        down_for: Duration,
        kill: bool,
    ) -> bool {
        let cp = self.cluster.cp();
        let keys: Vec<ShardKey> = match only {
            Some(key) => vec![key.clone()],
            None => self.cluster.shard_keys().collect(),
        };
        // Each shard's leader and newest generation, as the control plane has
        // written them.
        let mut known: BTreeMap<String, (String, u64)> = cp
            .shard_views()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(shard, view)| (shard, (view.leader, view.generation)))
            .collect();
        let mut seq = 0;
        loop {
            let (items, next) = cp.assignment_changes(seq).await;
            seq = next;
            if items.is_empty() {
                break;
            }
        }
        let until = Instant::now() + within;
        while Instant::now() < until {
            if self.stopped.is_cancelled() {
                return false;
            }
            tokio::time::sleep(if only.is_some() {
                ONE_SHARD_POLL
            } else {
                PROMOTION_POLL
            })
            .await;
            let (items, next) = cp.assignment_changes(seq).await;
            seq = next;
            let mut named = Vec::new();
            for change in items {
                let Some(assignment) = change.assignment else {
                    continue;
                };
                let key = broker_key(&assignment.key);
                let entry = (assignment.leader.clone(), assignment.generation);
                let before = known.insert(describe(&key), entry);
                if before.is_some_and(|(leader, _)| leader == assignment.leader)
                    || !keys.contains(&key)
                {
                    continue;
                }
                if let Some(index) = self.cluster.index_of(&assignment.leader) {
                    named.push((key, index, assignment.generation, "named"));
                }
            }
            let mut elected = Vec::new();
            for key in &keys {
                let floor = known.get(&describe(key)).map_or(0, |(_, g)| *g);
                let stands = self.cluster.stands(key);
                if stands.iter().any(|(_, stand)| stand.serving) {
                    continue;
                }
                for (index, stand) in stands {
                    if let Some((leader, generation)) = &stand.routed_leader
                        && leader == self.cluster.node_id(index)
                        && *generation > floor
                    {
                        elected.push((key.clone(), index, *generation, "elected"));
                    }
                }
            }
            for (key, index, generation, how) in named.into_iter().chain(elected) {
                let shard = describe(&key);
                if let Some(entry) = known.get_mut(&shard) {
                    entry.1 = entry.1.max(generation);
                }
                let unopened = self.cluster.stands(&key).into_iter().any(|(node, stand)| {
                    node == index && stand.opened_generation != Some(generation)
                });
                if !unopened || !self.may_crash(index) {
                    continue;
                }
                self.recorder.fault(format!(
                    "{} b{}, {how} leader of {shard} at generation {generation}, before it opens",
                    if kill { "kill" } else { "crash" },
                    index + 1,
                ));
                let this = Arc::clone(self);
                tokio::spawn(async move { this.crash_for(index, down_for, kill).await });
                return true;
            }
        }
        false
    }

    /// Drain `node` unless another fault already has.
    async fn hold_drain(&self, node: usize) {
        let first = {
            let mut holds = self.holds.lock();
            let count = holds.drains.entry(node).or_default();
            *count += 1;
            *count == 1
        };
        if first {
            let id = self.cluster.node_id(node);
            let answer = self.cluster.cp().drain(id).await;
            self.recorder.fault(format!("drain {id}: {}", answer.text));
        }
    }

    /// Put `node` back once no fault holds it drained. Healing does it for
    /// every node at once.
    async fn release_drain(&self, node: usize) {
        if self.stopped.is_cancelled() {
            return;
        }
        let last = {
            let mut holds = self.holds.lock();
            match holds.drains.get_mut(&node) {
                Some(count) if *count > 1 => {
                    *count -= 1;
                    false
                }
                Some(_) => {
                    holds.drains.remove(&node);
                    true
                }
                None => false,
            }
        };
        if last {
            let id = self.cluster.node_id(node);
            if let Ok(answer) = self.cluster.cp().undrain(id).await {
                self.recorder
                    .fault(format!("put {id} back to live: {}", answer.text));
            }
        }
    }
}

fn broker_key(key: &felix_controlplane_service::model::ShardKey) -> ShardKey {
    ShardKey {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        kind: match key.kind {
            CpShardKind::Stream => ShardKind::Stream,
            CpShardKind::Cache => ShardKind::Cache,
        },
    }
}

#[cfg(test)]
mod tests;

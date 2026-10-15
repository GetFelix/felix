//! Which replicas hold a shard's log, as their leaders last reported.
//!
//! Promotion is gated on this. Without it a lost leader cannot be replaced at
//! all — see `docs/replication-design.md` — and with a *wrong* answer it can be
//! replaced by a broker holding less than it claims, which is worse.
//!
//! # Why the leader reports it
//!
//! The leader is the only party that knows both ends of the comparison: its own
//! tail, and how far each follower has acknowledged. A follower knows only where
//! it is, not whether that is caught up.
//!
//! # Why the reports live in the store
//!
//! The instance a report reaches and the instance that later promotes a
//! replica need not be the same process. Under Postgres every instance
//! serves, so a report held in one instance's memory was a position no other
//! promoter could use — an acknowledgement resting on it could not be made
//! good at failover. Reports go through [`ControlPlaneStore`] like every
//! other fact placement decides on, and this module is only the reading of
//! them: which replicas a plan may promote, as of one instant.
//!
//! # Why reports expire
//!
//! A report says a follower *was* caught up at the moment it was made. The
//! leader then keeps writing, and the follower may fall behind. Promoting on a
//! stale report is how a failover silently loses the records written after it.
//!
//! So a report is only believed for a bounded time — long enough to outlive the
//! window in which a leader's death is noticed, and no longer. The bound is
//! derived from the liveness settings rather than configured separately: those
//! already say how quickly a dead leader is detected, and a report has to
//! survive exactly that long to be usable.
//!
//! # Why a dead leader's report stops ageing
//!
//! Only the leader reports on its shard, so once it dies nothing refreshes
//! its last report. Measured against now, that report would expire while
//! placement or the control plane was slow, and the shard would stay
//! unplaceable for good. So once the leader is down, a stamped report's age
//! is measured to the leader's last heartbeat instead, while the assignment
//! is still at the report's generation.
//!
//! That is safe only because of the stamp. A frozen report can outlive a
//! follower's log: the follower may be wiped and register again under the
//! same node id. The store stamps every node a report names with its node
//! incarnation when it accepts the report, and placement counts a node from
//! the report only while that is still its incarnation. An unstamped report
//! is judged by its age alone, as before the stamp existed.
//!
//! [`ControlPlaneStore`]: crate::store::ControlPlaneStore
use std::collections::{BTreeSet, HashMap};

use crate::cluster::placement::CaughtUp;
use crate::config::NodeLivenessConfig;
use crate::model::{
    HaltedCopy, Node, NodeLifecycle, NodeSuspicion, ReplicaReport, ShardAssignment, ShardKey,
    ShardKind,
};

/// Believe a report for twice the expiry timeout plus one heartbeat.
///
/// It has to outlive the *detection* of a dead leader, and detection is
/// slower than it first looks. A leader is declared down an expiry timeout
/// after its last **heartbeat**, and its last **report** is older still —
/// by up to one reporting interval, which the control plane does not know
/// and cannot bound.
///
/// A window of `expiry + heartbeat` therefore closes at almost exactly the
/// moment the leader becomes eligible for replacement, and a report that
/// expires a moment too early makes the shard unpromotable *forever*:
/// nothing else will ever report on it, because the only broker that could
/// is the one that died. That failure is permanent, where believing a report
/// slightly too long costs at most the records written between the last
/// report and the death — which is the loss window `Leader` already
/// documents, and which `Quorum` bounds by requiring a majority anyway.
///
/// So the asymmetry decides it: too short is a shard that never comes back,
/// too long is a bounded and already-documented exposure.
pub(crate) fn report_ttl_millis(liveness: &NodeLivenessConfig) -> u64 {
    liveness.expiry_timeout_ms * 2 + liveness.heartbeat_interval_ms
}

/// Believe a suspicion for two heartbeats. A broker that still suspects
/// says so on every heartbeat, and one that has stopped says nothing, so
/// this is how long a cleared suspicion outlives the clearing.
pub(crate) fn suspicion_ttl_millis(liveness: &NodeLivenessConfig) -> u64 {
    liveness.heartbeat_interval_ms * 2
}

/// The store's reports, read as of one instant.
///
/// A snapshot rather than a live view, so every shard in one planning pass is
/// judged against the same instant. Planning that read the clock per shard
/// could promote on one side of a report's expiry and refuse on the other.
///
/// `now_millis` must come from the same clock the reports were stamped with —
/// the store's — or freshness is a subtraction between two hosts' clocks.
pub struct ReplicaPositions {
    reports: HashMap<ShardKey, ReplicaReport>,
    ttl_millis: u64,
    now_millis: u64,
    /// The fresh suspicions, by the broker that holds them.
    suspicions: HashMap<String, NodeSuspicion>,
    /// Each node's current incarnation, which a leader's `by_round` must
    /// have been reported from.
    incarnations: HashMap<String, u64>,
    /// The fleet acknowledges `Quorum` stream writes by their followers and
    /// confirms `Quorum` reads by a round, so a deposed stream leader is
    /// kept out without its lease running out.
    followers_decide: bool,
    /// When this instance last saw each restore's new copy move; see
    /// `progress`.
    progressed: HashMap<(ShardKey, String), u64>,
    /// Each node, as the stamps on reports and the age of a dead leader's
    /// report are judged against.
    nodes: HashMap<String, NodeView>,
    /// Each shard's assignment: its generation and leader.
    assignments: HashMap<ShardKey, (u64, String)>,
}

/// What reading a report needs of a node.
struct NodeView {
    incarnation: u64,
    down: bool,
    last_heartbeat_at_millis: u64,
}

impl ReplicaPositions {
    pub fn new(
        reports: Vec<ReplicaReport>,
        liveness: &NodeLivenessConfig,
        now_millis: u64,
    ) -> Self {
        Self {
            reports: reports
                .into_iter()
                .map(|report| (report.key.clone(), report))
                .collect(),
            ttl_millis: report_ttl_millis(liveness),
            now_millis,
            suspicions: HashMap::new(),
            incarnations: HashMap::new(),
            followers_decide: false,
            progressed: HashMap::new(),
            nodes: HashMap::new(),
            assignments: HashMap::new(),
        }
    }

    /// With the nodes and assignments from the same read.
    ///
    /// Without them a stamped report counts no node and never outlives the
    /// TTL, which is the safe reading.
    pub fn with_nodes(mut self, nodes: &[Node], assignments: &[ShardAssignment]) -> Self {
        self.nodes = nodes
            .iter()
            .map(|node| {
                let view = NodeView {
                    incarnation: node.status.incarnation,
                    down: matches!(
                        node.status.lifecycle,
                        NodeLifecycle::Down | NodeLifecycle::Left
                    ),
                    last_heartbeat_at_millis: node.status.last_heartbeat_at_millis,
                };
                (node.node_id.clone(), view)
            })
            .collect();
        self.assignments = assignments
            .iter()
            .map(|assignment| {
                (
                    assignment.key.clone(),
                    (assignment.generation, assignment.leader.clone()),
                )
            })
            .collect();
        self
    }

    /// With what brokers said about the leaders they cannot reach, and the
    /// fleet features that decide whether that may move a shard.
    ///
    /// All three of what brokers need to serve without the lease
    /// (`generation_start`, `majority_ack`, `lease_free_reads`): short of any
    /// of them a deposed leader still acknowledges or serves reads on its
    /// lease, and replacing it before that runs out would let it hand out
    /// what its successor has since overwritten.
    ///
    /// `incarnations` is each node's current incarnation, from the same read.
    pub fn with_suspicions(
        mut self,
        suspicions: Vec<NodeSuspicion>,
        incarnations: HashMap<String, u64>,
        liveness: &NodeLivenessConfig,
        enabled: &BTreeSet<String>,
    ) -> Self {
        self.incarnations = incarnations;
        let ttl = suspicion_ttl_millis(liveness);
        self.suspicions = suspicions
            .into_iter()
            .filter(|suspicion| self.now_millis.saturating_sub(suspicion.reported_at_millis) <= ttl)
            .map(|suspicion| (suspicion.node_id.clone(), suspicion))
            .collect();
        self.followers_decide = [
            felix_common::fleet::GENERATION_START,
            felix_common::fleet::MAJORITY_ACK,
            felix_common::fleet::LEASE_FREE_READS,
        ]
        .iter()
        .all(|feature| enabled.contains(feature.name()));
        self
    }

    /// With when this instance last saw each restore's new copy move.
    pub(super) fn set_progress(&mut self, progressed: HashMap<(ShardKey, String), u64>) {
        self.progressed = progressed;
    }

    /// Read every report the store holds, as of the store's clock.
    pub async fn load(
        store: &dyn crate::store::ControlPlaneStore,
        liveness: &NodeLivenessConfig,
    ) -> crate::store::StoreResult<Self> {
        let nodes = store.list_nodes().await?;
        let assignments = store.list_shard_assignments().await?;
        let reports = store.list_replica_reports().await?;
        let now_millis = store.now_millis().await?;
        Ok(Self::new(reports, liveness, now_millis).with_nodes(&nodes, &assignments))
    }

    /// The report for `key`, if there is one and it is still believed.
    fn fresh(&self, key: &ShardKey) -> Option<&ReplicaReport> {
        let report = self.reports.get(key)?;
        (self
            .aged_to(key, report)
            .saturating_sub(report.reported_at_millis)
            <= self.ttl_millis)
            .then_some(report)
    }

    /// The instant `report`'s age is measured to: now, or the last heartbeat
    /// of a leader that is down and will not report again.
    ///
    /// Only for a stamped report, whose nodes are checked one by one in
    /// [`Self::vouches_for`], and only while the assignment is at the
    /// report's generation: the store took the report from that
    /// generation's leader, so the down node is the one that made it. A
    /// live leader's report keeps ageing, so one that stopped reporting
    /// still expires.
    fn aged_to(&self, key: &ShardKey, report: &ReplicaReport) -> u64 {
        if report.follower_incarnations.is_none() {
            return self.now_millis;
        }
        let Some((generation, leader)) = self.assignments.get(key) else {
            return self.now_millis;
        };
        match self.nodes.get(leader) {
            Some(leader) if *generation == report.generation && leader.down => {
                leader.last_heartbeat_at_millis.min(self.now_millis)
            }
            _ => self.now_millis,
        }
    }

    /// Whether `report` speaks for `node_id`'s current process. A stamped
    /// report does only while the node has not registered again since: a new
    /// incarnation may have lost the log the report saw.
    fn vouches_for(&self, report: &ReplicaReport, node_id: &str) -> bool {
        match &report.follower_incarnations {
            None => true,
            Some(stamps) => stamps.get(node_id).is_some_and(|stamped| {
                self.nodes
                    .get(node_id)
                    .is_some_and(|node| node.incarnation == *stamped)
            }),
        }
    }

    /// The fresh report for `key`, if it vouches for `node_id`.
    fn fresh_for(&self, key: &ShardKey, node_id: &str) -> Option<&ReplicaReport> {
        self.fresh(key)
            .filter(|report| self.vouches_for(report, node_id))
    }
}

impl CaughtUp for ReplicaPositions {
    fn suspects(&self, key: &ShardKey, generation: u64, follower: &str, leader: &str) -> bool {
        // Streams only for now. A `Quorum` cache would also need
        // `fenced_caches`, and still fails over on the lease.
        //
        // The enabled set says what brokers may do, not what this leader
        // does: it may not have heard of the finalize yet, and one that has
        // not reported by round at this generation may still serve on its
        // lease. Its report is held only from the assignment's leader. A
        // report from before the leader last registered speaks for a process
        // that is gone: the new one serves on its lease until its first pass.
        self.followers_decide
            && key.kind == ShardKind::Stream
            && self.fresh(key).is_some_and(|report| {
                report.generation == generation
                    && report.by_round
                    && report.leader_incarnation.is_some()
                    && report.leader_incarnation == self.incarnations.get(leader).copied()
            })
            && self
                .suspicions
                .get(follower)
                .is_some_and(|suspicion| suspicion.suspects.contains(leader))
    }

    fn is_caught_up(&self, key: &ShardKey, node_id: &str) -> bool {
        self.fresh_for(key, node_id)
            .is_some_and(|report| report.caught_up.contains(node_id))
    }

    fn reported_offset(&self, key: &ShardKey, node_id: &str) -> Option<u64> {
        self.fresh_for(key, node_id)?.offsets.get(node_id).copied()
    }

    fn reported_counter_offset(&self, key: &ShardKey, node_id: &str) -> Option<u64> {
        self.fresh_for(key, node_id)?
            .counter_offsets
            .get(node_id)
            .copied()
    }

    fn lag_records(&self, key: &ShardKey, node_id: &str) -> Option<u64> {
        let report = self.fresh_for(key, node_id)?;
        let tail = report.leader_offset?;
        Some(tail.saturating_sub(*report.offsets.get(node_id)?))
    }

    fn leader_offset(&self, key: &ShardKey) -> Option<u64> {
        self.fresh(key)?.leader_offset
    }

    fn reported_generation(&self, key: &ShardKey) -> Option<u64> {
        self.fresh(key).map(|report| report.generation)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(self.now_millis)
    }

    fn halted(&self, key: &ShardKey, node_id: &str) -> Option<&HaltedCopy> {
        self.fresh(key)?.halted.get(node_id)
    }

    fn progressed_at_millis(&self, key: &ShardKey, node_id: &str) -> Option<u64> {
        self.progressed
            .get(&(key.clone(), node_id.to_string()))
            .copied()
    }

    fn is_drained(&self, key: &ShardKey, generation: u64) -> bool {
        // The held report may predate the fence, when the leader was still
        // writing.
        self.fresh(key)
            .is_some_and(|report| report.generation == generation && report.drained)
    }
}

#[cfg(test)]
mod tests;

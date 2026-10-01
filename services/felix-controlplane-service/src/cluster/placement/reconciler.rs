//! Applying a plan to the store, on a timer or when woken.
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use super::metrics::MoveClock;
use super::{MovePolicy, PlacementWakes, Plan, ReplicaPositions, assignment_for, plan_with};
use crate::model::{Cache, Node, ShardAssignment, ShardKey, Stream};
use crate::store::AssignmentWrite;

/// Shards assigned a leader.
pub const SHARDS_PLACED_TOTAL: &str = "felix_shards_placed_total";

/// Shards with no eligible leader right now. Non-zero means capacity or
/// liveness needs attention.
pub const SHARDS_UNPLACEABLE: &str = "felix_shards_unplaceable";

/// Shards whose copies share a zone although live brokers in other zones could
/// hold them; see `Plan::unspread`. Non-zero means a zone lacks capacity, or a
/// move that would spread the shard is waiting for a slot.
pub const SHARDS_ZONE_UNSPREAD: &str = "felix_shards_zone_unspread";

/// Shards with fewer copies on serving brokers than their replication
/// factor. Non-zero for longer than the restore delay and a copy's catch-up
/// means placement has nowhere to put the copy, or is paused.
pub const SHARDS_UNDER_REPLICATED: &str = "felix_shards_under_replicated";

/// The copies under-replicated shards are missing between them.
pub const SHARD_REPLICAS_MISSING: &str = "felix_shard_replicas_missing";

/// Copies whose leader has stopped shipping to them (a follower that diverged
/// or needs history the leader no longer holds). Placement keeps new copies
/// off those nodes and replaces a copy halted for longer than the restore
/// delay; `GET /v1/placement/replication` names them.
pub const SHARD_REPLICAS_HALTED: &str = "felix_shard_replicas_halted";

/// Passes that could not read the catalog at all.
pub const RECONCILE_FAILURES_TOTAL: &str = "felix_shard_reconcile_failures_total";

/// Move steps written, by step.
pub const SHARD_MOVE_STEPS_TOTAL: &str = "felix_shard_move_steps_total";

/// Moves and follower replacements given up because they did not get close
/// enough to the leader within the move timeout. Each one gave its slot to
/// the next move; a steady count means a copy that cannot finish.
pub const SHARD_MOVES_TIMED_OUT_TOTAL: &str = "felix_shard_moves_timed_out_total";

/// Moves that could not advance in the last pass.
pub const SHARD_MOVES_WAITING: &str = "felix_shard_moves_waiting";

/// Placements and move steps not written because the shard changed after the
/// pass read it. Expected now and then with several instances running
/// placement; the next pass re-plans from the new state.
pub const SHARD_ASSIGNMENT_WRITE_CONFLICTS_TOTAL: &str =
    "felix_shard_assignment_write_conflicts_total";

/// Passes and operator requests not written because another placement write
/// landed after they read the store. The rest of the pass is dropped and the
/// next one re-plans; an operator's request is decided again.
pub const PLACEMENT_WRITES_FENCED_TOTAL: &str = "felix_placement_writes_fenced_total";

/// 1 while this instance holds the placement lease and runs the timed passes.
/// Across instances the sum is 1, or 0 for up to a lease period after the
/// holder stops without releasing it.
pub const PLACEMENT_LEASE_HELD: &str = "felix_placement_lease_held";

/// Times this instance took the placement lease: from another holder, or
/// first.
pub const PLACEMENT_LEASE_TAKEOVERS_TOTAL: &str = "felix_placement_lease_takeovers_total";

/// The placement lease lasts this many reconcile intervals. The holder renews
/// it every pass, so it has to outlast a tick and a slow pass; a holder that
/// stops without releasing it holds the timed passes up for this long.
pub const PLACEMENT_LEASE_INTERVALS: u64 = 3;

/// Most shards one pass fails over to a node that holds no copy of them.
///
/// Brokers come back from an outage one at a time, and the first one back is
/// the only eligible node: without a limit it is handed every orphaned shard
/// in one pass, and the rebalancing that follows moves them all again. Spread
/// over passes, the rest of the fleet is back before most of them are placed.
/// Promotions to a caught-up replica are not limited; the data is already
/// there and the replica set was spread when it was chosen.
pub const FAILOVERS_PER_PASS: usize = 64;

/// What one reconciliation pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileOutcome {
    pub placed: usize,
    pub kept: usize,
    pub unplaceable: usize,
    pub failed: usize,
    /// Move steps written.
    pub moved: usize,
    /// Moves that could not advance this pass.
    pub waiting: usize,
    /// Writes skipped because another writer changed the shard after this
    /// pass read it.
    pub conflicts: usize,
    /// Whether another placement write landed after this pass read the
    /// store, so the rest of the pass was not written.
    pub fenced: bool,
    /// Failovers left for a later pass by [`FAILOVERS_PER_PASS`].
    pub deferred: usize,
}

/// A plan, and the generation of every assignment it was planned from.
pub(super) struct PlannedPass {
    /// The placement token read before anything else, which the pass's first
    /// write must find.
    fence: u64,
    plan: Plan,
    /// Absent for a shard that had no assignment when the pass read.
    read: HashMap<ShardKey, u64>,
    /// Shards that had a successor staged when the pass read.
    staged: HashSet<ShardKey>,
    /// Placements that take an assigned shard to a node that held no copy of
    /// it: the failovers [`FAILOVERS_PER_PASS`] limits.
    unseeded: HashSet<ShardKey>,
}

impl PlannedPass {
    #[cfg(test)]
    pub(super) fn plan(&self) -> &Plan {
        &self.plan
    }

    /// This pass as if planned at `fence`, for a test of the generation
    /// check alone.
    #[cfg(test)]
    pub(super) fn at_token(self, fence: u64) -> Self {
        Self { fence, ..self }
    }
}

/// Plan against the current catalog and write what the plan calls for.
///
/// Reads the whole catalog rather than a delta: placement is a function of the
/// snapshot, and reconstructing it incrementally would be a second
/// implementation of the same decision that could disagree with the first.
///
/// Idempotent. A pass over an already-placed cluster writes nothing, so running
/// it on a timer does not churn the persisted rows or the changefeed.
///
/// Safe to run on several instances at once: each write lands only if no
/// other placement write landed since this pass read the store, and only if
/// the shard is still at the generation this pass read. One that is not is
/// counted and left to the next pass.
pub async fn reconcile_once(
    store: &dyn crate::store::ControlPlaneStore,
    liveness: &crate::config::NodeLivenessConfig,
    policy: MovePolicy,
) -> ReconcileOutcome {
    match plan_pass(store, liveness, policy).await {
        Some(pass) => {
            apply_pass(
                store,
                &pass,
                &mut MoveClock::default(),
                &PlacementWakes::default(),
            )
            .await
        }
        None => ReconcileOutcome::default(),
    }
}

/// Everything placement decides from, read once.
pub struct PlacementRead {
    pub streams: Vec<Stream>,
    pub caches: Vec<Cache>,
    pub nodes: Vec<Node>,
    pub existing: Vec<ShardAssignment>,
    pub positions: ReplicaPositions,
    /// Whether placement's own moves are paused.
    pub paused: bool,
}

impl PlacementRead {
    /// The placement token, then [`Self::load`]. The token has to come
    /// first: a placement write that lands between the two reads is then
    /// either in the catalog or something the reader's writes are fenced by.
    pub async fn load_fenced(
        store: &dyn crate::store::ControlPlaneStore,
        liveness: &crate::config::NodeLivenessConfig,
    ) -> crate::store::StoreResult<(u64, Self)> {
        let fence = store.placement_token().await?;
        Ok((fence, Self::load(store, liveness).await?))
    }

    /// Read the catalog, the reports and the pause switch.
    pub async fn load(
        store: &dyn crate::store::ControlPlaneStore,
        liveness: &crate::config::NodeLivenessConfig,
    ) -> crate::store::StoreResult<Self> {
        let (streams, caches, nodes, existing) = load(store).await?;
        // Read once, as of the store's clock: one instant for the whole pass,
        // so a report cannot be fresh for one shard and stale for the next
        // within the same plan, and the same clock the reports and heartbeats
        // were stamped with.
        let reports = store.list_replica_reports().await?;
        let now_millis = store.now_millis().await?;
        let positions = ReplicaPositions::new(reports, liveness, now_millis);
        let nodes = fence_departed(nodes, liveness, now_millis);
        let paused = store.moves_paused().await?;
        Ok(Self {
            streams,
            caches,
            nodes,
            existing,
            positions,
            paused,
        })
    }

    /// `policy`, paused if placement is.
    pub fn policy(&self, policy: MovePolicy) -> MovePolicy {
        MovePolicy {
            paused: self.paused,
            ..policy
        }
    }

    /// What a pass over this read decides.
    pub fn plan(&self, policy: MovePolicy) -> Plan {
        plan_with(
            &self.streams,
            &self.caches,
            &self.nodes,
            &self.existing,
            &self.positions,
            self.policy(policy),
        )
    }

    /// This read, for deciding an operator's request.
    pub fn catalog(&self, policy: MovePolicy) -> super::Catalog<'_> {
        super::Catalog {
            streams: &self.streams,
            caches: &self.caches,
            nodes: &self.nodes,
            existing: &self.existing,
            caught_up: &self.positions,
            policy: self.policy(policy),
        }
    }
}

/// Read the catalog and plan against it. `None` when it could not be read.
pub(super) async fn plan_pass(
    store: &dyn crate::store::ControlPlaneStore,
    liveness: &crate::config::NodeLivenessConfig,
    policy: MovePolicy,
) -> Option<PlannedPass> {
    let read = match PlacementRead::load_fenced(store, liveness).await {
        Ok(read) => read,
        Err(err) => {
            tracing::error!(error = %err, "could not read the catalog to place shards");
            metrics::counter!(RECONCILE_FAILURES_TOTAL).increment(1);
            return None;
        }
    };
    let (fence, read) = read;
    let plan = read.plan(policy);
    let existing = read.existing;
    let read = existing
        .iter()
        .map(|assignment| (assignment.key.clone(), assignment.generation))
        .collect();
    let staged = existing
        .iter()
        .filter(|assignment| assignment.successor.is_some())
        .map(|assignment| assignment.key.clone())
        .collect();
    let previous: HashMap<&ShardKey, &ShardAssignment> = existing
        .iter()
        .map(|assignment| (&assignment.key, assignment))
        .collect();
    let unseeded = plan
        .to_place()
        .filter(|(key, leader, _)| {
            previous
                .get(key)
                .is_some_and(|previous| !previous.nodes().any(|node| node == leader))
        })
        .map(|(key, _, _)| key.clone())
        .collect();
    Some(PlannedPass {
        fence,
        plan,
        read,
        staged,
        unseeded,
    })
}

/// Write what a planned pass calls for, timing the moves it advances and
/// waking this instance's long-polls on every write.
///
/// The writes chain the token: each lands only if the one before it was the
/// last placement write. The first that finds another writer got in ends the
/// pass, since everything after it was planned from the same read.
pub(super) async fn apply_pass(
    store: &dyn crate::store::ControlPlaneStore,
    pass: &PlannedPass,
    clock: &mut MoveClock,
    wakes: &PlacementWakes,
) -> ReconcileOutcome {
    let plan = &pass.plan;
    let mut fence = pass.fence;
    clock.forget_changed(&pass.read);
    let mut outcome = ReconcileOutcome {
        kept: plan.kept(),
        ..ReconcileOutcome::default()
    };

    // Every write is conditional on the generation the plan was made from:
    // another instance may have moved the shard on since, and a step planned
    // from the old state must not undo a newer one.
    for (key, step, assignment) in plan.moves() {
        match store
            .put_shard_assignment_if(assignment.clone(), pass.read.get(key).copied(), fence)
            .await
        {
            Ok(AssignmentWrite::Fenced { token }) => {
                outcome.fenced = true;
                fenced(key, step.label(), fence, token);
                break;
            }
            Ok(AssignmentWrite::Stale { current }) => {
                outcome.conflicts += 1;
                conflict(key, step.label(), pass.read.get(key).copied(), current);
            }
            Ok(AssignmentWrite::Written(written)) => {
                fence += 1;
                wakes.assignment_written();
                outcome.moved += 1;
                if let Some(times) = clock.written(
                    key,
                    step,
                    written.generation,
                    pass.staged.contains(key),
                    Instant::now(),
                ) {
                    super::metrics::record(times);
                }
                metrics::counter!(SHARD_MOVE_STEPS_TOTAL, "step" => step.label()).increment(1);
                warn_if_unspread(plan, key, &written);
                if let super::MoveStep::Halted { successor, reason } = step {
                    tracing::warn!(
                        kind = %key.kind,
                        name = %key.stream,
                        shard = key.shard,
                        destination = %successor,
                        reason = %reason,
                        "a shard move's destination halted; placing the copy elsewhere",
                    );
                }
                if let super::MoveStep::TimedOut { successor } = step {
                    metrics::counter!(SHARD_MOVES_TIMED_OUT_TOTAL).increment(1);
                    tracing::warn!(
                        kind = %key.kind,
                        name = %key.stream,
                        shard = key.shard,
                        destination = %successor,
                        "a shard move ran past its timeout and was abandoned",
                    );
                }
                tracing::info!(
                    kind = %key.kind,
                    name = %key.stream,
                    shard = key.shard,
                    step = step.label(),
                    detail = ?step,
                    leader = %written.leader,
                    successor = ?written.successor,
                    generation = written.generation,
                    "shard move advanced",
                );
            }
            Err(err) => {
                outcome.failed += 1;
                tracing::warn!(
                    kind = %key.kind,
                    name = %key.stream,
                    shard = key.shard,
                    step = step.label(),
                    error = %err,
                    "could not persist a shard move step; retrying next pass",
                );
            }
        }
    }

    for (key, reason) in plan.waiting() {
        outcome.waiting += 1;
        // Waiting on a catch-up or a drain is what a move in progress looks
        // like; the gauge below is what an operator watches.
        tracing::debug!(
            kind = %key.kind,
            name = %key.stream,
            shard = key.shard,
            reason = %reason,
            "shard move waiting",
        );
    }

    let mut failovers = 0;
    for (key, leader, replicas) in plan.to_place() {
        if outcome.fenced {
            break;
        }
        if pass.unseeded.contains(key) {
            if failovers == FAILOVERS_PER_PASS {
                outcome.deferred += 1;
                continue;
            }
            failovers += 1;
        }
        match store
            .put_shard_assignment_if(
                assignment_for(key, leader, replicas.to_vec()),
                pass.read.get(key).copied(),
                fence,
            )
            .await
        {
            Ok(AssignmentWrite::Fenced { token }) => {
                outcome.fenced = true;
                fenced(key, "place", fence, token);
            }
            Ok(AssignmentWrite::Stale { current }) => {
                outcome.conflicts += 1;
                conflict(key, "place", pass.read.get(key).copied(), current);
            }
            Ok(AssignmentWrite::Written(assignment)) => {
                fence += 1;
                wakes.assignment_written();
                outcome.placed += 1;
                warn_if_unspread(plan, key, &assignment);
                tracing::info!(
                    kind = %key.kind,
                    name = %key.stream,
                    shard = key.shard,
                    leader = %leader,
                    replicas = replicas.len(),
                    generation = assignment.generation,
                    "shard placed",
                );
            }
            Err(err) => {
                // One shard failing must not abandon the rest: the next pass
                // retries it, and the others are placed now rather than later.
                outcome.failed += 1;
                tracing::warn!(
                    kind = %key.kind,
                    name = %key.stream,
                    shard = key.shard,
                    leader = %leader,
                    error = %err,
                    "could not persist a shard placement; retrying next pass",
                );
            }
        }
    }

    if outcome.deferred > 0 {
        tracing::info!(
            deferred = outcome.deferred,
            limit = FAILOVERS_PER_PASS,
            "failovers left for the next pass",
        );
    }

    for (key, reason) in plan.unplaceable() {
        outcome.unplaceable += 1;
        // Warn rather than error: an empty or full cluster is an operational
        // state to fix, not a control-plane fault.
        tracing::warn!(
            kind = %key.kind,
            name = %key.stream,
            shard = key.shard,
            reason = %reason,
            "shard has no eligible leader",
        );
    }

    metrics::counter!(SHARDS_PLACED_TOTAL).increment(outcome.placed as u64);
    metrics::counter!(SHARD_ASSIGNMENT_WRITE_CONFLICTS_TOTAL).increment(outcome.conflicts as u64);
    metrics::counter!(PLACEMENT_WRITES_FENCED_TOTAL).increment(u64::from(outcome.fenced));
    metrics::gauge!(SHARDS_UNPLACEABLE).set(outcome.unplaceable as f64);
    metrics::gauge!(SHARD_MOVES_WAITING).set(outcome.waiting as f64);
    metrics::gauge!(SHARDS_ZONE_UNSPREAD).set(plan.unspread.len() as f64);
    metrics::gauge!(SHARDS_UNDER_REPLICATED).set(plan.under_replicated.len() as f64);
    metrics::gauge!(SHARD_REPLICAS_MISSING).set(f64::from(plan.missing_copies));
    metrics::gauge!(SHARD_REPLICAS_HALTED).set(f64::from(plan.halted_copies));
    outcome
}

/// Say so when a write leaves a shard's copies sharing a zone that placement
/// could not avoid. Only on a write, so a settled cluster does not repeat it
/// every pass; the gauge carries the standing count.
fn warn_if_unspread(plan: &Plan, key: &ShardKey, written: &ShardAssignment) {
    if plan.unspread.contains(key) {
        tracing::warn!(
            kind = %key.kind,
            name = %key.stream,
            shard = key.shard,
            leader = %written.leader,
            replicas = ?written.replicas,
            "shard copies share a zone: no broker with room is in a zone it lacks",
        );
    }
}

/// Place shards on an interval, and whenever `wakes` asks for a pass, until
/// `shutdown` fires.
///
/// The timed passes run on the instance holding the placement lease, as
/// `holder`; the others only try to take it. A wake runs a pass wherever it
/// arrives, so a report or an operator's request does not wait for the
/// holder's tick. That is safe because every write is fenced by the token
/// its pass read, whoever holds the lease: the lease keeps instances from
/// planning the same moves on every tick, and the token keeps the limits.
///
/// One pass at a time: a wake that arrives mid-pass runs one more pass after
/// it, however many arrived.
#[allow(clippy::too_many_arguments)]
pub fn spawn_reconciler(
    store: std::sync::Arc<dyn crate::store::ControlPlaneStore + Send + Sync>,
    liveness: crate::config::NodeLivenessConfig,
    policy: MovePolicy,
    interval: std::time::Duration,
    holder: String,
    gate: crate::raft::LeadershipGate,
    wakes: std::sync::Arc<PlacementWakes>,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let ttl_millis = (interval.as_millis() as u64).saturating_mul(PLACEMENT_LEASE_INTERVALS);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // A pass that overruns must not then run back-to-back catching up.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut clock = MoveClock::default();
        let mut held = false;
        loop {
            let woken = tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = ticker.tick() => false,
                _ = wakes.pass_requested() => true,
            };
            // Placement decides from what it reads; under Raft the gate's
            // linearizable check also guarantees those reads are current
            // before any assignment is proposed.
            if !gate.holds().await {
                if held {
                    // Under Raft the next leader waits out a lease it did not
                    // see released; releasing it (forwarded to that leader)
                    // spares placement the wait.
                    let _ = store.release_placement_lease(&holder).await;
                }
                held = hold(&holder, held, None);
                continue;
            }
            let lease = match store.acquire_placement_lease(&holder, ttl_millis).await {
                Ok(lease) => lease,
                Err(err) => {
                    tracing::warn!(error = %err, "could not take or renew the placement lease");
                    None
                }
            };
            held = hold(&holder, held, lease);
            if !held && !woken {
                continue;
            }
            if let Some(pass) = plan_pass(store.as_ref(), &liveness, policy.clone()).await {
                apply_pass(store.as_ref(), &pass, &mut clock, &wakes).await;
            }
        }
        if held {
            // Hand over now rather than when the lease runs out.
            if let Err(err) = store.release_placement_lease(&holder).await {
                tracing::warn!(error = %err, "could not release the placement lease");
            }
            metrics::gauge!(PLACEMENT_LEASE_HELD).set(0.0);
        }
    })
}

/// Whether this instance holds the lease after an acquire, logged when that
/// changes.
fn hold(holder: &str, held: bool, lease: Option<crate::store::PlacementLease>) -> bool {
    let holds = lease.is_some();
    if let Some(lease) = lease
        && lease.taken
    {
        metrics::counter!(PLACEMENT_LEASE_TAKEOVERS_TOTAL).increment(1);
        tracing::info!(
            holder,
            token = lease.token,
            "this instance runs placement now"
        );
    } else if held && !holds {
        tracing::info!(holder, "this instance no longer runs placement");
    }
    metrics::gauge!(PLACEMENT_LEASE_HELD).set(if holds { 1.0 } else { 0.0 });
    holds
}

async fn load(
    store: &dyn crate::store::ControlPlaneStore,
) -> crate::store::StoreResult<(Vec<Stream>, Vec<Cache>, Vec<Node>, Vec<ShardAssignment>)> {
    let streams = store.stream_snapshot().await?.items;
    let caches = store.cache_snapshot().await?.items;
    let nodes = store.list_nodes().await?;
    let existing = store.list_shard_assignments().await?;
    Ok((streams, caches, nodes, existing))
}

/// The nodes as placement has to see them: one that left while its lease may
/// still be running is draining until that lease has provably run out.
///
/// Draining, not gone: its shards stay where they are, or move only through a
/// handoff it takes part in. Promoting a follower sooner is two leaders, and a
/// deregistration involves no clock at all to stop that on its own.
fn fence_departed(
    mut nodes: Vec<Node>,
    liveness: &crate::config::NodeLivenessConfig,
    now_millis: u64,
) -> Vec<Node> {
    for node in &mut nodes {
        if crate::cluster::membership::left_within_lease(node, liveness, now_millis) {
            node.status.lifecycle = crate::model::NodeLifecycle::Draining;
        }
    }
    nodes
}

fn fenced(key: &ShardKey, step: &str, fence: u64, token: u64) {
    tracing::info!(
        kind = %key.kind,
        name = %key.stream,
        shard = key.shard,
        step,
        fence,
        token,
        "another placement write landed since this pass read the store; the next pass re-plans",
    );
}

fn conflict(key: &ShardKey, step: &str, planned_from: Option<u64>, current: Option<u64>) {
    tracing::info!(
        kind = %key.kind,
        name = %key.stream,
        shard = key.shard,
        step,
        planned_from = ?planned_from,
        current = ?current,
        "shard changed since this pass read it; not writing, the next pass re-plans",
    );
}

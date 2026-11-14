//! Shard assignments, and what leaders report about their replicas.
use super::InMemoryStore;
use crate::model::{
    CacheKey, ElectedLeader, ReplicaReport, ShardAssignment, ShardAssignmentChange,
    ShardAssignmentChangeOp, ShardKey, ShardKind, StreamKey,
};
use crate::store::{
    AssignmentWrite, ChangeSet, PlacementLease, ReportWrite, Snapshot, StoreError, StoreResult,
};

/// `seq_generation`: see [`crate::model::first_generation`].
/// `start_placed`: see [`put_shard_assignment_if`].
pub(super) async fn put_shard_assignment(
    store: &InMemoryStore,
    assignment: ShardAssignment,
    seq_generation: bool,
    start_placed: bool,
) -> StoreResult<ShardAssignment> {
    match write_shard_assignment(
        store,
        assignment,
        None,
        None,
        seq_generation,
        None,
        0,
        false,
        start_placed,
        SwitchWrite::Drop,
    )
    .await?
    {
        AssignmentWrite::Written(stored) => Ok(stored),
        AssignmentWrite::Stale { .. } | AssignmentWrite::Fenced { .. } => {
            unreachable!("an unconditional write is never stale")
        }
    }
}

/// `fence`: `None` only for a Raft log entry written before placement
/// writes were fenced, which must apply as it did then. `at_least`: see
/// [`crate::store::ControlPlaneStore::put_shard_assignment_at_least`].
/// `record_previous`: a write landing past the next generation records the
/// one it replaced ([`ShardAssignment::previous_generation`]); false for a
/// Raft entry proposed below the level that stores it. `start_placed`: a
/// first assignment starts [`ShardAssignment::placed`]; false for a Raft
/// entry proposed below the level that stores it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn put_shard_assignment_if(
    store: &InMemoryStore,
    assignment: ShardAssignment,
    expected_generation: Option<u64>,
    fence: Option<u64>,
    seq_generation: bool,
    at_least: u64,
    record_previous: bool,
    start_placed: bool,
) -> StoreResult<AssignmentWrite> {
    write_shard_assignment(
        store,
        assignment,
        Some(expected_generation),
        fence,
        seq_generation,
        None,
        at_least,
        record_previous,
        start_placed,
        SwitchWrite::Drop,
    )
    .await
}

/// [`crate::store::ControlPlaneStore::adopt_elected_leader`]. Deterministic,
/// as a Raft apply must be: the decision is made from the assignment this
/// store holds, and the write is conditional on it. `elected_set` applies
/// [`ShardAssignment::adopting_elected_set`] instead of
/// [`ShardAssignment::adopting`].
pub(super) async fn adopt_elected_leader(
    store: &InMemoryStore,
    key: &ShardKey,
    expected_generation: u64,
    elected: &ElectedLeader,
    elected_set: bool,
    own_leader_set: bool,
) -> StoreResult<AssignmentWrite> {
    let current = get_shard_assignment(store, key).await?;
    if current.generation != expected_generation {
        return Ok(AssignmentWrite::Stale {
            current: Some(current.generation),
        });
    }
    // Sealed copies refuse hybrid ballots, so no election stands behind
    // this one; the write below would refuse it too.
    if current.switched() {
        return Err(StoreError::Conflict(
            crate::model::SealOrderError::Switched.to_string(),
        ));
    }
    let adopted = if elected_set {
        let nodes = store.nodes.read().await;
        let known = |node: &str| nodes.records.contains_key(node);
        if own_leader_set {
            current.adopting_elected_set(elected, &known)
        } else {
            current.adopting_elected_set_keeping_own_leader(elected, &known)
        }
    } else {
        current.adopting(elected)
    };
    let Some(adopted) = adopted else {
        return Ok(AssignmentWrite::Stale {
            current: Some(current.generation),
        });
    };
    // Measured on the elected generation: a reissue names the elected
    // leader one past it.
    crate::model::check_elected_generation(current.generation, elected.generation)
        .map_err(StoreError::Conflict)?;
    let generation = adopted.generation;
    write_shard_assignment(
        store,
        adopted,
        Some(Some(expected_generation)),
        None,
        false,
        Some(generation),
        0,
        false,
        false,
        SwitchWrite::Drop,
    )
    .await
}

/// What a write does with the shard's switch record
/// ([`crate::model::ShardSwitch`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SwitchWrite {
    /// An ordinary write. A seal order does not outlive its generation, so
    /// the write drops it: that is how a seal is given up.
    Drop,
    /// Order a seal at the generation this write takes.
    Order,
    /// Give up the seal standing at the expected generation, and only that.
    GiveUp,
}

/// `expected`: `None` writes unconditionally, `Some(generation)` only over
/// that generation (`Some(None)`: only where there is no assignment).
/// `elected`: an adoption's generation, written in place of the next one;
/// only over an assignment below it. `at_least`: written there when it is
/// above the next one. `record_previous`, `start_placed`: see
/// [`put_shard_assignment_if`].
///
/// No write lands on a shard that switched to Raft: its hybrid generation
/// is the base every voter started from, and a later one would open the
/// hybrid again beside the Raft group.
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_shard_assignment(
    store: &InMemoryStore,
    assignment: ShardAssignment,
    expected: Option<Option<u64>>,
    fence: Option<u64>,
    seq_generation: bool,
    elected: Option<u64>,
    at_least: u64,
    record_previous: bool,
    start_placed: bool,
    switch: SwitchWrite,
) -> StoreResult<AssignmentWrite> {
    assignment.validate().map_err(invalid_shard)?;

    // The stream or cache bounds the shard number, and it has to exist at
    // all. Which of the two is decided by the key's kind, not by looking in
    // both: a cache and a stream may share a name, and falling back from one
    // to the other would let a shard of the wrong thing validate.
    //
    // The stream and cache maps stay read-locked until the write lands, so a
    // delete waits for it and then takes it away, and one created again after
    // is read with its own incarnation, never the old one stamped on an
    // assignment of the new.
    let streams = store.streams.read().await;
    let caches = store.caches.read().await;
    let (shards, incarnation) = match assignment.key.kind {
        ShardKind::Stream => streams
            .get(&StreamKey {
                tenant_id: assignment.key.tenant_id.clone(),
                namespace: assignment.key.namespace.clone(),
                stream: assignment.key.stream.clone(),
            })
            .map(|stream| (stream.shards, stream.incarnation))
            .ok_or_else(|| StoreError::NotFound("stream".into()))?,
        ShardKind::Cache => caches
            .get(&CacheKey {
                tenant_id: assignment.key.tenant_id.clone(),
                namespace: assignment.key.namespace.clone(),
                cache: assignment.key.stream.clone(),
            })
            .map(|cache| (cache.shards, cache.incarnation))
            .ok_or_else(|| StoreError::NotFound("cache".into()))?,
    };
    assignment.validate_within(shards).map_err(invalid_shard)?;
    #[cfg(test)]
    store.faults.pause_shard_write().await;

    // Checked here rather than by a foreign key: the node reference has none
    // deliberately, so that deleting a node cannot cascade an assignment away.
    // The guard is held until the write lands, so `delete_node` cannot remove
    // a node between this check and the write.
    let nodes = store.nodes.read().await;
    for node_id in assignment.named_nodes() {
        if !nodes.records.contains_key(node_id) {
            return Err(StoreError::NotFound(format!("node {node_id}")));
        }
    }

    let mut state = store.shards.write().await;
    if let Some(fence) = fence
        && fence != state.placement.token
    {
        return Ok(AssignmentWrite::Fenced {
            token: state.placement.token,
        });
    }
    let current = state.records.get(&assignment.key).map(|a| a.generation);
    if let Some(expected) = expected
        && expected != current
    {
        return Ok(AssignmentWrite::Stale { current });
    }
    let mut previous_generation = None;
    let (op, generation) = match state.records.get(&assignment.key) {
        Some(existing) => {
            match switch {
                // Its report won the generation, so the give-up lost.
                SwitchWrite::GiveUp if existing.sealing().is_none() => {
                    return Ok(AssignmentWrite::Stale { current });
                }
                SwitchWrite::GiveUp => {}
                SwitchWrite::Drop if existing.switched() => {
                    return Err(StoreError::Conflict(
                        crate::model::SealOrderError::Switched.to_string(),
                    ));
                }
                SwitchWrite::Drop => {}
                SwitchWrite::Order => existing
                    .can_seal()
                    .map_err(|err| StoreError::Conflict(err.to_string()))?,
            }
            if !existing.state.can_transition_to(assignment.state) {
                return Err(invalid_shard(
                    crate::model::ShardValidationError::UnsupportedTransition {
                        from: existing.state,
                        to: assignment.state,
                    },
                ));
            }
            let generation = match elected {
                Some(elected) if elected <= existing.generation => {
                    return Ok(AssignmentWrite::Stale { current });
                }
                // The adoption checked the window on the elected generation.
                Some(elected) if elected > i64::MAX as u64 => {
                    return Err(StoreError::Conflict(format!(
                        "generation {elected} is past the last the control plane can issue"
                    )));
                }
                Some(elected) => elected,
                None => crate::model::generation_at_least(existing.generation, at_least)
                    .map_err(StoreError::Conflict)?,
            };
            previous_generation =
                crate::model::previous_generation(existing.generation, generation, elected)
                    .filter(|_| record_previous);
            (ShardAssignmentChangeOp::Updated, generation)
        }
        // Only an assignment can be adopted, or sealed.
        None if elected.is_some() || switch != SwitchWrite::Drop => {
            return Ok(AssignmentWrite::Stale { current });
        }
        None => (
            ShardAssignmentChangeOp::Assigned,
            crate::model::first_generation(incarnation, state.changes.next_seq, seq_generation),
        ),
    };

    assignment
        .check_pending_generation(state.records.get(&assignment.key), generation)
        .map_err(invalid_shard)?;

    // Store-owned, so a caller cannot pin a generation and make its own
    // stale report look current.
    let mut stored = ShardAssignment {
        generation,
        incarnation,
        previous_generation,
        ..assignment
    };
    stored.switch = (switch == SwitchWrite::Order).then(|| stored.seal_order(generation));
    let existing = state.records.get(&stored.key);
    let held = existing
        .map(|existing| existing.readmitted.clone())
        .unwrap_or_default();
    stored.carry_readmitted(&held);
    // In the same write that names a broker, so it is on the record before
    // any leader acts on the set.
    // A shard of a restored stream or cache may have had brokers before
    // the import that its export does not show.
    let start_placed = start_placed && !state.unplaced.contains(&stored.key.owner(incarnation));
    stored.carry_placed(existing, start_placed);
    state.records.insert(stored.key.clone(), stored.clone());
    state.record(op, &stored.key, Some(stored.clone()));
    if fence.is_some() {
        state.placement.token += 1;
    }
    metrics::counter!("felix_shard_assignment_changes_total", "op" => match op {
        ShardAssignmentChangeOp::Assigned => "assigned",
        ShardAssignmentChangeOp::Updated => "updated",
        ShardAssignmentChangeOp::Unassigned => "unassigned",
    })
    .increment(1);
    Ok(AssignmentWrite::Written(stored))
}

pub(super) async fn get_shard_assignment(
    store: &InMemoryStore,
    key: &ShardKey,
) -> StoreResult<ShardAssignment> {
    store
        .shards
        .read()
        .await
        .records
        .get(key)
        .cloned()
        .ok_or_else(|| StoreError::NotFound("shard assignment".into()))
}

pub(super) async fn list_shard_assignments(
    store: &InMemoryStore,
) -> StoreResult<Vec<ShardAssignment>> {
    let mut items: Vec<ShardAssignment> = store
        .shards
        .read()
        .await
        .records
        .values()
        .cloned()
        .collect();
    items.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
    Ok(items)
}

pub(super) async fn list_shard_assignments_for_node(
    store: &InMemoryStore,
    node_id: &str,
) -> StoreResult<Vec<ShardAssignment>> {
    let mut items: Vec<ShardAssignment> = store
        .shards
        .read()
        .await
        .records
        .values()
        .filter(|assignment| assignment.leader == node_id)
        .cloned()
        .collect();
    items.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
    Ok(items)
}

pub(super) async fn delete_shard_assignment(
    store: &InMemoryStore,
    key: &ShardKey,
) -> StoreResult<()> {
    let mut state = store.shards.write().await;
    if state.records.remove(key).is_none() {
        return Err(StoreError::NotFound("shard assignment".into()));
    }
    state.record(ShardAssignmentChangeOp::Unassigned, key, None);
    // Taken under the assignment lock, so a report cannot slip in between.
    store.replica_reports.write().await.remove(key);
    metrics::counter!("felix_shard_assignment_changes_total", "op" => "unassigned").increment(1);
    Ok(())
}

pub(super) async fn shard_assignment_snapshot(
    store: &InMemoryStore,
) -> StoreResult<Snapshot<ShardAssignment>> {
    let state = store.shards.read().await;
    let mut items: Vec<ShardAssignment> = state.records.values().cloned().collect();
    items.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
    Ok(Snapshot {
        items,
        next_seq: state.changes.next_seq,
    })
}

pub(super) async fn first_shard_assignments(
    store: &InMemoryStore,
) -> StoreResult<Vec<(ShardKey, u64, u64)>> {
    let state = store.shards.read().await;
    Ok(state
        .changes
        .items
        .iter()
        .filter(|change| change.op == ShardAssignmentChangeOp::Assigned)
        .filter_map(|change| change.assignment.as_ref())
        .filter(|first| first.incarnation > 0)
        .map(|first| (first.key.clone(), first.incarnation, first.generation))
        .collect())
}

pub(super) async fn previous_shard_assignments(
    store: &InMemoryStore,
) -> StoreResult<Vec<ShardAssignment>> {
    let state = store.shards.read().await;
    let mut last_two: std::collections::HashMap<&ShardKey, [Option<&ShardAssignment>; 2]> =
        std::collections::HashMap::new();
    for change in &state.changes.items {
        let held = last_two.entry(&change.key).or_default();
        *held = [held[1], change.assignment.as_ref()];
    }
    Ok(last_two
        .into_iter()
        .filter_map(|(key, [before, last])| {
            let current = state.records.get(key)?;
            (last?.generation == current.generation).then_some(())?;
            before.cloned()
        })
        .collect())
}

pub(super) async fn shard_assignment_changes(
    store: &InMemoryStore,
    since: u64,
) -> StoreResult<ChangeSet<ShardAssignmentChange>> {
    let state = store.shards.read().await;
    Ok(state
        .changes
        .page(since, store.limit(), |change| change.seq))
}

/// `readmit`: also record the [`crate::model::Readmitted`] entries the report
/// adds to the assignment; false for a Raft entry proposed below the level
/// that stores them.
pub(super) async fn record_replica_report(
    store: &InMemoryStore,
    report: ReplicaReport,
    leader: Option<&str>,
    readmit: bool,
) -> StoreResult<ReportWrite> {
    // Nodes before shards, the order an assignment write takes them in.
    let nodes = store.nodes.read().await;
    // Held across the leadership check and the write, so a promotion or
    // delete either lands first and is seen, or waits for the report.
    let mut shards = store.shards.write().await;
    let Some(assignment) = shards.records.get_mut(&report.key) else {
        return Err(StoreError::NotFound("shard assignment".into()));
    };
    // `None` only from a Raft entry proposed before the check existed, which
    // must apply as it did then.
    if let Some(leader) = leader {
        if assignment.leader != leader {
            return Ok(ReportWrite::NotLeader);
        }
        if assignment.generation != report.generation {
            return Ok(ReportWrite::Stale);
        }
    }
    // The API checked this against a read from before the lock; a recreate
    // can land in between.
    if report
        .stream_incarnation
        .is_some_and(|incarnation| incarnation != assignment.incarnation)
    {
        return Ok(ReportWrite::Stale);
    }
    let mut reports = store.replica_reports.write().await;
    let mut report = report;
    report.stream_incarnation = None;
    if let Some(held) = reports.get(&report.key) {
        if !report.supersedes(held) {
            return Ok(ReportWrite::Stale);
        }
        report.carry_halts(&held.halted);
    } else {
        report.carry_halts(&Default::default());
    }
    // A report the leader check above did not hold to its generation adds
    // nothing: `readmitted_by` needs the stamped leader anyway.
    if readmit {
        let registered = |node: &str| {
            let node = nodes.records.get(node)?;
            Some((node.status.storage_id.clone()?, node.status.incarnation))
        };
        let entries = assignment.readmitted_by(&report, &registered);
        assignment.readmit(entries);
    }
    reports.insert(report.key.clone(), report);
    Ok(ReportWrite::Stored)
}

pub(super) async fn list_replica_reports(store: &InMemoryStore) -> StoreResult<Vec<ReplicaReport>> {
    Ok(sorted_replica_reports(store).await)
}

/// Every report, in shard order.
pub(super) async fn sorted_replica_reports(store: &InMemoryStore) -> Vec<ReplicaReport> {
    let mut reports: Vec<ReplicaReport> = store
        .replica_reports
        .read()
        .await
        .values()
        .cloned()
        .collect();
    reports.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
    reports
}

fn invalid_shard(err: crate::model::ShardValidationError) -> StoreError {
    StoreError::Conflict(err.to_string())
}

/// Sort key giving a stable stream-then-shard order.
fn shard_order(key: &ShardKey) -> (&str, &str, &str, u32) {
    (&key.tenant_id, &key.namespace, &key.stream, key.shard)
}

pub(super) async fn acquire_placement_lease(
    store: &InMemoryStore,
    holder: &str,
    ttl_millis: u64,
    now_millis: u64,
) -> Option<PlacementLease> {
    let placement = &mut store.shards.write().await.placement;
    let ours = placement.holder.as_deref() == Some(holder);
    if !ours && placement.holder.is_some() && placement.expires_at_millis > now_millis {
        return None;
    }
    if !ours {
        placement.holder = Some(holder.to_string());
        placement.token += 1;
    }
    placement.expires_at_millis = now_millis.saturating_add(ttl_millis);
    Some(PlacementLease {
        token: placement.token,
        taken: !ours,
    })
}

pub(super) async fn release_placement_lease(store: &InMemoryStore, holder: &str, now_millis: u64) {
    let placement = &mut store.shards.write().await.placement;
    // The holder stays named, so whoever takes it next still advances the
    // token.
    if placement.holder.as_deref() == Some(holder) {
        placement.expires_at_millis = now_millis;
    }
}

impl InMemoryStore {
    /// The Raft leader taking the lease: whatever the expiry, since
    /// leadership is the lease there. Deterministic, as a Raft apply must be.
    pub(crate) async fn take_placement_lease(&self, holder: &str) -> PlacementLease {
        let placement = &mut self.shards.write().await.placement;
        let taken = placement.holder.as_deref() != Some(holder);
        if taken {
            placement.holder = Some(holder.to_string());
            placement.token += 1;
        }
        PlacementLease {
            token: placement.token,
            taken,
        }
    }

    /// Who holds the lease, as this store last applied it.
    pub(crate) async fn placement_holder(&self) -> Option<String> {
        self.shards.read().await.placement.holder.clone()
    }

    /// A conditional write from a Raft log entry that predates fencing.
    pub(crate) async fn put_shard_assignment_unfenced_if(
        &self,
        assignment: ShardAssignment,
        expected_generation: Option<u64>,
        seq_generation: bool,
    ) -> StoreResult<AssignmentWrite> {
        put_shard_assignment_if(
            self,
            assignment,
            expected_generation,
            None,
            seq_generation,
            0,
            false,
            false,
        )
        .await
    }

    /// [`ControlPlaneStore::put_shard_assignment`] from a Raft entry, which
    /// says how a first assignment's generation is chosen.
    pub(crate) async fn apply_put_shard_assignment(
        &self,
        assignment: ShardAssignment,
        seq_generation: bool,
        start_placed: bool,
    ) -> StoreResult<ShardAssignment> {
        put_shard_assignment(self, assignment, seq_generation, start_placed).await
    }

    /// [`ControlPlaneStore::put_shard_assignment_if`] from a Raft entry, as
    /// [`Self::apply_put_shard_assignment`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn apply_put_shard_assignment_if(
        &self,
        assignment: ShardAssignment,
        expected_generation: Option<u64>,
        fence: u64,
        seq_generation: bool,
        at_least: u64,
        record_previous: bool,
        start_placed: bool,
    ) -> StoreResult<AssignmentWrite> {
        put_shard_assignment_if(
            self,
            assignment,
            expected_generation,
            Some(fence),
            seq_generation,
            at_least,
            record_previous,
            start_placed,
        )
        .await
    }
}

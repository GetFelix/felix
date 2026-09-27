//! Shard assignments, and what leaders report about their replicas.
use super::InMemoryStore;
use crate::model::{
    CacheKey, ReplicaReport, ShardAssignment, ShardAssignmentChange, ShardAssignmentChangeOp,
    ShardKey, ShardKind, StreamKey,
};
use crate::store::{
    AssignmentWrite, ChangeSet, PlacementLease, ReportWrite, Snapshot, StoreError, StoreResult,
};

pub(super) async fn put_shard_assignment(
    store: &InMemoryStore,
    assignment: ShardAssignment,
) -> StoreResult<ShardAssignment> {
    match write_shard_assignment(store, assignment, None, None).await? {
        AssignmentWrite::Written(stored) => Ok(stored),
        AssignmentWrite::Stale { .. } | AssignmentWrite::Fenced { .. } => {
            unreachable!("an unconditional write is never stale")
        }
    }
}

/// `fence`: `None` only for a Raft log entry written before placement
/// writes were fenced, which must apply as it did then.
pub(super) async fn put_shard_assignment_if(
    store: &InMemoryStore,
    assignment: ShardAssignment,
    expected_generation: Option<u64>,
    fence: Option<u64>,
) -> StoreResult<AssignmentWrite> {
    write_shard_assignment(store, assignment, Some(expected_generation), fence).await
}

/// `expected`: `None` writes unconditionally, `Some(generation)` only over
/// that generation (`Some(None)`: only where there is no assignment).
async fn write_shard_assignment(
    store: &InMemoryStore,
    assignment: ShardAssignment,
    expected: Option<Option<u64>>,
    fence: Option<u64>,
) -> StoreResult<AssignmentWrite> {
    assignment.validate().map_err(invalid_shard)?;

    // The stream or cache bounds the shard number, and it has to exist at
    // all. Which of the two is decided by the key's kind, not by looking in
    // both: a cache and a stream may share a name, and falling back from one
    // to the other would let a shard of the wrong thing validate.
    let shards = match assignment.key.kind {
        ShardKind::Stream => store
            .streams
            .read()
            .await
            .get(&StreamKey {
                tenant_id: assignment.key.tenant_id.clone(),
                namespace: assignment.key.namespace.clone(),
                stream: assignment.key.stream.clone(),
            })
            .map(|stream| stream.shards)
            .ok_or_else(|| StoreError::NotFound("stream".into()))?,
        ShardKind::Cache => store
            .caches
            .read()
            .await
            .get(&CacheKey {
                tenant_id: assignment.key.tenant_id.clone(),
                namespace: assignment.key.namespace.clone(),
                cache: assignment.key.stream.clone(),
            })
            .map(|cache| cache.shards)
            .ok_or_else(|| StoreError::NotFound("cache".into()))?,
    };
    assignment.validate_within(shards).map_err(invalid_shard)?;

    // Checked here rather than by a foreign key: the node reference has none
    // deliberately, so that deleting a node cannot cascade an assignment away.
    // The guard is held until the write lands, so `delete_node` cannot remove
    // a node between this check and the write.
    let nodes = store.nodes.read().await;
    for node_id in assignment.nodes() {
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
    let (op, generation) = match state.records.get(&assignment.key) {
        Some(existing) => {
            if !existing.state.can_transition_to(assignment.state) {
                return Err(invalid_shard(
                    crate::model::ShardValidationError::UnsupportedTransition {
                        from: existing.state,
                        to: assignment.state,
                    },
                ));
            }
            (
                ShardAssignmentChangeOp::Updated,
                existing.generation.saturating_add(1),
            )
        }
        None => (ShardAssignmentChangeOp::Assigned, 0),
    };

    // Store-owned, so a caller cannot pin a generation and make its own
    // stale report look current.
    let stored = ShardAssignment {
        generation,
        ..assignment
    };
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

pub(super) async fn shard_assignment_changes(
    store: &InMemoryStore,
    since: u64,
) -> StoreResult<ChangeSet<ShardAssignmentChange>> {
    let state = store.shards.read().await;
    let items = state
        .changes
        .items
        .iter()
        .filter(|item| item.seq >= since)
        .take(store.limit())
        .cloned()
        .collect();
    Ok(ChangeSet {
        items,
        next_seq: state.changes.next_seq,
    })
}

pub(super) async fn record_replica_report(
    store: &InMemoryStore,
    report: ReplicaReport,
    leader: Option<&str>,
) -> StoreResult<ReportWrite> {
    // Held across the leadership check and the write, so a promotion or
    // delete either lands first and is seen, or waits for the report.
    let shards = store.shards.read().await;
    let Some(assignment) = shards.records.get(&report.key) else {
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
    let mut reports = store.replica_reports.write().await;
    if let Some(held) = reports.get(&report.key)
        && !report.supersedes(held)
    {
        return Ok(ReportWrite::Stale);
    }
    reports.insert(report.key.clone(), report);
    Ok(ReportWrite::Stored)
}

pub(super) async fn list_replica_reports(store: &InMemoryStore) -> StoreResult<Vec<ReplicaReport>> {
    let mut reports: Vec<ReplicaReport> = store
        .replica_reports
        .read()
        .await
        .values()
        .cloned()
        .collect();
    reports.sort_by(|a, b| shard_order(&a.key).cmp(&shard_order(&b.key)));
    Ok(reports)
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
    ) -> StoreResult<AssignmentWrite> {
        put_shard_assignment_if(self, assignment, expected_generation, None).await
    }
}

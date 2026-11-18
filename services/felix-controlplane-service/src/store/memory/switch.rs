//! A shard's switch to Raft: ordering the seal, recording its one report,
//! and giving it up. See [`crate::model::ShardSwitch`].
//!
//! Each is a compare-and-swap on the seal order standing at the shard's
//! generation. A report is recorded in place and ends the order; a give-up
//! moves the shard to a later generation, which ends it too. So one of them
//! wins a generation, never both, and never two reports.
use super::InMemoryStore;
use super::shards::{SwitchWrite, get_shard_assignment, write_shard_assignment};
use crate::model::{SealReport, ShardAssignmentChangeOp, ShardKey};
use crate::store::{AssignmentWrite, StoreError, StoreResult};

/// [`crate::store::ControlPlaneStore::seal_shard`].
pub(super) async fn seal_shard(
    store: &InMemoryStore,
    key: &ShardKey,
    expected_generation: u64,
) -> StoreResult<AssignmentWrite> {
    rewrite(store, key, expected_generation, SwitchWrite::Order).await
}

/// [`crate::store::ControlPlaneStore::give_up_seal`].
pub(super) async fn give_up_seal(
    store: &InMemoryStore,
    key: &ShardKey,
    generation: u64,
    sealer: &str,
) -> StoreResult<AssignmentWrite> {
    rewrite(store, key, generation, SwitchWrite::GiveUp(sealer)).await
}

/// Name the shard's leader and set again at the next generation, over
/// `expected_generation` only. The write checks the switch record under its
/// own lock.
async fn rewrite(
    store: &InMemoryStore,
    key: &ShardKey,
    expected_generation: u64,
    switch: SwitchWrite<'_>,
) -> StoreResult<AssignmentWrite> {
    let current = match get_shard_assignment(store, key).await {
        Ok(current) => current,
        Err(StoreError::NotFound(_)) => return Ok(AssignmentWrite::Stale { current: None }),
        Err(err) => return Err(err),
    };
    write_shard_assignment(
        store,
        current,
        Some(Some(expected_generation)),
        None,
        false,
        None,
        0,
        false,
        false,
        switch,
    )
    .await
}

/// [`crate::store::ControlPlaneStore::accept_seal_report`]. Deterministic,
/// as a Raft apply must be: decided from the assignment this store holds.
pub(super) async fn accept_seal_report(
    store: &InMemoryStore,
    key: &ShardKey,
    report: SealReport,
    sealer: &str,
    keep_ordered: bool,
) -> StoreResult<AssignmentWrite> {
    let mut state = store.shards.write().await;
    let Some(existing) = state.records.get(key) else {
        return Ok(AssignmentWrite::Stale { current: None });
    };
    let stale = AssignmentWrite::Stale {
        current: Some(existing.generation),
    };
    // The report this shard switched on, sent again: its answer was lost.
    // Only the sealer is answered as if it won.
    if let Some(switch) = existing
        .switch
        .as_ref()
        .filter(|switch| switch.report.as_ref() == Some(&report))
    {
        if switch.sealer != sealer {
            return Err(not_sealer(sealer));
        }
        return Ok(AssignmentWrite::Written(existing.clone()));
    }
    // The compare: the seal this report is for still stands. A later
    // generation gave it up, and a recorded report already won it.
    let Some(switch) = existing.sealing() else {
        return Ok(stale);
    };
    if report.generation != switch.generation {
        return Ok(stale);
    }
    if switch.sealer != sealer {
        return Err(not_sealer(sealer));
    }
    report
        .names_voters_of(existing, switch)
        .map_err(|err| StoreError::Conflict(err.to_string()))?;
    let mut stored = existing.clone();
    stored.switch = Some(crate::model::ShardSwitch {
        report: Some(report),
        ..switch.clone()
    });
    // Epoch 0 of the Raft group starts here, before any target. Below the
    // level that keeps the list none is started, so the shard reads as
    // unknown for the whole epoch.
    if keep_ordered {
        stored.start_raft_ordered();
    }
    // In place, at the seal's generation: a later one would be a hybrid
    // promotion, and the base every voter starts from is this generation's.
    state.records.insert(key.clone(), stored.clone());
    state.record(ShardAssignmentChangeOp::Updated, key, Some(stored.clone()));
    Ok(AssignmentWrite::Written(stored))
}

/// What a copy id report carries from level 41: the stream incarnation of
/// the copy's group, and whether the cluster runs durable streams on Raft.
/// Only such a report is taken for a shard that started on Raft.
pub(super) struct NativeShard {
    shard_incarnation: u64,
    raft_streams: bool,
}

/// [`crate::store::ControlPlaneStore::record_raft_copy_id`]. Deterministic,
/// as a Raft apply must be: decided from the record and acknowledgements
/// this store holds.
#[allow(clippy::too_many_arguments)]
pub(super) async fn record_raft_copy_id(
    store: &InMemoryStore,
    key: &ShardKey,
    node_id: &str,
    node_registered_at_millis: u64,
    node_incarnation: u64,
    copy_id: u64,
    keep_ordered: bool,
    native: Option<NativeShard>,
) -> StoreResult<bool> {
    // Read before the locks below, which no stream write takes. A stream's
    // durability never changes, and a recreated one has another incarnation.
    let durable_stream = store
        .streams
        .read()
        .await
        .get(&crate::model::StreamKey {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
        })
        .map(|stream| {
            (
                stream.incarnation,
                stream.durable,
                stream.replication_factor,
            )
        });
    // Nodes, then shards, then acknowledgements, the order a node delete
    // takes them in. Held to the write, so no registration or ack lands
    // between the gate and the target.
    let nodes = store.nodes.read().await;
    let mut state = store.shards.write().await;
    let placement = store.placement.read().await;
    let Some(existing) = state.records.get(key) else {
        return Err(StoreError::NotFound("shard assignment".into()));
    };
    // A copy belongs to one stream incarnation's group, so a late report
    // from before a delete and recreate never lands on the new group.
    if let Some(native) = &native
        && native.shard_incarnation != existing.incarnation
    {
        return Err(StoreError::Conflict(format!(
            "the report is for stream incarnation {}, the shard is at {}",
            native.shard_incarnation, existing.incarnation
        )));
    }
    let started_on_raft = native.as_ref().is_some_and(|native| {
        native.raft_streams
            && existing.switch.is_none()
            && existing.key.kind == crate::model::ShardKind::Stream
            && durable_stream.is_some_and(|(incarnation, durable, _)| {
                durable && incarnation == existing.incarnation
            })
    });
    // A target names only the set it was written from, so one from a set
    // placement has not filled yet would form a group short of the
    // stream's replica count. Decided here, not by the reporting broker.
    if started_on_raft {
        let wanted = durable_stream.map_or(0, |(_, _, factor)| factor as usize);
        let placed = 1 + existing.replicas.len();
        if placed < wanted {
            return Err(StoreError::Conflict(format!(
                "placement names {placed} brokers, the stream's replica count is {wanted}"
            )));
        }
    }
    if !existing.switched() && !started_on_raft {
        return Err(StoreError::Conflict(
            "the shard is not on Raft: it neither switched from the hybrid nor \
             started on Raft"
                .into(),
        ));
    }
    if existing.leader != node_id && !existing.replicas.iter().any(|r| r == node_id) {
        return Err(StoreError::Conflict(format!(
            "{node_id} is not in the shard's set"
        )));
    }
    // Under the nodes lock, so no registration lands between this and the
    // write: a wiped copy's old id, retried late, never replaces the id its
    // successor process reported. The registration time too, since a node
    // registered again after a delete restarts at incarnation 0.
    if nodes.records.get(node_id).is_none_or(|node| {
        node.status.registered_at_millis != node_registered_at_millis
            || node.status.incarnation != node_incarnation
    }) {
        return Err(StoreError::Conflict(format!(
            "the report is not from {node_id}'s registered process"
        )));
    }
    if existing.raft_copy_ids.get(node_id) == Some(&copy_id) {
        return Ok(false);
    }
    let mut stored = existing.clone();
    if started_on_raft && keep_ordered {
        stored.start_native_raft_ordered();
    }
    stored.raft_copy_ids.insert(node_id.to_string(), copy_id);
    let moved = stored
        .settle_raft_target()
        .map_err(|err| StoreError::Conflict(err.to_string()))?;
    // In the same write as the target, so the list holds it before any
    // broker can read it.
    if moved {
        stored.order_raft_target(keep_ordered);
    }
    if moved && !super::all_registered_acked(&nodes, &placement) {
        return Err(StoreError::Conflict(
            "a registered broker has not acknowledged the placement incarnation; \
             the shard's Raft target waits for it"
                .into(),
        ));
    }
    // In place: a switched shard's generation is the base its voters
    // started from.
    state.records.insert(key.clone(), stored.clone());
    state.record(ShardAssignmentChangeOp::Updated, key, Some(stored));
    Ok(true)
}

/// [`crate::store::ControlPlaneStore::record_raft_floors`]. Deterministic:
/// each entry is decided from the record it lands on alone.
///
/// A raise is written in place and records no change: leaders report every
/// second, and a change per report would push placement's changes out of the
/// feed's window. Nothing that watches the feed reads the floor.
pub(super) async fn record_raft_floors(
    store: &InMemoryStore,
    node_id: &str,
    floors: &[crate::model::RaftFloorReport],
) -> u32 {
    let mut state = store.shards.write().await;
    let mut raised = 0;
    for report in floors {
        let Some(record) = state.records.get_mut(&report.key) else {
            tracing::debug!(
                node_id,
                name = %report.key.stream,
                shard = report.key.shard,
                "skipped a Raft floor report for a shard with no assignment",
            );
            continue;
        };
        match record.raise_raft_floor(report) {
            Ok(true) => raised += 1,
            Ok(false) => {}
            // Brokers report cache groups too, every second, so a shard
            // with no floor to keep is not worth a warning.
            Err(crate::model::RaftFloorError::NotSwitched) => tracing::debug!(
                node_id,
                name = %report.key.stream,
                shard = report.key.shard,
                "skipped a Raft floor report for a shard not on Raft",
            ),
            Err(err) => tracing::warn!(
                node_id,
                name = %report.key.stream,
                shard = report.key.shard,
                error = %err,
                "skipped a Raft floor report",
            ),
        }
    }
    raised
}

/// Only the node a seal was ordered from reports it or gives it up.
pub(super) fn not_sealer(node: &str) -> StoreError {
    StoreError::Conflict(format!("the seal was not ordered from {node}"))
}

impl InMemoryStore {
    pub(crate) async fn apply_seal_shard(
        &self,
        key: &ShardKey,
        expected_generation: u64,
    ) -> StoreResult<AssignmentWrite> {
        seal_shard(self, key, expected_generation).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn apply_record_raft_copy_id(
        &self,
        key: &ShardKey,
        node_id: &str,
        node_registered_at_millis: u64,
        node_incarnation: u64,
        copy_id: u64,
        keep_ordered: bool,
        shard_incarnation: Option<u64>,
        in_group: bool,
    ) -> StoreResult<bool> {
        // A group decides the engine from its recorded identity alone, so
        // every member applies the same; one with none recorded holds no
        // shard started on Raft.
        let raft_streams = match self.recorded_identity().await {
            Some(identity) => identity.shard_engine == crate::config::ShardEngine::Raft.as_str(),
            None => !in_group && self.shard_engine == crate::config::ShardEngine::Raft.as_str(),
        };
        let native = shard_incarnation.map(|shard_incarnation| NativeShard {
            shard_incarnation,
            raft_streams,
        });
        record_raft_copy_id(
            self,
            key,
            node_id,
            node_registered_at_millis,
            node_incarnation,
            copy_id,
            keep_ordered,
            native,
        )
        .await
    }

    pub(crate) async fn apply_record_raft_floors(
        &self,
        node_id: &str,
        floors: &[crate::model::RaftFloorReport],
    ) -> u32 {
        record_raft_floors(self, node_id, floors).await
    }

    pub(crate) async fn apply_accept_seal_report(
        &self,
        key: &ShardKey,
        report: SealReport,
        sealer: &str,
        keep_ordered: bool,
    ) -> StoreResult<AssignmentWrite> {
        accept_seal_report(self, key, report, sealer, keep_ordered).await
    }

    pub(crate) async fn apply_give_up_seal(
        &self,
        key: &ShardKey,
        generation: u64,
        sealer: &str,
    ) -> StoreResult<AssignmentWrite> {
        give_up_seal(self, key, generation, sealer).await
    }
}

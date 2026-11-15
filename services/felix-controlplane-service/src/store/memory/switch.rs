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
    // In place, at the seal's generation: a later one would be a hybrid
    // promotion, and the base every voter starts from is this generation's.
    state.records.insert(key.clone(), stored.clone());
    state.record(ShardAssignmentChangeOp::Updated, key, Some(stored.clone()));
    Ok(AssignmentWrite::Written(stored))
}

/// [`crate::store::ControlPlaneStore::record_raft_copy_id`]. Deterministic,
/// as a Raft apply must be: decided from the record and acknowledgements
/// this store holds.
pub(super) async fn record_raft_copy_id(
    store: &InMemoryStore,
    key: &ShardKey,
    node_id: &str,
    node_incarnation: u64,
    copy_id: u64,
) -> StoreResult<bool> {
    // Nodes, then shards, then acknowledgements, the order a node delete
    // takes them in. Held to the write, so no registration or ack lands
    // between the gate and the target.
    let nodes = store.nodes.read().await;
    let mut state = store.shards.write().await;
    let placement = store.placement.read().await;
    let Some(existing) = state.records.get(key) else {
        return Err(StoreError::NotFound("shard assignment".into()));
    };
    if !existing.switched() {
        return Err(StoreError::Conflict(
            "the shard has not switched to Raft".into(),
        ));
    }
    if existing.leader != node_id && !existing.replicas.iter().any(|r| r == node_id) {
        return Err(StoreError::Conflict(format!(
            "{node_id} is not in the shard's set"
        )));
    }
    // Under the nodes lock, so no registration lands between this and the
    // write: a wiped copy's old id, retried late, never replaces the id its
    // successor process reported.
    if nodes
        .records
        .get(node_id)
        .is_none_or(|node| node.status.incarnation != node_incarnation)
    {
        return Err(StoreError::Conflict(format!(
            "the report is not from {node_id}'s registered process"
        )));
    }
    if existing.raft_copy_ids.get(node_id) == Some(&copy_id) {
        return Ok(false);
    }
    let mut stored = existing.clone();
    stored.raft_copy_ids.insert(node_id.to_string(), copy_id);
    let moved = stored
        .settle_raft_target()
        .map_err(|err| StoreError::Conflict(err.to_string()))?;
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

    pub(crate) async fn apply_record_raft_copy_id(
        &self,
        key: &ShardKey,
        node_id: &str,
        node_incarnation: u64,
        copy_id: u64,
    ) -> StoreResult<bool> {
        record_raft_copy_id(self, key, node_id, node_incarnation, copy_id).await
    }

    pub(crate) async fn apply_accept_seal_report(
        &self,
        key: &ShardKey,
        report: SealReport,
        sealer: &str,
    ) -> StoreResult<AssignmentWrite> {
        accept_seal_report(self, key, report, sealer).await
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

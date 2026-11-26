//! A shard's switch to Raft: ordering the seal, recording its one report,
//! and giving it up. See [`crate::model::ShardSwitch`].
//!
//! Each is a compare-and-swap on the seal order standing at the shard's
//! generation. A report is recorded in place and ends the order; a give-up
//! moves the shard to a later generation, which ends it too. So one of them
//! wins a generation, never both, and never two reports.
use super::InMemoryStore;
use super::shards::{SwitchWrite, get_shard_assignment, write_shard_assignment};
use crate::model::{SealReport, ShardAssignmentChangeOp, ShardKey, ShardKind};
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
        // The current set again, which a held set allows.
        super::SetHold::NONE,
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

/// Which rules a floor entry applies, by the metadata level it was
/// proposed at ([`record_raft_floors`]).
#[derive(Debug, Clone, Copy)]
pub(crate) struct FloorRules {
    pub(crate) mark_formed: bool,
    pub(crate) vouched_voters: bool,
    pub(crate) note_leader: bool,
}

/// What a copy id report carries from level 41: the stream or cache
/// incarnation of the copy's group, and whether the cluster runs durable
/// shards on Raft.
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
    hold_formed: bool,
    group_epoch: Option<u64>,
    epochs: bool,
) -> StoreResult<bool> {
    // The stream or cache the shard belongs to, read before the locks below,
    // which no stream or cache write takes. A stream's durability never
    // changes, and a recreated stream or cache has another incarnation.
    let owner = match key.kind {
        ShardKind::Stream => store
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
            }),
        // The control plane keeps no durability for a cache. A broker runs
        // one on Raft only when its cache is a log, and only then has a
        // copy id to report.
        ShardKind::Cache => store
            .caches
            .read()
            .await
            .get(&crate::model::CacheKey {
                tenant_id: key.tenant_id.clone(),
                namespace: key.namespace.clone(),
                cache: key.stream.clone(),
            })
            .map(|cache| (cache.incarnation, true, cache.replication_factor)),
    };
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
            && owner.is_some_and(|(incarnation, durable, _)| {
                durable && incarnation == existing.incarnation
            })
    });
    // A target names only the set it was written from, so one from a set
    // placement has not filled yet would form a group short of the
    // replica count. Decided here, not by the reporting broker.
    if started_on_raft {
        let wanted = owner.map_or(0, |(_, _, factor)| factor as usize);
        let placed = 1 + existing.replicas.len();
        if placed < wanted {
            return Err(StoreError::Conflict(format!(
                "placement names {placed} brokers, the replica count is {wanted}"
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
    // A copy reports the epoch it runs, so a stale view of an earlier
    // epoch, such as a directory restored from an old backup, never names
    // the current group's member (#2882). A report without one is from a
    // broker that predates re-forms and is only taken at epoch 0.
    let epoch = existing.placed.as_ref().map_or(0, |placed| placed.epoch);
    if epochs && group_epoch.unwrap_or(0) != epoch {
        return Err(StoreError::Conflict(format!(
            "the report is for group epoch {}, the shard is at {epoch}",
            group_epoch.unwrap_or(0)
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
        .settle_raft_target(hold_formed)
        .map_err(|err| StoreError::Conflict(err.to_string()))?;
    // In the same write as the target, so the list holds it before any
    // broker can read it.
    if moved {
        stored.order_raft_target(keep_ordered);
        if epochs {
            stored.seed_from_first_target();
        }
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
///
/// `mark_formed`: a report from a broker the stored target names also marks
/// the group formed, also in place; false for a Raft entry proposed below
/// the level that knows the mark.
///
/// `vouched_voters`: voters count only from a reporter
/// [`crate::model::ShardAssignment::may_report_voters`] admits, whatever
/// the floor did with the report (#2909); false below level 48.
///
/// `note_leader`: each report also records its reporter as the broker the
/// group elected ([`crate::model::ShardAssignment::note_raft_leader`]), in
/// place like the floor; false below level 52.
pub(super) async fn record_raft_floors(
    store: &InMemoryStore,
    node_id: &str,
    floors: &[crate::model::RaftFloorReport],
    rules: FloorRules,
) -> u32 {
    let FloorRules {
        mark_formed,
        vouched_voters,
        note_leader,
    } = rules;
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
        // Only a leader reports, so a member of the target leads a group at
        // this incarnation and epoch. Before the floor, which a shard started
        // on Raft does not keep.
        let in_target = record
            .raft_target
            .as_ref()
            .is_some_and(|target| target.members.contains_key(node_id));
        if mark_formed && in_target {
            record.mark_raft_formed(report.incarnation, report.epoch);
        }
        // A set change clears the target until the new one is stored, and
        // the voters may be what lets it be stored, so meanwhile a voter
        // already kept may report.
        let may_report = match &report.voters {
            Some(voters) if vouched_voters => record.may_report_voters(node_id, voters),
            Some(voters) => {
                voters.contains_key(node_id)
                    && (in_target
                        || (record.raft_target.is_none()
                            && record
                                .raft_voters
                                .as_ref()
                                .is_some_and(|held| held.members.contains_key(node_id))))
            }
            None => false,
        };
        if note_leader {
            record.note_raft_leader(node_id, report);
        }
        let floor = record.raise_raft_floor(report);
        // Only a proposer at level 46 leaves voters in. Below 48 they count
        // only in a report the floor took; from 48 the reporter gate alone
        // decides, since any broker can push the floor past honest reports.
        let taken = vouched_voters
            || matches!(
                floor,
                Ok(_) | Err(crate::model::RaftFloorError::NotSwitched)
            );
        if may_report
            && taken
            && let Some(voters) = &report.voters
        {
            record.note_raft_voters(report, voters, !vouched_voters);
        }
        match floor {
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
            // A leader cut off before a re-form reports its old epoch again
            // on every refresh; the floor check already shuts it out.
            Err(err @ crate::model::RaftFloorError::OtherEpoch { .. }) => tracing::debug!(
                node_id,
                name = %report.key.stream,
                shard = report.key.shard,
                error = %err,
                "skipped a Raft floor report from another reform epoch",
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

/// [`crate::store::ControlPlaneStore::start_reformed_epoch`]. Decided from
/// the record and the command alone, so every member writes the same epoch.
pub(super) async fn start_reformed_epoch(
    store: &InMemoryStore,
    key: &ShardKey,
    start: &crate::model::ReformEpochStart,
) -> StoreResult<crate::model::ShardAssignment> {
    let mut state = store.shards.write().await;
    let Some(existing) = state.records.get(key) else {
        return Err(StoreError::NotFound("shard assignment".into()));
    };
    let mut stored = existing.clone();
    stored
        .start_reform(start)
        .map_err(|err| StoreError::Conflict(err.to_string()))?;
    // In place, like a copy id: the generation of a switched shard is the
    // base its voters started from.
    state.records.insert(key.clone(), stored.clone());
    state.record(ShardAssignmentChangeOp::Updated, key, Some(stored.clone()));
    Ok(stored)
}

/// [`crate::store::ControlPlaneStore::abandon_raft_epoch`]. Decided from the
/// store's records and the command alone, so every member writes the same
/// epoch.
pub(super) async fn abandon_raft_epoch(
    store: &InMemoryStore,
    key: &ShardKey,
    start: &crate::model::AbandonEpochStart,
) -> StoreResult<crate::model::ShardAssignment> {
    // Nodes, then shards, as a node delete takes them. The dead set is read
    // under the same locks as the write, so the check and the write are one
    // step even if the operator's read is older.
    let nodes = store.nodes.read().await;
    let dead = nodes.dead_node_ids();
    let mut state = store.shards.write().await;
    let Some(existing) = state.records.get(key) else {
        return Err(StoreError::NotFound("shard assignment".into()));
    };
    if existing.abandoned_by(start) {
        return Ok(existing.clone());
    }
    let stored = existing
        .abandon_epoch(start, &dead)
        .map_err(|err| StoreError::Conflict(err.to_string()))?;
    state.records.insert(key.clone(), stored.clone());
    state.record(ShardAssignmentChangeOp::Updated, key, Some(stored.clone()));
    Ok(stored)
}

/// Only the node a seal was ordered from reports it or gives it up.
pub(super) fn not_sealer(node: &str) -> StoreError {
    StoreError::Conflict(format!("the seal was not ordered from {node}"))
}

impl InMemoryStore {
    /// `StartReformedEpoch`.
    pub(crate) async fn apply_start_reformed_epoch(
        &self,
        key: &ShardKey,
        start: &crate::model::ReformEpochStart,
    ) -> StoreResult<crate::model::ShardAssignment> {
        start_reformed_epoch(self, key, start).await
    }

    /// `AbandonRaftEpoch`.
    pub(crate) async fn apply_abandon_raft_epoch(
        &self,
        key: &ShardKey,
        start: &crate::model::AbandonEpochStart,
    ) -> StoreResult<crate::model::ShardAssignment> {
        abandon_raft_epoch(self, key, start).await
    }

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
        hold_formed: bool,
        group_epoch: Option<u64>,
        epochs: bool,
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
            hold_formed,
            group_epoch,
            epochs,
        )
        .await
    }

    pub(crate) async fn apply_record_raft_floors(
        &self,
        node_id: &str,
        floors: &[crate::model::RaftFloorReport],
        rules: FloorRules,
    ) -> u32 {
        record_raft_floors(self, node_id, floors, rules).await
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

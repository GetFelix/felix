//! The metadata state machine: the in-memory store, driven by commands.
//!
//! This is the application half of the Raft seam
//! ([`crate::raft::AppStateMachine`]), exactly as the design promised: no
//! new state container, just [`InMemoryStore`] — already implementing every
//! store trait, already contract-tested against Postgres — fed committed
//! [`MetaCommand`]s in log order. Its snapshot is the store's deterministic
//! export, so two replicas that applied the same log serialize
//! byte-identical state; the harness in `state_machine/tests.rs` holds it
//! to that.
//!
//! Nothing here consults a clock or generates a value: heartbeat and expiry
//! commands carry their timestamps, bootstrap carries its candidate keys.
//! If a command cannot be decoded — a newer envelope version, an unknown
//! operation — the answer is a [`MetaError::Unsupported`](super::command::MetaError::Unsupported) *response*,
//! identical on every replica running this build. A newer leader that did
//! apply it has moved on without this member, which is why the upgrade order
//! in `docs/metadata-raft-design.md` matters and why the event is counted.
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::raft::AppStateMachine;
use crate::store::memory::{FleetRule, InMemoryStore};
use crate::store::raft::command::{
    MetaCommand, MetaResponse, MetaResult, decode_command, encode_result,
};
use crate::store::{AssignmentWrite, AuthStore, ControlPlaneStore};

/// How many applied request ids to remember.
///
/// Bounds what a retry can be answered from. It only has to outlive one
/// write's budget -- `RaftStore::write` gives up after `write_timeout`, so an
/// id older than the commands applied since then can never be re-proposed by
/// anyone still waiting. A few thousand is far past that on any real load and
/// costs a few hundred kilobytes.
///
/// Evicted in apply order rather than by age: every replica applies the same
/// log in the same order, so the same ids are forgotten at the same point.
/// A clock-based bound would not be deterministic, and a state machine whose
/// replicas disagree is worse than one that forgets early.
const APPLIED_IDS_KEPT: usize = 4096;

/// Responses to the writes already applied, by request id.
///
/// A retry re-proposes a command that may have committed; this is what lets it
/// be answered with what that command actually returned rather than with the
/// conflict its effect now produces (#529).
#[derive(Default, Clone, Serialize, Deserialize)]
struct AppliedIds {
    /// Apply order, for deterministic eviction.
    order: VecDeque<String>,
    /// Ordered, because this is serialized into the snapshot and two replicas
    /// must produce byte-identical ones -- a `HashMap` here would serialize in
    /// arbitrary order and show up as replicas disagreeing. The determinism
    /// harness in this module's tests is what catches that.
    responses: BTreeMap<String, Vec<u8>>,
}

impl AppliedIds {
    fn get(&self, rid: &str) -> Option<&Vec<u8>> {
        self.responses.get(rid)
    }

    fn insert(&mut self, rid: String, response: Vec<u8>) {
        if self.responses.insert(rid.clone(), response).is_none() {
            self.order.push_back(rid);
        }
        while self.order.len() > APPLIED_IDS_KEPT {
            if let Some(evicted) = self.order.pop_front() {
                self.responses.remove(&evicted);
            }
        }
    }
}

/// What a snapshot carries: the store's state, and the ids applied into it.
///
/// A wrapper rather than a field on the store's exported state, because
/// deduplicating Raft proposals is this layer's concern and not the metadata
/// store's.
#[derive(Serialize, Deserialize)]
struct Snapshot {
    state: crate::store::export::ExportedState,
    #[serde(default)]
    applied: AppliedIds,
}

pub struct MetadataStateMachine {
    store: Arc<InMemoryStore>,
    applied: tokio::sync::RwLock<AppliedIds>,
    node_reset: std::sync::OnceLock<NodeReset>,
}

/// Called with a node id when that node's record is replaced or removed, and
/// with `None` when the whole state is.
pub(crate) type NodeReset = Box<dyn Fn(Option<&str>) + Send + Sync>;

impl MetadataStateMachine {
    pub fn new(store: Arc<InMemoryStore>) -> Self {
        Self {
            store,
            applied: tokio::sync::RwLock::new(AppliedIds::default()),
            node_reset: std::sync::OnceLock::new(),
        }
    }

    /// Have `reset` told about node records being replaced, so state kept
    /// beside the log (the leader's heartbeat view) never outlives the
    /// record it describes. Not replicated state: nothing it does may feed
    /// back into apply.
    pub(crate) fn on_node_reset(&self, reset: NodeReset) {
        let _ = self.node_reset.set(reset);
    }

    fn node_reset(&self, node_id: Option<&str>) {
        if let Some(reset) = self.node_reset.get() {
            reset(node_id);
        }
    }

    /// The applied state, for serving reads. Reads need no consensus hop —
    /// the watch contract is pull-based and eventually consistent — which is
    /// the whole reason the state machine is the serving store itself.
    pub fn store(&self) -> &Arc<InMemoryStore> {
        &self.store
    }

    /// Apply one already-decoded command. Public for the determinism
    /// harness; the Raft path arrives through [`AppStateMachine::apply`].
    pub async fn dispatch(&self, command: MetaCommand) -> MetaResult {
        let store = self.store.as_ref();
        match command {
            MetaCommand::CreateTenant { tenant } => store
                .create_tenant(tenant)
                .await
                .map(|tenant| MetaResponse::Tenant { tenant })
                .map_err(Into::into),
            MetaCommand::DeleteTenant { tenant_id } => store
                .delete_tenant(&tenant_id)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::CreateNamespace { namespace } => store
                .create_namespace(namespace)
                .await
                .map(|namespace| MetaResponse::Namespace { namespace })
                .map_err(Into::into),
            MetaCommand::DeleteNamespace { key } => store
                .delete_namespace(&key)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::CreateStream { stream } => store
                .create_stream(stream)
                .await
                .map(|stream| MetaResponse::Stream { stream })
                .map_err(Into::into),
            MetaCommand::PatchStream { key, patch } => store
                .patch_stream(&key, patch)
                .await
                .map(|stream| MetaResponse::Stream { stream })
                .map_err(Into::into),
            MetaCommand::DeleteStream { key } => store
                .delete_stream(&key)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::CreateCache { cache } => store
                .create_cache(cache)
                .await
                .map(|cache| MetaResponse::Cache { cache })
                .map_err(Into::into),
            MetaCommand::PatchCache { key, patch } => store
                .patch_cache(&key, patch)
                .await
                .map(|cache| MetaResponse::Cache { cache })
                .map_err(Into::into),
            MetaCommand::DeleteCache { key } => store
                .delete_cache(&key)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::RegisterNode { node } => {
                let registered = store.apply_register_node(node, FleetRule::Ignore).await?;
                self.node_reset(Some(&registered.node_id));
                Ok(MetaResponse::Node { node: registered })
            }
            MetaCommand::RegisterNodeInFleet { node } => {
                let registered = store.apply_register_node(node, FleetRule::Enforce).await?;
                self.node_reset(Some(&registered.node_id));
                let fleet = store.enabled_fleet_features().await?;
                Ok(MetaResponse::RegisteredNode {
                    node: registered,
                    fleet,
                })
            }
            MetaCommand::FinalizeFleetFeature { feature } => store
                .finalize_fleet_feature(&feature)
                .await
                .map(|enabled| MetaResponse::FleetFeatures { enabled })
                .map_err(Into::into),
            MetaCommand::PatchNode { node_id, patch } => store
                .patch_node(&node_id, patch)
                .await
                .map(|node| MetaResponse::Node { node })
                .map_err(Into::into),
            MetaCommand::DeleteNode { node_id } => {
                store.delete_node(&node_id).await?;
                self.node_reset(Some(&node_id));
                Ok(MetaResponse::Unit)
            }
            MetaCommand::RecordNodeHeartbeat {
                node_id,
                incarnation,
                at_millis,
            } => store
                .apply_node_heartbeat(&node_id, incarnation, at_millis)
                .await
                .map(|node| MetaResponse::Node { node })
                .map_err(Into::into),
            MetaCommand::ExpireStaleNodes {
                expiry_before_millis,
            } => store
                .expire_stale_nodes(expiry_before_millis)
                .await
                .map(|nodes| MetaResponse::Nodes { nodes })
                .map_err(Into::into),
            MetaCommand::SetNodeLifecycle { node_id, lifecycle } => store
                .set_node_lifecycle(&node_id, lifecycle)
                .await
                .map(|node| MetaResponse::MaybeNode { node })
                .map_err(Into::into),
            MetaCommand::PutShardAssignment { assignment } => store
                .put_shard_assignment(assignment)
                .await
                .map(|assignment| MetaResponse::Assignment { assignment })
                .map_err(Into::into),
            MetaCommand::PutShardAssignmentIf {
                assignment,
                expected_generation,
            } => store
                .put_shard_assignment_unfenced_if(assignment, expected_generation)
                .await
                .map(assignment_written)
                .map_err(Into::into),
            MetaCommand::PutShardAssignmentFenced {
                assignment,
                expected_generation,
                fence,
            } => store
                .put_shard_assignment_if(assignment, expected_generation, fence)
                .await
                .map(assignment_written)
                .map_err(Into::into),
            MetaCommand::TakePlacementLease { holder } => {
                let lease = store.take_placement_lease(&holder).await;
                Ok(MetaResponse::PlacementLease {
                    token: lease.token,
                    taken: lease.taken,
                })
            }
            MetaCommand::RecordReplicaReport { report, leader } => store
                .apply_replica_report(report, leader.as_deref())
                .await
                .map(|written| match written {
                    crate::store::ReportWrite::Stored => MetaResponse::Unit,
                    crate::store::ReportWrite::Stale => MetaResponse::StaleReport,
                    crate::store::ReportWrite::NotLeader => MetaResponse::NotLeaderReport,
                })
                .map_err(Into::into),
            MetaCommand::SetMovesPaused { paused } => store
                .set_moves_paused(paused)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::DeleteShardAssignment { key } => store
                .delete_shard_assignment(&key)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::InsertRefreshToken { token } => store
                .insert_refresh_token(token)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::TakeRefreshToken {
                tenant_id,
                token_id,
                now_secs,
            } => store
                .take_refresh_token(&tenant_id, &token_id, now_secs)
                .await
                .map(|take| MetaResponse::RefreshTokenTake { take })
                .map_err(Into::into),
            MetaCommand::RevokeRefreshFamily {
                tenant_id,
                family_id,
            } => store
                .revoke_refresh_family(&tenant_id, &family_id)
                .await
                .map(|count| MetaResponse::Count { count })
                .map_err(Into::into),
            MetaCommand::RevokeRefreshTokensForPrincipal {
                tenant_id,
                principal_id,
            } => store
                .revoke_refresh_tokens_for_principal(&tenant_id, &principal_id)
                .await
                .map(|count| MetaResponse::Count { count })
                .map_err(Into::into),
            MetaCommand::PurgeExpiredRefreshTokens { before_secs } => store
                .purge_expired_refresh_tokens(before_secs)
                .await
                .map(|count| MetaResponse::Count { count })
                .map_err(Into::into),
            MetaCommand::UpsertIdpIssuer { tenant_id, issuer } => store
                .upsert_idp_issuer(&tenant_id, issuer)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::DeleteIdpIssuer { tenant_id, issuer } => store
                .delete_idp_issuer(&tenant_id, &issuer)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::AddRbacPolicy { tenant_id, policy } => store
                .add_rbac_policy(&tenant_id, policy)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::AddRbacGrouping {
                tenant_id,
                grouping,
            } => store
                .add_rbac_grouping(&tenant_id, grouping)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::RemoveRbacPolicy { tenant_id, policy } => store
                .remove_rbac_policy(&tenant_id, policy)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::RemoveRbacGrouping {
                tenant_id,
                grouping,
            } => store
                .remove_rbac_grouping(&tenant_id, grouping)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::StageSigningKey { tenant_id, key } => store
                .stage_signing_key(&tenant_id, key)
                .await
                .map(|keys| MetaResponse::SigningKeys { keys })
                .map_err(Into::into),
            MetaCommand::ActivateSigningKey { tenant_id, kid } => store
                .activate_signing_key(&tenant_id, &kid)
                .await
                .map(|keys| MetaResponse::SigningKeys { keys })
                .map_err(Into::into),
            MetaCommand::RetireSigningKey { tenant_id, kid } => store
                .retire_signing_key(&tenant_id, &kid)
                .await
                .map(|keys| MetaResponse::SigningKeys { keys })
                .map_err(Into::into),
            MetaCommand::SetTenantSigningKeys { tenant_id, keys } => store
                .set_tenant_signing_keys(&tenant_id, keys)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::EnsureSigningKeys {
                tenant_id,
                candidate,
            } => match store.get_tenant_signing_keys(&tenant_id).await {
                Ok(existing) => Ok(MetaResponse::SigningKeys { keys: existing }),
                Err(crate::store::StoreError::NotFound(_)) => store
                    .set_tenant_signing_keys(&tenant_id, candidate.clone())
                    .await
                    .map(|()| MetaResponse::SigningKeys { keys: candidate })
                    .map_err(Into::into),
                Err(err) => Err(err.into()),
            },
            MetaCommand::SetTenantAuthBootstrapped {
                tenant_id,
                bootstrapped,
            } => store
                .set_tenant_auth_bootstrapped(&tenant_id, bootstrapped)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::SeedRbac {
                tenant_id,
                policies,
                groupings,
            } => store
                .seed_rbac_policies_and_groupings(&tenant_id, policies, groupings)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::BootstrapTenantAuth { tenant_id, seed } => store
                .bootstrap_tenant_auth(&tenant_id, seed)
                .await
                .map(|keys| MetaResponse::SigningKeys { keys })
                .map_err(Into::into),
            MetaCommand::ExpireNodes { nodes } => {
                let expired = store.expire_nodes(&nodes).await;
                Ok(MetaResponse::Nodes { nodes: expired })
            }
            MetaCommand::CheckpointHeartbeats { beats } => {
                for beat in beats {
                    // A node deleted or re-registered since the leader saw it
                    // answers an error here; that beat is simply moot.
                    let _ = store
                        .apply_node_heartbeat(&beat.node_id, beat.incarnation, beat.at_millis)
                        .await;
                }
                Ok(MetaResponse::Unit)
            }
            MetaCommand::ImportState { state, overwrite } => {
                // A store with any history has consumers whose checkpoints
                // this would silently invalidate; only an operator saying
                // `overwrite` — the restore ceremony — may replace it.
                if !overwrite && !store.is_unused().await {
                    return Err(crate::store::raft::command::MetaError::Conflict(
                        "store already holds state; import requires overwrite".to_string(),
                    ));
                }
                store.import_state(*state).await?;
                self.node_reset(None);
                Ok(MetaResponse::Unit)
            }
        }
    }
}

#[async_trait::async_trait]
impl AppStateMachine for MetadataStateMachine {
    async fn apply(&self, command: &[u8]) -> Vec<u8> {
        // A proposal this state machine has already applied is a retry of a
        // write that succeeded, not a second write. Answering it from the
        // response it produced the first time is what stops a caller being
        // told its own tenant already exists (#529).
        //
        // Read before dispatching, because dispatching is the thing that must
        // not happen twice.
        let rid = crate::store::raft::command::request_id_of(command);
        if let Some(rid) = rid.as_deref()
            && let Some(response) = self.applied.read().await.get(rid)
        {
            metrics::counter!("felix_meta_raft_deduplicated_proposals_total").increment(1);
            return response.clone();
        }

        let result: MetaResult = match decode_command(command) {
            Ok(command) => self.dispatch(command).await,
            Err(err) => {
                // Deterministic among members running the same build, but a
                // newer leader applied this for real: in a mixed-version group
                // this member's state now differs from the leader's. The
                // counter is what the rolling-upgrade runbook watches
                // (docs/metadata-raft-design.md, "Upgrading").
                metrics::counter!("felix_meta_raft_unsupported_commands_total").increment(1);
                tracing::error!(
                    error = %err,
                    "committed raft command this build cannot apply; this member's metadata \
                     may now differ from the leader's until it is upgraded and rebuilt"
                );
                Err(err)
            }
        };
        let encoded = encode_result(&result);
        if let Some(rid) = rid {
            self.applied.write().await.insert(rid, encoded.clone());
        }
        encoded
    }

    async fn snapshot(&self) -> Vec<u8> {
        // One lock acquisition, not two: `order` and `responses` are updated
        // together on every insert, so reading them under separate guards
        // could interleave with a concurrent apply and snapshot a `responses`
        // entry with no matching `order` entry (or the reverse).
        let applied = self.applied.read().await.clone();
        serde_json::to_vec(&Snapshot {
            state: self.store.export_state().await,
            applied,
        })
        .expect("exported state serializes by construction")
    }

    async fn restore(&self, snapshot: &[u8]) {
        // Two shapes: this build's, and one from before applied ids were
        // carried. A rolling upgrade installs the older one, and refusing it
        // would make the upgrade the outage.
        let (state, applied) = match serde_json::from_slice::<Snapshot>(snapshot) {
            Ok(snapshot) => (snapshot.state, snapshot.applied),
            Err(_) => (
                serde_json::from_slice(snapshot).expect("snapshot produced by export_state"),
                AppliedIds::default(),
            ),
        };
        self.store
            .import_state(state)
            .await
            .expect("snapshot version produced by this cluster");
        *self.applied.write().await = applied;
        self.node_reset(None);
    }

    fn restamp(&self, command: &[u8], now_millis: u64) -> Option<Vec<u8>> {
        crate::store::raft::command::restamp(command, now_millis)
    }

    fn version(&self) -> u16 {
        crate::store::raft::command::METADATA_VERSION
    }
}

#[cfg(test)]
mod tests;

fn assignment_written(written: AssignmentWrite) -> MetaResponse {
    match written {
        AssignmentWrite::Written(assignment) => MetaResponse::Assignment { assignment },
        AssignmentWrite::Stale { current } => MetaResponse::StaleAssignment {
            current_generation: current,
        },
        AssignmentWrite::Fenced { token } => MetaResponse::FencedAssignment { token },
    }
}

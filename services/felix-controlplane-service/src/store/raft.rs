//! The Raft-backed store: the third backend behind the store traits.
//!
//! Reads are served straight from the local applied state — the same
//! `InMemoryStore` the state machine applies into — because the broker
//! contract (pull-based snapshots and change feeds, resnapshot signals) is
//! eventually consistent by design; a follower's answer is at worst one
//! poll stale, which is the normal case brokers already handle. Writes are
//! proposed through the Raft seam: encoded as commands, committed on a
//! majority, applied everywhere, with the response decoded back into the
//! store vocabulary. A write sent to a follower forwards to the leader
//! inside the seam; callers cannot tell which instance they hit — the same
//! promise the HTTP API makes for the Postgres backend behind a load
//! balancer.
//!
//! Nothing in this file interprets a command. It is a typed shim between
//! two vocabularies that must not drift: the store traits on one side, the
//! versioned command set on the other.
pub mod command;
pub mod state_machine;

use std::sync::Arc;

use async_trait::async_trait;

use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::auth::refresh_token::{RefreshToken, RefreshTokenTake};
use crate::model::{
    Cache, CacheChange, CacheKey, CachePatchRequest, Namespace, NamespaceChange, NamespaceKey,
    Node, NodeChange, NodeLifecycle, NodePatchRequest, ReplicaReport, ShardAssignment,
    ShardAssignmentChange, ShardKey, Stream, StreamChange, StreamKey, StreamPatchRequest, Tenant,
    TenantChange,
};
use crate::raft::RaftHandle;
use crate::store::memory::InMemoryStore;
use crate::store::raft::command::{MetaCommand, MetaResponse, decode_result, encode_command};
use crate::store::raft::state_machine::MetadataStateMachine;
use crate::store::{
    AssignmentWrite, AuthStore, ChangeSet, ControlPlaneStore, PlacementLease, ReportWrite,
    Snapshot, StoreError, StoreResult, TenantAuthSeed,
};

pub struct RaftStore {
    handle: RaftHandle,
    machine: Arc<MetadataStateMachine>,
}

impl RaftStore {
    pub fn new(handle: RaftHandle, machine: Arc<MetadataStateMachine>) -> Self {
        Self { handle, machine }
    }

    /// The local applied state, for reads.
    fn local(&self) -> &InMemoryStore {
        self.machine.store()
    }

    /// Propose one command and decode its outcome.
    async fn propose(&self, command: MetaCommand) -> StoreResult<MetaResponse> {
        let bytes = self
            .handle
            .write(encode_command(&command))
            .await
            .map_err(StoreError::Unexpected)?;
        let result = decode_result(&bytes).map_err(StoreError::from)?;
        result.map_err(StoreError::from)
    }

    async fn propose_for_keys(&self, command: MetaCommand) -> StoreResult<TenantSigningKeys> {
        match self.propose(command).await? {
            MetaResponse::SigningKeys { keys } => Ok(keys),
            _ => Err(unexpected_shape("signing keys")),
        }
    }
}

#[async_trait]
impl ControlPlaneStore for RaftStore {
    async fn list_tenants(&self) -> StoreResult<Vec<Tenant>> {
        self.local().list_tenants().await
    }

    async fn create_tenant(&self, tenant: Tenant) -> StoreResult<Tenant> {
        match self.propose(MetaCommand::CreateTenant { tenant }).await? {
            MetaResponse::Tenant { tenant } => Ok(tenant),
            _ => Err(unexpected_shape("tenant")),
        }
    }

    async fn delete_tenant(&self, tenant_id: &str) -> StoreResult<()> {
        self.propose(MetaCommand::DeleteTenant {
            tenant_id: tenant_id.to_string(),
        })
        .await
        .map(|_| ())
    }

    async fn tenant_snapshot(&self) -> StoreResult<Snapshot<Tenant>> {
        self.local().tenant_snapshot().await
    }

    async fn tenant_changes(&self, since: u64) -> StoreResult<ChangeSet<TenantChange>> {
        self.local().tenant_changes(since).await
    }

    async fn list_namespaces(&self, tenant_id: &str) -> StoreResult<Vec<Namespace>> {
        self.local().list_namespaces(tenant_id).await
    }

    async fn create_namespace(&self, namespace: Namespace) -> StoreResult<Namespace> {
        match self
            .propose(MetaCommand::CreateNamespace { namespace })
            .await?
        {
            MetaResponse::Namespace { namespace } => Ok(namespace),
            _ => Err(unexpected_shape("namespace")),
        }
    }

    async fn delete_namespace(&self, key: &NamespaceKey) -> StoreResult<()> {
        self.propose(MetaCommand::DeleteNamespace { key: key.clone() })
            .await
            .map(|_| ())
    }

    async fn namespace_snapshot(&self) -> StoreResult<Snapshot<Namespace>> {
        self.local().namespace_snapshot().await
    }

    async fn namespace_changes(&self, since: u64) -> StoreResult<ChangeSet<NamespaceChange>> {
        self.local().namespace_changes(since).await
    }

    async fn list_streams(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Stream>> {
        self.local().list_streams(tenant_id, namespace).await
    }

    async fn get_stream(&self, key: &StreamKey) -> StoreResult<Stream> {
        self.local().get_stream(key).await
    }

    async fn create_stream(&self, stream: Stream) -> StoreResult<Stream> {
        match self.propose(MetaCommand::CreateStream { stream }).await? {
            MetaResponse::Stream { stream } => Ok(stream),
            _ => Err(unexpected_shape("stream")),
        }
    }

    async fn patch_stream(
        &self,
        key: &StreamKey,
        patch: StreamPatchRequest,
    ) -> StoreResult<Stream> {
        match self
            .propose(MetaCommand::PatchStream {
                key: key.clone(),
                patch,
            })
            .await?
        {
            MetaResponse::Stream { stream } => Ok(stream),
            _ => Err(unexpected_shape("stream")),
        }
    }

    async fn delete_stream(&self, key: &StreamKey) -> StoreResult<()> {
        self.propose(MetaCommand::DeleteStream { key: key.clone() })
            .await
            .map(|_| ())
    }

    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>> {
        self.local().stream_snapshot().await
    }

    async fn stream_changes(&self, since: u64) -> StoreResult<ChangeSet<StreamChange>> {
        self.local().stream_changes(since).await
    }

    async fn list_caches(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Cache>> {
        self.local().list_caches(tenant_id, namespace).await
    }

    async fn get_cache(&self, key: &CacheKey) -> StoreResult<Cache> {
        self.local().get_cache(key).await
    }

    async fn create_cache(&self, cache: Cache) -> StoreResult<Cache> {
        match self.propose(MetaCommand::CreateCache { cache }).await? {
            MetaResponse::Cache { cache } => Ok(cache),
            _ => Err(unexpected_shape("cache")),
        }
    }

    async fn patch_cache(&self, key: &CacheKey, patch: CachePatchRequest) -> StoreResult<Cache> {
        match self
            .propose(MetaCommand::PatchCache {
                key: key.clone(),
                patch,
            })
            .await?
        {
            MetaResponse::Cache { cache } => Ok(cache),
            _ => Err(unexpected_shape("cache")),
        }
    }

    async fn delete_cache(&self, key: &CacheKey) -> StoreResult<()> {
        self.propose(MetaCommand::DeleteCache { key: key.clone() })
            .await
            .map(|_| ())
    }

    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>> {
        self.local().cache_snapshot().await
    }

    async fn cache_changes(&self, since: u64) -> StoreResult<ChangeSet<CacheChange>> {
        self.local().cache_changes(since).await
    }

    async fn register_node(&self, node: Node) -> StoreResult<Node> {
        match self.propose(MetaCommand::RegisterNode { node }).await? {
            MetaResponse::Node { node } => Ok(node),
            _ => Err(unexpected_shape("node")),
        }
    }

    async fn get_node(&self, node_id: &str) -> StoreResult<Node> {
        self.local().get_node(node_id).await
    }

    async fn list_nodes(&self) -> StoreResult<Vec<Node>> {
        self.local().list_nodes().await
    }

    async fn patch_node(&self, node_id: &str, patch: NodePatchRequest) -> StoreResult<Node> {
        match self
            .propose(MetaCommand::PatchNode {
                node_id: node_id.to_string(),
                patch,
            })
            .await?
        {
            MetaResponse::Node { node } => Ok(node),
            _ => Err(unexpected_shape("node")),
        }
    }

    async fn delete_node(&self, node_id: &str) -> StoreResult<()> {
        self.propose(MetaCommand::DeleteNode {
            node_id: node_id.to_string(),
        })
        .await
        .map(|_| ())
    }

    async fn record_node_heartbeat(
        &self,
        node_id: &str,
        incarnation: u64,
        at_millis: u64,
    ) -> StoreResult<Node> {
        match self
            .propose(MetaCommand::RecordNodeHeartbeat {
                node_id: node_id.to_string(),
                incarnation,
                at_millis,
            })
            .await?
        {
            MetaResponse::Node { node } => Ok(node),
            _ => Err(unexpected_shape("node")),
        }
    }

    async fn expire_stale_nodes(&self, expiry_before_millis: u64) -> StoreResult<Vec<Node>> {
        match self
            .propose(MetaCommand::ExpireStaleNodes {
                expiry_before_millis,
            })
            .await?
        {
            MetaResponse::Nodes { nodes } => Ok(nodes),
            _ => Err(unexpected_shape("nodes")),
        }
    }

    async fn set_node_lifecycle(
        &self,
        node_id: &str,
        lifecycle: NodeLifecycle,
    ) -> StoreResult<Option<Node>> {
        match self
            .propose(MetaCommand::SetNodeLifecycle {
                node_id: node_id.to_string(),
                lifecycle,
            })
            .await?
        {
            MetaResponse::MaybeNode { node } => Ok(node),
            _ => Err(unexpected_shape("node")),
        }
    }

    async fn node_snapshot(&self) -> StoreResult<Snapshot<Node>> {
        self.local().node_snapshot().await
    }

    async fn node_changes(&self, since: u64) -> StoreResult<ChangeSet<NodeChange>> {
        self.local().node_changes(since).await
    }

    async fn put_shard_assignment(
        &self,
        assignment: ShardAssignment,
    ) -> StoreResult<ShardAssignment> {
        match self
            .propose(MetaCommand::PutShardAssignment { assignment })
            .await?
        {
            MetaResponse::Assignment { assignment } => Ok(assignment),
            _ => Err(unexpected_shape("assignment")),
        }
    }

    async fn put_shard_assignment_if(
        &self,
        assignment: ShardAssignment,
        expected_generation: Option<u64>,
        fence: u64,
    ) -> StoreResult<AssignmentWrite> {
        match self
            .propose(MetaCommand::PutShardAssignmentFenced {
                assignment,
                expected_generation,
                fence,
            })
            .await?
        {
            MetaResponse::Assignment { assignment } => Ok(AssignmentWrite::Written(assignment)),
            MetaResponse::StaleAssignment { current_generation } => Ok(AssignmentWrite::Stale {
                current: current_generation,
            }),
            MetaResponse::FencedAssignment { token } => Ok(AssignmentWrite::Fenced { token }),
            _ => Err(unexpected_shape("assignment")),
        }
    }

    async fn get_shard_assignment(&self, key: &ShardKey) -> StoreResult<ShardAssignment> {
        self.local().get_shard_assignment(key).await
    }

    async fn list_shard_assignments(&self) -> StoreResult<Vec<ShardAssignment>> {
        self.local().list_shard_assignments().await
    }

    async fn list_shard_assignments_for_node(
        &self,
        node_id: &str,
    ) -> StoreResult<Vec<ShardAssignment>> {
        self.local().list_shard_assignments_for_node(node_id).await
    }

    async fn delete_shard_assignment(&self, key: &ShardKey) -> StoreResult<()> {
        self.propose(MetaCommand::DeleteShardAssignment { key: key.clone() })
            .await
            .map(|_| ())
    }

    async fn shard_assignment_snapshot(&self) -> StoreResult<Snapshot<ShardAssignment>> {
        self.local().shard_assignment_snapshot().await
    }

    async fn shard_assignment_changes(
        &self,
        since: u64,
    ) -> StoreResult<ChangeSet<ShardAssignmentChange>> {
        self.local().shard_assignment_changes(since).await
    }

    async fn record_replica_report(&self, report: ReplicaReport) -> StoreResult<ReportWrite> {
        match self
            .propose(MetaCommand::RecordReplicaReport { report })
            .await?
        {
            MetaResponse::Unit => Ok(ReportWrite::Stored),
            MetaResponse::StaleReport => Ok(ReportWrite::Stale),
            _ => Err(unexpected_shape("unit")),
        }
    }

    async fn list_replica_reports(&self) -> StoreResult<Vec<ReplicaReport>> {
        self.local().list_replica_reports().await
    }

    async fn moves_paused(&self) -> StoreResult<bool> {
        self.local().moves_paused().await
    }

    async fn set_moves_paused(&self, paused: bool) -> StoreResult<()> {
        match self.propose(MetaCommand::SetMovesPaused { paused }).await? {
            MetaResponse::Unit => Ok(()),
            _ => Err(unexpected_shape("unit")),
        }
    }

    async fn placement_token(&self) -> StoreResult<u64> {
        self.local().placement_token().await
    }

    /// The caller is the confirmed leader (`LeadershipGate`), so it takes the
    /// lease whatever the expiry. Proposed only when the holder changes: a
    /// renewal needs no log entry, since nobody else takes a lease here.
    async fn acquire_placement_lease(
        &self,
        holder: &str,
        _ttl_millis: u64,
    ) -> StoreResult<Option<PlacementLease>> {
        if self.local().placement_holder().await.as_deref() == Some(holder) {
            return Ok(Some(PlacementLease {
                token: self.local().placement_token().await?,
                taken: false,
            }));
        }
        match self
            .propose(MetaCommand::TakePlacementLease {
                holder: holder.to_string(),
            })
            .await?
        {
            MetaResponse::PlacementLease { token, taken } => {
                Ok(Some(PlacementLease { token, taken }))
            }
            _ => Err(unexpected_shape("placement lease")),
        }
    }

    /// Nothing to give up: the next leader takes the lease when it is
    /// confirmed.
    async fn release_placement_lease(&self, _holder: &str) -> StoreResult<()> {
        Ok(())
    }

    async fn tenant_exists(&self, tenant_id: &str) -> StoreResult<bool> {
        self.local().tenant_exists(tenant_id).await
    }

    async fn namespace_exists(&self, key: &NamespaceKey) -> StoreResult<bool> {
        self.local().namespace_exists(key).await
    }

    /// Ready when this member can serve — the same question readiness asks
    /// the Postgres backend, answered from consensus state: a leader is
    /// known, this member is applying what its log holds, and a leader
    /// answers only while a quorum has recently acknowledged it. All from
    /// local metrics; a probe must never cost a consensus round trip.
    async fn health_check(&self) -> StoreResult<()> {
        self.handle
            .readiness()
            .map_err(|reason| StoreError::Unexpected(anyhow::anyhow!(reason)))
    }

    fn is_durable(&self) -> bool {
        true
    }

    fn backend_name(&self) -> &'static str {
        "raft"
    }
}

#[async_trait]
impl AuthStore for RaftStore {
    async fn list_idp_issuers(&self, tenant_id: &str) -> StoreResult<Vec<IdpIssuerConfig>> {
        self.local().list_idp_issuers(tenant_id).await
    }

    async fn upsert_idp_issuer(&self, tenant_id: &str, issuer: IdpIssuerConfig) -> StoreResult<()> {
        self.propose(MetaCommand::UpsertIdpIssuer {
            tenant_id: tenant_id.to_string(),
            issuer,
        })
        .await
        .map(|_| ())
    }

    async fn delete_idp_issuer(&self, tenant_id: &str, issuer: &str) -> StoreResult<()> {
        self.propose(MetaCommand::DeleteIdpIssuer {
            tenant_id: tenant_id.to_string(),
            issuer: issuer.to_string(),
        })
        .await
        .map(|_| ())
    }

    async fn list_rbac_policies(&self, tenant_id: &str) -> StoreResult<Vec<PolicyRule>> {
        self.local().list_rbac_policies(tenant_id).await
    }

    async fn list_rbac_groupings(&self, tenant_id: &str) -> StoreResult<Vec<GroupingRule>> {
        self.local().list_rbac_groupings(tenant_id).await
    }

    async fn add_rbac_policy(&self, tenant_id: &str, policy: PolicyRule) -> StoreResult<()> {
        self.propose(MetaCommand::AddRbacPolicy {
            tenant_id: tenant_id.to_string(),
            policy,
        })
        .await
        .map(|_| ())
    }

    async fn add_rbac_grouping(&self, tenant_id: &str, grouping: GroupingRule) -> StoreResult<()> {
        self.propose(MetaCommand::AddRbacGrouping {
            tenant_id: tenant_id.to_string(),
            grouping,
        })
        .await
        .map(|_| ())
    }

    async fn remove_rbac_policy(&self, tenant_id: &str, policy: PolicyRule) -> StoreResult<()> {
        self.propose(MetaCommand::RemoveRbacPolicy {
            tenant_id: tenant_id.to_string(),
            policy,
        })
        .await
        .map(|_| ())
    }

    async fn remove_rbac_grouping(
        &self,
        tenant_id: &str,
        grouping: GroupingRule,
    ) -> StoreResult<()> {
        self.propose(MetaCommand::RemoveRbacGrouping {
            tenant_id: tenant_id.to_string(),
            grouping,
        })
        .await
        .map(|_| ())
    }

    async fn get_tenant_signing_keys(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        self.local().get_tenant_signing_keys(tenant_id).await
    }

    async fn stage_signing_key(
        &self,
        tenant_id: &str,
        key: SigningKey,
    ) -> StoreResult<TenantSigningKeys> {
        self.propose_for_keys(MetaCommand::StageSigningKey {
            tenant_id: tenant_id.to_string(),
            key,
        })
        .await
    }

    async fn activate_signing_key(
        &self,
        tenant_id: &str,
        kid: &str,
    ) -> StoreResult<TenantSigningKeys> {
        self.propose_for_keys(MetaCommand::ActivateSigningKey {
            tenant_id: tenant_id.to_string(),
            kid: kid.to_string(),
        })
        .await
    }

    async fn retire_signing_key(
        &self,
        tenant_id: &str,
        kid: &str,
    ) -> StoreResult<TenantSigningKeys> {
        self.propose_for_keys(MetaCommand::RetireSigningKey {
            tenant_id: tenant_id.to_string(),
            kid: kid.to_string(),
        })
        .await
    }

    async fn set_tenant_signing_keys(
        &self,
        tenant_id: &str,
        keys: TenantSigningKeys,
    ) -> StoreResult<()> {
        self.propose(MetaCommand::SetTenantSigningKeys {
            tenant_id: tenant_id.to_string(),
            keys,
        })
        .await
        .map(|_| ())
    }

    async fn tenant_auth_is_bootstrapped(&self, tenant_id: &str) -> StoreResult<bool> {
        self.local().tenant_auth_is_bootstrapped(tenant_id).await
    }

    async fn set_tenant_auth_bootstrapped(
        &self,
        tenant_id: &str,
        bootstrapped: bool,
    ) -> StoreResult<()> {
        self.propose(MetaCommand::SetTenantAuthBootstrapped {
            tenant_id: tenant_id.to_string(),
            bootstrapped,
        })
        .await
        .map(|_| ())
    }

    /// Generation happens here — the propose side — and the candidate rides
    /// an install-if-absent command, exactly like the bootstrap path: apply
    /// stays deterministic, and a candidate proposed off a stale local read
    /// cannot clobber keys that committed in between.
    async fn ensure_signing_key_current(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        if let Ok(existing) = self.local().get_tenant_signing_keys(tenant_id).await {
            return Ok(existing);
        }
        let candidate = crate::auth::keys::generate_signing_keys()?;
        match self
            .propose(MetaCommand::EnsureSigningKeys {
                tenant_id: tenant_id.to_string(),
                candidate,
            })
            .await?
        {
            MetaResponse::SigningKeys { keys } => Ok(keys),
            _ => Err(unexpected_shape("signing keys")),
        }
    }

    async fn seed_rbac_policies_and_groupings(
        &self,
        tenant_id: &str,
        policies: Vec<PolicyRule>,
        groupings: Vec<GroupingRule>,
    ) -> StoreResult<()> {
        self.propose(MetaCommand::SeedRbac {
            tenant_id: tenant_id.to_string(),
            policies,
            groupings,
        })
        .await
        .map(|_| ())
    }

    async fn bootstrap_tenant_auth(
        &self,
        tenant_id: &str,
        seed: TenantAuthSeed,
    ) -> StoreResult<TenantSigningKeys> {
        match self
            .propose(MetaCommand::BootstrapTenantAuth {
                tenant_id: tenant_id.to_string(),
                seed,
            })
            .await?
        {
            MetaResponse::SigningKeys { keys } => Ok(keys),
            _ => Err(unexpected_shape("signing keys")),
        }
    }

    async fn insert_refresh_token(&self, token: RefreshToken) -> StoreResult<()> {
        self.propose(MetaCommand::InsertRefreshToken { token })
            .await
            .map(|_| ())
    }

    async fn take_refresh_token(
        &self,
        tenant_id: &str,
        token_id: &str,
        now_secs: i64,
    ) -> StoreResult<RefreshTokenTake> {
        // A write, not a read of the local replica: spending a single-use token
        // has to be agreed on. Answering from `local()` would let each replica
        // spend the same token once.
        match self
            .propose(MetaCommand::TakeRefreshToken {
                tenant_id: tenant_id.to_string(),
                token_id: token_id.to_string(),
                now_secs,
            })
            .await?
        {
            MetaResponse::RefreshTokenTake { take } => Ok(take),
            _ => Err(unexpected_shape("refresh token take")),
        }
    }

    async fn revoke_refresh_family(&self, tenant_id: &str, family_id: &str) -> StoreResult<u64> {
        match self
            .propose(MetaCommand::RevokeRefreshFamily {
                tenant_id: tenant_id.to_string(),
                family_id: family_id.to_string(),
            })
            .await?
        {
            MetaResponse::Count { count } => Ok(count),
            _ => Err(unexpected_shape("count")),
        }
    }

    async fn revoke_refresh_tokens_for_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> StoreResult<u64> {
        match self
            .propose(MetaCommand::RevokeRefreshTokensForPrincipal {
                tenant_id: tenant_id.to_string(),
                principal_id: principal_id.to_string(),
            })
            .await?
        {
            MetaResponse::Count { count } => Ok(count),
            _ => Err(unexpected_shape("count")),
        }
    }

    async fn purge_expired_refresh_tokens(&self, before_secs: i64) -> StoreResult<u64> {
        match self
            .propose(MetaCommand::PurgeExpiredRefreshTokens { before_secs })
            .await?
        {
            MetaResponse::Count { count } => Ok(count),
            _ => Err(unexpected_shape("count")),
        }
    }
}

/// The command answered with a payload its own definition rules out — a
/// version-skew bug between proposer and applier, worth a loud error over a
/// quiet wrong answer.
fn unexpected_shape(what: &'static str) -> StoreError {
    StoreError::Unexpected(anyhow::anyhow!(
        "raft response did not carry the expected {what}"
    ))
}

#[cfg(test)]
mod tests;

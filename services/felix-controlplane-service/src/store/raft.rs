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
//! Heartbeats, expiry and the placement lease are the exception: they are
//! the leader's soft state (`soft_state`), asked of the leader rather than
//! proposed, and only their consequences reach the log.
//!
//! Nothing in this file interprets a command. It is a typed shim between
//! two vocabularies that must not drift: the store traits on one side, the
//! versioned command set on the other.
pub mod command;
mod soft_state;
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
use crate::raft::{AskLeaderError, RaftHandle};

/// The metadata level whose members store a stream's incarnation.
const INCARNATION_LEVEL: u16 = 10;

/// The metadata level whose members store a node's storage identity and the
/// identities a replica report names.
const STORAGE_IDENTITY_LEVEL: u16 = 12;

/// The metadata level whose members store which node made a replica report
/// and the incarnations it was taken at.
const REPORT_STAMP_LEVEL: u16 = 13;

/// The metadata level whose members store a cache's incarnation.
const CACHE_INCARNATION_LEVEL: u16 = 14;

/// The metadata level whose members store that a replica report's leader
/// gave the shard up.
const GIVEN_UP_LEVEL: u16 = 15;

/// The metadata level whose members store the storage identities a
/// promotion's assignment hands the promoted leader's fence.
const FENCE_STORAGE_LEVEL: u16 = 16;

/// The metadata level whose members record an unassignment for each shard a
/// delete takes, and start a shard's first generation at the change seq at
/// any incarnation (#1435).
const SHARD_RENEWAL_LEVEL: u16 = 17;

/// The metadata level whose members record deleted stream and cache names
/// and refuse to create one again before its incarnations are finalized
/// (#1447).
const DELETED_NAMES_LEVEL: u16 = 18;

/// The metadata level whose members advance the placement token when a
/// stream's consistency or durability changes (#1253).
const PATCH_FENCE_LEVEL: u16 = 19;
use crate::store::memory::InMemoryStore;
use crate::store::raft::command::{
    MetaCommand, MetaError, MetaResponse, decode_result, encode_command,
};
use crate::store::raft::soft_state::{LeaderRequest, SoftState};
use crate::store::raft::state_machine::MetadataStateMachine;
use crate::store::{
    AssignmentWrite, AuthStore, ChangeSet, ControlPlaneStore, Page, PageRequest, PlacementLease,
    ReportWrite, Snapshot, StoreError, StoreResult, TenantAuthSeed,
};

pub struct RaftStore {
    handle: RaftHandle,
    machine: Arc<MetadataStateMachine>,
    soft: Arc<SoftState>,
}

impl RaftStore {
    /// Also registers this member's leader service with `handle`, so build
    /// one store per handle.
    pub fn new(handle: RaftHandle, machine: Arc<MetadataStateMachine>) -> Self {
        let soft = SoftState::install(handle.clone(), Arc::clone(&machine));
        Self {
            handle,
            machine,
            soft,
        }
    }

    /// The local applied state, for reads.
    fn local(&self) -> &InMemoryStore {
        self.machine.store()
    }

    /// The metadata level the group can apply, for gating what is proposed.
    /// A member that has not answered a probe since this process started
    /// counts at the join floor: the level the applied log already depends
    /// on, through an entry that needed it or a finalized feature. A member
    /// below it could not apply what the log already holds, so this lets
    /// through no level above what it was already sent. A member that
    /// answered counts at what it said, so a rolled-back member or an old
    /// build that joined still holds newer commands back. Counted at 0, a
    /// restarted member with a peer down dropped what the group had been
    /// storing, such as a node's storage identity.
    async fn group_level(&self) -> u16 {
        use crate::raft::AppStateMachine;
        self.handle
            .cluster_version_with_unknown_at(self.machine.join_floor())
            .await
    }

    /// Without the level an older member would drop the map and diverge, so
    /// the promoted leader's fence falls back to counting every answer, as
    /// before identities. Cleared before proposing.
    async fn drop_unstorable_fence_ids(&self, assignment: &mut ShardAssignment) {
        if !assignment.fence_storage_ids.is_empty()
            && self.group_level().await < FENCE_STORAGE_LEVEL
        {
            assignment.fence_storage_ids.clear();
        }
    }

    /// Whether a delete takes the deleted resources' shard assignments and
    /// replica reports. Only once every member applies the delete that way;
    /// before then the entry leaves them, on every member, as older members
    /// do. A recreated name may then pick up the leftovers until the group
    /// reaches the level.
    #[cfg(test)]
    async fn deletes_take_shards(&self) -> bool {
        Self::takes_shards_at(self.group_level().await)
    }

    /// [`Self::deletes_take_shards`] at a level already read.
    fn takes_shards_at(level: u16) -> bool {
        let needs = MetaCommand::DeleteTenant {
            tenant_id: String::new(),
            take_shards: true,
            incarnations: false,
            unassign: false,
            names: false,
        }
        .version();
        level >= needs
    }

    /// Whether creates and deletes are proposed as level-14 entries, which
    /// decide incarnations and tombstones from their own fields. Not before
    /// every member applies them: a member at 13 would decide its own way
    /// from the finalizes, and its copy would drift. A create then always
    /// carries a proposal, so one proposing nothing still gets an incarnation
    /// after the finalize.
    fn decides_at(level: u16) -> bool {
        level >= CACHE_INCARNATION_LEVEL
    }

    /// [`Self::decides_at`] for an entry creating `caches`. A cache proposal
    /// is a level-14 field, so an entry carrying one always decides from it,
    /// and the level gate refuses it below 14 rather than sending it as an
    /// older entry that would store the cache at 0.
    fn caches_decide_at(level: u16, caches: &[Cache]) -> bool {
        Self::decides_at(level) || caches.iter().any(|cache| cache.incarnation != 0)
    }

    /// Propose one command and decode its outcome. A command newer than
    /// some member can apply is refused here, before it reaches the log.
    async fn propose(&self, command: MetaCommand) -> StoreResult<MetaResponse> {
        self.propose_at(self.group_level().await, command).await
    }

    /// [`Self::propose`] at a level the caller already read and built the
    /// command from, so a refresh in between cannot gate it on another.
    async fn propose_at(&self, level: u16, command: MetaCommand) -> StoreResult<MetaResponse> {
        let needs = command.version();
        if needs > 0 && level < needs {
            return Err(StoreError::Conflict(format!(
                "this change needs every control-plane member at metadata version {needs}, \
                 but the group is at {level}; finish upgrading every member first"
            )));
        }
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

    /// Ask the leader, wherever it is. `None` when the leader predates
    /// leader-only requests, or declines them because some member predates
    /// the commands they propose; both only happen part-way through a rolling
    /// upgrade, and the caller then falls back to the log.
    async fn ask_leader(&self, request: LeaderRequest) -> StoreResult<Option<MetaResponse>> {
        let bytes = serde_json::to_vec(&request)?;
        match self.handle.ask_leader(bytes).await {
            Ok(bytes) => match decode_result(&bytes).map_err(StoreError::from)? {
                Ok(response) => Ok(Some(response)),
                Err(MetaError::Unsupported(_)) => Ok(None),
                Err(err) => Err(err.into()),
            },
            Err(AskLeaderError::Unsupported) => Ok(None),
            Err(AskLeaderError::Failed(err)) => Err(StoreError::Unexpected(err)),
        }
    }
}

#[async_trait]
impl ControlPlaneStore for RaftStore {
    async fn list_tenants(&self) -> StoreResult<Vec<Tenant>> {
        self.local().list_tenants().await
    }

    async fn list_tenants_page(&self, page: PageRequest<String>) -> StoreResult<Page<Tenant>> {
        self.local().list_tenants_page(page).await
    }

    async fn create_tenant(&self, tenant: Tenant) -> StoreResult<Tenant> {
        match self.propose(MetaCommand::CreateTenant { tenant }).await? {
            MetaResponse::Tenant { tenant } => Ok(tenant),
            _ => Err(unexpected_shape("tenant")),
        }
    }

    async fn delete_tenant(&self, tenant_id: &str) -> StoreResult<()> {
        let level = self.group_level().await;
        self.propose_at(
            level,
            MetaCommand::DeleteTenant {
                tenant_id: tenant_id.to_string(),
                take_shards: Self::takes_shards_at(level),
                incarnations: Self::decides_at(level),
                unassign: level >= SHARD_RENEWAL_LEVEL,
                names: level >= DELETED_NAMES_LEVEL,
            },
        )
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

    async fn list_namespaces_page(
        &self,
        tenant_id: &str,
        page: PageRequest<String>,
    ) -> StoreResult<Page<Namespace>> {
        self.local().list_namespaces_page(tenant_id, page).await
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
        let level = self.group_level().await;
        self.propose_at(
            level,
            MetaCommand::DeleteNamespace {
                key: key.clone(),
                take_shards: Self::takes_shards_at(level),
                incarnations: Self::decides_at(level),
                unassign: level >= SHARD_RENEWAL_LEVEL,
                names: level >= DELETED_NAMES_LEVEL,
            },
        )
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

    async fn list_streams_page(
        &self,
        tenant_id: &str,
        namespace: &str,
        page: PageRequest<String>,
    ) -> StoreResult<Page<Stream>> {
        self.local()
            .list_streams_page(tenant_id, namespace, page)
            .await
    }

    async fn get_stream(&self, key: &StreamKey) -> StoreResult<Stream> {
        self.local().get_stream(key).await
    }

    async fn create_stream(&self, mut stream: Stream) -> StoreResult<Stream> {
        let level = self.group_level().await;
        let incarnations = Self::decides_at(level);
        if incarnations && stream.incarnation == 0 {
            stream.incarnation = crate::model::proposed_incarnation();
        }
        match self
            .propose_at(
                level,
                MetaCommand::CreateStream {
                    stream,
                    incarnations,
                    names: level >= DELETED_NAMES_LEVEL,
                },
            )
            .await?
        {
            MetaResponse::Stream { stream } => Ok(stream),
            _ => Err(unexpected_shape("stream")),
        }
    }

    async fn patch_stream(
        &self,
        key: &StreamKey,
        patch: StreamPatchRequest,
    ) -> StoreResult<Stream> {
        // Only where it can matter, so other patches stay at level 0.
        let fence = (patch.consistency.is_some() || patch.durable.is_some())
            && self.group_level().await >= PATCH_FENCE_LEVEL;
        match self
            .propose(MetaCommand::PatchStream {
                key: key.clone(),
                patch,
                fence,
            })
            .await?
        {
            MetaResponse::Stream { stream } => Ok(stream),
            _ => Err(unexpected_shape("stream")),
        }
    }

    async fn delete_stream(&self, key: &StreamKey) -> StoreResult<()> {
        let level = self.group_level().await;
        self.propose_at(
            level,
            MetaCommand::DeleteStream {
                key: key.clone(),
                take_reports: Self::takes_shards_at(level),
                incarnations: Self::decides_at(level),
                unassign: level >= SHARD_RENEWAL_LEVEL,
                names: level >= DELETED_NAMES_LEVEL,
            },
        )
        .await
        .map(|_| ())
    }

    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>> {
        self.local().stream_snapshot().await
    }

    async fn stream_tombstones(&self) -> StoreResult<Vec<crate::model::StreamTombstone>> {
        self.local().stream_tombstones().await
    }

    async fn stream_changes(&self, since: u64) -> StoreResult<ChangeSet<StreamChange>> {
        self.local().stream_changes(since).await
    }

    async fn list_caches(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Cache>> {
        self.local().list_caches(tenant_id, namespace).await
    }

    async fn list_caches_page(
        &self,
        tenant_id: &str,
        namespace: &str,
        page: PageRequest<String>,
    ) -> StoreResult<Page<Cache>> {
        self.local()
            .list_caches_page(tenant_id, namespace, page)
            .await
    }

    async fn get_cache(&self, key: &CacheKey) -> StoreResult<Cache> {
        self.local().get_cache(key).await
    }

    async fn create_cache(&self, mut cache: Cache) -> StoreResult<Cache> {
        let level = self.group_level().await;
        let incarnations = Self::caches_decide_at(level, std::slice::from_ref(&cache));
        if incarnations && cache.incarnation == 0 {
            cache.incarnation = crate::model::proposed_incarnation();
        }
        match self
            .propose_at(
                level,
                MetaCommand::CreateCache {
                    cache,
                    incarnations,
                    names: level >= DELETED_NAMES_LEVEL,
                },
            )
            .await?
        {
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
        let level = self.group_level().await;
        self.propose_at(
            level,
            MetaCommand::DeleteCache {
                key: key.clone(),
                take_reports: Self::takes_shards_at(level),
                incarnations: Self::decides_at(level),
                unassign: level >= SHARD_RENEWAL_LEVEL,
                names: level >= DELETED_NAMES_LEVEL,
            },
        )
        .await
        .map(|_| ())
    }

    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>> {
        self.local().cache_snapshot().await
    }

    async fn cache_tombstones(&self) -> StoreResult<Vec<crate::model::CacheTombstone>> {
        self.local().cache_tombstones().await
    }

    async fn deleted_stream_names(&self) -> StoreResult<Vec<StreamKey>> {
        self.local().deleted_stream_names().await
    }

    async fn deleted_cache_names(&self) -> StoreResult<Vec<CacheKey>> {
        self.local().deleted_cache_names().await
    }

    async fn cache_changes(&self, since: u64) -> StoreResult<ChangeSet<CacheChange>> {
        self.local().cache_changes(since).await
    }

    async fn create_resources(
        &self,
        streams: Vec<Stream>,
        caches: Vec<Cache>,
    ) -> StoreResult<(Vec<Stream>, Vec<Cache>)> {
        let level = self.group_level().await;
        let incarnations = Self::caches_decide_at(level, &caches);
        let (mut streams, mut caches) = (streams, caches);
        if incarnations {
            for stream in streams.iter_mut().filter(|s| s.incarnation == 0) {
                stream.incarnation = crate::model::proposed_incarnation();
            }
            for cache in caches.iter_mut().filter(|c| c.incarnation == 0) {
                cache.incarnation = crate::model::proposed_incarnation();
            }
        }
        // Before a finalize, everything in the batch is stored at 0, and the
        // answer leaves out what has none.
        let at_zero = |caches: Vec<Cache>| {
            caches
                .into_iter()
                .map(|cache| Cache {
                    incarnation: 0,
                    ..cache
                })
                .collect::<Vec<_>>()
        };
        match self
            .propose_at(
                level,
                MetaCommand::CreateResources {
                    streams: streams.clone(),
                    caches: caches.clone(),
                    incarnations,
                    names: level >= DELETED_NAMES_LEVEL,
                },
            )
            .await?
        {
            MetaResponse::Resources { streams, caches } => Ok((streams, caches)),
            MetaResponse::Streams { streams } => Ok((streams, at_zero(caches))),
            MetaResponse::Unit => Ok((
                streams
                    .into_iter()
                    .map(|stream| Stream {
                        incarnation: 0,
                        ..stream
                    })
                    .collect(),
                at_zero(caches),
            )),
            _ => Err(unexpected_shape("streams")),
        }
    }

    async fn takes_incarnation_proposals(&self) -> bool {
        self.handle.cluster_version().await >= INCARNATION_LEVEL
    }

    async fn takes_cache_incarnation_proposals(&self) -> bool {
        self.handle.cluster_version().await >= CACHE_INCARNATION_LEVEL
    }

    /// An unknown member counts at 0, so one that never answered holds this
    /// at no.
    async fn every_member_current(&self) -> bool {
        self.handle.cluster_version().await >= command::METADATA_VERSION
    }

    async fn create_resources_ready(&self) -> bool {
        let needs = MetaCommand::CreateResources {
            streams: Vec::new(),
            caches: Vec::new(),
            incarnations: false,
            names: false,
        }
        .version();
        self.group_level().await >= needs
    }

    async fn register_node(&self, node: Node) -> StoreResult<Node> {
        self.register_node_in_fleet(node)
            .await
            .map(|(node, _)| node)
    }

    /// Answered from the entry's own apply, so the enabled set is current
    /// even when this member's copy lags.
    ///
    /// Until every member can apply `RegisterNodeInFleet` the node is
    /// registered without its features or zone, as an older member would
    /// store it.
    /// Nothing can be enabled before then either, since finalizing is the
    /// same level. It reports them again when it next registers.
    async fn register_node_in_fleet(
        &self,
        mut node: Node,
    ) -> StoreResult<(Node, std::collections::BTreeSet<String>)> {
        // Stored without it, the node reads as one that sent none, which
        // placement cannot check a report against. Dropped first: the level
        // below is measured with it gone.
        if node.status.storage_id.is_some() && self.group_level().await < STORAGE_IDENTITY_LEVEL {
            node.status.storage_id = None;
        }
        let command = MetaCommand::RegisterNodeInFleet { node: node.clone() };
        if self.handle.cluster_version().await >= command.version() {
            return match self.propose(command).await? {
                MetaResponse::RegisteredNode { node, fleet } => Ok((node, fleet)),
                _ => Err(unexpected_shape("registered node")),
            };
        }
        if !node.status.features.is_empty() || node.spec.zone.is_some() {
            tracing::info!(
                node_id = %node.node_id,
                "registering without fleet features or zone until every control-plane member supports them",
            );
            node.status.features.clear();
            node.spec.zone = None;
        }
        match self.propose(MetaCommand::RegisterNode { node }).await? {
            MetaResponse::Node { node } => Ok((node, Default::default())),
            _ => Err(unexpected_shape("node")),
        }
    }

    async fn enabled_fleet_features(&self) -> StoreResult<std::collections::BTreeSet<String>> {
        self.local().enabled_fleet_features().await
    }

    async fn finalize_fleet_feature(
        &self,
        feature: &str,
    ) -> StoreResult<std::collections::BTreeSet<String>> {
        let command = MetaCommand::FinalizeFleetFeature {
            feature: feature.to_string(),
        };
        if self.handle.cluster_version().await < command.version() {
            return Err(StoreError::Conflict(
                "cannot finalize a fleet feature until every control-plane member runs a release that supports it"
                    .to_string(),
            ));
        }
        match self.propose(command).await? {
            MetaResponse::FleetFeatures { enabled } => Ok(enabled),
            _ => Err(unexpected_shape("fleet features")),
        }
    }

    /// On the leader, with the latest heartbeat it has seen; elsewhere, as
    /// last checkpointed into the log.
    async fn get_node(&self, node_id: &str) -> StoreResult<Node> {
        self.local()
            .get_node(node_id)
            .await
            .map(|node| self.soft.overlay(node))
    }

    async fn list_nodes(&self) -> StoreResult<Vec<Node>> {
        Ok(self
            .local()
            .list_nodes()
            .await?
            .into_iter()
            .map(|node| self.soft.overlay(node))
            .collect())
    }

    /// With the leader's latest heartbeats, as [`Self::list_nodes`].
    async fn list_nodes_page(&self, page: PageRequest<String>) -> StoreResult<Page<Node>> {
        let page = self.local().list_nodes_page(page).await?;
        Ok(Page {
            items: page
                .items
                .into_iter()
                .map(|node| self.soft.overlay(node))
                .collect(),
            more: page.more,
        })
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

    /// Soft state on the leader, not a log entry; `at_millis` is ignored in
    /// favour of the leader's clock.
    async fn record_node_heartbeat(
        &self,
        node_id: &str,
        incarnation: u64,
        at_millis: u64,
    ) -> StoreResult<Node> {
        match self
            .ask_leader(LeaderRequest::Heartbeat {
                node_id: node_id.to_string(),
                incarnation,
            })
            .await?
        {
            Some(MetaResponse::Node { node }) => return Ok(node),
            Some(_) => return Err(unexpected_shape("node")),
            None => {}
        }
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

    /// Nothing to do: the leader ages heartbeats on its monotonic clock and
    /// does not trust a stamp that lies in the future (see `soft_state`), and
    /// rewriting the log's copies would cost an entry for a display value.
    async fn clamp_future_heartbeats(&self, _now_millis: u64) -> StoreResult<u64> {
        Ok(0)
    }

    /// Judged by the leader from what it has heard since it began leading;
    /// see `soft_state`.
    async fn expire_stale_nodes(&self, expiry_before_millis: u64) -> StoreResult<Vec<Node>> {
        match self
            .ask_leader(LeaderRequest::ExpireStaleNodes {
                expiry_before_millis,
            })
            .await?
        {
            Some(MetaResponse::Nodes { nodes }) => return Ok(nodes),
            Some(_) => return Err(unexpected_shape("nodes")),
            None => {}
        }
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
        mut assignment: ShardAssignment,
    ) -> StoreResult<ShardAssignment> {
        self.drop_unstorable_fence_ids(&mut assignment).await;
        let level = self.group_level().await;
        match self
            .propose_at(
                level,
                MetaCommand::PutShardAssignment {
                    assignment,
                    seq_generation: level >= SHARD_RENEWAL_LEVEL,
                },
            )
            .await?
        {
            MetaResponse::Assignment { assignment } => Ok(assignment),
            _ => Err(unexpected_shape("assignment")),
        }
    }

    async fn put_shard_assignment_if(
        &self,
        mut assignment: ShardAssignment,
        expected_generation: Option<u64>,
        fence: u64,
    ) -> StoreResult<AssignmentWrite> {
        self.drop_unstorable_fence_ids(&mut assignment).await;
        let level = self.group_level().await;
        match self
            .propose_at(
                level,
                MetaCommand::PutShardAssignmentFenced {
                    assignment,
                    expected_generation,
                    fence,
                    seq_generation: level >= SHARD_RENEWAL_LEVEL,
                },
            )
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

    async fn list_shard_assignments_page(
        &self,
        leader: Option<&str>,
        page: PageRequest<ShardKey>,
    ) -> StoreResult<Page<ShardAssignment>> {
        self.local().list_shard_assignments_page(leader, page).await
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

    async fn record_replica_report(
        &self,
        report: ReplicaReport,
        leader: &str,
    ) -> StoreResult<ReportWrite> {
        let mut report = report;
        // Without it the leader is replaced only on its storage identity, and
        // never reclaimed. Dropped first.
        if report.given_up && self.group_level().await < GIVEN_UP_LEVEL {
            report.given_up = false;
        }
        // Without them the report is read as before: from any leader, and a
        // follower without an identity across its restarts. Dropped first.
        if (report.leader.is_some() || !report.node_incarnations.is_empty())
            && self.group_level().await < REPORT_STAMP_LEVEL
        {
            report.leader = None;
            report.node_incarnations.clear();
        }
        // Without them every follower is judged by the report's age alone,
        // as before identities existed. Dropped first, like `by_round`.
        if !report.follower_storage.is_empty() && self.group_level().await < STORAGE_IDENTITY_LEVEL
        {
            report.follower_storage.clear();
        }
        // The API's own check still stands for a group that cannot apply
        // the field.
        if report.stream_incarnation.is_some()
            && self.handle.cluster_version().await < INCARNATION_LEVEL
        {
            report.stream_incarnation = None;
        }
        // Without `by_round` placement waits the leader's lease out, which is
        // safe, so a group that cannot apply it records the rest. Dropped
        // first: the levels below are measured with it gone.
        let by_round_level = MetaCommand::RecordReplicaReport {
            report: ReplicaReport {
                halted: Default::default(),
                counter_offsets: Default::default(),
                ..report.clone()
            },
            leader: None,
        }
        .version();
        if report.by_round && self.handle.cluster_version().await < by_round_level {
            report.by_round = false;
            report.leader_incarnation = None;
        }
        // Halts and counter positions are advisory: a group with a member that
        // would drop them records the rest of the report, which placement
        // needs.
        let counters_level = MetaCommand::RecordReplicaReport {
            report: ReplicaReport {
                halted: Default::default(),
                ..report.clone()
            },
            leader: None,
        }
        .version();
        if !report.counter_offsets.is_empty()
            && self.handle.cluster_version().await < counters_level
        {
            report.counter_offsets.clear();
        }
        let halts_level = MetaCommand::RecordReplicaReport {
            report: report.clone(),
            leader: None,
        }
        .version();
        if !report.halted.is_empty() && self.handle.cluster_version().await < halts_level {
            report.halted.clear();
        }
        let mut command = MetaCommand::RecordReplicaReport {
            report,
            leader: Some(leader.to_string()),
        };
        // A member that predates the leadership check would apply the report
        // unchecked while the others refuse it. Until every member has it,
        // every member applies it unchecked.
        if self.handle.cluster_version().await < command.version()
            && let MetaCommand::RecordReplicaReport { leader, .. } = &mut command
        {
            *leader = None;
        }
        match self.propose(command).await? {
            MetaResponse::Unit => Ok(ReportWrite::Stored),
            MetaResponse::StaleReport => Ok(ReportWrite::Stale),
            MetaResponse::NotLeaderReport => Ok(ReportWrite::NotLeader),
            _ => Err(unexpected_shape("unit")),
        }
    }

    async fn list_replica_reports(&self) -> StoreResult<Vec<ReplicaReport>> {
        self.local().list_replica_reports().await
    }

    async fn record_suspicion(&self, suspicion: crate::model::NodeSuspicion) -> StoreResult<()> {
        // `None` is a leader without soft state, which keeps none.
        self.ask_leader(LeaderRequest::Suspicion {
            node_id: suspicion.node_id,
            incarnation: suspicion.incarnation,
            suspects: suspicion.suspects,
        })
        .await
        .map(|_| ())
    }

    async fn list_suspicions(&self) -> StoreResult<Vec<crate::model::NodeSuspicion>> {
        Ok(self.soft.suspicions())
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

    /// Expires like the other backends' lease, by the leader's clock, but a
    /// renewal is leader soft state: only a change of holder is proposed. A
    /// new leader counts a lease it has not seen renewed as renewed when it
    /// took over.
    async fn acquire_placement_lease(
        &self,
        holder: &str,
        ttl_millis: u64,
    ) -> StoreResult<Option<PlacementLease>> {
        match self
            .ask_leader(LeaderRequest::AcquirePlacementLease {
                holder: holder.to_string(),
                ttl_millis,
            })
            .await?
        {
            Some(MetaResponse::PlacementLease { token, taken }) => {
                return Ok(Some(PlacementLease { token, taken }));
            }
            Some(MetaResponse::NoPlacementLease) => return Ok(None),
            Some(_) => return Err(unexpected_shape("placement lease")),
            None => {}
        }
        // A leader from before leader-only requests: it takes the lease at
        // once, as that build did.
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

    async fn release_placement_lease(&self, holder: &str) -> StoreResult<()> {
        self.ask_leader(LeaderRequest::ReleasePlacementLease {
            holder: holder.to_string(),
        })
        .await
        .map(|_| ())
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

    async fn list_rbac_policies_page(
        &self,
        tenant_id: &str,
        page: PageRequest<PolicyRule>,
    ) -> StoreResult<Page<PolicyRule>> {
        self.local().list_rbac_policies_page(tenant_id, page).await
    }

    async fn list_rbac_groupings_page(
        &self,
        tenant_id: &str,
        page: PageRequest<GroupingRule>,
    ) -> StoreResult<Page<GroupingRule>> {
        self.local().list_rbac_groupings_page(tenant_id, page).await
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

    async fn change_auth(
        &self,
        tenant_id: &str,
        change: crate::store::AuthChange,
        revocation: Option<crate::store::Revocation>,
    ) -> StoreResult<()> {
        use crate::store::AuthChange;
        let Some(revocation) = revocation else {
            return match change {
                AuthChange::RemovePolicy(policy) => {
                    self.remove_rbac_policy(tenant_id, policy).await
                }
                AuthChange::RemoveGrouping(grouping) => {
                    self.remove_rbac_grouping(tenant_id, grouping).await
                }
                AuthChange::UpsertIssuer(issuer) => self.upsert_idp_issuer(tenant_id, issuer).await,
                AuthChange::DeleteIssuer(issuer) => {
                    self.delete_idp_issuer(tenant_id, &issuer).await
                }
            };
        };
        let command = MetaCommand::ChangeAuthAndRevoke {
            tenant_id: tenant_id.to_string(),
            change,
            revocation,
        };
        if self.handle.cluster_version().await < command.version() {
            // Some member predates the combined entry: the change, then the
            // revocation, as that member's build does.
            let MetaCommand::ChangeAuthAndRevoke {
                change, revocation, ..
            } = command
            else {
                unreachable!("built above")
            };
            self.change_auth(tenant_id, change, None).await?;
            return match self
                .revoke_principals(tenant_id, revocation.principal_ids, revocation.revoked_at)
                .await
            {
                // Below level 7 nothing can record a revocation; the change
                // stands, as it did before revocations existed.
                Err(StoreError::Conflict(what)) => {
                    tracing::warn!(
                        tenant_id,
                        reason = %what,
                        "made an auth change but cannot record its revocation until every \
                         member is upgraded; the sessions it reached keep their rights \
                         until their tokens expire",
                    );
                    Ok(())
                }
                other => other,
            };
        }
        self.propose(command).await.map(|_| ())
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

    async fn peek_refresh_token(
        &self,
        tenant_id: &str,
        token_id: &str,
    ) -> StoreResult<Option<RefreshToken>> {
        // On the leader: a token minted through another member a moment ago
        // may not be applied here yet.
        match self
            .ask_leader(LeaderRequest::PeekRefreshToken {
                tenant_id: tenant_id.to_string(),
                token_id: token_id.to_string(),
            })
            .await?
        {
            Some(MetaResponse::RefreshToken { token }) => Ok(token),
            Some(_) => Err(unexpected_shape("refresh token")),
            None => self.local().peek_refresh_token(tenant_id, token_id).await,
        }
    }

    async fn rotate_refresh_token(
        &self,
        tenant_id: &str,
        token_id: &str,
        now_secs: i64,
        next: RefreshToken,
    ) -> StoreResult<RefreshTokenTake> {
        let command = MetaCommand::RotateRefreshToken {
            tenant_id: tenant_id.to_string(),
            token_id: token_id.to_string(),
            now_secs,
            next,
        };
        if self.handle.cluster_version().await < command.version() {
            // Some member predates the combined entry: spend, then insert, as
            // that member's build does.
            let MetaCommand::RotateRefreshToken { next, .. } = command else {
                unreachable!("built above")
            };
            let take = self
                .take_refresh_token(tenant_id, token_id, now_secs)
                .await?;
            if matches!(take, RefreshTokenTake::Taken(_)) {
                self.insert_refresh_token(next).await?;
            }
            return Ok(take);
        }
        match self.propose(command).await? {
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

    async fn revoke_principals(
        &self,
        tenant_id: &str,
        principal_ids: Vec<String>,
        revoked_at: i64,
    ) -> StoreResult<()> {
        self.propose(MetaCommand::RevokePrincipals {
            tenant_id: tenant_id.to_string(),
            principal_ids,
            revoked_at,
        })
        .await?;
        Ok(())
    }

    async fn principal_revocations(
        &self,
        since: u64,
    ) -> StoreResult<ChangeSet<crate::auth::revocation::PrincipalRevocation>> {
        self.local().principal_revocations(since).await
    }

    async fn principal_revoked_at(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> StoreResult<Option<i64>> {
        Ok(self
            .principals_revoked_at(tenant_id, &[principal_id.to_string()])
            .await?
            .pop()
            .flatten())
    }

    /// Read on the leader, so the answer includes every revocation already
    /// acknowledged, not only those this member has applied. Falls back to
    /// the local state while the leader predates the request.
    async fn principals_revoked_at(
        &self,
        tenant_id: &str,
        principal_ids: &[String],
    ) -> StoreResult<Vec<Option<i64>>> {
        match self
            .ask_leader(LeaderRequest::RevocationCutoffs {
                tenant_id: tenant_id.to_string(),
                principal_ids: principal_ids.to_vec(),
            })
            .await?
        {
            Some(MetaResponse::RevokedAt { cutoffs }) if cutoffs.len() == principal_ids.len() => {
                Ok(cutoffs)
            }
            Some(_) => Err(unexpected_shape("revocation cutoffs")),
            None => {
                self.local()
                    .principals_revoked_at(tenant_id, principal_ids)
                    .await
            }
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

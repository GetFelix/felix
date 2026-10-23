//! In-memory implementation of the control-plane store.
//!
//! This store implements the `ControlPlaneStore` trait entirely in memory using `HashMap`s guarded
//! by `tokio::sync::RwLock`. It exists for:
//! - local development and tests (no external dependencies)
//! - deployments where durability is not required
//! - as a fallback when a durable backend (e.g., Postgres) is not configured
//!
//! # Durability and consistency
//! - **Not durable**: all state is lost on process restart.
//! - **Single-process consistency**: operations are consistent within one process. We use write locks
//!   for mutations and read locks for reads.
//! - **No multi-node coordination**: multiple controlplane instances each have independent state.
//!
//! # Change streams
//! The controlplane exposes two ways to consume state:
//! 1) A full **snapshot** of current state.
//! 2) An incremental **change stream** since a sequence number (`seq`).
//!
//! This store maintains per-entity in-memory change logs with a bounded retention window
//! (`StoreConfig::change_window`). When the window overflows, old changes are evicted.
//! Consumers that fall behind past the window must re-bootstrap via snapshot.
//!
//! # Performance characteristics
//! - Reads are cheap and concurrent (many readers).
//! - Writes are serialized per map/log (write lock per structure).
//! - Deletes cascade by scanning keys to find dependents; this is acceptable for small in-memory
//!   dev workloads but would be inefficient at very large scale.
//!
//! # Metrics
//! This store updates a small set of gauges/counters to keep observability behavior consistent with
//! durable backends.
mod auth;
mod caches;
mod change_log;
mod export;
mod namespaces;
mod nodes;
mod pages;
mod refresh_tokens;
mod resources;
mod revocations;
mod shards;
mod streams;
mod tenants;

pub(crate) use nodes::FleetRule;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::{
    AuthStore, ChangeSet, ControlPlaneStore, Page, PageRequest, ReportWrite, Snapshot, StoreConfig,
    StoreResult,
};
use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::auth::refresh_token::{RefreshToken, RefreshTokenTake};
use crate::model::{
    Cache, CacheChange, CacheKey, CachePatchRequest, Namespace, NamespaceChange, NamespaceKey,
    Node, NodeChange, NodeChangeOp, NodeLifecycle, NodePatchRequest, ReplicaReport,
    ShardAssignment, ShardAssignmentChange, ShardAssignmentChangeOp, ShardKey, ShardKind, Stream,
    StreamChange, StreamKey, StreamPatchRequest, Tenant, TenantChange,
};
use change_log::ChangeLog;

/// In-memory control-plane store.
///
/// ## Data structures
/// - Authoritative state is stored in `HashMap`s.
/// - Change streams are stored in `ChangeLog`s.
///
/// All maps/logs are wrapped in `Arc<RwLock<...>>` so:
/// - the store can be cloned and shared across async request handlers
/// - reads can proceed concurrently
/// - writes are serialized to preserve invariants
///
/// ## Cascading deletes
/// Deleting a tenant/namespace removes dependent namespaces/streams/caches by scanning keys.
/// This keeps behavior correct and predictable for dev/test usage.
/// Durable backends should implement cascades via SQL constraints/transactions.
pub struct InMemoryStore {
    /// Store-level configuration (change window, paging limit, etc.).
    config: StoreConfig,
    /// Failures a test asks for.
    #[cfg(test)]
    pub(crate) faults: Faults,
    /// Authoritative tenant objects keyed by `tenant_id`.
    tenants: Arc<RwLock<HashMap<String, Tenant>>>,
    /// Authoritative namespaces keyed by `(tenant_id, namespace)`.
    namespaces: Arc<RwLock<HashMap<NamespaceKey, Namespace>>>,
    /// Authoritative streams keyed by `(tenant_id, namespace, stream)`.
    streams: Arc<RwLock<HashMap<StreamKey, Stream>>>,
    /// For each deleted stream name, the incarnation through which it is
    /// deleted. Written only once the fleet finalized `stream_incarnations`.
    stream_tombstones: Arc<RwLock<HashMap<StreamKey, u64>>>,
    /// Authoritative caches keyed by `(tenant_id, namespace, cache)`.
    caches: Arc<RwLock<HashMap<CacheKey, Cache>>>,
    /// As `stream_tombstones`, for caches, once the fleet finalized
    /// `cache_incarnations`.
    cache_tombstones: Arc<RwLock<HashMap<CacheKey, u64>>>,
    /// Every stream name deleted, so one is not created again before the
    /// fleet finalized `stream_incarnations` (#1447).
    deleted_stream_names: Arc<RwLock<std::collections::HashSet<StreamKey>>>,
    /// As `deleted_stream_names`, for caches and `cache_incarnations`.
    deleted_cache_names: Arc<RwLock<std::collections::HashSet<CacheKey>>>,
    /// Broker membership keyed by `node_id`, with its change log.
    ///
    /// One lock over both, unlike the entities above: a snapshot has to read
    /// the records and the log position as one value, or a change committed
    /// between the two reads is missed by every consumer that resumes from it.
    nodes: Arc<RwLock<NodeState>>,
    /// Shard ownership and its change log, under one lock for the same reason
    /// as `nodes`: a snapshot must read records and log position as one value.
    shards: Arc<RwLock<ShardState>>,
    /// What each shard's leader last reported about its replicas. Transient:
    /// not exported, because a report expires within seconds and the next one
    /// replaces it, and a report a restored snapshot carried would be judged
    /// stale by then anyway.
    replica_reports: Arc<RwLock<HashMap<ShardKey, ReplicaReport>>>,
    /// What each broker last said about the leaders it cannot reach. As
    /// transient as the reports, and not exported for the same reason.
    suspicions: Arc<RwLock<HashMap<String, crate::model::NodeSuspicion>>>,
    /// Bounded change log for tenant changes.
    ///
    /// `next_seq` is per-entity-type (not a global sequence across all entities).
    tenant_changes: Arc<RwLock<ChangeLog<TenantChange>>>,
    /// Bounded change log for namespace changes.
    ///
    /// `next_seq` is per-entity-type (not a global sequence across all entities).
    namespace_changes: Arc<RwLock<ChangeLog<NamespaceChange>>>,
    /// Bounded change log for stream changes.
    ///
    /// `next_seq` is per-entity-type (not a global sequence across all entities).
    stream_changes: Arc<RwLock<ChangeLog<StreamChange>>>,
    /// Bounded change log for cache changes.
    ///
    /// `next_seq` is per-entity-type (not a global sequence across all entities).
    cache_changes: Arc<RwLock<ChangeLog<CacheChange>>>,
    /// Per-tenant identity provider issuer configs.
    idp_issuers: Arc<RwLock<HashMap<String, Vec<IdpIssuerConfig>>>>,
    /// Per-tenant signing keys for Felix tokens.
    tenant_signing_keys: Arc<RwLock<HashMap<String, TenantSigningKeys>>>,
    /// Per-tenant RBAC policy rules.
    rbac_policies: Arc<RwLock<HashMap<String, Vec<PolicyRule>>>>,
    /// Per-tenant RBAC group bindings.
    rbac_groupings: Arc<RwLock<HashMap<String, Vec<GroupingRule>>>>,
    /// Per-tenant auth bootstrap completion.
    auth_bootstrapped: Arc<RwLock<HashMap<String, bool>>>,
    /// Serializes `bootstrap_tenant_auth`, standing in for the row lock the
    /// Postgres backend takes. Held across the whole operation; nothing else
    /// acquires it, so it cannot deadlock against the field locks above.
    bootstrap_serial: Arc<tokio::sync::Mutex<()>>,
    /// Refresh tokens keyed by `(tenant_id, token_id)`.
    ///
    /// One lock over the whole map, because `take_refresh_token` has to read a
    /// record and mark it spent without anything in between — a single-use
    /// token checked and then marked under separate locks is a token two
    /// concurrent refreshes can both spend.
    refresh_tokens: Arc<RwLock<HashMap<(String, String), RefreshToken>>>,
    /// Whether placement starts moves of its own; see
    /// `ControlPlaneStore::moves_paused`.
    moves_paused: Arc<RwLock<bool>>,
    /// Principal revocations; see `AuthStore::revoke_principals`.
    revocations: Arc<RwLock<revocations::Revocations>>,
}

impl InMemoryStore {
    /// [`ControlPlaneStore::delete_tenant`], taking `cleanup` of its shards.
    pub(crate) async fn apply_delete_tenant(
        &self,
        tenant_id: &str,
        cleanup: ShardCleanup,
        decide: Decide,
    ) -> StoreResult<()> {
        tenants::delete_tenant(self, tenant_id, cleanup, decide).await
    }

    /// [`ControlPlaneStore::delete_namespace`], taking `cleanup` of its shards.
    pub(crate) async fn apply_delete_namespace(
        &self,
        key: &NamespaceKey,
        cleanup: ShardCleanup,
        decide: Decide,
    ) -> StoreResult<()> {
        namespaces::delete_namespace(self, key, cleanup, decide).await
    }

    /// [`ControlPlaneStore::create_stream`], with incarnations decided by `decide`.
    pub(crate) async fn apply_create_stream(
        &self,
        stream: Stream,
        decide: Decide,
    ) -> StoreResult<Stream> {
        streams::create_stream(self, stream, decide).await
    }

    /// [`ControlPlaneStore::create_cache`], with incarnations decided by `decide`.
    pub(crate) async fn apply_create_cache(
        &self,
        cache: Cache,
        decide: Decide,
    ) -> StoreResult<Cache> {
        caches::create_cache(self, cache, decide).await
    }

    /// [`ControlPlaneStore::create_resources`], with incarnations decided by
    /// `decide`.
    pub(crate) async fn apply_create_resources(
        &self,
        streams: Vec<Stream>,
        caches: Vec<Cache>,
        decide: Decide,
    ) -> StoreResult<(Vec<Stream>, Vec<Cache>)> {
        resources::create_resources(self, streams, caches, decide).await
    }

    /// [`ControlPlaneStore::delete_stream`], taking `cleanup` of its shards.
    pub(crate) async fn apply_delete_stream(
        &self,
        key: &StreamKey,
        cleanup: ShardCleanup,
        decide: Decide,
    ) -> StoreResult<()> {
        streams::delete_stream(self, key, cleanup, decide).await
    }

    /// [`ControlPlaneStore::delete_cache`], taking `cleanup` of its shards.
    pub(crate) async fn apply_delete_cache(
        &self,
        key: &CacheKey,
        cleanup: ShardCleanup,
        decide: Decide,
    ) -> StoreResult<()> {
        caches::delete_cache(self, key, cleanup, decide).await
    }

    /// [`ControlPlaneStore::record_replica_report`], with the leadership
    /// check optional for the Raft apply of entries that predate it.
    pub(crate) async fn apply_replica_report(
        &self,
        report: ReplicaReport,
        leader: Option<&str>,
    ) -> StoreResult<ReportWrite> {
        shards::record_replica_report(self, report, leader).await
    }

    /// [`ControlPlaneStore::register_node`], with the fleet check optional
    /// for the Raft apply of entries that predate it.
    pub(crate) async fn apply_register_node(
        &self,
        node: Node,
        rule: FleetRule,
    ) -> StoreResult<Node> {
        nodes::register_node(self, node, rule).await
    }

    /// Forget every deleted name, so a test can replace a fixture's stream
    /// under the same name before the finalize.
    #[cfg(test)]
    pub(crate) async fn forget_deleted_names(&self) {
        self.deleted_stream_names.write().await.clear();
        self.deleted_cache_names.write().await.clear();
    }

    pub fn new(config: StoreConfig) -> Self {
        let capacity = config.change_window();
        // The change window is a retention bound for incremental sync consumers.
        // Once capacity is exceeded, oldest changes are evicted.
        Self {
            config,
            #[cfg(test)]
            faults: Faults::default(),
            tenants: Arc::new(RwLock::new(HashMap::new())),
            namespaces: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(HashMap::new())),
            stream_tombstones: Arc::new(RwLock::new(HashMap::new())),
            caches: Arc::new(RwLock::new(HashMap::new())),
            cache_tombstones: Arc::new(RwLock::new(HashMap::new())),
            deleted_stream_names: Default::default(),
            deleted_cache_names: Default::default(),
            nodes: Arc::new(RwLock::new(NodeState {
                records: HashMap::new(),
                changes: ChangeLog::new(capacity),
                fleet_enabled: Default::default(),
            })),
            shards: Arc::new(RwLock::new(ShardState {
                records: HashMap::new(),
                changes: ChangeLog::new(capacity),
                placement: PlacementState::default(),
            })),
            replica_reports: Arc::new(RwLock::new(HashMap::new())),
            suspicions: Arc::new(RwLock::new(HashMap::new())),
            tenant_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            namespace_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            stream_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            cache_changes: Arc::new(RwLock::new(ChangeLog::new(capacity))),
            idp_issuers: Arc::new(RwLock::new(HashMap::new())),
            tenant_signing_keys: Arc::new(RwLock::new(HashMap::new())),
            rbac_policies: Arc::new(RwLock::new(HashMap::new())),
            rbac_groupings: Arc::new(RwLock::new(HashMap::new())),
            auth_bootstrapped: Arc::new(RwLock::new(HashMap::new())),
            bootstrap_serial: Arc::new(tokio::sync::Mutex::new(())),
            refresh_tokens: Arc::new(RwLock::new(HashMap::new())),
            moves_paused: Arc::new(RwLock::new(false)),
            revocations: Arc::new(RwLock::new(Default::default())),
        }
    }

    /// Forget every shard assignment belonging to a stream or cache that has
    /// just been deleted.
    ///
    /// Postgres does this with `ON DELETE CASCADE`, and the reason is in the
    /// migration: ownership records for shards that no longer exist leave
    /// placement chasing ghosts. Doing it only there would be the more
    /// dangerous divergence of the two — a suite running in memory would see
    /// stale assignments the deployed system never produces, so a placement bug
    /// that needs orphaned rows is invisible in memory and real in Postgres.
    ///
    /// [`ShardCleanup::Unassign`] records an unassignment for each shard, as
    /// Postgres does: a broker follows the shard change log, not the stream's,
    /// and without one keeps leading a shard of a stream that is gone (#1435).
    ///
    /// Anything but [`ShardCleanup::Legacy`] also drops the shards' replica
    /// reports, as Postgres cascades them: a name created again can start at
    /// the same generation after a restore, and the old log's report would
    /// read as the new one's.
    async fn drop_shard_assignments_for(
        &self,
        kind: ShardKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        cleanup: ShardCleanup,
    ) {
        let of_it = |key: &ShardKey| {
            key.kind == kind
                && key.tenant_id == tenant_id
                && key.namespace == namespace
                && key.stream == name
        };
        let mut state = self.shards.write().await;
        let mut dropped: Vec<ShardKey> = state
            .records
            .keys()
            .filter(|key| of_it(key))
            .cloned()
            .collect();
        state.records.retain(|key, _| !of_it(key));
        if cleanup == ShardCleanup::Unassign {
            // Sorted so every Raft member records them at the same seqs.
            dropped.sort_by_key(|key| key.shard);
            for key in &dropped {
                state.record(ShardAssignmentChangeOp::Unassigned, key, None);
            }
        }
        if cleanup.takes_shards() {
            self.replica_reports
                .write()
                .await
                .retain(|key, _| !of_it(key));
        }
    }

    fn limit(&self) -> usize {
        // Max number of changes returned per `*_changes()` call.
        // This prevents unbounded responses and keeps polling predictable.
        self.config.changes_limit as usize
    }
}

#[async_trait]
impl ControlPlaneStore for InMemoryStore {
    async fn list_tenants(&self) -> StoreResult<Vec<Tenant>> {
        tenants::list_tenants(self).await
    }

    async fn list_tenants_page(&self, page: PageRequest<String>) -> StoreResult<Page<Tenant>> {
        pages::tenants(self, page).await
    }

    async fn create_tenant(&self, tenant: Tenant) -> StoreResult<Tenant> {
        tenants::create_tenant(self, tenant).await
    }

    async fn delete_tenant(&self, tenant_id: &str) -> StoreResult<()> {
        tenants::delete_tenant(self, tenant_id, ShardCleanup::Unassign, Decide::Here).await
    }

    async fn tenant_snapshot(&self) -> StoreResult<Snapshot<Tenant>> {
        tenants::tenant_snapshot(self).await
    }

    async fn tenant_changes(&self, since: u64) -> StoreResult<ChangeSet<TenantChange>> {
        tenants::tenant_changes(self, since).await
    }

    async fn list_namespaces(&self, tenant_id: &str) -> StoreResult<Vec<Namespace>> {
        namespaces::list_namespaces(self, tenant_id).await
    }

    async fn list_namespaces_page(
        &self,
        tenant_id: &str,
        page: PageRequest<String>,
    ) -> StoreResult<Page<Namespace>> {
        pages::namespaces(self, tenant_id, page).await
    }

    async fn create_namespace(&self, namespace: Namespace) -> StoreResult<Namespace> {
        namespaces::create_namespace(self, namespace).await
    }

    async fn delete_namespace(&self, key: &NamespaceKey) -> StoreResult<()> {
        namespaces::delete_namespace(self, key, ShardCleanup::Unassign, Decide::Here).await
    }

    async fn namespace_snapshot(&self) -> StoreResult<Snapshot<Namespace>> {
        namespaces::namespace_snapshot(self).await
    }

    async fn namespace_changes(&self, since: u64) -> StoreResult<ChangeSet<NamespaceChange>> {
        namespaces::namespace_changes(self, since).await
    }

    async fn list_streams(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Stream>> {
        streams::list_streams(self, tenant_id, namespace).await
    }

    async fn list_streams_page(
        &self,
        tenant_id: &str,
        namespace: &str,
        page: PageRequest<String>,
    ) -> StoreResult<Page<Stream>> {
        pages::streams(self, tenant_id, namespace, page).await
    }

    async fn get_stream(&self, key: &StreamKey) -> StoreResult<Stream> {
        streams::get_stream(self, key).await
    }

    async fn create_stream(&self, stream: Stream) -> StoreResult<Stream> {
        streams::create_stream(self, stream, Decide::Here).await
    }

    async fn patch_stream(
        &self,
        key: &StreamKey,
        patch: StreamPatchRequest,
    ) -> StoreResult<Stream> {
        streams::patch_stream(self, key, patch).await
    }

    async fn delete_stream(&self, key: &StreamKey) -> StoreResult<()> {
        streams::delete_stream(self, key, ShardCleanup::Unassign, Decide::Here).await
    }

    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>> {
        streams::stream_snapshot(self).await
    }

    async fn stream_tombstones(&self) -> StoreResult<Vec<crate::model::StreamTombstone>> {
        Ok(streams::stream_tombstones(self).await)
    }

    async fn stream_changes(&self, since: u64) -> StoreResult<ChangeSet<StreamChange>> {
        streams::stream_changes(self, since).await
    }

    async fn list_caches(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Cache>> {
        caches::list_caches(self, tenant_id, namespace).await
    }

    async fn list_caches_page(
        &self,
        tenant_id: &str,
        namespace: &str,
        page: PageRequest<String>,
    ) -> StoreResult<Page<Cache>> {
        pages::caches(self, tenant_id, namespace, page).await
    }

    async fn get_cache(&self, key: &CacheKey) -> StoreResult<Cache> {
        caches::get_cache(self, key).await
    }

    async fn create_cache(&self, cache: Cache) -> StoreResult<Cache> {
        caches::create_cache(self, cache, Decide::Here).await
    }

    async fn patch_cache(&self, key: &CacheKey, patch: CachePatchRequest) -> StoreResult<Cache> {
        caches::patch_cache(self, key, patch).await
    }

    async fn delete_cache(&self, key: &CacheKey) -> StoreResult<()> {
        caches::delete_cache(self, key, ShardCleanup::Unassign, Decide::Here).await
    }

    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>> {
        caches::cache_snapshot(self).await
    }

    async fn cache_tombstones(&self) -> StoreResult<Vec<crate::model::CacheTombstone>> {
        Ok(caches::cache_tombstones(self).await)
    }

    async fn deleted_stream_names(&self) -> StoreResult<Vec<StreamKey>> {
        Ok(streams::deleted_stream_names(self).await)
    }

    async fn deleted_cache_names(&self) -> StoreResult<Vec<CacheKey>> {
        Ok(caches::deleted_cache_names(self).await)
    }

    async fn cache_changes(&self, since: u64) -> StoreResult<ChangeSet<CacheChange>> {
        caches::cache_changes(self, since).await
    }

    async fn create_resources(
        &self,
        streams: Vec<Stream>,
        caches: Vec<Cache>,
    ) -> StoreResult<(Vec<Stream>, Vec<Cache>)> {
        resources::create_resources(self, streams, caches, Decide::Here).await
    }

    async fn register_node(&self, node: Node) -> StoreResult<Node> {
        nodes::register_node(self, node, nodes::FleetRule::Enforce).await
    }

    async fn enabled_fleet_features(&self) -> StoreResult<std::collections::BTreeSet<String>> {
        Ok(self.nodes.read().await.fleet_enabled.clone())
    }

    async fn finalize_fleet_feature(
        &self,
        feature: &str,
    ) -> StoreResult<std::collections::BTreeSet<String>> {
        nodes::finalize_fleet_feature(self, feature).await
    }

    async fn get_node(&self, node_id: &str) -> StoreResult<Node> {
        nodes::get_node(self, node_id).await
    }

    async fn list_nodes(&self) -> StoreResult<Vec<Node>> {
        nodes::list_nodes(self).await
    }

    async fn list_nodes_page(&self, page: PageRequest<String>) -> StoreResult<Page<Node>> {
        pages::nodes(self, page).await
    }

    async fn patch_node(&self, node_id: &str, patch: NodePatchRequest) -> StoreResult<Node> {
        nodes::patch_node(self, node_id, patch).await
    }

    async fn delete_node(&self, node_id: &str) -> StoreResult<()> {
        nodes::delete_node(self, node_id).await
    }

    async fn record_node_heartbeat(
        &self,
        node_id: &str,
        incarnation: u64,
        at_millis: u64,
    ) -> StoreResult<Node> {
        nodes::record_node_heartbeat(self, node_id, incarnation, at_millis, false).await
    }

    async fn expire_stale_nodes(&self, expiry_before_millis: u64) -> StoreResult<Vec<Node>> {
        nodes::expire_stale_nodes(self, expiry_before_millis).await
    }

    async fn clamp_future_heartbeats(&self, now_millis: u64) -> StoreResult<u64> {
        nodes::clamp_future_heartbeats(self, now_millis).await
    }

    async fn set_node_lifecycle(
        &self,
        node_id: &str,
        lifecycle: NodeLifecycle,
    ) -> StoreResult<Option<Node>> {
        nodes::set_node_lifecycle(self, node_id, lifecycle).await
    }

    async fn node_snapshot(&self) -> StoreResult<Snapshot<Node>> {
        nodes::node_snapshot(self).await
    }

    async fn node_changes(&self, since: u64) -> StoreResult<ChangeSet<NodeChange>> {
        nodes::node_changes(self, since).await
    }

    async fn put_shard_assignment(
        &self,
        assignment: ShardAssignment,
    ) -> StoreResult<ShardAssignment> {
        shards::put_shard_assignment(self, assignment, true).await
    }

    async fn put_shard_assignment_if(
        &self,
        assignment: ShardAssignment,
        expected_generation: Option<u64>,
        fence: u64,
    ) -> StoreResult<crate::store::AssignmentWrite> {
        shards::put_shard_assignment_if(self, assignment, expected_generation, Some(fence), true)
            .await
    }

    async fn get_shard_assignment(&self, key: &ShardKey) -> StoreResult<ShardAssignment> {
        shards::get_shard_assignment(self, key).await
    }

    async fn list_shard_assignments(&self) -> StoreResult<Vec<ShardAssignment>> {
        shards::list_shard_assignments(self).await
    }

    async fn list_shard_assignments_page(
        &self,
        leader: Option<&str>,
        page: PageRequest<ShardKey>,
    ) -> StoreResult<Page<ShardAssignment>> {
        pages::shard_assignments(self, leader, page).await
    }

    async fn list_shard_assignments_for_node(
        &self,
        node_id: &str,
    ) -> StoreResult<Vec<ShardAssignment>> {
        shards::list_shard_assignments_for_node(self, node_id).await
    }

    async fn delete_shard_assignment(&self, key: &ShardKey) -> StoreResult<()> {
        shards::delete_shard_assignment(self, key).await
    }

    async fn shard_assignment_snapshot(&self) -> StoreResult<Snapshot<ShardAssignment>> {
        shards::shard_assignment_snapshot(self).await
    }

    async fn shard_assignment_changes(
        &self,
        since: u64,
    ) -> StoreResult<ChangeSet<ShardAssignmentChange>> {
        shards::shard_assignment_changes(self, since).await
    }

    async fn record_replica_report(
        &self,
        report: ReplicaReport,
        leader: &str,
    ) -> StoreResult<ReportWrite> {
        shards::record_replica_report(self, report, Some(leader)).await
    }

    async fn list_replica_reports(&self) -> StoreResult<Vec<ReplicaReport>> {
        shards::list_replica_reports(self).await
    }

    async fn record_suspicion(&self, suspicion: crate::model::NodeSuspicion) -> StoreResult<()> {
        self.suspicions
            .write()
            .await
            .insert(suspicion.node_id.clone(), suspicion);
        Ok(())
    }

    async fn list_suspicions(&self) -> StoreResult<Vec<crate::model::NodeSuspicion>> {
        Ok(self.suspicions.read().await.values().cloned().collect())
    }

    async fn moves_paused(&self) -> StoreResult<bool> {
        Ok(*self.moves_paused.read().await)
    }

    async fn set_moves_paused(&self, paused: bool) -> StoreResult<()> {
        *self.moves_paused.write().await = paused;
        Ok(())
    }

    async fn placement_token(&self) -> StoreResult<u64> {
        Ok(self.shards.read().await.placement.token)
    }

    async fn acquire_placement_lease(
        &self,
        holder: &str,
        ttl_millis: u64,
    ) -> StoreResult<Option<crate::store::PlacementLease>> {
        let now = self.now_millis().await?;
        Ok(shards::acquire_placement_lease(self, holder, ttl_millis, now).await)
    }

    async fn release_placement_lease(&self, holder: &str) -> StoreResult<()> {
        let now = self.now_millis().await?;
        shards::release_placement_lease(self, holder, now).await;
        Ok(())
    }

    async fn tenant_exists(&self, tenant_id: &str) -> StoreResult<bool> {
        tenants::tenant_exists(self, tenant_id).await
    }

    async fn namespace_exists(&self, key: &NamespaceKey) -> StoreResult<bool> {
        namespaces::namespace_exists(self, key).await
    }

    async fn health_check(&self) -> StoreResult<()> {
        // In-memory backend is always "healthy" if the process is running.
        // Durable backends should probe connectivity (e.g., a simple SELECT).
        Ok(())
    }

    /// Whether this backend provides persistence across restarts.
    ///
    /// In-memory store does not persist state and is therefore not durable.
    fn is_durable(&self) -> bool {
        false
    }

    /// Human-readable backend identifier used in logs/metrics/diagnostics.
    fn backend_name(&self) -> &'static str {
        "memory"
    }
}

#[async_trait]
impl AuthStore for InMemoryStore {
    async fn list_idp_issuers(&self, tenant_id: &str) -> StoreResult<Vec<IdpIssuerConfig>> {
        auth::list_idp_issuers(self, tenant_id).await
    }

    async fn upsert_idp_issuer(&self, tenant_id: &str, issuer: IdpIssuerConfig) -> StoreResult<()> {
        auth::upsert_idp_issuer(self, tenant_id, issuer).await
    }

    async fn delete_idp_issuer(&self, tenant_id: &str, issuer: &str) -> StoreResult<()> {
        auth::delete_idp_issuer(self, tenant_id, issuer).await
    }

    async fn list_rbac_policies(&self, tenant_id: &str) -> StoreResult<Vec<PolicyRule>> {
        #[cfg(test)]
        Faults::take(&self.faults.rbac_reads)?;
        auth::list_rbac_policies(self, tenant_id).await
    }

    async fn list_rbac_groupings(&self, tenant_id: &str) -> StoreResult<Vec<GroupingRule>> {
        auth::list_rbac_groupings(self, tenant_id).await
    }

    async fn list_rbac_policies_page(
        &self,
        tenant_id: &str,
        page: PageRequest<PolicyRule>,
    ) -> StoreResult<Page<PolicyRule>> {
        pages::rbac_policies(self, tenant_id, page).await
    }

    async fn list_rbac_groupings_page(
        &self,
        tenant_id: &str,
        page: PageRequest<GroupingRule>,
    ) -> StoreResult<Page<GroupingRule>> {
        pages::rbac_groupings(self, tenant_id, page).await
    }

    async fn add_rbac_policy(&self, tenant_id: &str, policy: PolicyRule) -> StoreResult<()> {
        auth::add_rbac_policy(self, tenant_id, policy).await
    }

    async fn add_rbac_grouping(&self, tenant_id: &str, grouping: GroupingRule) -> StoreResult<()> {
        auth::add_rbac_grouping(self, tenant_id, grouping).await
    }

    async fn remove_rbac_policy(&self, tenant_id: &str, policy: PolicyRule) -> StoreResult<()> {
        auth::remove_rbac_policy(self, tenant_id, &policy).await
    }

    async fn remove_rbac_grouping(
        &self,
        tenant_id: &str,
        grouping: GroupingRule,
    ) -> StoreResult<()> {
        auth::remove_rbac_grouping(self, tenant_id, &grouping).await
    }

    async fn change_auth(
        &self,
        tenant_id: &str,
        change: crate::store::AuthChange,
        revocation: Option<crate::store::Revocation>,
    ) -> StoreResult<()> {
        #[cfg(test)]
        if revocation.is_some() {
            Faults::take(&self.faults.revocation_writes)?;
        }
        auth::change_auth(self, tenant_id, change).await?;
        // Nothing below can fail, so the pair is all or nothing.
        if let Some(revocation) = revocation {
            revocations::revoke_principals(
                self,
                tenant_id,
                revocation.principal_ids,
                revocation.revoked_at,
            )
            .await?;
        }
        Ok(())
    }

    async fn get_tenant_signing_keys(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        auth::get_tenant_signing_keys(self, tenant_id).await
    }

    async fn stage_signing_key(
        &self,
        tenant_id: &str,
        key: SigningKey,
    ) -> StoreResult<TenantSigningKeys> {
        auth::stage_signing_key(self, tenant_id, key).await
    }

    async fn activate_signing_key(
        &self,
        tenant_id: &str,
        kid: &str,
    ) -> StoreResult<TenantSigningKeys> {
        auth::activate_signing_key(self, tenant_id, kid).await
    }

    async fn retire_signing_key(
        &self,
        tenant_id: &str,
        kid: &str,
    ) -> StoreResult<TenantSigningKeys> {
        auth::retire_signing_key(self, tenant_id, kid).await
    }

    async fn set_tenant_signing_keys(
        &self,
        tenant_id: &str,
        keys: TenantSigningKeys,
    ) -> StoreResult<()> {
        auth::set_tenant_signing_keys(self, tenant_id, keys).await
    }

    async fn tenant_auth_is_bootstrapped(&self, tenant_id: &str) -> StoreResult<bool> {
        auth::tenant_auth_is_bootstrapped(self, tenant_id).await
    }

    async fn set_tenant_auth_bootstrapped(
        &self,
        tenant_id: &str,
        bootstrapped: bool,
    ) -> StoreResult<()> {
        auth::set_tenant_auth_bootstrapped(self, tenant_id, bootstrapped).await
    }

    async fn ensure_signing_key_current(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        auth::ensure_signing_key_current(self, tenant_id).await
    }

    async fn seed_rbac_policies_and_groupings(
        &self,
        tenant_id: &str,
        policies: Vec<PolicyRule>,
        groupings: Vec<GroupingRule>,
    ) -> StoreResult<()> {
        auth::seed_rbac_policies_and_groupings(self, tenant_id, policies, groupings).await
    }

    async fn bootstrap_tenant_auth(
        &self,
        tenant_id: &str,
        seed: crate::store::TenantAuthSeed,
    ) -> StoreResult<TenantSigningKeys> {
        auth::bootstrap_tenant_auth(self, tenant_id, seed).await
    }

    async fn insert_refresh_token(&self, token: RefreshToken) -> StoreResult<()> {
        #[cfg(test)]
        Faults::take(&self.faults.refresh_writes)?;
        refresh_tokens::insert_refresh_token(self, token).await
    }

    async fn take_refresh_token(
        &self,
        tenant_id: &str,
        token_id: &str,
        now_secs: i64,
    ) -> StoreResult<RefreshTokenTake> {
        refresh_tokens::take_refresh_token(self, tenant_id, token_id, now_secs).await
    }

    async fn peek_refresh_token(
        &self,
        tenant_id: &str,
        token_id: &str,
    ) -> StoreResult<Option<RefreshToken>> {
        refresh_tokens::peek_refresh_token(self, tenant_id, token_id).await
    }

    async fn rotate_refresh_token(
        &self,
        tenant_id: &str,
        token_id: &str,
        now_secs: i64,
        next: RefreshToken,
    ) -> StoreResult<RefreshTokenTake> {
        #[cfg(test)]
        Faults::take(&self.faults.refresh_writes)?;
        refresh_tokens::rotate_refresh_token(self, tenant_id, token_id, now_secs, next).await
    }

    async fn revoke_refresh_family(&self, tenant_id: &str, family_id: &str) -> StoreResult<u64> {
        refresh_tokens::revoke_refresh_family(self, tenant_id, family_id).await
    }

    async fn revoke_refresh_tokens_for_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> StoreResult<u64> {
        refresh_tokens::revoke_refresh_tokens_for_principal(self, tenant_id, principal_id).await
    }

    async fn purge_expired_refresh_tokens(&self, before_secs: i64) -> StoreResult<u64> {
        refresh_tokens::purge_expired_refresh_tokens(self, before_secs).await
    }

    async fn revoke_principals(
        &self,
        tenant_id: &str,
        principal_ids: Vec<String>,
        revoked_at: i64,
    ) -> StoreResult<()> {
        #[cfg(test)]
        Faults::take(&self.faults.revocation_writes)?;
        revocations::revoke_principals(self, tenant_id, principal_ids, revoked_at).await
    }

    async fn principal_revocations(
        &self,
        since: u64,
    ) -> StoreResult<ChangeSet<crate::auth::revocation::PrincipalRevocation>> {
        revocations::principal_revocations(self, since).await
    }

    async fn principal_revoked_at(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> StoreResult<Option<i64>> {
        #[cfg(test)]
        self.faults
            .cutoff_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        revocations::principal_revoked_at(self, tenant_id, principal_id).await
    }

    async fn principals_revoked_at(
        &self,
        tenant_id: &str,
        principal_ids: &[String],
    ) -> StoreResult<Vec<Option<i64>>> {
        #[cfg(test)]
        self.faults
            .cutoff_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut cutoffs = Vec::with_capacity(principal_ids.len());
        for principal_id in principal_ids {
            cutoffs.push(revocations::principal_revoked_at(self, tenant_id, principal_id).await?);
        }
        Ok(cutoffs)
    }
}

/// How much of a deleted stream's or cache's shard state a delete takes.
///
/// The store's own deletes take everything. A Raft entry says which, so every
/// member applies it the same way whatever build it runs; see
/// `MetaCommand::DeleteTenant`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShardCleanup {
    /// What a member before metadata level 10 does: a stream or cache delete
    /// takes its assignments and leaves its replica reports, and a namespace
    /// or tenant delete takes neither.
    Legacy,
    /// Assignments and replica reports, on every delete.
    Full,
    /// As `Full`, and an unassignment on the shard change log for each
    /// assignment taken. Metadata level 17.
    Unassign,
}

impl ShardCleanup {
    /// Whether every delete takes assignments and replica reports.
    pub(crate) fn takes_shards(self) -> bool {
        self != Self::Legacy
    }
}

/// Who decides incarnations and tombstones on a create or delete.
///
/// A Raft entry from a proposer at metadata level 14 says so, and then
/// decides from its own fields; an entry without it applies as a member
/// before level 14 applies it, which keeps such a member identical even after
/// `cache_incarnations` is finalized under it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decide {
    /// The store's own call: the finalizes as they stand.
    Here,
    /// A level-14 entry: as `Here`, but only a proposed incarnation gives one.
    Entry,
    /// An entry from before level 14. A stream proposing nothing stays at 0
    /// and an unincarnated stream's delete writes no tombstone, as a member
    /// before incarnations applies them; a proposal still gets one, as at
    /// levels 10 to 13. Caches are never incarnated or tombstoned.
    Legacy,
    /// A level-18 entry: as `Entry`, and deleted names are recorded and
    /// refused for a create before the finalize, as `Here` does.
    Named,
}

impl Decide {
    /// Whether this records deleted names and refuses to create one again
    /// before incarnations are finalized (`streams::refuse_reused_name`).
    pub(crate) fn guards_names(self) -> bool {
        matches!(self, Self::Here | Self::Named)
    }
}

/// Node records and their change log under one lock.
///
/// Kept together so a snapshot cannot observe a record set and a `next_seq`
/// that disagree.
#[derive(Debug)]
struct NodeState {
    records: HashMap<String, Node>,
    changes: ChangeLog<NodeChange>,
    /// Enabled fleet features. Under the node lock so a finalize and a
    /// registration cannot interleave.
    fleet_enabled: std::collections::BTreeSet<String>,
}

impl NodeState {
    fn record(&mut self, op: NodeChangeOp, node_id: &str, node: Option<Node>) {
        self.changes.record(|seq| NodeChange {
            seq,
            op,
            node_id: node_id.to_string(),
            node,
        });
    }
}

/// Shard assignments and their change log under one lock.
#[derive(Debug)]
struct ShardState {
    records: HashMap<ShardKey, ShardAssignment>,
    changes: ChangeLog<ShardAssignmentChange>,
    /// Under the same lock as the records, so a fenced write checks and
    /// advances the token in the same step as it writes.
    placement: PlacementState,
}

/// The placement lease and token (`ControlPlaneStore::acquire_placement_lease`).
#[derive(Debug, Default)]
struct PlacementState {
    token: u64,
    holder: Option<String>,
    /// Process clock. Not exported: under Raft the leader holds the lease
    /// and expiry is not used.
    expires_at_millis: u64,
}

impl ShardState {
    fn record(
        &mut self,
        op: ShardAssignmentChangeOp,
        key: &ShardKey,
        assignment: Option<ShardAssignment>,
    ) {
        self.changes.record(|seq| ShardAssignmentChange {
            seq,
            op,
            key: key.clone(),
            assignment,
        });
    }
}

/// How many of the next calls of each kind fail, for tests of what a caller
/// does with a store error.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct Faults {
    pub(crate) rbac_reads: std::sync::atomic::AtomicU32,
    pub(crate) revocation_writes: std::sync::atomic::AtomicU32,
    pub(crate) refresh_writes: std::sync::atomic::AtomicU32,
    /// Counts reads of revocation cutoffs, not a failure.
    pub(crate) cutoff_reads: std::sync::atomic::AtomicU32,
    /// Once set, the next shard write says it has read its stream or cache,
    /// then waits to be resumed (or 300 ms) before it writes.
    pub(crate) shard_write_pause: std::sync::Mutex<Option<ShardWritePause>>,
}

#[cfg(test)]
pub(crate) struct ShardWritePause {
    pub(crate) reached: tokio::sync::oneshot::Sender<()>,
    pub(crate) resume: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
impl Faults {
    async fn pause_shard_write(&self) {
        let pause = self.shard_write_pause.lock().expect("pause").take();
        if let Some(pause) = pause {
            let _ = pause.reached.send(());
            let _ = tokio::time::timeout(std::time::Duration::from_millis(300), pause.resume).await;
        }
    }

    fn take(remaining: &std::sync::atomic::AtomicU32) -> StoreResult<()> {
        use std::sync::atomic::Ordering;
        match remaining.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            left.checked_sub(1)
        }) {
            Ok(_) => Err(crate::store::StoreError::Unexpected(anyhow::anyhow!(
                "injected store failure"
            ))),
            Err(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;

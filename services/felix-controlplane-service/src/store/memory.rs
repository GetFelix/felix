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
    /// Authoritative tenant objects keyed by `tenant_id`.
    tenants: Arc<RwLock<HashMap<String, Tenant>>>,
    /// Authoritative namespaces keyed by `(tenant_id, namespace)`.
    namespaces: Arc<RwLock<HashMap<NamespaceKey, Namespace>>>,
    /// Authoritative streams keyed by `(tenant_id, namespace, stream)`.
    streams: Arc<RwLock<HashMap<StreamKey, Stream>>>,
    /// Authoritative caches keyed by `(tenant_id, namespace, cache)`.
    caches: Arc<RwLock<HashMap<CacheKey, Cache>>>,
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
}

impl InMemoryStore {
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

    pub fn new(config: StoreConfig) -> Self {
        let capacity = config.change_window();
        // The change window is a retention bound for incremental sync consumers.
        // Once capacity is exceeded, oldest changes are evicted.
        Self {
            config,
            tenants: Arc::new(RwLock::new(HashMap::new())),
            namespaces: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(HashMap::new())),
            caches: Arc::new(RwLock::new(HashMap::new())),
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
    /// No change-log entry, deliberately, because Postgres writes none either:
    /// a cascade happens inside the database and never reaches the code that
    /// records unassignments. The delete is already announced on the stream or
    /// cache change log, and a consumer that has been told the stream is gone
    /// does not need to be told separately about the shards of a stream that no
    /// longer exists.
    async fn drop_shard_assignments_for(
        &self,
        kind: ShardKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
    ) {
        let mut state = self.shards.write().await;
        state.records.retain(|key, _| {
            !(key.kind == kind
                && key.tenant_id == tenant_id
                && key.namespace == namespace
                && key.stream == name)
        });
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
        tenants::delete_tenant(self, tenant_id).await
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
        namespaces::delete_namespace(self, key).await
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
        streams::create_stream(self, stream).await
    }

    async fn patch_stream(
        &self,
        key: &StreamKey,
        patch: StreamPatchRequest,
    ) -> StoreResult<Stream> {
        streams::patch_stream(self, key, patch).await
    }

    async fn delete_stream(&self, key: &StreamKey) -> StoreResult<()> {
        streams::delete_stream(self, key).await
    }

    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>> {
        streams::stream_snapshot(self).await
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
        caches::create_cache(self, cache).await
    }

    async fn patch_cache(&self, key: &CacheKey, patch: CachePatchRequest) -> StoreResult<Cache> {
        caches::patch_cache(self, key, patch).await
    }

    async fn delete_cache(&self, key: &CacheKey) -> StoreResult<()> {
        caches::delete_cache(self, key).await
    }

    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>> {
        caches::cache_snapshot(self).await
    }

    async fn cache_changes(&self, since: u64) -> StoreResult<ChangeSet<CacheChange>> {
        caches::cache_changes(self, since).await
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
        shards::put_shard_assignment(self, assignment).await
    }

    async fn put_shard_assignment_if(
        &self,
        assignment: ShardAssignment,
        expected_generation: Option<u64>,
        fence: u64,
    ) -> StoreResult<crate::store::AssignmentWrite> {
        shards::put_shard_assignment_if(self, assignment, expected_generation, Some(fence)).await
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

#[cfg(test)]
mod tests;

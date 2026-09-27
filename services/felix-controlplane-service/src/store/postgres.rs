//! Postgres-backed implementation of `ControlPlaneStore`.
//!
//! Stores control-plane *metadata* (tenants, namespaces, streams, caches). Not
//! the data plane: no events, cache payloads, or broker logs live here.
//!
//! State is kept twice, and the pairing is the point. Authoritative tables hold
//! current state and serve `get_*`/`list_*`. Append-only `*_changes` tables hold
//! an ordered log with a `seq` per table, so a client can bootstrap from
//! `*_snapshot()` once and then poll `*_changes(since = next_seq)` cheaply.
//! Every mutation writes both in one transaction, so "object exists but no
//! change was emitted" cannot happen.
//!
//! Three things about `seq` that are easy to get wrong:
//! - it is monotonic *per change table*, not global across entity types;
//! - `since` is inclusive (`seq >= since`);
//! - `next_seq` is `MAX(seq) + 1`, i.e. the first sequence not yet observed.
//!
//! The optional retention task bounds each change table to the most recent N
//! rows. That caps growth at the cost of the catch-up window: a client that
//! falls further behind than N must re-bootstrap from a snapshot. It is
//! best-effort and never fatal -- a failed pass just retries on the next tick.
//!
//! Dynamic SQL is confined to those retention deletes, against a fixed
//! allowlist of table names defined in code.
mod auth;
mod caches;
mod codec;
mod namespaces;
mod nodes;
mod refresh_tokens;
mod shards;
mod streams;
mod tenants;

use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::{
    AuthStore, ChangeSet, ControlPlaneStore, ReportWrite, Snapshot, StoreConfig, StoreError,
    StoreResult,
};
use crate::auth::felix_token::TenantSigningKeys;
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::auth::refresh_token::{RefreshToken, RefreshTokenTake};
use crate::config::PostgresConfig;
use crate::model::{
    Cache, CacheChange, CacheKey, CachePatchRequest, Namespace, NamespaceChange, NamespaceKey,
    Node, NodeChange, NodeLifecycle, NodePatchRequest, ReplicaReport, ShardAssignment,
    ShardAssignmentChange, ShardKey, Stream, StreamChange, StreamKey, StreamPatchRequest, Tenant,
    TenantChange,
};

#[cfg(feature = "pg-tests")]
const RETENTION_TICK: Duration = Duration::from_secs(1);

#[cfg(not(feature = "pg-tests"))]
const RETENTION_TICK: Duration = Duration::from_secs(60);

/// Durable control-plane store: [`ControlPlaneStore`] and [`AuthStore`] on
/// Postgres. Connection URLs may embed credentials — never log them.
pub struct PostgresStore {
    pool: PgPool,
    config: StoreConfig,
}

impl PostgresStore {
    /// Connect, apply embedded migrations, and start the best-effort
    /// retention task when configured.
    ///
    /// # Errors
    /// Connection, migration, or pool setup failures.
    pub async fn connect(pg: &PostgresConfig, config: StoreConfig) -> StoreResult<Self> {
        #[cfg(any(test, feature = "pg-tests"))]
        let _ = Self::connect_without_migrations;
        Self::connect_internal(pg, config, true).await
    }

    /// Connect without running migrations, for tests that manage the schema
    /// themselves.
    #[cfg(any(test, feature = "pg-tests"))]
    pub async fn connect_without_migrations(
        pg: &PostgresConfig,
        config: StoreConfig,
    ) -> StoreResult<Self> {
        Self::connect_internal(pg, config, false).await
    }

    async fn connect_internal(
        pg: &PostgresConfig,
        config: StoreConfig,
        run_migrations: bool,
    ) -> StoreResult<Self> {
        // Bounded pool and acquire timeout: fail fast and surface a health
        // failure rather than hang when the DB is overloaded. `pg.url` may
        // carry credentials, so it is never logged.
        let connect_options = PgConnectOptions::from_str(&pg.url)?;
        let pool = PgPoolOptions::new()
            .max_connections(pg.max_connections)
            .acquire_timeout(Duration::from_millis(pg.acquire_timeout_ms))
            .connect_with(connect_options)
            .await?;

        if run_migrations {
            // Before serving anything, so handlers can assume the schema; a
            // failed migration fails startup rather than serving a partial API.
            sqlx::migrate!("./migrations").run(&pool).await?;
        }

        // Optional change-log retention: bounds append-only tables so they don't grow forever.
        // Tradeoff: reduces how far behind a client can fall before requiring a full snapshot bootstrap.
        if let Some(retention) = config.change_retention_max_rows {
            spawn_retention_task(pool.clone(), retention);
        }

        Ok(Self { pool, config })
    }

    /// The newest migration this build carries.
    ///
    /// A database *ahead* of it is fine — that is the first half of a rolling
    /// deploy, and the old code keeps working against the new schema because
    /// migrations are additive. Behind it is not: this build would use a column
    /// that is not there yet.
    fn newest_migration() -> i64 {
        sqlx::migrate!("./migrations")
            .iter()
            .map(|migration| migration.version)
            .max()
            .unwrap_or(0)
    }

    /// Page size limit for change queries.
    ///
    /// This bounds response size, memory usage, and tail latency for callers polling changes.
    fn limit(&self) -> i64 {
        self.config.changes_limit as i64
    }

    async fn refresh_counts(&self) -> StoreResult<()> {
        let stream_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM streams")
            .fetch_one(&self.pool)
            .await?;
        metrics::gauge!("felix_streams_total").set(stream_total as f64);

        let cache_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM caches")
            .fetch_one(&self.pool)
            .await?;
        metrics::gauge!("felix_caches_total").set(cache_total as f64);
        Ok(())
    }
}

#[async_trait]
impl ControlPlaneStore for PostgresStore {
    async fn list_tenants(&self) -> StoreResult<Vec<Tenant>> {
        tenants::list_tenants(self).await
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
        nodes::register_node(self, node).await
    }

    async fn get_node(&self, node_id: &str) -> StoreResult<Node> {
        nodes::get_node(self, node_id).await
    }

    async fn list_nodes(&self) -> StoreResult<Vec<Node>> {
        nodes::list_nodes(self).await
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
        nodes::record_node_heartbeat(self, node_id, incarnation, at_millis).await
    }

    async fn now_millis(&self) -> StoreResult<u64> {
        nodes::now_millis(self).await
    }

    async fn expire_stale_nodes(&self, expiry_before_millis: u64) -> StoreResult<Vec<Node>> {
        nodes::expire_stale_nodes(self, expiry_before_millis).await
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
        shards::put_shard_assignment_if(self, assignment, expected_generation, fence).await
    }

    async fn get_shard_assignment(&self, key: &ShardKey) -> StoreResult<ShardAssignment> {
        shards::get_shard_assignment(self, key).await
    }

    async fn list_shard_assignments(&self) -> StoreResult<Vec<ShardAssignment>> {
        shards::list_shard_assignments(self).await
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

    async fn record_replica_report(&self, report: ReplicaReport) -> StoreResult<ReportWrite> {
        shards::record_replica_report(self, report).await
    }

    async fn list_replica_reports(&self) -> StoreResult<Vec<ReplicaReport>> {
        shards::list_replica_reports(self).await
    }

    async fn moves_paused(&self) -> StoreResult<bool> {
        shards::moves_paused(self).await
    }

    async fn set_moves_paused(&self, paused: bool) -> StoreResult<()> {
        shards::set_moves_paused(self, paused).await
    }

    async fn placement_token(&self) -> StoreResult<u64> {
        shards::placement_token(self).await
    }

    async fn acquire_placement_lease(
        &self,
        holder: &str,
        ttl_millis: u64,
    ) -> StoreResult<Option<crate::store::PlacementLease>> {
        shards::acquire_placement_lease(self, holder, ttl_millis).await
    }

    async fn release_placement_lease(&self, holder: &str) -> StoreResult<()> {
        shards::release_placement_lease(self, holder).await
    }

    async fn tenant_exists(&self, tenant_id: &str) -> StoreResult<bool> {
        tenants::tenant_exists(self, tenant_id).await
    }

    async fn namespace_exists(&self, key: &NamespaceKey) -> StoreResult<bool> {
        namespaces::namespace_exists(self, key).await
    }

    /// Connectivity *and* schema, in one query.
    ///
    /// `SELECT 1` proves only that a connection was available. An instance can
    /// hold connections to a database whose schema is older than the code —
    /// during a rolling deploy, or when someone points it at the wrong
    /// database — and it would answer that probe while failing every request
    /// that touches a table it expects. Comparing the applied migration to the
    /// newest embedded one catches that, and costs the same round trip.
    async fn health_check(&self) -> StoreResult<()> {
        let applied: Option<i64> =
            sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations WHERE success")
                .fetch_one(&self.pool)
                .await?;
        let Some(applied) = applied else {
            return Err(StoreError::Unexpected(anyhow!(
                "the database has no migrations applied"
            )));
        };
        let expected = Self::newest_migration();
        if applied < expected {
            return Err(StoreError::Unexpected(anyhow!(
                "the database is at migration {applied}, this build expects {expected}"
            )));
        }
        Ok(())
    }

    fn is_durable(&self) -> bool {
        true
    }

    fn backend_name(&self) -> &'static str {
        "postgres"
    }
}

#[async_trait]
impl AuthStore for PostgresStore {
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

    async fn add_rbac_policy(&self, tenant_id: &str, policy: PolicyRule) -> StoreResult<()> {
        auth::add_rbac_policy(self, tenant_id, policy).await
    }

    async fn add_rbac_grouping(&self, tenant_id: &str, grouping: GroupingRule) -> StoreResult<()> {
        auth::add_rbac_grouping(self, tenant_id, grouping).await
    }

    async fn get_tenant_signing_keys(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        auth::get_tenant_signing_keys(self, tenant_id).await
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

/// Spawn a best-effort background task that bounds each change table to the most recent `max_rows`.
///
/// Why:
/// - Change tables are append-only, which is great for incremental sync, but they can grow without bound.
/// - Bounding them keeps storage predictable for long-lived clusters.
///
/// How:
/// - Every 60s, for each change table:
///   - compute a cutoff: `MAX(seq) - max_rows + 1`
///   - delete rows where `seq < cutoff`
/// - If the table is empty, `COALESCE(..., 0)` makes the delete a no-op.
///
/// Tradeoffs / failure modes:
/// - If a client falls behind beyond the retained window, it cannot catch up from changes alone and
///   must re-bootstrap from a snapshot.
/// - This is not time-based retention; it is “last N changes”.
/// - This is best-effort: transient DB errors are ignored; the task retries next tick.
///
/// Indexing:
/// - For predictable performance, `seq` should be indexed (typically via the primary key) so deletes
///   and `MAX(seq)` are efficient even as tables grow.
fn spawn_retention_task(pool: PgPool, max_rows: i64) {
    // Delete all rows older than the newest `max_rows` entries.
    //
    // The inner SELECT computes the cutoff seq: MAX(seq) - max_rows + 1. If the table is empty,
    // COALESCE returns 0 and the DELETE is a no-op.
    //
    // The table name is baked into each statement at compile time rather than formatted in at
    // runtime, so no caller can reach this query text — that is also what lets sqlx accept these
    // as `&'static str` without an injection-audit escape hatch.
    macro_rules! retention_delete {
        ($table:literal) => {
            concat!(
                "DELETE FROM ",
                $table,
                " WHERE seq < (SELECT COALESCE(MAX(seq) - $1 + 1, 0) FROM ",
                $table,
                ")"
            )
        };
    }

    const RETENTION_DELETES: [&str; 4] = [
        retention_delete!("tenant_changes"),
        retention_delete!("namespace_changes"),
        retention_delete!("stream_changes"),
        retention_delete!("cache_changes"),
    ];

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RETENTION_TICK);
        loop {
            ticker.tick().await;
            for stmt in RETENTION_DELETES {
                let _ = sqlx::query(stmt).bind(max_rows).execute(&pool).await;
            }
        }
    });
}

/// Make the rest of the transaction read one consistent snapshot of the
/// database, rather than re-reading between statements.
async fn begin_consistent_read(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> StoreResult<()> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        return db_err.code().map(|code| code == "23505").unwrap_or(false);
    }
    false
}

#[cfg(test)]
mod tests;

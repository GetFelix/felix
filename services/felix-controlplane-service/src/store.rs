//! The metadata store: the traits every backend implements, and the types
//! they share.
//!
//! Three backends implement them (`memory`, `postgres` and `raft`) and all
//! three run the same contract tests. Callers cannot tell which backend they
//! are talking to, so change ordering, conflicts and not-found must behave
//! the same on each.
pub mod export;
pub mod memory;
pub mod postgres;
pub mod raft;

#[cfg(test)]
pub(crate) mod contract;

use async_trait::async_trait;
use thiserror::Error;

use crate::auth::felix_token::TenantSigningKeys;
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::auth::refresh_token::{RefreshToken, RefreshTokenTake};
use crate::model::{
    Cache, CacheChange, CacheKey, CachePatchRequest, Namespace, NamespaceChange, NamespaceKey,
    Node, NodeChange, NodePatchRequest, ReplicaReport, ShardAssignment, ShardAssignmentChange,
    ShardKey, Stream, StreamChange, StreamKey, StreamPatchRequest, Tenant, TenantChange,
};

/// Tenants, namespaces, streams, caches, nodes and shard assignments: every
/// operation the API and the background loops need.
#[async_trait]
pub trait ControlPlaneStore: Send + Sync {
    async fn list_tenants(&self) -> StoreResult<Vec<Tenant>>;
    async fn create_tenant(&self, tenant: Tenant) -> StoreResult<Tenant>;
    async fn delete_tenant(&self, tenant_id: &str) -> StoreResult<()>;
    async fn tenant_snapshot(&self) -> StoreResult<Snapshot<Tenant>>;
    async fn tenant_changes(&self, since: u64) -> StoreResult<ChangeSet<TenantChange>>;

    async fn list_namespaces(&self, tenant_id: &str) -> StoreResult<Vec<Namespace>>;
    async fn create_namespace(&self, namespace: Namespace) -> StoreResult<Namespace>;
    async fn delete_namespace(&self, key: &NamespaceKey) -> StoreResult<()>;
    async fn namespace_snapshot(&self) -> StoreResult<Snapshot<Namespace>>;
    async fn namespace_changes(&self, since: u64) -> StoreResult<ChangeSet<NamespaceChange>>;

    async fn list_streams(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Stream>>;
    async fn get_stream(&self, key: &StreamKey) -> StoreResult<Stream>;
    async fn create_stream(&self, stream: Stream) -> StoreResult<Stream>;
    async fn patch_stream(&self, key: &StreamKey, patch: StreamPatchRequest)
    -> StoreResult<Stream>;
    async fn delete_stream(&self, key: &StreamKey) -> StoreResult<()>;
    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>>;
    async fn stream_changes(&self, since: u64) -> StoreResult<ChangeSet<StreamChange>>;

    async fn list_caches(&self, tenant_id: &str, namespace: &str) -> StoreResult<Vec<Cache>>;
    async fn get_cache(&self, key: &CacheKey) -> StoreResult<Cache>;
    async fn create_cache(&self, cache: Cache) -> StoreResult<Cache>;
    async fn patch_cache(&self, key: &CacheKey, patch: CachePatchRequest) -> StoreResult<Cache>;
    async fn delete_cache(&self, key: &CacheKey) -> StoreResult<()>;
    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>>;
    async fn cache_changes(&self, since: u64) -> StoreResult<ChangeSet<CacheChange>>;

    /// Register a node, or revive the record a restarting node already has.
    ///
    /// `node_id` identifies a broker across restarts, so registering an id that
    /// already exists updates its spec and bumps `incarnation` rather than
    /// conflicting. `registered_at_millis` is preserved from the first
    /// registration; the caller's value is used only for a new node.
    ///
    /// Conflicts only when `advertise_addr` belongs to a different node.
    async fn register_node(&self, node: Node) -> StoreResult<Node>;
    async fn get_node(&self, node_id: &str) -> StoreResult<Node>;
    async fn list_nodes(&self) -> StoreResult<Vec<Node>>;
    /// Apply an operator patch. Rejects a lifecycle transition the model
    /// disallows, and never touches heartbeat-derived fields.
    async fn patch_node(&self, node_id: &str, patch: NodePatchRequest) -> StoreResult<Node>;
    /// Remove a node.
    ///
    /// Rejected while the node still leads a shard. Cascading instead would
    /// delete the only record of where that shard's data lives, turning an
    /// operator's tidy-up into silent data orphaning; refusing forces the shard
    /// to be reassigned first. There is deliberately no foreign key doing this,
    /// because a database-level cascade is exactly the behaviour being avoided.
    async fn delete_node(&self, node_id: &str) -> StoreResult<()>;
    /// Record liveness without emitting a change.
    ///
    /// Heartbeats arrive per node per interval; putting each one in the
    /// changefeed would evict every real membership change from the retention
    /// window. Only a lifecycle move a heartbeat causes is worth publishing,
    /// and that is [`ControlPlaneStore::set_node_lifecycle`].
    ///
    /// `incarnation` is the caller's own, and one older than the stored value
    /// is rejected: it comes from a process the broker has already replaced,
    /// and honouring it would report a dead incarnation as live.
    ///
    /// Never revives. A node the cluster marked `Down` stays down until it
    /// registers again, because a heartbeat proves a process is running, not
    /// that it still owns the identity.
    ///
    /// `at_millis` never moves the stored value backwards, so a delayed
    /// heartbeat is a no-op rather than a regression.
    ///
    /// The stored time may not be the `at_millis` passed in. Under Raft the
    /// leader overwrites it as it accepts the proposal, because the instance
    /// a broker's heartbeat happens to reach is not the one that later judges
    /// it stale. Pass [`ControlPlaneStore::now_millis`] and treat the returned
    /// node as the record of what was stored.
    async fn record_node_heartbeat(
        &self,
        node_id: &str,
        incarnation: u64,
        at_millis: u64,
    ) -> StoreResult<Node>;
    /// The clock that heartbeats are stamped with and expiry is judged against.
    ///
    /// One clock, because the two sides are compared. With several stateless
    /// instances over one database, the instance that records a heartbeat and
    /// the instance that runs the expiry sweep are different processes, so
    /// reading each one's own `SystemTime` makes safety depend on their wall
    /// clocks agreeing to within the margin — a much stronger assumption than
    /// the bound on drift *rate* the design assumes, and one NTP steps break
    /// outright.
    ///
    /// Backends with a shared clock return it — Postgres answers with
    /// `clock_timestamp()`. The default is the process clock, which is
    /// correct for a single-process store and for the Raft backend, where
    /// the leader overwrites a proposer's reading before the command enters
    /// the log (`store::raft::command::restamp`) and the sweep that reads it back
    /// runs only on that same leader. Both sides of the comparison are one
    /// process's clock either way; the two backends just reach that
    /// differently.
    async fn now_millis(&self) -> StoreResult<u64> {
        Ok(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_millis() as u64)
            .unwrap_or(0))
    }
    /// Mark every node whose last heartbeat predates `expiry_before_millis` as
    /// down, and return the ones this call moved.
    ///
    /// Safe to run from several control-plane instances at once: each node is
    /// moved by exactly one of them, and only that one publishes the change.
    async fn expire_stale_nodes(&self, expiry_before_millis: u64) -> StoreResult<Vec<Node>>;
    /// Move a node's lifecycle from an observed signal rather than an operator.
    ///
    /// Unlike [`ControlPlaneStore::patch_node`] this may drive transitions an
    /// operator cannot, because liveness expiry and graceful shutdown are not
    /// admin actions. Returns `Ok(None)` when the node is already there, so a
    /// repeated sweep does not publish a change per pass.
    async fn set_node_lifecycle(
        &self,
        node_id: &str,
        lifecycle: crate::model::NodeLifecycle,
    ) -> StoreResult<Option<Node>>;
    async fn node_snapshot(&self) -> StoreResult<Snapshot<Node>>;
    async fn node_changes(&self, since: u64) -> StoreResult<ChangeSet<NodeChange>>;

    /// Write the assignment for one shard, replacing whatever it had.
    ///
    /// The caller supplies the assignment; the store owns `generation` and
    /// increments it on every write, so a broker reporting against a generation
    /// it read earlier can be told it is stale.
    ///
    /// Rejects a shard outside the stream's shard count, a leader or replica
    /// that is not a registered node, and a state transition the model
    /// disallows. Referential integrity is checked here rather than left to the
    /// database because the node reference deliberately has no foreign key --
    /// see [`ControlPlaneStore::delete_node`].
    async fn put_shard_assignment(
        &self,
        assignment: ShardAssignment,
    ) -> StoreResult<ShardAssignment>;
    /// [`ControlPlaneStore::put_shard_assignment`], but only if the shard is
    /// still at `expected_generation` (`None`: still has no assignment).
    ///
    /// Placement writes through this. Every instance may run it, each deciding
    /// from its own read, and an unconditional write planned from an old read
    /// can undo a newer step -- a fence written after the cut-over it preceded
    /// hands the shard back to a leader missing writes the new one acknowledged.
    ///
    /// A changed shard is [`AssignmentWrite::Stale`], not an error: nothing is
    /// written, and the caller re-plans from a fresh read. It is checked before
    /// the state transition, so a stale write is reported as stale rather than
    /// as a transition the model disallows.
    ///
    /// It is also fenced: it lands only if the placement token is still
    /// `fence`, and landing advances the token by one. A generation guards one
    /// shard, but the move limits are about all of them, and two writers that
    /// each read a free slot would otherwise start moves on two different
    /// shards. The caller reads [`ControlPlaneStore::placement_token`] before
    /// it reads anything it decides from, and a write that finds another
    /// placement write since is [`AssignmentWrite::Fenced`]. Checked before the
    /// generation, under the same lock or log entry as the write.
    async fn put_shard_assignment_if(
        &self,
        assignment: ShardAssignment,
        expected_generation: Option<u64>,
        fence: u64,
    ) -> StoreResult<AssignmentWrite>;
    async fn get_shard_assignment(&self, key: &ShardKey) -> StoreResult<ShardAssignment>;
    /// Every assignment, ordered by stream then shard.
    async fn list_shard_assignments(&self) -> StoreResult<Vec<ShardAssignment>>;
    /// Assignments a node currently leads. The question placement asks when a
    /// node fails or is drained.
    async fn list_shard_assignments_for_node(
        &self,
        node_id: &str,
    ) -> StoreResult<Vec<ShardAssignment>>;
    /// Remove one assignment, leaving the shard unowned.
    async fn delete_shard_assignment(&self, key: &ShardKey) -> StoreResult<()>;
    async fn shard_assignment_snapshot(&self) -> StoreResult<Snapshot<ShardAssignment>>;
    async fn shard_assignment_changes(
        &self,
        since: u64,
    ) -> StoreResult<ChangeSet<ShardAssignmentChange>>;

    /// Record what a shard's leader reports about its replicas.
    ///
    /// A report that does not supersede the one held is dropped and answered
    /// [`ReportWrite::Stale`], not an error: an older generation's view is
    /// about a replica set that may no longer exist, and an older report in
    /// the same generation would roll the positions back. The caller must
    /// not treat a stale report as recorded. `NotFound` when the shard has no
    /// assignment -- nobody leads it, so nobody can report on it -- and a
    /// deleted assignment takes its report with it, so a shard that is
    /// removed and recreated does not inherit the old one's promotability.
    ///
    /// Stamp it with [`ControlPlaneStore::now_millis`]: freshness is judged
    /// against that same clock by whichever instance runs placement, which
    /// is the whole reason the report is in the store. Under Raft the leader
    /// overwrites the stamp as it accepts the proposal, as for a heartbeat.
    async fn record_replica_report(&self, report: ReplicaReport) -> StoreResult<ReportWrite>;
    /// Every report held, fresh or not; the reader judges freshness.
    async fn list_replica_reports(&self) -> StoreResult<Vec<ReplicaReport>>;

    /// Whether placement is paused: it starts no moves of its own, while
    /// moves in flight finish and an operator may still start one. Read by
    /// every instance's placement on every pass, so it lives in the store.
    async fn moves_paused(&self) -> StoreResult<bool>;
    /// Pause or resume placement's moves. Setting the value it already has is
    /// not an error.
    async fn set_moves_paused(&self, paused: bool) -> StoreResult<()>;

    /// The placement token: advanced by every placement write, and by every
    /// change of lease holder. See [`ControlPlaneStore::put_shard_assignment_if`].
    async fn placement_token(&self) -> StoreResult<u64>;
    /// Take or renew the placement lease for `holder`, until `ttl_millis` from
    /// now by the store's clock. `None` while another holder's lease is
    /// unexpired.
    ///
    /// The lease decides which instance runs placement's timed passes, so
    /// several instances do not plan the same moves over and over. It is not
    /// what keeps them safe: the token is. A new holder advances it, so an
    /// ex-holder that paused past its lease finds its next write fenced.
    ///
    /// Under Raft the confirmed leader is the holder and takes the lease at
    /// once; `ttl_millis` is not used.
    async fn acquire_placement_lease(
        &self,
        holder: &str,
        ttl_millis: u64,
    ) -> StoreResult<Option<PlacementLease>>;
    /// Give up the lease if `holder` has it, so another instance takes over
    /// without waiting for it to expire.
    async fn release_placement_lease(&self, holder: &str) -> StoreResult<()>;

    async fn tenant_exists(&self, tenant_id: &str) -> StoreResult<bool>;
    async fn namespace_exists(&self, key: &NamespaceKey) -> StoreResult<bool>;

    async fn health_check(&self) -> StoreResult<()>;
    fn is_durable(&self) -> bool;
    fn backend_name(&self) -> &'static str;
}

/// Per-tenant auth state: IdP issuers, RBAC rules, signing keys, the
/// bootstrap flag, and refresh tokens.
#[async_trait]
pub trait AuthStore: Send + Sync {
    async fn list_idp_issuers(&self, tenant_id: &str) -> StoreResult<Vec<IdpIssuerConfig>>;
    async fn upsert_idp_issuer(&self, tenant_id: &str, issuer: IdpIssuerConfig) -> StoreResult<()>;
    async fn delete_idp_issuer(&self, tenant_id: &str, issuer: &str) -> StoreResult<()>;

    async fn list_rbac_policies(&self, tenant_id: &str) -> StoreResult<Vec<PolicyRule>>;
    async fn list_rbac_groupings(&self, tenant_id: &str) -> StoreResult<Vec<GroupingRule>>;
    async fn add_rbac_policy(&self, tenant_id: &str, policy: PolicyRule) -> StoreResult<()>;
    async fn add_rbac_grouping(&self, tenant_id: &str, grouping: GroupingRule) -> StoreResult<()>;

    async fn get_tenant_signing_keys(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys>;
    async fn set_tenant_signing_keys(
        &self,
        tenant_id: &str,
        keys: TenantSigningKeys,
    ) -> StoreResult<()>;

    async fn tenant_auth_is_bootstrapped(&self, tenant_id: &str) -> StoreResult<bool>;
    async fn set_tenant_auth_bootstrapped(
        &self,
        tenant_id: &str,
        bootstrapped: bool,
    ) -> StoreResult<()>;

    async fn ensure_signing_key_current(&self, tenant_id: &str) -> StoreResult<TenantSigningKeys>;
    async fn seed_rbac_policies_and_groupings(
        &self,
        tenant_id: &str,
        policies: Vec<PolicyRule>,
        groupings: Vec<GroupingRule>,
    ) -> StoreResult<()>;

    /// Perform the whole tenant auth bootstrap — signing keys, issuers, RBAC
    /// seed, and the bootstrapped flag — as one atomic, exactly-once operation.
    ///
    /// Any number of control-plane instances may receive the same bootstrap
    /// request concurrently; exactly one wins and returns the signing keys the
    /// tenant ends up with, and every other caller gets
    /// [`StoreError::Conflict`]. Written as a single store operation because
    /// the pieces are only correct together: a winner decided by a check
    /// outside the transaction can interleave with another instance's writes —
    /// two racing initializes each generating keys leaves one caller holding a
    /// `kid` the other overwrote.
    ///
    /// A failure part-way must leave the tenant *not* bootstrapped, so the
    /// operator can simply retry.
    async fn bootstrap_tenant_auth(
        &self,
        tenant_id: &str,
        seed: TenantAuthSeed,
    ) -> StoreResult<TenantSigningKeys>;

    /// Record a freshly minted refresh token.
    ///
    /// The record carries the secret's hash, never the secret.
    async fn insert_refresh_token(&self, token: RefreshToken) -> StoreResult<()>;

    /// Spend a refresh token, if it is live, and say what was found.
    ///
    /// **This is one atomic step, and that is the whole point.** Checking
    /// liveness and marking the token spent as two operations lets two
    /// concurrent refreshes both pass the check, which turns a single-use token
    /// into a reusable one exactly when someone is racing to use a stolen copy.
    ///
    /// It does not verify the secret — the caller does that against the
    /// returned record. A store that compared secrets would need the secret,
    /// and the secret is the one thing that must not travel to the store.
    async fn take_refresh_token(
        &self,
        tenant_id: &str,
        token_id: &str,
        now_secs: i64,
    ) -> StoreResult<RefreshTokenTake>;

    /// Revoke every token in one rotation chain, returning how many were live.
    ///
    /// The response to a replay. One of the two holders is an attacker and the
    /// store cannot tell which, so the chain ends for both.
    async fn revoke_refresh_family(&self, tenant_id: &str, family_id: &str) -> StoreResult<u64>;

    /// Revoke every refresh token a principal holds in this tenant.
    ///
    /// The operator-facing half: a compromised principal is cut off without
    /// waiting out any token's expiry.
    async fn revoke_refresh_tokens_for_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> StoreResult<u64>;

    /// Drop records that expired before `before_secs`.
    ///
    /// Housekeeping, not security: an expired token is already refused. This
    /// stops the table growing without bound, and the record of a replay is
    /// worth keeping until it can no longer be presented.
    async fn purge_expired_refresh_tokens(&self, before_secs: i64) -> StoreResult<u64>;
}

/// Everything [`AuthStore::bootstrap_tenant_auth`] writes besides the keys it
/// generates.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TenantAuthSeed {
    pub issuers: Vec<IdpIssuerConfig>,
    pub policies: Vec<PolicyRule>,
    pub groupings: Vec<GroupingRule>,
    /// The signing keys to install **if the tenant has none yet** — a tenant
    /// that already holds keys keeps them, and these are ignored.
    ///
    /// Generated by the caller rather than inside the store, because key
    /// generation is randomness and the Raft state machine applies this
    /// operation on every replica: whatever is nondeterministic must be
    /// decided once, before the operation is proposed, and carried in it.
    pub signing_keys: TenantSigningKeys,
}

pub trait ControlPlaneAuthStore: ControlPlaneStore + AuthStore {}

impl<T> ControlPlaneAuthStore for T where T: ControlPlaneStore + AuthStore {}

#[derive(Debug, Clone)]
pub struct Snapshot<T> {
    pub items: Vec<T>,
    pub next_seq: u64,
}

#[derive(Debug, Clone)]
pub struct ChangeSet<T> {
    pub items: Vec<T>,
    pub next_seq: u64,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

impl From<sqlx::Error> for StoreError {
    fn from(err: sqlx::Error) -> Self {
        StoreError::Unexpected(err.into())
    }
}

impl From<sqlx::migrate::MigrateError> for StoreError {
    fn from(err: sqlx::migrate::MigrateError) -> Self {
        StoreError::Unexpected(err.into())
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(err: serde_json::Error) -> Self {
        StoreError::Unexpected(err.into())
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

/// What [`ControlPlaneStore::put_shard_assignment_if`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssignmentWrite {
    Written(ShardAssignment),
    /// The shard was not at the expected generation, and nothing was written.
    Stale {
        /// The generation it is at, or `None` if it has no assignment.
        current: Option<u64>,
    },
    /// Another placement write landed since the caller read the token, and
    /// nothing was written.
    Fenced {
        /// The token now.
        token: u64,
    },
}

/// What [`ControlPlaneStore::record_replica_report`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportWrite {
    /// The report is now the one held for its shard.
    Stored,
    /// The store holds a report this one does not supersede (see
    /// [`ReplicaReport::supersedes`]), and kept it.
    Stale,
}

/// What [`ControlPlaneStore::acquire_placement_lease`] granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementLease {
    /// The placement token as of the grant.
    pub token: u64,
    /// Whether the lease changed hands, rather than being renewed.
    pub taken: bool,
}

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub changes_limit: u64,
    pub change_retention_max_rows: Option<i64>,
}

impl StoreConfig {
    pub fn change_window(&self) -> usize {
        // Clamp to at least changes_limit; the DB retention may be higher but never lower.
        self.change_retention_max_rows
            .unwrap_or(self.changes_limit as i64)
            .max(self.changes_limit as i64) as usize
    }
}

#[cfg(test)]
mod tests;

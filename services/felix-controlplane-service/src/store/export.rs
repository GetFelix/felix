//! The whole metadata state as one serializable value.
//!
//! This is the Raft state machine's snapshot format and what `migrate` moves
//! between backends, so its serialized shape is a compatibility surface.
use crate::auth::felix_token::TenantSigningKeys;
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::{
    Cache, CacheChange, CacheKey, Namespace, NamespaceChange, NamespaceKey, Node, NodeChange,
    ShardAssignment, ShardAssignmentChange, ShardKey, Stream, StreamChange, StreamKey, Tenant,
    TenantChange,
};
use crate::store::{StoreError, StoreResult};

/// The exported snapshot format version. Bump on shape changes; an import
/// refuses a newer version rather than misreading it.
pub(super) const EXPORTED_STATE_VERSION: u16 = 1;

/// The whole store as one serializable value — the Raft state machine's
/// snapshot format.
///
/// Every map is exported as a **sorted** vector: two replicas that applied
/// the same command log must serialize byte-identical state, and HashMap
/// iteration order is the one thing in this store that would differ between
/// them. Change logs come with their sequence positions, so a restored
/// store keeps answering `changes(since)` exactly as the original —
/// including the resnapshot signals a stale `since` triggers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExportedState {
    pub(super) v: u16,
    pub(super) tenants: Vec<(String, Tenant)>,
    pub(super) namespaces: Vec<(NamespaceKey, Namespace)>,
    pub(super) streams: Vec<(StreamKey, Stream)>,
    pub(super) caches: Vec<(CacheKey, Cache)>,
    pub(super) nodes: Vec<(String, Node)>,
    pub(super) node_changes: ExportedLog<NodeChange>,
    pub(super) shards: Vec<(ShardKey, ShardAssignment)>,
    pub(super) shard_changes: ExportedLog<ShardAssignmentChange>,
    pub(super) tenant_changes: ExportedLog<TenantChange>,
    pub(super) namespace_changes: ExportedLog<NamespaceChange>,
    pub(super) stream_changes: ExportedLog<StreamChange>,
    pub(super) cache_changes: ExportedLog<CacheChange>,
    pub(super) idp_issuers: Vec<(String, Vec<IdpIssuerConfig>)>,
    pub(super) tenant_signing_keys: Vec<(String, TenantSigningKeys)>,
    pub(super) rbac_policies: Vec<(String, Vec<PolicyRule>)>,
    pub(super) rbac_groupings: Vec<(String, Vec<GroupingRule>)>,
    pub(super) auth_bootstrapped: Vec<(String, bool)>,
    /// Absent from a state exported before moves could be paused, and left
    /// out while they are not, so such a snapshot is byte for byte what it
    /// was.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(super) moves_paused: bool,
    /// The placement token and lease holder. In a Raft snapshot because a
    /// fenced write's outcome depends on them, and every replica has to
    /// decide it the same way. Absent from older snapshots, as for
    /// `moves_paused`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(super) placement_token: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) placement_holder: Option<String>,
    /// Refresh tokens, by hash only. In a Raft snapshot because a follower
    /// that installs one must agree with the leader on which tokens exist and
    /// which are spent or revoked. Absent from older snapshots, and from a
    /// `migrate` export, which has no store-wide way to list them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) refresh_tokens: Vec<crate::auth::refresh_token::RefreshToken>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

impl ExportedState {
    /// What the operator is about to move, for the tool's own output —
    /// the cheap sanity check before and after a cutover.
    pub fn summary(&self) -> String {
        format!(
            "{} tenants, {} namespaces, {} streams, {} caches, {} nodes, {} shard assignments",
            self.tenants.len(),
            self.namespaces.len(),
            self.streams.len(),
            self.caches.len(),
            self.nodes.len(),
            self.shards.len(),
        )
    }
}

/// One change stream, exported: position and retained window, but not
/// capacity — that is configuration, and every instance applies its own.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExportedLog<T> {
    pub(super) next_seq: u64,
    pub(super) items: Vec<T>,
}

/// Export any store — Postgres included — through the traits it already
/// implements, into the Raft state machine's snapshot format.
///
/// This is the migration's whole read side, and it deliberately reuses the
/// snapshot endpoints (`*_snapshot()` returns records **and** the feed
/// position as one value) so the exported change feeds carry the source's
/// sequence high-water marks with **empty retained windows**. A broker whose
/// checkpoint equals the head continues without noticing; one behind the
/// head gets the ordinary "your checkpoint predates the window, resnapshot"
/// signal — the at-most-one-resnapshot cost the migration accepts instead
/// of dragging Postgres's change rows along.
///
/// Consistency is the caller's job: run this only against a store whose
/// writes are frozen (the cutover ceremony's first step), because the reads
/// span many calls.
pub async fn export_state_from(
    store: &(dyn crate::store::ControlPlaneAuthStore + Send + Sync),
) -> StoreResult<ExportedState> {
    let tenant_snapshot = store.tenant_snapshot().await?;
    let namespace_snapshot = store.namespace_snapshot().await?;
    let stream_snapshot = store.stream_snapshot().await?;
    let cache_snapshot = store.cache_snapshot().await?;
    let node_snapshot = store.node_snapshot().await?;
    let shard_snapshot = store.shard_assignment_snapshot().await?;

    let mut tenants: Vec<(String, Tenant)> = tenant_snapshot
        .items
        .into_iter()
        .map(|tenant| (tenant.tenant_id.clone(), tenant))
        .collect();
    tenants.sort_by(|a, b| a.0.cmp(&b.0));

    let mut idp_issuers = Vec::new();
    let mut tenant_signing_keys = Vec::new();
    let mut rbac_policies = Vec::new();
    let mut rbac_groupings = Vec::new();
    let mut auth_bootstrapped = Vec::new();
    for (tenant_id, _) in &tenants {
        let issuers = store.list_idp_issuers(tenant_id).await?;
        if !issuers.is_empty() {
            idp_issuers.push((tenant_id.clone(), issuers));
        }
        match store.get_tenant_signing_keys(tenant_id).await {
            Ok(keys) => tenant_signing_keys.push((tenant_id.clone(), keys)),
            Err(StoreError::NotFound(_)) => {}
            Err(err) => return Err(err),
        }
        let policies = store.list_rbac_policies(tenant_id).await?;
        if !policies.is_empty() {
            rbac_policies.push((tenant_id.clone(), policies));
        }
        let groupings = store.list_rbac_groupings(tenant_id).await?;
        if !groupings.is_empty() {
            rbac_groupings.push((tenant_id.clone(), groupings));
        }
        if store.tenant_auth_is_bootstrapped(tenant_id).await? {
            auth_bootstrapped.push((tenant_id.clone(), true));
        }
    }

    let mut namespaces: Vec<(NamespaceKey, Namespace)> = namespace_snapshot
        .items
        .into_iter()
        .map(|namespace| {
            (
                NamespaceKey {
                    tenant_id: namespace.tenant_id.clone(),
                    namespace: namespace.namespace.clone(),
                },
                namespace,
            )
        })
        .collect();
    namespaces
        .sort_by(|a, b| (&a.0.tenant_id, &a.0.namespace).cmp(&(&b.0.tenant_id, &b.0.namespace)));

    let mut streams: Vec<(StreamKey, Stream)> = stream_snapshot
        .items
        .into_iter()
        .map(|stream| {
            (
                StreamKey {
                    tenant_id: stream.tenant_id.clone(),
                    namespace: stream.namespace.clone(),
                    stream: stream.stream.clone(),
                },
                stream,
            )
        })
        .collect();
    streams.sort_by(|a, b| {
        (&a.0.tenant_id, &a.0.namespace, &a.0.stream).cmp(&(
            &b.0.tenant_id,
            &b.0.namespace,
            &b.0.stream,
        ))
    });

    let mut caches: Vec<(CacheKey, Cache)> = cache_snapshot
        .items
        .into_iter()
        .map(|cache| {
            (
                CacheKey {
                    tenant_id: cache.tenant_id.clone(),
                    namespace: cache.namespace.clone(),
                    cache: cache.cache.clone(),
                },
                cache,
            )
        })
        .collect();
    caches.sort_by(|a, b| {
        (&a.0.tenant_id, &a.0.namespace, &a.0.cache).cmp(&(
            &b.0.tenant_id,
            &b.0.namespace,
            &b.0.cache,
        ))
    });

    let mut nodes: Vec<(String, Node)> = node_snapshot
        .items
        .into_iter()
        .map(|node| (node.node_id.clone(), node))
        .collect();
    nodes.sort_by(|a, b| a.0.cmp(&b.0));

    let mut shards: Vec<(ShardKey, ShardAssignment)> = shard_snapshot
        .items
        .into_iter()
        .map(|assignment| (assignment.key.clone(), assignment))
        .collect();
    shards.sort_by(|a, b| {
        (
            &a.0.tenant_id,
            &a.0.namespace,
            a.0.kind,
            &a.0.stream,
            a.0.shard,
        )
            .cmp(&(
                &b.0.tenant_id,
                &b.0.namespace,
                b.0.kind,
                &b.0.stream,
                b.0.shard,
            ))
    });

    fn empty_log_at<T>(next_seq: u64) -> ExportedLog<T> {
        ExportedLog {
            next_seq,
            items: Vec::new(),
        }
    }

    Ok(ExportedState {
        v: EXPORTED_STATE_VERSION,
        tenants,
        namespaces,
        streams,
        caches,
        nodes,
        node_changes: empty_log_at(node_snapshot.next_seq),
        shards,
        shard_changes: empty_log_at(shard_snapshot.next_seq),
        tenant_changes: empty_log_at(tenant_snapshot.next_seq),
        namespace_changes: empty_log_at(namespace_snapshot.next_seq),
        stream_changes: empty_log_at(stream_snapshot.next_seq),
        cache_changes: empty_log_at(cache_snapshot.next_seq),
        idp_issuers,
        tenant_signing_keys,
        rbac_policies,
        rbac_groupings,
        auth_bootstrapped,
        moves_paused: store.moves_paused().await?,
        // Not the holder: whoever runs placement against the new store takes
        // the lease there, and advancing past this token fences every write
        // planned against the old one.
        placement_token: store.placement_token().await?,
        placement_holder: None,
        // The store traits cannot list them. Holders re-exchange after a
        // migration, as they would after any lost refresh chain.
        refresh_tokens: Vec::new(),
    })
}

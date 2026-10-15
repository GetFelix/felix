//! The control plane's snapshot and change-feed bodies, as this broker reads
//! them.

use serde::{Deserialize, Serialize};

/// Full snapshot response for streams; used to seed a cold-start broker.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct StreamSnapshotResponse {
    pub(super) items: Vec<Stream>,
    pub(super) next_seq: u64,
}

/// Full snapshot response for caches; used to seed a cold-start broker.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct CacheSnapshotResponse {
    pub(super) items: Vec<Cache>,
    pub(super) next_seq: u64,
}

/// Full snapshot response for tenants; used to seed a cold-start broker.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct TenantSnapshotResponse {
    pub(super) items: Vec<Tenant>,
    pub(super) next_seq: u64,
}

/// Full snapshot response for namespaces; used to seed a cold-start broker.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct NamespaceSnapshotResponse {
    pub(super) items: Vec<Namespace>,
    pub(super) next_seq: u64,
}

/// Incremental change feed response for streams; used after snapshot seeding.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct StreamChangesResponse {
    pub(super) items: Vec<StreamChange>,
    pub(super) next_seq: u64,
}

/// Incremental change feed response for caches; used after snapshot seeding.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct CacheChangesResponse {
    pub(super) items: Vec<CacheChange>,
    pub(super) next_seq: u64,
}

/// Incremental change feed response for tenants; used after snapshot seeding.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct TenantChangesResponse {
    pub(super) items: Vec<TenantChange>,
    pub(super) next_seq: u64,
}

/// Incremental change feed response for namespaces; used after snapshot seeding.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct NamespaceChangesResponse {
    pub(super) items: Vec<NamespaceChange>,
    pub(super) next_seq: u64,
}

/// Represents a single change-feed event for a tenant.
/// - `op` is the operation (Created/Deleted).
/// - `tenant_id` identifies the tenant.
/// - `tenant` is present for Created.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct TenantChange {
    pub(super) op: TenantChangeOp,
    pub(super) tenant_id: String,
    pub(super) tenant: Option<Tenant>,
}

/// Represents a single change-feed event for a namespace.
/// - `op` is the operation (Created/Deleted).
/// - `key` identifies the namespace (tenant_id + namespace).
/// - `namespace` is present for Created.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct NamespaceChange {
    pub(super) op: NamespaceChangeOp,
    pub(super) key: NamespaceKey,
    pub(super) namespace: Option<Namespace>,
}

/// Represents a single change-feed event for a stream.
/// - `op` is the operation (Created/Updated/Deleted).
/// - `key` identifies the stream (tenant_id + namespace + stream).
/// - `stream` is present for Created/Updated.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct StreamChange {
    pub(super) op: StreamChangeOp,
    pub(super) key: StreamKey,
    pub(super) stream: Option<Stream>,
}

/// Represents a single change-feed event for a cache.
/// - `op` is the operation (Created/Updated/Deleted).
/// - `key` identifies the cache (tenant_id + namespace + cache).
/// - `cache` is present for Created/Updated.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct CacheChange {
    pub(super) op: CacheChangeOp,
    pub(super) key: CacheKey,
    pub(super) cache: Option<Cache>,
}

/// Represents a tenant as registered in the broker registry.
/// Maps to the broker's tenant registry.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Tenant {
    pub(super) tenant_id: String,
}

/// Represents a namespace as registered in the broker registry.
/// Maps to the broker's namespace registry.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Namespace {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
}

/// Represents a stream as registered in the broker registry.
/// Maps to the broker's stream registry.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Stream {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) stream: String,
    pub(super) shards: u32,
    pub(super) durable: bool,
    /// Absent from a control plane that predates replication, which reads as
    /// `Leader` -- the behaviour every stream had before this existed.
    #[serde(default)]
    pub(super) consistency: Option<String>,
    /// Absent from a control plane that predates per-stream retention, which
    /// reads as the broker's own bounds.
    #[serde(default)]
    pub(super) retention: Option<Retention>,
}

/// A stream's retention as the control plane stores it.
#[derive(Debug, Default, Deserialize, Serialize)]
pub(super) struct Retention {
    #[serde(default)]
    pub(super) max_age_seconds: Option<u64>,
    #[serde(default)]
    pub(super) max_size_bytes: Option<u64>,
}

impl Stream {
    /// What the broker registers this stream with.
    pub(super) fn metadata(&self) -> anyhow::Result<felix_broker::StreamMetadata> {
        let retention = self.retention.as_ref();
        // Zero is no bound. The control plane refuses it, and one stored
        // before it did must not fail the whole sync.
        let bound = |value: Option<u64>| value.filter(|value| *value > 0);
        Ok(felix_broker::StreamMetadata {
            durable: self.durable,
            shards: self.shards,
            consistency: super::apply::read_consistency(self.consistency.as_deref())?,
            retention: felix_storage::log::Retention {
                bytes: bound(retention.and_then(|r| r.max_size_bytes)),
                age: bound(retention.and_then(|r| r.max_age_seconds))
                    .map(std::time::Duration::from_secs),
            },
        })
    }
}

/// Represents a cache as registered in the broker registry.
/// Maps to the broker's cache registry.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Cache {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) cache: String,
    /// Absent from a control plane that predates cache consistency, which reads
    /// as `Leader`, the only guarantee a cache had before.
    #[serde(default)]
    pub(super) consistency: Option<String>,
}

/// Identifies a stream (tenant_id, namespace, stream).
/// Used as a key in broker registries.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct StreamKey {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) stream: String,
}

/// Identifies a cache (tenant_id, namespace, cache).
/// Used as a key in broker registries.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct CacheKey {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) cache: String,
}

/// Identifies a namespace (tenant_id, namespace).
/// Used as a key in broker registries.
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct NamespaceKey {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
}

/// Change operation for tenants.
///
/// Serialized in camelCase for wire compatibility.
/// Only Created and Deleted are valid (no update).
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum TenantChangeOp {
    Created,
    Deleted,
}

/// Change operation for namespaces.
///
/// Serialized in camelCase for wire compatibility.
/// Only Created and Deleted are valid (no update).
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum NamespaceChangeOp {
    Created,
    Deleted,
}

/// Change operation for streams.
///
/// Serialized in camelCase for wire compatibility.
/// Streams may be Created, Updated, or Deleted.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum StreamChangeOp {
    Created,
    Updated,
    Deleted,
}

/// Change operation for caches.
///
/// Serialized in camelCase for wire compatibility.
/// Caches may be Created, Updated, or Deleted.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum CacheChangeOp {
    Created,
    Updated,
    Deleted,
}

//! Cache model definitions and change-log payloads.
//!
//! Defines cache identifiers, records, and change events used by the store and API.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub tenant_id: String,
    pub namespace: String,
    pub cache: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct Cache {
    pub tenant_id: String,
    pub namespace: String,
    pub cache: String,
    pub display_name: String,
    /// How many shards the keyspace is split across.
    ///
    /// A key is placed by hashing it, so this is what decides which broker owns
    /// it. Defaults to `1`, which is what every cache created before caches
    /// were placed reads back as — one owner for the whole keyspace.
    #[serde(default = "default_cache_shards")]
    pub shards: u32,
    /// How many brokers hold a copy of each shard, leader included.
    #[serde(default = "default_cache_replication_factor")]
    pub replication_factor: u32,
    /// What acknowledging a write to this cache means. `Leader` unless asked,
    /// which is what every cache created before this existed reads back as.
    #[serde(default = "default_cache_consistency")]
    pub consistency: crate::model::ConsistencyLevel,
    /// Which creation of this name the cache is, given when it is created. A
    /// cache deleted and created again under the same name gets a new one,
    /// and brokers keep each one's entries apart. 0 is a cache created before
    /// the fleet finalized `cache_incarnations`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub incarnation: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// The payload of a single cache's `Deleted` change: the cache, when it has
/// an incarnation. What brokers act on is the change's `deleted_through`.
///
/// None for incarnation 0, as before incarnations, so Raft members on
/// different builds record the same change.
pub(crate) fn deleted_cache_payload(cache: Cache) -> Option<Cache> {
    (cache.incarnation != 0).then_some(cache)
}

/// One shard: the whole keyspace has a single owner unless asked otherwise.
pub(crate) fn default_cache_shards() -> u32 {
    1
}

/// Leader-only, matching streams.
pub(crate) fn default_cache_replication_factor() -> u32 {
    1
}

pub(crate) fn default_cache_consistency() -> crate::model::ConsistencyLevel {
    crate::model::ConsistencyLevel::Leader
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheChange {
    pub seq: u64,
    pub op: CacheChangeOp,
    pub key: CacheKey,
    pub cache: Option<Cache>,
    /// On a `Deleted` change written once the fleet finalized
    /// `cache_incarnations`: every incarnation of the name up to and
    /// including this one is deleted, so brokers delete their data. Absent
    /// before the finalize, when a recreate at incarnation 0 may still reopen
    /// the deleted cache's data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_through: Option<u64>,
}

/// A deleted cache name: every incarnation of it up to and including
/// `incarnation` is deleted. Kept for good, one per name, and served with the
/// cache snapshot, so a broker that missed the delete deletes the data.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct CacheTombstone {
    pub tenant_id: String,
    pub namespace: String,
    pub cache: String,
    pub incarnation: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub enum CacheChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CachePatchRequest {
    pub display_name: Option<String>,
}

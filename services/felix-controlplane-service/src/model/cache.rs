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

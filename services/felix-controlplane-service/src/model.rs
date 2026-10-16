//! Control-plane data model module.
//!
//! Re-exports the core tenant/namespace/stream/cache/node models and change
//! payloads used by the API and store layers.
mod cache;
mod identifier;
mod incarnation;
mod namespace;
mod node;
mod shard;
mod stream;
mod tenant;

pub use cache::{Cache, CacheChange, CacheChangeOp, CacheKey, CachePatchRequest, CacheTombstone};
pub(crate) use cache::{
    default_cache_consistency, default_cache_replication_factor, default_cache_shards,
    deleted_cache_payload,
};
pub(crate) use identifier::validate_identifier;
pub(crate) use incarnation::{next_incarnation, proposed_incarnation};
pub use namespace::{Namespace, NamespaceChange, NamespaceChangeOp, NamespaceKey};
pub(crate) use node::validate_storage_id;
pub use node::{
    Node, NodeCapacity, NodeChange, NodeChangeOp, NodeLifecycle, NodePatchRequest, NodeSpec,
    NodeStatus, NodeSuspicion, NodeValidationError,
};
pub(crate) use shard::first_generation;
pub use shard::{
    HaltedCopy, MoveReason, ReplicaReport, ShardAssignment, ShardAssignmentChange,
    ShardAssignmentChangeOp, ShardKey, ShardKind, ShardState, ShardValidationError,
};
pub(crate) use stream::default_replication_factor;
pub(crate) use stream::deleted_stream_payload;
pub use stream::{
    ConsistencyLevel, DeliveryGuarantee, RetentionPolicy, Stream, StreamChange, StreamChangeOp,
    StreamKey, StreamKind, StreamPatchRequest, StreamRouting, StreamTombstone,
};
pub use tenant::{Tenant, TenantChange, TenantChangeOp};

//! Routing: which node serves a shard, and whether we are allowed to reach it.
//!
//! Two independent questions, and keeping them separate is the point.
//! [`ShardRouter`] answers *where* a shard lives, from assignments the control
//! plane published. [`RegionRouter`] answers *whether* traffic may cross from
//! here to there. A route is only usable when both agree, and folding them
//! together would make a placement decision look like a policy decision.
mod region;
mod shard;

pub use region::{BridgeSpecError, RegionRouter, parse_bridges};
pub use shard::{
    NodeRef, Placed, ReplicaRole, Resolution, Route, Routes, RoutingTable, ShardKey, ShardKind,
    ShardRouter, StreamPlacement, Unavailable,
};

//! Where a shard lives, resolved without touching the control plane.
//!
//! The publish path asks this on every request, so a lookup must not take a
//! lock a writer can hold and must never make a network call. Routes are
//! published as an immutable snapshot and swapped in whole: readers take a
//! cheap atomic load and read a table nobody can mutate underneath them, and an
//! update costs one pointer swap regardless of how many readers are in flight.
//!
//! Every outcome is explicit. There is deliberately no "not sure, handle it
//! locally": a broker that treats an unknown route as its own is a broker
//! writing a shard it does not own.
mod router;
mod table;

pub use router::{ReplicaRole, Resolution, Routes, ShardRouter, Unavailable};
pub use table::{NodeRef, Placed, Route, RoutingTable, StreamPlacement};

/// One shard of one stream or cache.
///
/// Defined here rather than shared with the control plane or the storage layer,
/// both of which have their own: the router is a library that must not depend on
/// either, and the duplication is three fields, a number, and a kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardKey {
    pub tenant_id: String,
    pub namespace: String,
    /// The stream or cache name, disambiguated by `kind`.
    pub stream: String,
    pub shard: u32,
    pub kind: ShardKind,
}

/// Whether a shard belongs to a stream or to a cache.
///
/// A cache and a stream may share a name within one namespace. When they do,
/// their shards are unrelated, and a table that could not tell them apart would
/// hand a cache request the stream's owner.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ShardKind {
    #[default]
    Stream,
    Cache,
}

impl ShardKind {
    /// The prefix this kind contributes to a stream identity string.
    fn prefix(self) -> &'static str {
        match self {
            Self::Stream => "s",
            Self::Cache => "c",
        }
    }
}

#[cfg(test)]
mod tests;

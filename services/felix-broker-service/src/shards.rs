//! Owning shards.
//!
//! [`watch`] follows what the control plane assigned, by snapshot and then by
//! change feed. [`lifecycle`] is what this broker has actually *done* about
//! that, which is deliberately separate state: a shard serves only once its log
//! is open, and only at the generation the control plane currently names.
//! [`routing`] answers the question every publish asks first: is this shard
//! mine, should it be forwarded, or can nobody serve it right now?
//! `engine` is which replication engine a shard's writes go to, and `raft`
//! holds this broker's Raft members of the shards a group runs. `memory`
//! serves the in-memory stream shards no group runs, on a broker whose
//! durable shards run on Raft.
//!
//! [`ShardKey`] and [`ShardKind`] are re-exported here because all three use
//! them; they are defined in `felix-replication`, which keys on them too.

mod engine;
pub(crate) mod engine_match;
pub mod lifecycle;
pub(crate) mod memory;
#[cfg(feature = "raft")]
pub(crate) mod raft;
pub mod routing;
pub mod watch;

pub use felix_replication::{ShardKey, ShardKind};

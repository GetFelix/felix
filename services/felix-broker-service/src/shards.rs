//! Owning shards.
//!
//! [`watch`] follows what the control plane assigned, by snapshot and then by
//! change feed. [`lifecycle`] is what this broker has actually *done* about
//! that, which is deliberately separate state: a shard serves only once its log
//! is open, and only at the generation the control plane currently names.
//! [`routing`] answers the question every publish asks first: is this shard
//! mine, should it be forwarded, or can nobody serve it right now?
//!
//! [`ShardKey`] and [`ShardKind`] are re-exported here because all three use
//! them; they are defined in `felix-replication`, which keys on them too.

pub mod lifecycle;
pub mod routing;
pub mod watch;

pub use felix_replication::{ShardKey, ShardKind};

//! Who is in the fleet, and which broker leads each shard.
//!
//! Both halves run on a timer against a store snapshot. [`membership`] expires
//! nodes that stopped heartbeating, since silence is the only signal that a
//! broker is gone. [`placement`] decides shard leadership from what membership
//! and the replicas' reports leave standing. [`fleet`] is the feature set every
//! serving broker has in common.
pub mod fleet;
pub mod membership;
pub mod placement;

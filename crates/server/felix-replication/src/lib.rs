//! Broker-to-broker replication: a Raft group per shard (`raft`, built with
//! the `raft` feature, which the broker service enables by default), whose
//! messages travel over the broker-internal transport in [`peer`]. The
//! engine seam the broker writes and reads through is [`engine`]; the broker
//! service hands in its write fence through [`engine::WriteFence`], so
//! nothing here depends on the service.

pub mod peer;

pub mod controlplane_link;
pub mod engine;
pub mod halted;
#[cfg(feature = "raft")]
pub mod raft;
mod shard;

pub use shard::{ShardKey, ShardKind};

// Tests here open many logs at once; they get the open-file limit a broker
// runs with instead of the shell's.
#[cfg(test)]
felix_storage::raise_soft_limit_at_start!();

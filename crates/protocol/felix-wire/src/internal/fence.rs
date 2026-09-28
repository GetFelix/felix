//! The promotion fence: a newly promoted leader asks each replica to refuse
//! every older leader, and learns how far each one's log reaches.
//!
//! See "Fencing a promotion" in `docs/replication-design.md`.

use super::{ReplicaLog, ShardRef};

/// What a peer can do beyond the protocol every build speaks.
///
/// Carried by `Hello` and `HelloOk` once a peer knows the bits exist. Unlike a
/// client frame's flags these select no layout: each bit says a peer answers
/// some request, so a bit this build does not know is kept, never asked
/// about, rather than refused.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct PeerCapabilities(u64);

impl PeerCapabilities {
    /// Nothing beyond the base protocol: what a peer that predates the bits
    /// can do.
    pub const NONE: Self = Self(0);
    /// Answers [`Fence`], and refuses every older leader of the shard once it
    /// has.
    pub const FENCE: Self = Self(1 << 0);
    pub fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    pub fn bits(self) -> u64 {
        self.0
    }

    /// Whether every bit of `other` is set here.
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// A leader at `shard.generation` asks a replica to accept it and refuse any
/// leader older than that from here on.
///
/// The replica persists the generation before it answers, so the promise
/// survives a restart. `log` names the shard's own log, `Stream` or `Cache`;
/// the cursor, dead-letter and counter logs belong to the shard and are
/// fenced with it.
///
/// Sent only to a peer that advertised [`PeerCapabilities::FENCE`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fence {
    pub correlation_id: u64,
    pub shard: ShardRef,
    pub log: ReplicaLog,
}

/// The replica took the fence, and where its copy of the shard's log stands
/// after it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceOk {
    pub correlation_id: u64,
    /// One past the last record the replica holds.
    pub log_end: u64,
    /// One past the last record the replica knows is committed.
    pub commit_offset: u64,
    /// The generation its last record was written at, zero for an empty log.
    /// With `log_end` it orders two replicas' logs the way promotion does.
    pub last_generation: u64,
}

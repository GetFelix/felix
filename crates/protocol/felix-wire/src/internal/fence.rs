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
    /// Answers [`ReplicateFetch`] from the leader that fenced it.
    pub const TAIL_FETCH: Self = Self(1 << 1);
    /// Reads records labelled with the generations that wrote them
    /// ([`ReplicateRecords::generations`]), and answers a labelled
    /// [`ReplicateFetch`] with them.
    ///
    /// [`ReplicateRecords::generations`]: super::ReplicateRecords::generations
    pub const GENERATION_LABELS: Self = Self(1 << 2);
    /// Answers a forwarded publish with the offsets the batch landed at, and
    /// with an empty range when the stream has no log (see
    /// [`ForwardPublishOk::offsets`]). An owner without it answered an
    /// ephemeral stream's batch with `0..=0`, which reads as a real offset.
    ///
    /// [`ForwardPublishOk::offsets`]: super::ForwardPublishOk::offsets
    pub const FORWARD_OFFSETS: Self = Self(1 << 3);
    /// Answers [`Fence`] and [`ReplicateFetch`] for a cache shard's counter
    /// log ([`ReplicaLog::Counters`]), and reads cache and counter batches
    /// labelled with their generations. A promoted cache shard is fenced only
    /// when every replica offers it.
    pub const CACHE_FENCE: Self = Self(1 << 4);
    /// Reads batches that carry each record's append time on the leader
    /// ([`ReplicateRecords::times`]) and stores that time, and answers a
    /// timed [`ReplicateFetch`] with them. A follower without it stamps
    /// replicated records with its own clock.
    ///
    /// [`ReplicateRecords::times`]: super::ReplicateRecords::times
    pub const RECORD_TIMES: Self = Self(1 << 5);
    /// Keeps a ballot with each accepted generation, naming the leader it was
    /// accepted from by the node id that leader gave in its `Hello`, and
    /// refuses a fence, a batch or a fetch from any other node at that
    /// generation.
    pub const BALLOTS: Self = Self(1 << 6);

    pub fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    pub fn bits(self) -> u64 {
        self.0
    }

    /// Whether every bit of `other` is set here.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// A leader at `shard.generation` asks a replica to accept it and refuse any
/// leader older than that from here on.
///
/// The replica persists the generation before it answers, so the promise
/// survives a restart. `log` names the shard's own log, `Stream` or `Cache`;
/// the cursor, dead-letter and counter logs belong to the shard and are
/// fenced with it. A promoted cache leader also fences `Counters`, to learn
/// where each replica's counter log ends.
///
/// Sent only to a peer that advertised [`PeerCapabilities::FENCE`], and for
/// `Counters` only to one that advertised [`PeerCapabilities::CACHE_FENCE`].
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

/// The leader that fenced a replica reads the replica's copy of the shard's
/// log from `from_offset`, to take a tail it does not hold.
///
/// Answered with the records as a leader ships them (`ReplicateRecords`, or
/// `ReplicateMarkedRecords` when any carries a producer mark), at most
/// `max_bytes` of them, and none past the replica's end. Only a leader at the
/// generation the replica last accepted is answered: a fenced replica's log
/// is not for an older leader to read. Sent only to a peer that advertised
/// [`PeerCapabilities::TAIL_FETCH`], and for `Counters` only to one that
/// advertised [`PeerCapabilities::CACHE_FENCE`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateFetch {
    pub correlation_id: u64,
    pub shard: ShardRef,
    pub log: ReplicaLog,
    pub from_offset: u64,
    pub max_bytes: u32,
    /// Answer with the records' generations, as a labelled batch. Travels as
    /// `ReplicateLabelledFetch`, sent only to a peer that advertised
    /// [`PeerCapabilities::GENERATION_LABELS`].
    pub labelled: bool,
    /// Answer with the records' append times as well. Only a labelled fetch
    /// can ask: it travels as `ReplicateTimedFetch`, sent only to a peer that
    /// advertised [`PeerCapabilities::RECORD_TIMES`].
    pub timed: bool,
}

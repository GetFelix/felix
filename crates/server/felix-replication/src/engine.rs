//! The replication engine as the broker sees it: what a write waits on before
//! it is acknowledged, and how far a reader may see. The Raft engine
//! (`crate::raft`) implements it. See `docs/replication-engine.md`.

use std::future::Future;
use std::time::Duration;

mod host;

#[cfg(feature = "raft")]
pub(crate) use host::EngineHost;
#[cfg(feature = "raft")]
pub(crate) use host::Host;
pub use host::WriteFence;

use crate::ShardKey;
use felix_wire::internal::{InternalMessage, PeerCapabilities};

/// Why the engine cannot say a write is committed. Unless the variant says
/// otherwise the leader may have written it, so the client is told the
/// outcome is unknown rather than that the write failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommitError {
    /// No majority within the budget. This broker may not have written it.
    #[error("the {what} did not reach a majority within {timeout:?}")]
    TimedOut {
        what: &'static str,
        timeout: Duration,
    },
    /// This broker stopped leading the shard before a majority held the write.
    #[error("{detail} before the {what} could reach a quorum")]
    LeadershipLost {
        what: &'static str,
        detail: &'static str,
    },
    /// The Raft engine refused the write before it appended any of it: its
    /// lease had lapsed, or it no longer led the term the write was admitted
    /// at. Nothing was written, so the write can be sent again.
    #[error("the {what} was refused: {detail}")]
    NotAppended {
        what: &'static str,
        detail: &'static str,
    },
    /// A conditional publish on a Raft shard did not land at its expected
    /// offset. Unlike the others this outcome is known: the committed entry
    /// there is a blank, and `tail` is the shard's next offset after it.
    #[error("the shard's next offset was not the expected one; it is now {tail}")]
    OffsetMismatch { tail: u64 },
    /// A record larger than a Raft shard's appends can carry, refused before
    /// anything was proposed. Sending it again cannot succeed.
    #[error(
        "a record of {bytes} bytes is larger than a Raft shard takes ({max}); nothing was written"
    )]
    TooLarge { bytes: usize, max: usize },
}

/// Why [`Engine::read_bound`] has no bound for a `Quorum` shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unreadable {
    /// This broker does not serve the shard.
    NotServed,
    /// This broker serves the shard but has no committed position for it yet.
    Settling,
}

/// Where a write landed: the offset one past its last record, and the
/// generation it was written at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    pub end: u64,
    pub generation: u64,
}

impl Written {
    /// Where the batch in `outcome`, written at `generation`, landed. `None`
    /// for an ephemeral stream, which has no offsets.
    pub fn of(outcome: &felix_broker::PublishOutcome, generation: u64) -> Option<Self> {
        let (_, last) = outcome.offsets?;
        Some(Self {
            end: last + 1,
            generation,
        })
    }
}

/// The brokers the control plane placed a shard on, by node id.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Placement {
    pub nodes: Vec<String>,
}

impl Placement {
    /// A placement on `nodes`.
    pub fn new(nodes: Vec<String>) -> Self {
        Self { nodes }
    }
}

/// What a durable write and a read ask of a shard's replication.
pub trait Engine: Send + Sync {
    /// Wait until the batch in `outcome`, written at `generation`, is
    /// committed: held by enough of the shard's replicas that no later leader
    /// can lose it. Returns at once for a stream that does not acknowledge by
    /// its replicas, or when `shard` is `None` (no replica set).
    ///
    /// An error means this broker cannot say the batch is committed, not that
    /// it is lost.
    fn await_commit(
        &self,
        handle: &felix_broker::StreamHandle,
        shard: Option<&ShardKey>,
        outcome: &felix_broker::PublishOutcome,
        generation: u64,
        timeout: Duration,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Whether a write to `shard` at `generation` may be acknowledged on
    /// local durability alone, as a `Leader` stream's is. On an error the
    /// caller waits on [`Engine::await_commit`] instead. A refusal never
    /// closes the shard.
    fn admit_local_ack(&self, shard: &ShardKey, generation: u64) -> Result<(), CommitError>;

    /// The end of what a reader of `shard` may see, or `None` when its reads
    /// are not bounded by replication.
    fn read_bound(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        shard: &ShardKey,
    ) -> Result<Option<u64>, Unreadable>;

    /// `end`, a bound of `shard`, less the offsets just below it that hold
    /// no record. See `felix_broker::ReadBounds::records_end`.
    fn records_end(&self, _shard: &ShardKey, end: u64) -> u64 {
        end
    }

    /// Confirm, after a read of `shard`'s state took its value, that this
    /// broker still leads `shard`.
    fn confirm_read(&self, shard: &ShardKey) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// The control plane placed `shard` on `placement`. Safe to repeat.
    fn placement_changed(&self, shard: &ShardKey, placement: &Placement);

    /// Answer a peer's replication request, follower side: a batch to store,
    /// a bootstrap or rebuild, a fence, a fetch or a pre-vote. `from` is the
    /// peer's node id when it named itself, `caller` what it advertised.
    /// Anything else is refused as malformed.
    fn handle(
        &self,
        from: Option<&str>,
        caller: PeerCapabilities,
        request: InternalMessage,
    ) -> impl Future<Output = InternalMessage> + Send;
}

#[cfg(test)]
mod tests;

//! The Raft engine as the publish path sees it. Nothing routes a shard here
//! yet, and the rest of [`crate::engine::Engine`] comes with the commit
//! index. See `docs/per-shard-raft.md`, "Writing through openraft".

use std::future::Future;

use super::propose::{self, Batch};
use super::runtime::GroupRuntime;
use crate::ShardKey;
use crate::engine::{CommitError, Written};

const NO_GROUP: &str = "this broker holds no group for the shard";
const NO_LEASE: &str = "a Raft shard acknowledges nothing on its own log yet";

/// Writes to the Raft shards a [`GroupRuntime`] holds.
///
/// Cheap to clone; every clone is the same engine.
#[derive(Clone)]
pub struct RaftEngine {
    runtime: GroupRuntime,
}

impl RaftEngine {
    pub fn new(runtime: GroupRuntime) -> Self {
        Self { runtime }
    }

    /// Write `batch` on this leader. openraft gives each record the next
    /// index, which is its offset, and the batch's records are the offsets
    /// just below the `end` returned. Returns when they are on this
    /// leader's log as far as `batch.ack` asks: written, or fsynced.
    ///
    /// Call it in the shard's commit order. The batch takes its place when
    /// this is called, before the future is polled, and keeps it if the
    /// future is dropped: it is still written, and the outcome is unknown
    /// to that caller.
    ///
    /// Written is not committed. A `Quorum` write waits for the commit
    /// index next, and so does a `Leader` write while
    /// [`Self::admit_local_ack`] refuses.
    ///
    /// An error means this broker cannot say where the batch landed. It may
    /// be in the log and may commit, so the caller must not propose it
    /// again as if nothing was written.
    pub fn propose(
        &self,
        shard: &ShardKey,
        batch: Batch,
    ) -> impl Future<Output = Result<Written, CommitError>> + Send + use<> {
        let proposed = self
            .runtime
            .group(shard)
            .map(|group| group.propose(batch))
            .ok_or_else(|| propose::lost(NO_GROUP));
        async move { proposed?.await }
    }

    /// Whether a write at `generation` may be acknowledged on this leader's
    /// log alone. Always refused: the leader lease is not built, so every
    /// `Leader` write waits for commit. A refusal never closes the shard.
    pub fn admit_local_ack(&self, _shard: &ShardKey, _generation: u64) -> Result<(), CommitError> {
        Err(propose::lost(NO_LEASE))
    }
}

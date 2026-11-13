//! The Raft engine as the publish and read paths see it. Nothing routes a
//! shard here yet. See `docs/per-shard-raft.md`, "Writing through openraft"
//! and "Write fence, leases and read bound under Raft".

use std::future::Future;
use std::time::Duration;

use super::propose::{self, Batch};
use super::runtime::GroupRuntime;
use crate::ShardKey;
use crate::engine::{CommitError, Written};
use crate::quorum::Unreadable;

const NO_GROUP: &str = "this broker holds no group for the shard";

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

    /// Whether a write at `generation`, the term [`Self::propose`] wrote
    /// it in, may be acknowledged on this leader's log alone. That is the
    /// leader lease: this broker leads that term, a majority answered a
    /// message it sent less than [`super::RuntimeConfig::lease`] ago, and
    /// it has neither given the term up nor just woken from a suspend.
    ///
    /// On an error the write waits for its commit instead
    /// ([`Self::await_ack`]). A refusal never closes the shard.
    pub fn admit_local_ack(&self, shard: &ShardKey, generation: u64) -> Result<(), CommitError> {
        let group = self.runtime.group(shard);
        group
            .ok_or_else(|| propose::lost(NO_GROUP))?
            .admit_local_ack(generation)
    }

    /// The acknowledgement of a `Leader` write that [`Self::propose`]
    /// returned as `written`: at once while the lease holds, and otherwise
    /// when the write is committed, as [`Self::await_commit`] says it.
    pub fn await_ack(
        &self,
        shard: &ShardKey,
        written: Written,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + use<> {
        let admitted = self.admit_local_ack(shard, written.generation);
        let committed = admitted
            .is_err()
            .then(|| self.await_commit(shard, written, timeout));
        async move {
            match committed {
                Some(committed) => committed.await,
                None => Ok(()),
            }
        }
    }

    /// Wait until `written`, as [`Self::propose`] returned it, is
    /// committed: the commit index has passed it and the entry at its last
    /// offset has the term it was written at.
    ///
    /// An error means this broker cannot say so, not that the write is
    /// lost. A leader deposed while it waits gets leadership lost, and so
    /// does a write whose offset another leader's entry took.
    pub fn await_commit(
        &self,
        shard: &ShardKey,
        written: Written,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + use<> {
        let committed = self
            .runtime
            .group(shard)
            .map(|group| group.await_commit(written, timeout))
            .ok_or_else(|| propose::lost(NO_GROUP));
        async move { committed?.await }
    }

    /// One past the highest offset this broker knows committed, for every
    /// reader at every consistency level. It is the commit index plus one
    /// as this member has applied it, so it can trail the leader's and
    /// never passes it. A follower has one too.
    pub fn read_bound(&self, shard: &ShardKey) -> Result<u64, Unreadable> {
        let group = self.runtime.group(shard).ok_or(Unreadable::NotServed)?;
        Ok(group.read_bound())
    }

    /// Confirm that this broker still leads `term`, by one read-index
    /// round: a majority answers a heartbeat sent after the call. A reader
    /// of `shard`'s state takes [`Self::open_term`] first, then its value,
    /// then calls this with that term. A broker deposed and elected again
    /// in between is refused: its value can miss what the leader in
    /// between committed.
    ///
    /// No answer from a majority is a timeout, not a lost leadership, and
    /// does not close the shard.
    pub async fn confirm_read(&self, shard: &ShardKey, term: u64) -> Result<(), CommitError> {
        let Some(group) = self.runtime.group(shard) else {
            return Err(CommitError::LeadershipLost {
                what: "read",
                detail: NO_GROUP,
            });
        };
        group.confirm_read(term).await
    }

    /// A transfer of `shard`'s leadership away from this broker is about
    /// to be asked for. The lease for the term is given up and the shard
    /// closes (`deposed`) before this returns. Both come back only in a
    /// later term. Returns the term that was closed, read from the group's
    /// core. `None` means this broker leads no term of the shard.
    pub async fn transfer_starting(&self, shard: &ShardKey) -> Option<u64> {
        self.runtime.group(shard)?.transfer_starting().await
    }

    /// Hand `shard`'s leadership to `to`: [`Self::transfer_starting`], then
    /// the request to openraft. Every transfer goes through here. Returns
    /// the term given up, or `None` if this broker leads none, and then
    /// nothing was asked. The transfer itself may still not go through.
    pub async fn transfer(&self, shard: &ShardKey, to: super::MemberId) -> Option<u64> {
        self.runtime.group(shard)?.transfer(to).await
    }

    /// `shard`'s commit table on this broker, if it holds a member.
    pub(crate) fn commits(
        &self,
        shard: &ShardKey,
    ) -> Option<tokio::sync::watch::Receiver<super::commit::Commits>> {
        Some(self.runtime.group(shard)?.commits())
    }

    /// The term this broker has `shard` open in, for a reader to take
    /// before it reads state and hand to [`Self::confirm_read`].
    pub fn open_term(&self, shard: &ShardKey) -> Option<u64> {
        self.runtime.group(shard)?.open_term()
    }
}

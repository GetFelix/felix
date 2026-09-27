//! The second half of a cache write: wait for durability and for the write's
//! turn, then fold it into the index and tell the observer.
//!
//! Once a write has its offset, its record is on disk and will be replicated
//! whether or not the caller is still waiting. So the second half runs to
//! completion even when the caller's future is dropped. Stopping would release
//! the turn before the fsync finished, letting a reader fold a record a crash
//! can still take away, and the observer would never hear of it.

use std::sync::Arc;

use parking_lot::Mutex as SyncMutex;

use super::CacheOp;
use super::shard::{CacheShard, ShardState};
use crate::Result;
use crate::cache::{CacheChange, CacheObserver};
use crate::commit_order::CommitTurn;
use crate::disk_log::{DiskLog, PendingAppend};

pub(super) type Observer = Arc<SyncMutex<Option<Arc<dyn CacheObserver>>>>;

/// A write whose offset is claimed and whose turn is reserved.
pub(super) struct StagedWrite {
    pub(super) shard: Arc<CacheShard>,
    pub(super) log: DiskLog,
    pub(super) pending: PendingAppend,
    pub(super) turn: CommitTurn<'static>,
    pub(super) op: CacheOp,
    pub(super) bytes: u64,
    pub(super) change: CacheChange,
    pub(super) observer: Observer,
}

impl StagedWrite {
    /// Wait until the record is durable and every earlier write has applied.
    async fn commit(&self) -> Result<()> {
        self.log.commit(&self.pending).await?;
        // Compaction is the one reset that can land with a turn held, and it
        // runs only once every staged write has applied, so nothing waiting
        // here can be superseded.
        let _ = self.turn.wait().await;
        Ok(())
    }

    /// Fold the record into the index and tell the observer. Called under the
    /// shard lock, in turn order, which is what makes the order watchers see
    /// the shard's disk order.
    fn apply(&self, state: &mut ShardState) {
        CacheShard::apply_op(state, &self.op, self.pending.first_offset(), self.bytes);
        let observer = self.observer.lock().clone();
        if let Some(observer) = observer {
            observer.cache_changed(self.change.clone());
        }
    }
}

/// Holds a [`StagedWrite`] while its caller drives it, and finishes it on a
/// detached task if the caller is dropped first.
pub(super) struct FinishOnDrop(Option<StagedWrite>);

impl FinishOnDrop {
    pub(super) fn new(write: StagedWrite) -> Self {
        Self(Some(write))
    }

    /// Wait for durability and the write's turn.
    pub(super) async fn commit(&mut self) -> Result<()> {
        let write = self.0.as_ref().expect("armed until applied");
        if let Err(err) = write.commit().await {
            // A failed commit poisons the log, so there is nothing to finish:
            // what reached the disk is re-read on the next open.
            self.0 = None;
            return Err(err);
        }
        Ok(())
    }

    /// Apply a committed write under the shard lock. The returned write still
    /// holds its turn; keep it until anything that must stay ordered behind
    /// this write (compaction) is done.
    pub(super) fn apply(mut self, state: &mut ShardState) -> StagedWrite {
        let write = self.0.take().expect("armed until applied");
        write.apply(state);
        write
    }
}

impl Drop for FinishOnDrop {
    fn drop(&mut self) {
        let Some(write) = self.0.take() else {
            return;
        };
        // No runtime means the process is going down; the next open replays
        // the record from the log.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            if write.commit().await.is_err() {
                return;
            }
            let mut state = write.shard.state.lock().await;
            write.apply(&mut state);
        });
    }
}

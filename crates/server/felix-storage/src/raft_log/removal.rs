//! Segment removal behind a purge.
//!
//! A purge returns once its id is on disk. The sealed segments wholly below
//! the new first index are unlinked behind it, oldest first, each unlink
//! made durable before the next, one removal at a time. Until then they hold
//! only records below the log's first index, which no read returns as
//! entries, so a removal a crash cut short leaves a longer log and never a
//! gap. The next purge, or [`RaftLog::remove_purged`] at open, finishes it.
//!
//! [`RaftLog::remove_purged`]: super::RaftLog::remove_purged

use std::sync::Arc;

use tokio::sync::watch;

use crate::log::Offset;
use crate::{DiskLog, StorageError};

pub(super) struct Removals {
    /// Removals asked for and not yet finished.
    pending: Arc<watch::Sender<usize>>,
    /// Taken by each removal, so they run one at a time in the order asked.
    turn: Arc<tokio::sync::Mutex<()>>,
}

impl Default for Removals {
    fn default() -> Self {
        Self {
            pending: Arc::new(watch::channel(0).0),
            turn: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

impl Removals {
    /// Remove `log`'s sealed segments wholly below `before`, behind the
    /// caller. A failure is logged; the next purge or open tries again.
    pub(super) fn behind(&self, log: &DiskLog, before: Offset) {
        self.pending.send_modify(|pending| *pending += 1);
        let settle = Settle(Arc::clone(&self.pending));
        let turn = Arc::clone(&self.turn);
        let log = log.clone();
        tokio::spawn(async move {
            let _settle = settle;
            let _turn = turn.lock().await;
            match log.trim_before(before).await {
                Ok(_) | Err(StorageError::Closed(_)) => {}
                Err(err) => tracing::warn!(
                    shard = %log.label(),
                    before,
                    error = %err,
                    "removing a purged Raft log's segments failed; the next purge retries"
                ),
            }
        });
    }

    /// Returns once every removal asked for so far has finished.
    pub(super) async fn settled(&self) {
        let mut pending = self.pending.subscribe();
        // The sender is `self.pending`, so this cannot fail.
        let _ = pending.wait_for(|pending| *pending == 0).await;
    }
}

/// Counts a removal finished however its task ends, dropped unpolled
/// included.
struct Settle(Arc<watch::Sender<usize>>);

impl Drop for Settle {
    fn drop(&mut self) {
        self.0
            .send_modify(|pending| *pending = pending.saturating_sub(1));
    }
}

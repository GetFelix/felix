//! `flushed` under written commit, raised by the syncs themselves.
//!
//! Raft counts a written-commit entry at the write, so only `flushed` waits
//! for the fsync. The sync that covers an append raises it on the thread
//! that ran the sync, rather than each append waking a task to wait for it.

use std::collections::VecDeque;

use super::{LogPoint, Shared};
use crate::StorageError;
use crate::log::Offset;

pub(super) struct Synced {
    /// Appends their sync has not covered yet, oldest first: the durable
    /// bound each needs and its last entry.
    waiting: parking_lot::Mutex<VecDeque<(Offset, LogPoint)>>,
    /// The log tells this log of its syncs. Without it, appends go through
    /// the flush queue.
    pub(super) told: bool,
}

impl Synced {
    pub(super) fn new(told: bool) -> Self {
        Self {
            waiting: parking_lot::Mutex::default(),
            told,
        }
    }
}

impl<K> Shared<K> {
    /// Raise `flushed` to `last` once the log's own syncs cover `target`,
    /// after every append handed here before it.
    pub(super) fn raise_when_synced(&self, target: Offset, last: LogPoint) {
        let mut waiting = self.synced.waiting.lock();
        waiting.push_back((target, last));
        self.settle_synced(waiting);
    }

    /// The log's durable bound rose, or the log stopped.
    pub(super) fn on_synced(&self) {
        self.settle_synced(self.synced.waiting.lock());
    }

    /// Whether an append still waits here for its sync.
    #[cfg(test)]
    pub(super) fn waits_for_sync(&self) -> bool {
        !self.synced.waiting.lock().is_empty()
    }

    /// Drop every append still waiting, once a sync has covered them all.
    /// After this a raise can no longer arrive, so a cut may lower `flushed`.
    pub(super) fn forget_synced(&self) {
        self.synced.waiting.lock().clear();
    }

    /// Answer the covered appends at the front, as the flush queue's task
    /// would: one raise to the newest covered, and on a failure the log
    /// stops, since Raft already counted these entries.
    fn settle_synced(
        &self,
        mut waiting: parking_lot::MutexGuard<'_, VecDeque<(Offset, LogPoint)>>,
    ) {
        let mut newest = None;
        let mut failed = None;
        while let Some(&(target, last)) = waiting.front() {
            match self.log.synced_now(target) {
                None => break,
                Some(Ok(())) => {
                    newest = Some(last);
                    waiting.pop_front();
                }
                Some(Err(err)) => {
                    waiting.clear();
                    failed = Some(err);
                }
            }
        }
        // Under the lock, so a truncation that takes it after the sync
        // finds no raise still to come.
        if let Some(last) = newest {
            self.raise_flushed(Some(last));
        }
        drop(waiting);
        match failed {
            // A close flushes everything itself, so a sync it beat to the
            // log lost nothing.
            None | Some(StorageError::Closed(_)) => {}
            // Poisoning again calls back here, which finds nothing waiting.
            Some(err) => self.log.poison(format!(
                "a written-commit flush or exact log mark failed ({})",
                super::writer::io_error(err)
            )),
        }
    }
}

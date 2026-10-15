//! The bounded change log behind every `*_changes` feed.
use std::collections::VecDeque;

use crate::store::ChangeSet;
use crate::store::export::ExportedLog;

/// Bounded, in-memory append-only log of changes for a single entity type.
///
/// The log is keyed by a monotonically increasing `seq` that is assigned by this process.
/// - `record()` assigns the next sequence number, appends the change, and evicts older items when
///   the configured capacity is exceeded.
/// - Eviction means a consumer may miss changes if it polls too slowly; in that case it must
///   re-bootstrap by calling the corresponding `*_snapshot()` API.
///
/// This structure is intentionally simple. Unlike the Postgres backend:
/// - There is no transactional coupling between authoritative state and the change log.
/// - Atomicity is achieved by sequencing operations under locks.
#[derive(Debug)]
pub(super) struct ChangeLog<T> {
    pub(super) next_seq: u64,
    pub(super) capacity: usize,
    pub(super) items: VecDeque<T>,
}

impl<T> ChangeLog<T> {
    pub(super) fn new(capacity: usize) -> Self {
        // Pre-allocate the deque to the configured capacity to reduce reallocations.
        Self {
            next_seq: 0,
            capacity,
            items: VecDeque::with_capacity(capacity),
        }
    }

    pub(super) fn record(&mut self, item: impl FnOnce(u64) -> T) -> u64 {
        // Assign a strictly increasing sequence number for this change stream.
        // Consumers use `since` checkpoints to resume from a known position.
        let seq = self.next_seq;
        self.next_seq += 1;
        self.items.push_back(item(seq));
        // Enforce a fixed retention window: keep only the most recent `capacity` changes.
        while self.items.len() > self.capacity {
            self.items.pop_front();
        }
        seq
    }

    /// The retained changes at or after `since`, at most `limit` of them.
    pub(super) fn page(&self, since: u64, limit: usize, seq: impl Fn(&T) -> u64) -> ChangeSet<T>
    where
        T: Clone,
    {
        let items = self
            .items
            .iter()
            .filter(|item| seq(item) >= since)
            .take(limit)
            .cloned()
            .collect();
        ChangeSet::page(items, limit, self.next_seq, seq)
    }
}

// Conversions to and from the exported snapshot format live here, next to
// the fields they read.
impl<T> ExportedLog<T> {
    pub(super) fn from_log(log: &ChangeLog<T>) -> Self
    where
        T: Clone,
    {
        Self {
            next_seq: log.next_seq,
            items: log.items.iter().cloned().collect(),
        }
    }

    pub(super) fn into_log(self, capacity: usize) -> ChangeLog<T> {
        ChangeLog {
            next_seq: self.next_seq,
            capacity,
            items: self.items.into(),
        }
    }
}

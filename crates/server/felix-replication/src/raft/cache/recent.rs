//! The changes a cache group's member applied most recently, so a watch that
//! resumes from a recent offset is replayed what it missed, as the hybrid
//! replays its shard log, rather than handed every key again.
//!
//! Kept by every member whether or not anyone watches: a client resumes on
//! whichever member it reaches, often one it never watched before.

use std::collections::VecDeque;

use bytes::Bytes;

#[cfg(test)]
mod tests;

/// Most changes kept per cache shard.
const MAX_CHANGES: usize = 4096;
/// Most bytes of keys and values kept per cache shard.
const MAX_BYTES: usize = 1 << 20;
/// What a kept change costs beyond its key and value.
const OVERHEAD: usize = 48;

/// One applied change to a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Change {
    /// The index of the entry that made it.
    pub(crate) offset: u64,
    pub(crate) key: String,
    /// The value written, or `None` for a removal.
    pub(crate) value: Option<Bytes>,
    /// In the log's microseconds; zero is never.
    pub(crate) expires_at: u64,
}

impl Change {
    fn cost(&self) -> usize {
        OVERHEAD + self.key.len() + self.value.as_ref().map_or(0, Bytes::len)
    }
}

/// A bounded run of the latest changes. Every change at or past `floor` is
/// held, so a replay from there is exact.
#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
pub(crate) struct Recent {
    changes: VecDeque<Change>,
    bytes: usize,
    floor: u64,
    max_changes: usize,
    max_bytes: usize,
}

impl Default for Recent {
    fn default() -> Self {
        Self::bounded(MAX_CHANGES, MAX_BYTES)
    }
}

impl Recent {
    pub(crate) fn bounded(max_changes: usize, max_bytes: usize) -> Self {
        Self {
            changes: VecDeque::new(),
            bytes: 0,
            floor: 0,
            max_changes,
            max_bytes,
        }
    }

    /// Keep `change`, dropping the oldest past either bound. Changes come in
    /// offset order.
    pub(crate) fn push(&mut self, change: Change) {
        self.bytes += change.cost();
        self.changes.push_back(change);
        while self.changes.len() > self.max_changes || self.bytes > self.max_bytes {
            let Some(oldest) = self.changes.pop_front() else {
                break;
            };
            self.bytes -= oldest.cost();
            self.floor = oldest.offset + 1;
        }
    }

    /// Forget every change: the state was replaced by one as of `index`,
    /// so nothing at or below it can be replayed.
    pub(crate) fn restart(&mut self, index: u64) {
        self.changes.clear();
        self.bytes = 0;
        // Index 0 is before any entry, so nothing was missed below 1.
        self.floor = if index == 0 { 0 } else { index + 1 };
    }

    /// The change the entry at `index` made, if it was the last kept.
    pub(crate) fn at(&self, index: u64) -> Option<&Change> {
        self.changes.back().filter(|change| change.offset == index)
    }

    /// Every kept change at or past `from` that `keep` takes, in offset
    /// order, or `None` when changes from `from` on may have been dropped.
    pub(crate) fn since(&self, from: u64, keep: &dyn Fn(&str) -> bool) -> Option<Vec<Change>> {
        if from < self.floor {
            return None;
        }
        let start = self.changes.partition_point(|change| change.offset < from);
        Some(
            self.changes
                .range(start..)
                .filter(|change| keep(&change.key))
                .cloned()
                .collect(),
        )
    }
}

//! The changes a cache group's member applied most recently, so a watch that
//! resumes from a recent offset is replayed what it missed, as the hybrid
//! replays its shard log, rather than handed every key again.
//!
//! Kept only while a member's shard is watched and for [`WINDOW_MICROS`]
//! after, so an unwatched shard records nothing and holds nothing. A resume
//! on a member that kept no changes for it is resnapshotted.

use std::collections::VecDeque;

use bytes::Bytes;

#[cfg(test)]
mod tests;

/// Most changes kept per cache shard.
const MAX_CHANGES: usize = 4096;
/// Most bytes of keys and values kept per cache shard.
const MAX_BYTES: usize = 1 << 20;
/// How long after a member last saw a watch of a shard it keeps recording,
/// in the log's microseconds. Covers a client that reconnects after a
/// dropped connection, a restart or a broker failover.
pub(crate) const WINDOW_MICROS: u64 = 5 * 60 * 1_000_000;
/// What a kept change costs beyond its key and value: its slot, plus the
/// allocator's share of the key's own allocation.
const OVERHEAD: usize = std::mem::size_of::<Change>() + 16;

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
/// held, so a replay from there is exact. The deque's slots are allocated
/// for at most `max_changes`, which bounds the memory beyond `max_bytes`.
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

    /// Recording that starts at `floor`: nothing below it was kept.
    pub(crate) fn starting_at(floor: u64) -> Self {
        Self {
            floor,
            ..Self::default()
        }
    }

    /// Keep `change`, dropping the oldest past either bound. Changes come in
    /// offset order. A change bigger than the byte bound is not kept, and
    /// the floor moves past it.
    pub(crate) fn push(&mut self, change: Change) {
        let cost = change.cost();
        if cost > self.max_bytes {
            self.changes.clear();
            self.bytes = 0;
            self.floor = change.offset + 1;
            return;
        }
        // Make room first, so the deque never grows past `max_changes`.
        while !self.changes.is_empty()
            && (self.changes.len() >= self.max_changes || self.bytes + cost > self.max_bytes)
        {
            self.drop_oldest();
        }
        self.bytes += cost;
        self.changes.push_back(change);
    }

    fn drop_oldest(&mut self) {
        if let Some(oldest) = self.changes.pop_front() {
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

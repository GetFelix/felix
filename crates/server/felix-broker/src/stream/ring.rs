//! The replay ring: the newest records of a stream shard, kept in memory for
//! cursor replay.
//!
//! Bounded by entries and by payload bytes. The byte bound is per ring, and
//! an optional budget is shared by every ring on the broker. Either way the
//! oldest entries go first, so what is left is always the tail. A batch that
//! cannot fit even into an empty ring is not kept: the ring is left empty, as
//! a reset leaves it, and a reader below the live edge is served from disk
//! (or told its cursor is too old, for an in-memory stream).

use std::collections::VecDeque;

use bytes::Bytes;

use super::budget::{ByteBudget, PooledBytes};

/// The ring's oldest entries go first once it holds more than `per_ring`
/// payload bytes, or once the broker-wide budget is full.
#[derive(Debug, Clone, Default)]
pub(crate) struct RingBytes {
    /// Zero for no per-ring bound.
    per_ring: usize,
    broker: Option<ByteBudget>,
}

impl RingBytes {
    /// Zero for either leaves that bound off.
    pub(crate) fn new(per_ring: usize, broker: usize) -> Self {
        Self {
            per_ring,
            broker: ByteBudget::new(broker),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Ring {
    entries: VecDeque<LogEntry>,
    held: PooledBytes,
    per_ring: usize,
}

impl Ring {
    pub(crate) fn new(bytes: &RingBytes) -> Self {
        Self {
            entries: VecDeque::new(),
            held: PooledBytes::new(bytes.broker.clone()),
            per_ring: bytes.per_ring,
        }
    }

    pub(crate) fn front(&self) -> Option<&LogEntry> {
        self.entries.front()
    }

    pub(crate) fn back(&self) -> Option<&LogEntry> {
        self.entries.back()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &LogEntry> {
        self.entries.iter()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Payload bytes the ring holds.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.held.bytes()
    }

    /// Make room for a batch of `bytes` under the broker-wide budget,
    /// evicting this ring's oldest entries as needed. `false` when even an
    /// empty ring cannot take it; the ring is then empty, and the caller
    /// skips the batch so nothing newer sits above a missing record.
    pub(crate) fn admit(&mut self, bytes: usize) -> bool {
        let mut freed = 0;
        let admitted = loop {
            if self.held.try_grow(bytes) {
                break true;
            }
            match self.entries.pop_front() {
                Some(entry) => {
                    freed += entry.payload.len();
                    self.held.shrink(entry.payload.len());
                }
                None => break false,
            }
        };
        let grown = if admitted { bytes as f64 } else { 0.0 };
        if grown != freed as f64 {
            metrics::gauge!("felix_replay_ring_bytes").increment(grown - freed as f64);
        }
        admitted
    }

    /// Append one entry of a batch [`Self::admit`] made room for.
    pub(crate) fn push_back(&mut self, entry: LogEntry) {
        self.entries.push_back(entry);
    }

    /// Keep at most `capacity` entries and, past the per-ring byte bound,
    /// drop the oldest down to it. The newest entry stays even when it alone
    /// is over the bound.
    pub(crate) fn trim(&mut self, capacity: usize) {
        // Counted here and given back once: this runs under the stream's log
        // lock on every publish.
        let mut bytes = self.held.bytes();
        let mut freed = 0;
        while self.entries.len() > capacity
            || (self.per_ring > 0 && bytes > self.per_ring && self.entries.len() > 1)
        {
            let Some(entry) = self.entries.pop_front() else {
                break;
            };
            bytes -= entry.payload.len();
            freed += entry.payload.len();
        }
        self.release(freed);
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.release(self.held.bytes());
    }

    fn release(&mut self, bytes: usize) {
        if bytes > 0 {
            self.held.shrink(bytes);
            metrics::gauge!("felix_replay_ring_bytes").decrement(bytes as f64);
        }
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        self.clear();
    }
}

#[derive(Debug)]
pub(crate) struct LogEntry {
    pub(crate) seq: u64,
    pub(crate) payload: Bytes,
    pub(crate) publisher: Option<Bytes>,
    pub(crate) timestamp_micros: Option<u64>,
}

#[cfg(test)]
mod tests;

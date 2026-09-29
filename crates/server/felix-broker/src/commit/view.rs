//! A stream shard's keyed state, projected from its commit records.
//!
//! Derived, never stored: it is rebuilt from the log when first read and
//! dropped whenever the ring is (a reset, records arriving by replication).
//! Kept beside the ring under its lock, so a commit's event and its state
//! become visible at the same instant.

use std::collections::HashMap;

use bytes::Bytes;

use super::StateOp;

#[derive(Debug, Default)]
pub(crate) struct StateView {
    entries: HashMap<String, Entry>,
    /// Offset of the last commit applied.
    version: Option<u64>,
}

/// A key's value and the offset of the commit that wrote it.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) value: Bytes,
    pub(crate) version: u64,
}

impl StateView {
    pub(crate) fn get(&self, key: &str) -> Option<&Entry> {
        self.entries.get(key)
    }

    pub(crate) fn version(&self) -> Option<u64> {
        self.version
    }

    /// Apply one commit's operations, all of them, at `offset`.
    pub(crate) fn apply(&mut self, offset: u64, ops: &[StateOp]) {
        for op in ops {
            match op {
                StateOp::Put { key, value } => {
                    self.entries.insert(
                        key.clone(),
                        Entry {
                            value: value.clone(),
                            version: offset,
                        },
                    );
                }
                StateOp::Delete { key } => {
                    self.entries.remove(key);
                }
            }
        }
        self.version = Some(offset);
    }
}

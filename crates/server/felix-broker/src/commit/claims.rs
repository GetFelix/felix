//! Which keys a commit has claimed but readers cannot see yet.
//!
//! A commit takes its offset before it is durable and enters the state view
//! only afterwards. A version check that read the view alone would miss a
//! commit claimed just ahead of it and let two writers both win on the same
//! version. The claims close that window: a commit records the keys it
//! writes, under the same lock that its own checks ran under, before the lock
//! is released.

use std::collections::HashMap;

/// Claims past this many are pruned of those the view has caught up with.
const PRUNE_AT: usize = 1024;

/// What a commit requires of one key's version: `None` for no value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionCheck {
    pub key: String,
    pub required: Option<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct KeyClaims {
    /// Key to the offset of the last commit that claimed it, and the version
    /// that commit leaves: its offset for a put, none for a delete.
    claimed: HashMap<String, (u64, Option<u64>)>,
}

impl KeyClaims {
    /// The version `key` holds once every claimed commit lands. `read` is
    /// what the view holds, and `as_of` the last commit it reflects; a claim
    /// at or below `as_of` is already in `read`.
    pub(crate) fn current(
        &mut self,
        key: &str,
        read: Option<u64>,
        as_of: Option<u64>,
    ) -> Option<u64> {
        if self.claimed.len() >= PRUNE_AT {
            self.claimed
                .retain(|_, (offset, _)| as_of.is_none_or(|as_of| *offset > as_of));
        }
        match self.claimed.get(key) {
            Some((offset, version)) if as_of.is_none_or(|as_of| *offset > as_of) => *version,
            _ => read,
        }
    }

    /// Record that the commit at `offset` writes these `ops`.
    pub(crate) fn claim(&mut self, offset: u64, ops: &[super::StateOp]) {
        for op in ops {
            let (key, version) = match op {
                super::StateOp::Put { key, .. } => (key, Some(offset)),
                super::StateOp::Delete { key } => (key, None),
            };
            self.claimed.insert(key.clone(), (offset, version));
        }
    }
}

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

/// Offset, the version the commit leaves, and the epoch it was claimed in.
pub(crate) type Claim = (u64, Option<u64>, u64);

#[derive(Debug, Default)]
pub(crate) struct KeyClaims {
    /// Key to the offset of the last commit that claimed it, and the version
    /// that commit leaves: its offset for a put, none for a delete.
    claimed: HashMap<String, Claim>,
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
        epoch: u64,
    ) -> Option<u64> {
        if self.claimed.len() >= PRUNE_AT {
            self.claimed.retain(|_, (offset, _, e)| {
                *e == epoch && as_of.is_none_or(|as_of| *offset > as_of)
            });
        }
        match self.claimed.get(key) {
            Some((offset, version, e))
                if *e == epoch && as_of.is_none_or(|as_of| *offset > as_of) =>
            {
                *version
            }
            _ => read,
        }
    }

    /// Record that the commit at `offset` writes these `ops`. The returned
    /// undo puts back what the claims replaced.
    pub(crate) fn claim(
        &mut self,
        offset: u64,
        epoch: u64,
        ops: &[super::StateOp],
    ) -> Vec<(String, Option<Claim>)> {
        let mut undo = Vec::with_capacity(ops.len());
        for op in ops {
            let (key, version) = match op {
                super::StateOp::Put { key, .. } => (key, Some(offset)),
                super::StateOp::Delete { key } => (key, None),
            };
            // Commits without checks claim in parallel, so a later offset may
            // be recorded first; the highest offset is the key's version.
            let prev = self.claimed.get(key).copied();
            if prev.is_none_or(|p| p.0 < offset || p.2 < epoch) {
                self.claimed.insert(key.clone(), (offset, version, epoch));
            }
            undo.push((key.clone(), prev));
        }
        undo
    }

    /// Withdraw the commit at `offset`, which did not land. A key a later
    /// commit has claimed since is left alone.
    pub(crate) fn withdraw(&mut self, offset: u64, undo: Vec<(String, Option<Claim>)>) {
        for (key, prev) in undo.into_iter().rev() {
            if self.claimed.get(&key).is_some_and(|c| c.0 == offset) {
                match prev {
                    Some(prev) => self.claimed.insert(key, prev),
                    None => self.claimed.remove(&key),
                };
            }
        }
    }
}

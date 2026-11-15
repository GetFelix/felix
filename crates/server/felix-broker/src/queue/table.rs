//! A Raft stream shard's consumer groups, as every member folds them from
//! the shard's committed group entries ([`GroupEntry`]), in log order.
//!
//! The fold is `Step` in `docs/formal/FelixRaftGroups.tla`, for every group
//! of the shard at once. A group's state is its creation token, its cursor,
//! its dead letters and the dead letters redriven and not yet settled. The
//! shard keeps one more number, the index of its latest delete of any group,
//! and nothing per deleted name.
//!
//! A write decided by a tracker carries the token of the group incarnation
//! the tracker served. Apply drops it when the live group has another token
//! (a seek or a delete and re-creation came between), or when it names an
//! absent group and its token is below the latest delete (the group it was
//! decided for is gone). Seeks and deletes always apply.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::entry::{CreatedBy, CreationToken, GroupEntry, GroupOp};

/// One group's state on a Raft stream shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupState {
    /// The incarnation the group's writes must carry.
    pub cre: CreationToken,
    /// Everything below this is settled, dead, or redriven.
    pub cursor: u64,
    pub dead: BTreeSet<u64>,
    /// Dead letters put back, owed to the group until settled.
    pub redriven: BTreeSet<u64>,
}

/// What applying one entry did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Folded {
    /// The entry changed the table, or was a write with nothing to change
    /// (an advance below the cursor, a redrive of a record not dead).
    Applied,
    /// The entry was decided for an incarnation of the group that is gone,
    /// and changed nothing. `tracker_dropped` says the leader's tracker for
    /// the group carried that incarnation and has been forgotten, so its
    /// next poll hydrates afresh from the table.
    Dropped { tracker_dropped: bool },
    /// At or below an index already applied: a replay, ignored.
    Replayed,
}

/// The consumer groups of one Raft stream shard. Two members that applied
/// the same entries hold equal tables.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupTable {
    groups: BTreeMap<String, GroupState>,
    /// The index of the shard's latest delete of any group, 0 for none.
    last_delete: u64,
    /// The last index applied.
    applied: u64,
}

/// A [`GroupTable`] and the creation tokens of the trackers this member
/// serves the shard's groups from while it leads. Trackers are bound to a
/// term: [`Self::end_term`] forgets them all.
#[derive(Debug, Clone, Default)]
pub struct ShardGroups {
    table: GroupTable,
    trackers: BTreeMap<String, CreationToken>,
}

impl GroupTable {
    pub fn group(&self, name: &str) -> Option<&GroupState> {
        self.groups.get(name)
    }

    pub fn groups(&self) -> impl Iterator<Item = (&str, &GroupState)> {
        self.groups
            .iter()
            .map(|(name, state)| (name.as_str(), state))
    }

    pub fn last_delete(&self) -> u64 {
        self.last_delete
    }

    /// The last index applied, 0 before any.
    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// Apply `entry`, committed at `index` in `term`. Entries must come in
    /// log order; one at or below the last index applied is ignored, so a
    /// rebuild may overlap what is applied after it.
    pub fn apply(&mut self, term: u64, index: u64, entry: &GroupEntry) -> Folded {
        if index <= self.applied {
            return Folded::Replayed;
        }
        self.applied = index;
        let group = &entry.group;
        match entry.op {
            GroupOp::Seek { to, .. } => {
                let cre = CreationToken {
                    term,
                    index,
                    by: CreatedBy::Seek,
                };
                match self.groups.get_mut(group) {
                    Some(state) => {
                        state.cre = cre;
                        state.cursor = to;
                    }
                    None => {
                        self.groups.insert(group.clone(), fresh(cre, to));
                    }
                }
                return Folded::Applied;
            }
            GroupOp::Delete => {
                self.groups.remove(group);
                self.last_delete = index;
                return Folded::Applied;
            }
            _ => {}
        }
        let Some(cre) = entry.op.creation() else {
            unreachable!("every other op carries a token");
        };
        if self.stale(group, cre) {
            return Folded::Dropped {
                tracker_dropped: false,
            };
        }
        match entry.op {
            GroupOp::Advance { to, .. } => {
                let state = self
                    .groups
                    .entry(group.clone())
                    .or_insert_with(|| fresh(cre, to));
                state.cursor = state.cursor.max(to);
            }
            GroupOp::DeadLetter { offset, .. } => {
                // A group's first write is the advance to where it starts, so
                // a dead letter should never create one. If it does, the
                // cursor starts at 0: never past a record nobody settled.
                let state = self
                    .groups
                    .entry(group.clone())
                    .or_insert_with(|| fresh(cre, 0));
                state.dead.insert(offset);
                state.redriven.remove(&offset);
            }
            GroupOp::Redrive { offset, .. } => {
                if let Some(state) = self.groups.get_mut(group)
                    && state.dead.remove(&offset)
                {
                    state.redriven.insert(offset);
                }
            }
            GroupOp::FinishRedrive { offset, .. } => {
                if let Some(state) = self.groups.get_mut(group) {
                    state.redriven.remove(&offset);
                }
            }
            GroupOp::Discard { offset, .. } => {
                if let Some(state) = self.groups.get_mut(group) {
                    state.dead.remove(&offset);
                }
            }
            GroupOp::Seek { .. } | GroupOp::Delete => unreachable!("applied above"),
        }
        Folded::Applied
    }

    /// Whether a write carrying `cre` was decided for an incarnation of
    /// `group` that is gone.
    fn stale(&self, group: &str, cre: CreationToken) -> bool {
        match self.groups.get(group) {
            Some(state) => state.cre != cre,
            None => cre.index < self.last_delete,
        }
    }
}

const TABLE_VERSION: u8 = 1;
const BY_HYDRATE: u8 = 0;
const BY_SEEK: u8 = 1;

impl GroupTable {
    /// The table as a stream shard's snapshot carries it: a member that
    /// installs the snapshot holds no group entry below it, so this is all
    /// it learns of them. One table always encodes to the same bytes.
    ///
    /// ```text
    /// u8   version (1)
    /// u64  applied, u64 last_delete
    /// u32  groups, then per group in name order:
    ///   u32 name length, name (UTF-8)
    ///   u64 term, u64 index, u8 by (0 hydrate, 1 seek)    creation token
    ///   u64 cursor
    ///   u32 dead letters, then each (u64, ascending)
    ///   u32 redriven, then each (u64, ascending)
    /// ```
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::new();
        out.put_u8(TABLE_VERSION);
        out.put_u64(self.applied);
        out.put_u64(self.last_delete);
        out.put_u32(self.groups.len() as u32);
        for (name, state) in &self.groups {
            out.put_u32(name.len() as u32);
            out.put_slice(name.as_bytes());
            out.put_u64(state.cre.term);
            out.put_u64(state.cre.index);
            out.put_u8(match state.cre.by {
                CreatedBy::Hydrate => BY_HYDRATE,
                CreatedBy::Seek => BY_SEEK,
            });
            out.put_u64(state.cursor);
            for set in [&state.dead, &state.redriven] {
                out.put_u32(set.len() as u32);
                for offset in set {
                    out.put_u64(*offset);
                }
            }
        }
        out.freeze()
    }

    /// `None` when `bytes` are not what [`Self::encode`] writes, including
    /// names or offsets out of order: a member must not load a table that
    /// differs from the one the sender folded.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut buf = bytes;
        if take(&mut buf, 1)?.get_u8() != TABLE_VERSION {
            return None;
        }
        let applied = take(&mut buf, 8)?.get_u64();
        let last_delete = take(&mut buf, 8)?.get_u64();
        let count = take(&mut buf, 4)?.get_u32();
        let mut groups = BTreeMap::new();
        let mut last_name: Option<String> = None;
        for _ in 0..count {
            let len = take(&mut buf, 4)?.get_u32() as usize;
            let name = std::str::from_utf8(take(&mut buf, len)?).ok()?.to_owned();
            if name.is_empty() || last_name.as_ref().is_some_and(|last| *last >= name) {
                return None;
            }
            let cre = CreationToken {
                term: take(&mut buf, 8)?.get_u64(),
                index: take(&mut buf, 8)?.get_u64(),
                by: match take(&mut buf, 1)?.get_u8() {
                    BY_HYDRATE => CreatedBy::Hydrate,
                    BY_SEEK => CreatedBy::Seek,
                    _ => return None,
                },
            };
            let cursor = take(&mut buf, 8)?.get_u64();
            let dead = offsets(&mut buf)?;
            let redriven = offsets(&mut buf)?;
            groups.insert(
                name.clone(),
                GroupState {
                    cre,
                    cursor,
                    dead,
                    redriven,
                },
            );
            last_name = Some(name);
        }
        buf.is_empty().then_some(Self {
            groups,
            last_delete,
            applied,
        })
    }
}

/// The next `len` bytes of `buf`, `None` past its end.
fn take<'a>(buf: &mut &'a [u8], len: usize) -> Option<&'a [u8]> {
    if buf.len() < len {
        return None;
    }
    let (head, rest) = buf.split_at(len);
    *buf = rest;
    Some(head)
}

/// A strictly ascending set of offsets, as [`GroupTable::encode`] writes one.
fn offsets(buf: &mut &[u8]) -> Option<BTreeSet<u64>> {
    let count = take(buf, 4)?.get_u32();
    let mut set = BTreeSet::new();
    let mut last = None;
    for _ in 0..count {
        let offset = take(buf, 8)?.get_u64();
        if last.is_some_and(|last| last >= offset) {
            return None;
        }
        set.insert(offset);
        last = Some(offset);
    }
    Some(set)
}

/// A group created by its first write or seek, starting at `cursor`.
fn fresh(cre: CreationToken, cursor: u64) -> GroupState {
    GroupState {
        cre,
        cursor,
        dead: BTreeSet::new(),
        redriven: BTreeSet::new(),
    }
}

impl ShardGroups {
    pub fn table(&self) -> &GroupTable {
        &self.table
    }

    /// Replace the table, as a rebuild from the log does. Trackers are
    /// forgotten with it: they were hydrated from the table being replaced.
    pub fn reset(&mut self, table: GroupTable) {
        self.table = table;
        self.trackers.clear();
    }

    /// Apply `entry` as [`GroupTable::apply`] does. A dropped entry that
    /// carries the token of this member's tracker for the group forgets
    /// the tracker: the table will never take that tracker's writes.
    pub fn apply(&mut self, term: u64, index: u64, entry: &GroupEntry) -> Folded {
        let folded = self.table.apply(term, index, entry);
        if let Folded::Dropped { .. } = folded
            && let Some(cre) = entry.op.creation()
            && self.trackers.get(&entry.group) == Some(&cre)
        {
            self.trackers.remove(&entry.group);
            return Folded::Dropped {
                tracker_dropped: true,
            };
        }
        folded
    }

    /// Note that this member serves `group` from a tracker carrying `cre`.
    pub fn track(&mut self, group: &str, cre: CreationToken) {
        self.trackers.insert(group.to_owned(), cre);
    }

    /// The token of this member's tracker for `group`, if it has one.
    pub fn tracker(&self, group: &str) -> Option<CreationToken> {
        self.trackers.get(group).copied()
    }

    pub fn untrack(&mut self, group: &str) {
        self.trackers.remove(group);
    }

    /// Forget every tracker: the term they were made in is over.
    pub fn end_term(&mut self) {
        self.trackers.clear();
    }
}

#[cfg(test)]
mod tests;

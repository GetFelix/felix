//! A Raft stream shard's consumer groups, as every member folds them from
//! the shard's committed group entries ([`GroupEntry`]), in log order.
//!
//! The fold is `Step` in `docs/formal/FelixRaftGroups.tla`, for every group
//! of the shard at once. A group's state is its creation token, its cursor,
//! its dead letters, the dead letters redriven and not yet settled, the acks
//! above the cursor, and the records handed out and not yet passed (claims
//! are not in the model). The
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

use super::entry::{ClaimHolder, CreatedBy, CreationToken, GroupEntry, GroupOp};

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
    /// Acknowledged above the cursor. Settled for a new leader too, so an
    /// answered ack is never handed out again. Pruned as the cursor passes.
    pub acked: BTreeSet<u64>,
    /// Records handed out and not yet passed by the cursor, acknowledged,
    /// dead-lettered or finished, so a new leader keeps each holder's claim.
    /// A nack is not written, so its record stays here until the cursor
    /// passes it.
    pub claims: BTreeMap<u64, ClaimState>,
}

/// One standing claim, as the table holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimState {
    /// The delivery that made it, counting from 1.
    pub attempts: u32,
    pub holder: Option<ClaimHolder>,
    /// How long it was granted for.
    pub visibility_ms: u64,
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
                        // A seek delivers everything from `to` again, dead
                        // letters included. Redriven ones are still owed.
                        state.dead.split_off(&to);
                        // Every claim and ack made before the seek is void.
                        state.claims.clear();
                        state.acked.clear();
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
                let (cursor, redriven) = (state.cursor, &state.redriven);
                state
                    .claims
                    .retain(|offset, _| *offset >= cursor || redriven.contains(offset));
                state.acked = state.acked.split_off(&cursor);
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
                state.acked.remove(&offset);
                state.claims.remove(&offset);
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
                    if offset < state.cursor {
                        state.claims.remove(&offset);
                    }
                }
            }
            GroupOp::Discard { offset, .. } => {
                if let Some(state) = self.groups.get_mut(group) {
                    state.dead.remove(&offset);
                }
            }
            GroupOp::Claim { ref claims, .. } => {
                // The creation advance comes first, so a claim never makes a
                // group. Below the cursor only a redriven record is in play.
                if let Some(state) = self.groups.get_mut(group) {
                    for &(offset, attempts) in &claims.records {
                        if (offset >= state.cursor && !state.acked.contains(&offset))
                            || state.redriven.contains(&offset)
                        {
                            state.claims.insert(
                                offset,
                                ClaimState {
                                    attempts,
                                    holder: claims.holder.clone(),
                                    visibility_ms: claims.visibility_ms,
                                },
                            );
                        }
                    }
                }
            }
            GroupOp::Ack { offset, .. } => {
                // Like a dead letter, written only after the creation advance.
                let state = self
                    .groups
                    .entry(group.clone())
                    .or_insert_with(|| fresh(cre, 0));
                if offset >= state.cursor {
                    state.acked.insert(offset);
                }
                state.claims.remove(&offset);
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

const TABLE_VERSION: u8 = 3;

/// Why [`GroupTable::decode`] refused a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TableRefused {
    #[error("the group table does not parse")]
    Damaged,
    /// Versions 1 and 2 hold no acks above the cursor, so loading one as
    /// "no acks" could redeliver acked records. 0.8 starts fresh: a
    /// preview's snapshot with an older table is refused, and the shard
    /// must be recreated.
    #[error("group table version {0} is not supported; only version {TABLE_VERSION} is read")]
    Version(u8),
}
const BY_HYDRATE: u8 = 0;
const BY_SEEK: u8 = 1;

impl GroupTable {
    /// The table as a stream shard's snapshot carries it: a member that
    /// installs the snapshot holds no group entry below it, so this is all
    /// it learns of them. One table always encodes to the same bytes.
    ///
    /// ```text
    /// u8   version (3)
    /// u64  applied, u64 last_delete
    /// u32  groups, then per group in name order:
    ///   u32 name length, name (UTF-8)
    ///   u64 term, u64 index, u8 by (0 hydrate, 1 seek)    creation token
    ///   u64 cursor
    ///   u32 dead letters, then each (u64, ascending)
    ///   u32 redriven, then each (u64, ascending)
    ///   u32 acked above the cursor, then each (u64, ascending)
    ///   u32 claims, then each ascending by offset:
    ///     u64 offset, u32 attempts, u64 visibility in milliseconds
    ///     u8 1 if a named member holds it, else 0
    ///     u32 principal length, principal, u32 name length, name   (if named)
    /// ```
    ///
    /// Versions 1 and 2, which lacked acks (and claims), are refused.
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
            for set in [&state.dead, &state.redriven, &state.acked] {
                out.put_u32(set.len() as u32);
                for offset in set {
                    out.put_u64(*offset);
                }
            }
            out.put_u32(state.claims.len() as u32);
            for (offset, claim) in &state.claims {
                out.put_u64(*offset);
                out.put_u32(claim.attempts);
                out.put_u64(claim.visibility_ms);
                match &claim.holder {
                    Some(holder) => {
                        out.put_u8(1);
                        for part in [&holder.principal, &holder.name] {
                            out.put_u32(part.len() as u32);
                            out.put_slice(part.as_bytes());
                        }
                    }
                    None => out.put_u8(0),
                }
            }
        }
        out.freeze()
    }

    /// Refused when `bytes` are not what [`Self::encode`] writes, including
    /// names or offsets out of order: a member must not load a table that
    /// differs from the one the sender folded.
    pub fn decode(bytes: &[u8]) -> Result<Self, TableRefused> {
        match bytes.first() {
            Some(&TABLE_VERSION) => Self::decode_body(&bytes[1..]).ok_or(TableRefused::Damaged),
            Some(&version) => Err(TableRefused::Version(version)),
            None => Err(TableRefused::Damaged),
        }
    }

    fn decode_body(bytes: &[u8]) -> Option<Self> {
        let mut buf = bytes;
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
            let acked = offsets(&mut buf)?;
            let claims = claims(&mut buf)?;
            groups.insert(
                name.clone(),
                GroupState {
                    cre,
                    cursor,
                    dead,
                    redriven,
                    acked,
                    claims,
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

/// Claims strictly ascending by offset, as [`GroupTable::encode`] writes them.
fn claims(buf: &mut &[u8]) -> Option<BTreeMap<u64, ClaimState>> {
    let count = take(buf, 4)?.get_u32();
    let mut claims = BTreeMap::new();
    let mut last = None;
    for _ in 0..count {
        let offset = take(buf, 8)?.get_u64();
        if last.is_some_and(|last| last >= offset) {
            return None;
        }
        let attempts = take(buf, 4)?.get_u32();
        let visibility_ms = take(buf, 8)?.get_u64();
        let holder = match take(buf, 1)?.get_u8() {
            0 => None,
            1 => Some(ClaimHolder {
                principal: text(buf)?,
                name: text(buf)?,
            }),
            _ => return None,
        };
        claims.insert(
            offset,
            ClaimState {
                attempts,
                holder,
                visibility_ms,
            },
        );
        last = Some(offset);
    }
    Some(claims)
}

fn text(buf: &mut &[u8]) -> Option<String> {
    let len = take(buf, 4)?.get_u32() as usize;
    Some(std::str::from_utf8(take(buf, len)?).ok()?.to_owned())
}

/// A group created by its first write or seek, starting at `cursor`.
fn fresh(cre: CreationToken, cursor: u64) -> GroupState {
    GroupState {
        cre,
        cursor,
        dead: BTreeSet::new(),
        redriven: BTreeSet::new(),
        acked: BTreeSet::new(),
        claims: BTreeMap::new(),
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

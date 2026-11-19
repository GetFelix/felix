//! What one broker answers a re-form survey with for one shard: its copies
//! as [`super::read_copies`] reads them, and the voter sets of every
//! membership of the group's epoch those copies hold.
//!
//! A membership's voters can live only in a snapshot's metadata once a
//! purge drops its entry, so the snapshot is read as well as the log. See
//! `docs/per-shard-raft.md`, "Re-forming a group".

use std::collections::BTreeSet;
use std::io;
use std::path::Path;

use bytes::Bytes;

use super::reform::{CopyRead, read_copies};
use super::vouch::BrokerReport;
use crate::raft_log::{held, snapshot};

/// The voter sets of one membership: one, or both halves of a joint one,
/// each voter a broker and its copy id.
pub type VoterSets<B> = Vec<BTreeSet<(B, u64)>>;

/// How the layer that owns the membership format reads one. The storage
/// crate stores memberships as opaque payloads.
pub trait MembershipReader<B> {
    /// The voter sets of a membership record's payload: one, or both
    /// halves of a joint membership.
    fn voter_sets(&self, payload: &[u8]) -> io::Result<VoterSets<B>>;

    /// The membership a snapshot's metadata holds, with the index of the
    /// entry it came from. `None` when the metadata names no entry.
    fn snapshot_voter_sets(&self, meta: &[u8]) -> io::Result<Option<(u64, VoterSets<B>)>>;
}

/// One broker's report for a shard whose control-plane record is at group
/// epoch `epoch`.
///
/// `memberships` holds both halves of every membership entry of that epoch
/// in the current copy and every kept one, committed or not, and the
/// membership in each copy's snapshot when that snapshot was taken in the
/// epoch. A copy whose `raft-epoch` names another epoch adds none: its
/// entries are no voter set of this one. A copy with no `raft-epoch` past
/// epoch 0 (a learner placed fresh, an old restore) adds every membership
/// it holds, since its epoch is not known. `names` is the brokers of the
/// newest membership of the epoch across the current and kept copies.
/// `leads` is the broker's own answer, which no disk holds.
///
/// Fails when any file it reads does not decode, or when a copy purged
/// entries of the epoch past what its snapshot covers; a copy that cannot
/// be read is never reported as holding nothing.
pub fn build_report<B: Ord + Clone>(
    dir: &Path,
    index_spacing_bytes: u64,
    probes: &BTreeSet<u64>,
    epoch: u64,
    leads: bool,
    reader: &impl MembershipReader<B>,
) -> io::Result<BrokerReport<B>> {
    let copies = read_copies(dir, index_spacing_bytes, probes)?;
    let mut memberships = Vec::new();
    let mut newest: Option<(u64, VoterSets<B>)> = None;
    for read in std::iter::once(&copies.current).chain(&copies.kept) {
        let Some(held) = epoch_memberships(read, index_spacing_bytes, epoch, reader)? else {
            continue;
        };
        for (index, sets) in held {
            // Only a copy known to be of the epoch names its voters: an
            // older epoch's broker would refuse the plan as off the record.
            if (read.epoch.is_some() || epoch == 0)
                && newest.as_ref().is_none_or(|(at, _)| index > *at)
            {
                newest = Some((index, sets.clone()));
            }
            memberships.extend(sets);
        }
    }
    let names = newest
        .into_iter()
        .flat_map(|(_, sets)| sets)
        .flatten()
        .map(|(broker, _)| broker)
        .collect();
    Ok(BrokerReport {
        leads,
        names,
        memberships: Some(memberships),
        copies,
    })
}

/// A membership's voter sets with the index of its entry.
type Indexed<B> = (u64, VoterSets<B>);

/// Every membership of `epoch` in one copy, by the index of its entry.
/// `None` for a copy of another epoch or one that does not exist.
fn epoch_memberships<B>(
    read: &CopyRead,
    index_spacing_bytes: u64,
    epoch: u64,
    reader: &impl MembershipReader<B>,
) -> io::Result<Option<Vec<Indexed<B>>>> {
    if !crate::fs::try_exists(&read.path)? {
        return Ok(None);
    }
    // The epoch's own entries start at its first index; below it is the
    // base, whose voters the record keeps as the epoch's seed. Without a
    // `raft-epoch` past epoch 0 the copy's epoch is unknown, so everything
    // it holds counts: more voter sets only mark a plan, never unmark it.
    let first = match &read.epoch {
        Some(file) if file.bases.len() as u64 != epoch => return Ok(None),
        Some(file) => file.bases.last().map_or(0, |base| base.first_index),
        None => 0,
    };
    let label = read.path.display().to_string();
    let mut out = Vec::new();
    let stored = snapshot::read(&read.path)?;
    // A purge drops entries only below a stored snapshot, whose membership
    // then is the newest of them. A purge past it would hide one.
    if let Some(purged) = read.purged
        && purged >= first
        && stored
            .as_ref()
            .and_then(|stored| stored.last)
            .is_none_or(|last| last.index < purged)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label}: entries through {purged} are purged past what its snapshot covers"),
        ));
    }
    if let Some(stored) = &stored
        && let Some((index, sets)) = reader.snapshot_voter_sets(&stored.meta)?
        && index >= first
    {
        out.push((index, sets));
    }
    let from = read.purged.map_or(first, |purged| first.max(purged + 1));
    for (index, payload) in held::memberships(&read.path, &label, index_spacing_bytes, from)? {
        out.push((index, decode(reader, &payload, index, &label)?));
    }
    Ok(Some(out))
}

fn decode<B>(
    reader: &impl MembershipReader<B>,
    payload: &Bytes,
    index: u64,
    label: &str,
) -> io::Result<VoterSets<B>> {
    reader.voter_sets(payload).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("{label}: the membership at index {index} does not decode: {err}"),
        )
    })
}

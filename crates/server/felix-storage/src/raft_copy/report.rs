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
use super::vouch::{BrokerReport, VoterId};
use crate::raft_log::{held, snapshot};

/// How the layer that owns the membership format reads one. The storage
/// crate stores memberships as opaque payloads.
pub trait MembershipReader<B> {
    /// The voter sets of a membership record's payload: one, or both
    /// halves of a joint membership.
    fn voter_sets(&self, payload: &[u8]) -> io::Result<Vec<BTreeSet<(B, u64)>>>;

    /// The membership a snapshot's metadata holds, with the index of the
    /// entry it came from. `None` when the metadata names no entry.
    fn snapshot_voter_sets(
        &self,
        meta: &[u8],
    ) -> io::Result<Option<(u64, Vec<BTreeSet<(B, u64)>>)>>;
}

/// One broker's report for a shard whose control-plane record is at group
/// epoch `epoch`.
///
/// `memberships` holds both halves of every membership entry of that epoch
/// in the current copy and every kept one, committed or not, and the
/// membership in each copy's snapshot when that snapshot was taken in the
/// epoch. A copy at another epoch adds none: its entries are no voter set
/// of this one. `names` is the brokers of the current copy's newest such
/// membership. `leads` is the broker's own answer, which no disk holds.
///
/// Fails when any file it reads does not decode; a copy that cannot be read
/// is never reported as holding nothing.
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
    let mut names = BTreeSet::new();
    for (current, read) in
        std::iter::once((true, &copies.current)).chain(copies.kept.iter().map(|read| (false, read)))
    {
        let held = epoch_memberships(read, index_spacing_bytes, epoch, reader)?;
        if current && let Some((_, newest)) = held.iter().max_by_key(|(index, _)| *index) {
            names = newest
                .iter()
                .flatten()
                .map(|(broker, _)| broker.clone())
                .collect();
        }
        memberships.extend(held.into_iter().flat_map(|(_, sets)| sets));
    }
    Ok(BrokerReport {
        leads,
        names,
        memberships: Some(memberships),
        copies,
    })
}

/// Every membership of `epoch` in one copy, by the index of its entry.
fn epoch_memberships<B>(
    read: &CopyRead,
    index_spacing_bytes: u64,
    epoch: u64,
    reader: &impl MembershipReader<B>,
) -> io::Result<Vec<(u64, Vec<BTreeSet<VoterId<B>>>)>> {
    let bases = read.epoch.as_ref().map_or(&[][..], |file| &file.bases[..]);
    if bases.len() as u64 != epoch || !crate::fs::try_exists(&read.path)? {
        return Ok(Vec::new());
    }
    // The epoch's own entries start at its first index; below it is the
    // base, whose voters the record keeps as the epoch's seed.
    let first = bases.last().map_or(0, |base| base.first_index);
    let label = read.path.display().to_string();
    let mut out = Vec::new();
    if let Some(stored) = snapshot::read(&read.path)?
        && let Some((index, sets)) = reader.snapshot_voter_sets(&stored.meta)?
        && index >= first
    {
        out.push((index, sets));
    }
    // A purged entry is no entry; the snapshot above holds the newest one.
    let from = read.purged.map_or(first, |purged| first.max(purged + 1));
    for (index, payload) in held::memberships(&read.path, &label, index_spacing_bytes, from)? {
        out.push((index, decode(reader, &payload, index, &label)?));
    }
    Ok(out)
}

fn decode<B>(
    reader: &impl MembershipReader<B>,
    payload: &Bytes,
    index: u64,
    label: &str,
) -> io::Result<Vec<BTreeSet<VoterId<B>>>> {
    reader.voter_sets(payload).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("{label}: the membership at index {index} does not decode: {err}"),
        )
    })
}

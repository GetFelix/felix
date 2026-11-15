//! What a re-form reads from a broker's copies of a shard: the reservations
//! it must pad above, and what each kept copy can vouch for as committed.
//!
//! Kept directories are evidence and are never opened, since an open
//! repairs a torn tail and rewrites the saved commit. They are read here the
//! way an open would read them, and nothing is written. See
//! `docs/per-shard-raft.md`, "Keeping the re-form floor".

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};

use super::epoch_file::{self, EpochFile};
use super::judge::{KeptCopy, highest_reserved, kept_copies};
use super::rotated_by::{self, RotatedBy};
use crate::raft_log::{LogPoint, held, reserved};

/// One kept copy as a re-form reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeptLog {
    pub(crate) path: std::path::PathBuf,
    /// The copy id it held, if it had one.
    pub(crate) copy: Option<u64>,
    /// The last index it holds as an open reads it (a tail that does not
    /// decode is cut), or its purged id if that is
    /// higher. None for a directory with neither.
    pub(crate) last: Option<u64>,
    /// The last index it can vouch for as committed: its saved commit, cut
    /// to the last entry that decodes as an open cuts it, or its purged id
    /// if that is higher. None when it saved no commit and purged nothing, or
    /// when the entry there is not the committed one's, which an open would
    /// not take either.
    pub(crate) committed: Option<u64>,
}

/// Every copy rotations moved aside from `dir`, read as [`KeptLog`]s. A
/// directory that cannot be read is an error, never skipped.
pub(crate) fn kept_logs(dir: &Path, index_spacing_bytes: u64) -> io::Result<Vec<KeptLog>> {
    kept_copies(dir)?
        .into_iter()
        .map(|KeptCopy { path, copy }| {
            let mut kept = read_kept(&path, index_spacing_bytes)?;
            kept.copy = copy;
            Ok(kept)
        })
        .collect()
}

/// [`KeptLog`] for one directory. Its copy id is left `None`.
pub(crate) fn read_kept(path: &Path, index_spacing_bytes: u64) -> io::Result<KeptLog> {
    let label = path.display().to_string();
    let held = held::read(path, &label, index_spacing_bytes)?;
    let (saved, generation) = crate::disk_log::saved_commit(path).map_err(io::Error::other)?;
    let tail = held.last.map_or(0, |last| last + 1);
    // openraft saves a commit behind the call, so a relaxed copy's saved
    // commit can name an entry its last sync never covered.
    let lowered = saved > tail;
    let committed = match saved.min(tail).checked_sub(1) {
        None => None,
        // Below every record: purged, so committed, as an open reads it.
        Some(index) if held.lowest.is_none_or(|lowest| index < lowest) => Some(index),
        // As an open: the entry must be the one the commit was saved for,
        // or, after a cut, an earlier entry of its log, never a later term.
        Some(index) => match held::generation_at(path, &label, index_spacing_bytes, index)? {
            Some(Some(found))
                if generation != 0 && (found == generation || (lowered && found < generation)) =>
            {
                Some(index)
            }
            _ => None,
        },
    };
    // Only committed entries are purged, so the purged id is committed even
    // when the saved commit is absent or below it, as an open reads it.
    let committed = committed.max(held.purged);
    Ok(KeptLog {
        path: path.to_path_buf(),
        copy: None,
        last: held.last,
        committed,
    })
}

/// The first index a re-form's new epoch may give out on this broker's
/// copy: above the base it starts from, above the stored offset floor, and
/// above every reservation in `dir` and its kept copies. Every entry a
/// relaxed copy counted toward a commit is at or below a reservation that
/// was synced first, so no committed offset is given out again.
pub(crate) fn first_free_index(
    dir: &Path,
    base_last: Option<u64>,
    floor: Option<u64>,
) -> io::Result<u64> {
    let highest = base_last.max(floor).max(highest_reserved(dir)?);
    highest.map_or(Ok(0), |index| {
        index
            .checked_add(1)
            .ok_or_else(|| io::Error::other("every index is reserved"))
    })
}

/// One copy of a shard as a re-form plan reads it: the current copy or a
/// kept one, read without opening. Every file it reads that does not decode
/// fails the read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyRead {
    pub(crate) path: PathBuf,
    /// Its copy id, if it has one.
    pub(crate) copy: Option<u64>,
    /// Its `raft-epoch`, `None` at epoch 0.
    pub(crate) epoch: Option<EpochFile>,
    /// Its last log id as a term and an index: the last entry, or the purged
    /// id if that is higher. The epoch is derived by the plan.
    pub(crate) last: Option<LogPoint>,
    /// The index of its purged id: below it no term is read.
    pub(crate) purged: Option<u64>,
    /// Its saved commit's last index, cut as an open cuts it ([`KeptLog`]).
    pub(crate) committed: Option<u64>,
    /// The term of the vote it saved, `None` when it never voted.
    pub(crate) vote: Option<u64>,
    /// Its `raft-reserved`.
    pub(crate) reserved: Option<u64>,
    /// Why it was rotated; `None` for the current copy.
    pub(crate) rotated_by: Option<RotatedBy>,
    /// The term of the entry at each index the plan asked about, `None`
    /// where it holds none or the entry is purged.
    pub(crate) terms: BTreeMap<u64, Option<u64>>,
}

/// A broker's copies of one shard: the current one and every kept one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BrokerCopies {
    pub(crate) current: CopyRead,
    pub(crate) kept: Vec<CopyRead>,
}

/// Read the current copy in `dir` and every kept copy beside it, with the
/// term of each at `probes`.
pub(crate) fn read_copies(
    dir: &Path,
    index_spacing_bytes: u64,
    probes: &BTreeSet<u64>,
) -> io::Result<BrokerCopies> {
    let mut current = read_copy(dir, index_spacing_bytes, probes)?;
    current.copy = super::copy_file::load(dir)?.map(|found| found.copy);
    let kept = kept_copies(dir)?
        .into_iter()
        .map(|KeptCopy { path, copy }| {
            let mut read = read_copy(&path, index_spacing_bytes, probes)?;
            read.copy = copy;
            read.rotated_by = Some(rotated_by::load(&path)?);
            Ok(read)
        })
        .collect::<io::Result<_>>()?;
    Ok(BrokerCopies { current, kept })
}

/// [`CopyRead`] for one directory, with its copy id and rotation reason
/// left `None`. A missing directory reads as an empty copy.
pub(crate) fn read_copy(
    path: &Path,
    index_spacing_bytes: u64,
    probes: &BTreeSet<u64>,
) -> io::Result<CopyRead> {
    let label = path.display().to_string();
    let named = |err: io::Error| io::Error::new(err.kind(), format!("{label}: {err}"));
    if !crate::fs::try_exists(path)? {
        return Ok(CopyRead {
            path: path.to_path_buf(),
            copy: None,
            epoch: None,
            last: None,
            purged: None,
            committed: None,
            vote: None,
            reserved: None,
            rotated_by: None,
            terms: probes.iter().map(|index| (*index, None)).collect(),
        });
    }
    let kept = read_kept(path, index_spacing_bytes)?;
    let held = held::read(path, &label, index_spacing_bytes)?;
    let epoch = epoch_file::load(path)?;
    let mut purged = crate::raft_log::purged_point(path).map_err(named)?;
    let mut last = match held.last_record {
        Some((index, generation)) if purged.is_none_or(|purged| index > purged.index) => {
            Some(LogPoint {
                index,
                term: term_of(generation, index, &label)?,
            })
        }
        _ => purged,
    };
    // A start puts its first entry at the epoch's first index by purging up
    // to the end of the gap, and keeps its base below it. That purged id is
    // no entry: the copy's log is its base, read from its records (#2640).
    if let Some(top) = epoch.as_ref().and_then(|file| file.bases.last())
        && purged.is_some_and(|purged| purged.index.checked_add(1) == Some(top.first_index))
    {
        let base_last = top.base_last.map(|last| last.index);
        let record_last = held.last_record.map(|(index, _)| index);
        if record_last >= Some(top.first_index) {
            purged = None;
        } else if record_last == base_last {
            // Only a base that ends at its last record: a record in the gap,
            // or a base whose records are gone, keeps the purged id and
            // refuses as before.
            purged = None;
            last = match (top.base_last, held.last_record) {
                (Some(want), Some((index, generation))) => {
                    let term = term_of(generation, index, &label)?;
                    if term != want.term {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "{label}: the record at index {index} has term {term}, its base says {}",
                                want.term
                            ),
                        ));
                    }
                    Some(LogPoint { index, term })
                }
                _ => None,
            };
        }
    }
    let vote = crate::disk_log::saved_vote_generation(path)
        .map_err(io::Error::other)?
        .checked_sub(1);
    let mut terms = BTreeMap::new();
    for &index in probes {
        let term = match purged {
            Some(purged) if index == purged.index => Some(purged.term),
            Some(purged) if index < purged.index => None,
            _ => match held::generation_at(path, &label, index_spacing_bytes, index)? {
                Some(generation) => Some(term_of(generation, index, &label)?),
                None => None,
            },
        };
        terms.insert(index, term);
    }
    Ok(CopyRead {
        path: path.to_path_buf(),
        copy: None,
        epoch,
        last,
        purged: purged.map(|purged| purged.index),
        committed: kept.committed,
        vote,
        reserved: reserved::load(path)?,
        rotated_by: None,
        terms,
    })
}

/// A record's term; one with no generation, or an unknown writer, has none
/// and fails the read.
fn term_of(generation: Option<u64>, index: u64, label: &str) -> io::Result<u64> {
    generation
        .and_then(crate::raft_log::term_of_generation)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{label}: the record at index {index} has no term"),
            )
        })
}

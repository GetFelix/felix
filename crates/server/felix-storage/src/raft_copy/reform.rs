//! What a re-form reads from a broker's copies of a shard: the reservations
//! it must pad above, and what each kept copy can vouch for as committed.
//!
//! Kept directories are evidence and are never opened, since an open
//! repairs a torn tail and rewrites the saved commit. They are read here the
//! way an open would read them, and nothing is written. See
//! `docs/per-shard-raft.md`, "Keeping the re-form floor".

use std::io;
use std::path::Path;

use super::judge::{KeptCopy, highest_reserved, kept_copies};
use crate::raft_log::held;

/// One kept copy as a re-form reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptLog {
    pub path: std::path::PathBuf,
    /// The copy id it held, if it had one.
    pub copy: Option<u64>,
    /// The last index it holds as an open reads it (a tail that does not
    /// decode is cut), or its purged id if that is
    /// higher. None for a directory with neither.
    pub last: Option<u64>,
    /// The last index it can vouch for as committed: its saved commit, cut
    /// to the last entry that decodes as an open cuts it, or its purged id
    /// if that is higher. None when it saved no commit and purged nothing, or
    /// when the entry there is not the committed one's, which an open would
    /// not take either.
    pub committed: Option<u64>,
}

/// Every copy rotations moved aside from `dir`, read as [`KeptLog`]s. A
/// directory that cannot be read is an error, never skipped.
pub fn kept_logs(dir: &Path, index_spacing_bytes: u64) -> io::Result<Vec<KeptLog>> {
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
pub fn read_kept(path: &Path, index_spacing_bytes: u64) -> io::Result<KeptLog> {
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
pub fn first_free_index(dir: &Path, base_last: Option<u64>, floor: Option<u64>) -> io::Result<u64> {
    let highest = base_last.max(floor).max(highest_reserved(dir)?);
    highest.map_or(Ok(0), |index| {
        index
            .checked_add(1)
            .ok_or_else(|| io::Error::other("every index is reserved"))
    })
}

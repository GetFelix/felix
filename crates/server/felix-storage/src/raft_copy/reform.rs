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
    /// The last index it holds up to its last sync, or its purged id if that
    /// is higher. None for a directory with neither.
    pub last: Option<u64>,
    /// The last index it can vouch for as committed: its saved commit, cut
    /// to its last synced entry as an open cuts it. None when it saved no
    /// commit, or when the entry the cut lands on is not the committed one's.
    pub committed: Option<u64>,
}

/// Every copy rotations moved aside from `dir`, read as [`KeptLog`]s. Damage
/// that an open would refuse is an error, never skipped.
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
    let held = held::read(path, &path.display().to_string(), index_spacing_bytes)?;
    let (saved, generation) = crate::disk_log::saved_commit(path).map_err(io::Error::other)?;
    let tail = held.last.map_or(0, |last| last + 1);
    // openraft saves a commit behind the call, so a relaxed copy's saved
    // commit can name an entry its last sync never covered.
    let lowered = saved > tail;
    let committed = saved.min(tail).checked_sub(1).filter(|index| {
        match held.last_record {
            // As an open: the entry the cut lands on is an earlier entry of
            // the committed one's log, so its generation is never higher.
            Some((offset, Some(found))) if lowered && offset == *index => {
                generation != 0 && found <= generation
            }
            Some((offset, None)) if lowered && offset == *index => false,
            // A saved commit the log held when it was saved: entries at or
            // below it are never cut, and purged ones were committed.
            _ => true,
        }
    });
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
    Ok(highest.map_or(0, |index| index.saturating_add(1)))
}

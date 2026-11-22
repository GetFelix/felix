//! Writing `raft-epoch` into a copy placed fresh in a group past epoch 0.
//!
//! Such a copy has no epoch file, so it proves no epoch and never votes
//! (`docs/per-shard-raft.md`, "A copy's group epoch"). Its log will come
//! whole from the leader, so it can take the epoch's bases from the
//! control plane's record, but only while it holds nothing an earlier
//! epoch could have written: a restored directory with records or a vote
//! would then vote in an epoch its log was never part of.

use std::io;
use std::path::Path;

use super::epoch_file::{self, EpochBase, EpochFile};
use super::{copy_file, judge, stop_file};
use crate::disk_log::{durable_mark, engine_marker};
use crate::raft_log::{held, state_file};
use crate::segment::parse_segment_file_name;

/// What [`write_fresh_epoch`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshEpoch {
    /// The copy holds nothing and now names the record's epoch.
    Written,
    /// The copy already names an epoch, which this left alone.
    Named,
    /// The copy holds something an earlier epoch may have written, so it
    /// keeps an unknown epoch and never votes.
    Refused(&'static str),
}

/// Write `raft-epoch` for `incarnation` with `bases` into `dir`, the
/// directory of a copy no member has open, if it holds nothing but its
/// copy id: no log entry, vote, reservation, kept copy or stop marker.
/// `index_spacing_bytes` is the log's, for reading its segments.
///
/// A file this wrote earlier from an older view, a prefix of `bases` in a
/// directory that still holds nothing, is replaced, so a stale view at open
/// does not leave the copy behind for good. Any other file names the copy's
/// own epoch and is left alone.
pub fn write_fresh_epoch(
    dir: &Path,
    incarnation: u64,
    bases: &[EpochBase],
    index_spacing_bytes: u64,
) -> io::Result<FreshEpoch> {
    if bases.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a fresh copy's epoch needs the bases of a group past epoch 0",
        ));
    }
    // Writing the current epoch over a stop marker keyed by an earlier one
    // would spend the marker, and the copy would rejoin under its old id.
    if state_file::load(dir, stop_file::FILE_NAME)?.is_some() {
        return Ok(FreshEpoch::Refused("a re-form stopped it"));
    }
    let want = EpochFile {
        incarnation,
        bases: bases.to_vec(),
    };
    if let Some(held) = epoch_file::load(dir)? {
        let behind = held.incarnation == incarnation
            && held.bases.len() < bases.len()
            && bases.starts_with(&held.bases);
        if !behind {
            return Ok(FreshEpoch::Named);
        }
    }
    if let Some(why) = holds_something(dir, index_spacing_bytes)? {
        return Ok(FreshEpoch::Refused(why));
    }
    crate::fs::create_dir_all(dir)?;
    epoch_file::store(dir, &want)?;
    Ok(FreshEpoch::Written)
}

/// What `dir` holds beyond its copy id, its epoch and an empty log, if
/// anything. An open with nothing written leaves an empty segment, its
/// index and the durable mark; a vote, reservation, saved commit, purged id
/// or snapshot is its own file, so any other file counts.
fn holds_something(dir: &Path, index_spacing_bytes: u64) -> io::Result<Option<&'static str>> {
    if !judge::kept_copies(dir)?.is_empty() {
        return Ok(Some("a rotation kept an earlier copy of it"));
    }
    let entries = match crate::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let identity = [
        dir.join(copy_file::FILE_NAME),
        state_file::temporary(dir, copy_file::FILE_NAME),
        dir.join(epoch_file::FILE_NAME),
        state_file::temporary(dir, epoch_file::FILE_NAME),
    ];
    let mut segments = false;
    for entry in entries {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if parse_segment_file_name(name).is_some() {
            segments = true;
        } else if !(identity.contains(&path)
            || engine_marker::is_marker(dir, &path)
            || name.ends_with(".index")
            || name == durable_mark::mark_file_name())
        {
            return Ok(Some("it holds a vote, a reservation or log state"));
        }
    }
    let label = dir.display().to_string();
    if segments && held::read(dir, &label, index_spacing_bytes)?.last.is_some() {
        return Ok(Some("it holds log entries"));
    }
    Ok(None)
}

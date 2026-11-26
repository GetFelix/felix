//! Cutting a stopped Raft copy back to a backup point, for `felix-broker
//! restore-point`.
//!
//! The point is one past the furthest entry the leader acknowledged, which
//! can be past its commit index while a write answered on a written
//! majority or the lease waits for its commit, and past what this copy
//! saved as committed. After the cut the copy ends at the point and saves
//! it as committed, since the operator chose to restore that state, so a
//! re-form reads the point as the end the copy vouches for. See
//! `docs/per-shard-raft.md`, "Restoring at a backup point".

use std::io;
use std::path::Path;

use super::{epoch_file, start};
use crate::DiskLog;
use crate::disk_log::dir_lock::DirLock;
use crate::error::{Result, StorageError};
use crate::log::{LogConfig, Offset};
use crate::raft_log::{generation_of, snapshot};

/// Cut the Raft copy in `dir` back to `point`: entries from `point` on go
/// through the truncation path and the saved commit is set to the entry
/// below it, raised or lowered. Offline only, with no member open on `dir`.
///
/// `term` is the term the point records for the entry below it. The cut is
/// refused, with nothing written, when that entry is of another term: the
/// copy's leader lost the entry and a later leader wrote over it.
///
/// Also refused, with nothing written, when the copy holds state past the
/// point that no cut can take back: a stored snapshot or purged id at or
/// past it, a retention floor past it, a re-form that started at or past
/// it, or a re-form start that did not finish. The saved vote is kept: it
/// names a term, never an index, and the re-form after a restore starts a
/// new epoch whatever it is. Running it again, after a crash part way or
/// once it finished, is a no-op.
pub async fn restore_copy(
    dir: &Path,
    label: &str,
    point: Offset,
    term: Option<u64>,
    config: LogConfig,
) -> Result<()> {
    // Before the checks, so the copy cut is the one checked.
    crate::io::create_dir_all_durable(dir)?;
    let lock = DirLock::take(dir)?;
    if let Some(why) = past_the_point(dir, point)? {
        return Err(StorageError::Io(io::Error::other(format!(
            "{label}: cannot cut the Raft copy in {} back to {point}: {why}",
            dir.display()
        ))));
    }
    let config = LogConfig {
        raft_log: true,
        retention_bytes: None,
        retention_age: None,
        offload: None,
        ..config
    };
    let log = DiskLog::open_holding(dir, label, config, None, lock)?;
    let cut = match term {
        Some(term) => match generation_of(term) {
            Ok(generation) => {
                let purged = id_below(dir, point)?;
                log.restore_raft_to(point, generation, purged).await
            }
            Err(err) => Err(StorageError::Io(err)),
        },
        None => log.restore_to(point).await,
    };
    log.close().await?;
    cut
}

/// The generation of the purged or snapshot id at `point - 1`, if `dir`
/// stores one there. A log that starts at the point has no record to read.
fn id_below(dir: &Path, point: Offset) -> io::Result<Option<u64>> {
    let Some(last) = point.checked_sub(1) else {
        return Ok(None);
    };
    if let Some(purged) = crate::raft_log::purged_point(dir)?
        && purged.index == last
    {
        return generation_of(purged.term).map(Some);
    }
    match snapshot::read(dir)?.and_then(|stored| stored.last) {
        Some(id) if id.index == last => generation_of(id.term).map(Some),
        _ => Ok(None),
    }
}

/// Why `dir` cannot be cut back to `point`, if it cannot.
fn past_the_point(dir: &Path, point: Offset) -> io::Result<Option<String>> {
    if start::building(dir)? {
        return Ok(Some("a re-form start did not finish here".to_string()));
    }
    // A snapshot holds the state as of its index, and the entries it
    // covers may be purged, so the state at the point is gone.
    if let Some(id) = snapshot::read(dir)?.and_then(|stored| stored.last)
        && id.index >= point
    {
        return Ok(Some(format!(
            "its snapshot covers index {}; copy it again before it snapshots past the point",
            id.index
        )));
    }
    if let Some(purged) = crate::raft_log::purged_point(dir)?
        && purged.index >= point
    {
        return Ok(Some(format!(
            "it purged its log through index {}",
            purged.index
        )));
    }
    if let Some(floor) = crate::disk_log::carried_floor::load(dir)?
        && floor.floor > point
    {
        return Ok(Some(format!(
            "retention trimmed it to {}, past the point",
            floor.floor
        )));
    }
    if let Some(top) = epoch_file::load(dir)?.and_then(|file| file.bases.last().cloned())
        && top.first_index >= point
    {
        return Ok(Some(format!(
            "it re-formed at index {} after the point was taken",
            top.first_index
        )));
    }
    Ok(None)
}

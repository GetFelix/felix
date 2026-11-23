//! Cutting a stopped Raft copy back to a backup point, for `felix-broker
//! restore-point`.
//!
//! The point is the leader's commit index plus one. After the cut the copy
//! ends there and its saved commit names the entry below, so a re-form
//! reads the point as the end the copy vouches for. See
//! `docs/per-shard-raft.md`, "Restoring at a backup point".

use std::io;
use std::path::Path;

use super::{epoch_file, start};
use crate::DiskLog;
use crate::error::{Result, StorageError};
use crate::log::{LogConfig, Offset};
use crate::raft_log::snapshot;

/// Cut the Raft copy in `dir` back to `point`: entries from `point` on go
/// through the truncation path and the saved commit is lowered to the
/// entry below it. Offline only, with no member open on `dir`.
///
/// Refused, with nothing written, when the copy holds state past the point
/// that no cut can take back: a stored snapshot or purged id at or past it,
/// a retention floor past it, a re-form that started at or past it, or a
/// re-form start that did not finish. The saved vote is kept: it names a
/// term, never an index, and the re-form after a restore starts a new
/// epoch whatever it is. Running it again, after a crash part way or
/// once it finished, is a no-op.
pub async fn restore_copy(dir: &Path, label: &str, point: Offset, config: LogConfig) -> Result<()> {
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
    let log = DiskLog::open(dir, label, config)?;
    let cut = log.restore_to(point).await;
    log.close().await?;
    cut
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

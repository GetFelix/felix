//! Replacing a whole log directory with a compacted copy, crash-safely.
//!
//! The cache and the counters both compact by writing their live set into a
//! sibling `.compacting` directory and swapping it in. The swap is two renames:
//! the live directory aside to `.retired`, then the compacted one into its
//! place. Each is synced before the next, so a crash lands on one of three
//! states, and [`recover_interrupted_swap`] reads all of them.

use std::path::Path;

use crate::io::sync_dir;
use crate::{Result, StorageError};

/// Swap the compacted log in `staging` into `dir`, then delete the old one.
///
/// Both logs must be shut down first. Without the directory syncs the renames
/// can reach disk in either order, or not at all, and the window in between is
/// total loss for the shard: the directory is missing, and an unguarded open
/// would create it empty.
pub(crate) fn swap_in_compacted(dir: &Path, staging: &Path) -> Result<()> {
    let retired = dir.with_extension("retired");
    if retired.exists() {
        std::fs::remove_dir_all(&retired).map_err(StorageError::Io)?;
    }
    let parent = dir.parent();
    std::fs::rename(dir, &retired).map_err(StorageError::Io)?;
    if let Some(parent) = parent {
        sync_dir(parent).map_err(StorageError::Io)?;
    }
    std::fs::rename(staging, dir).map_err(StorageError::Io)?;
    if let Some(parent) = parent {
        sync_dir(parent).map_err(StorageError::Io)?;
    }
    std::fs::remove_dir_all(&retired).map_err(StorageError::Io)?;
    Ok(())
}

/// Finish a compaction swap that a crash interrupted. Call before opening the
/// log in `dir`.
///
/// A crash between the two renames leaves `dir` missing and all of its data in
/// `.retired`. An open that ignored that would create the directory empty, and
/// the next compaction would delete the only copy. The retired directory is
/// the pre-compaction state, so restoring it loses the compaction and nothing
/// else.
pub(crate) fn recover_interrupted_swap(dir: &Path) -> Result<()> {
    let retired = dir.with_extension("retired");
    if dir.exists() || !retired.exists() {
        return Ok(());
    }
    tracing::warn!(
        dir = %dir.display(),
        "a compaction was interrupted; restoring the shard from its retired copy",
    );
    std::fs::rename(&retired, dir).map_err(StorageError::Io)?;
    if let Some(parent) = dir.parent() {
        sync_dir(parent).map_err(StorageError::Io)?;
    }
    Ok(())
}

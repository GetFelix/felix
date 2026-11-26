//! Finishing a directory swap that a pre-0.6 build left behind.
//!
//! Cache and counter compaction used to write the live set into a sibling
//! `<shard>.compacting` directory and swap it in with two synced renames: the
//! live directory aside to `<shard>.retired`, then the compacted one into its
//! place, then the retired one deleted. Compaction no longer does this, but a
//! broker upgraded from a build that did can find a shard stopped anywhere in
//! that sequence. [`recover_legacy_swap`] settles it the way the old code did
//! before the shard's log is opened.

use std::path::Path;

use crate::disk_log::dir_lock::DirLock;
use crate::io::sync_dir;
use crate::{Result, StorageError};

/// Settle an interrupted legacy compaction swap for the shard in `dir`, and
/// hand back the lock on `dir` for the log to open holding
/// ([`crate::DiskLog::open_holding`]). Taken first, so an open of the same
/// shard elsewhere is refused before either changes its siblings.
///
/// - `dir` missing, `.retired` present: the crash fell between the renames and
///   `.retired` holds the whole pre-compaction log. It is renamed back, which
///   loses the compaction and nothing else. Opening without this would create
///   `dir` empty and serve an empty shard.
/// - `dir` present: any `.compacting` was never swapped in, and any `.retired`
///   is a log the swap already replaced. Both are deleted, as the old code's
///   next compaction would have.
pub(crate) fn recover_legacy_swap(dir: &Path) -> Result<DirLock> {
    let retired = dir.with_extension("retired");
    let staging = dir.with_extension("compacting");
    let mut changed = false;
    if !crate::fs::exists(dir) && crate::fs::exists(&retired) {
        // Locked while it moves, so a second open settling the same swap is
        // refused. Let go after: a lock on the directory at `dir` is taken
        // below, as for any open.
        let _moving = DirLock::take(&retired).map_err(StorageError::Io)?;
        if !crate::fs::exists(dir) {
            tracing::warn!(
                dir = %dir.display(),
                "an older build was interrupted mid-compaction; restoring the shard from its retired copy",
            );
            crate::fs::rename(&retired, dir).map_err(StorageError::Io)?;
            changed = true;
        }
    }
    // Only with the shard in place: without it, `.compacting` alone is all
    // that is left, and deleting it could not be undone.
    let in_place = crate::fs::exists(dir);
    crate::io::create_dir_all_durable(dir).map_err(StorageError::Io)?;
    let lock = DirLock::take(dir).map_err(StorageError::Io)?;
    if in_place {
        for stale in [&retired, &staging] {
            if crate::fs::exists(stale) {
                crate::fs::remove_dir_all(stale).map_err(StorageError::Io)?;
                changed = true;
            }
        }
    }
    if changed && let Some(parent) = dir.parent() {
        sync_dir(parent).map_err(StorageError::Io)?;
    }
    Ok(lock)
}

#[cfg(test)]
pub(crate) mod fixture;

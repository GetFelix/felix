//! Deleting the shard directories rotations moved aside, once the group has
//! them covered.
//!
//! A kept directory may go once the copy now in the shard directory has a
//! committed index at or above the kept log's last index. Every entry the
//! kept directory holds is at or below an index the group has committed. A
//! committed entry is in the group's log, and so in the current copy's log or
//! snapshot, and an uncommitted one was never acknowledged. A directory whose
//! last index cannot be read is held, never reclaimed on a guess. See
//! `docs/per-shard-raft.md`, "Reclaiming kept copies".

use std::io;
use std::path::{Path, PathBuf};

use super::judge::{KeptCopy, kept_copies, parent_of};
use crate::disk_log::layout::KEPT_DIR;
use crate::log::LogConfig;
use crate::{DiskLog, metrics_names};

/// What one [`reclaim_kept`] pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeptReport {
    /// Directories deleted.
    pub reclaimed: Vec<PathBuf>,
    /// Directories left, because the copy has not committed up to them or
    /// they could not be read.
    pub held: Vec<HeldCopy>,
}

/// A kept directory a pass left in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldCopy {
    pub path: PathBuf,
    /// The copy id it held, if it had one.
    pub copy: Option<u64>,
    /// The last log index it holds. None when it could not be read.
    pub last_index: Option<u64>,
}

impl KeptReport {
    /// The highest last index among the held directories that could be read:
    /// the committed index at which another pass would reclaim them all.
    pub fn wait_for(&self) -> Option<u64> {
        self.held.iter().filter_map(|held| held.last_index).max()
    }
}

/// Delete the directories rotations moved aside from `dir` that the copy in
/// `current` has committed past. `config` opens the kept logs to read their
/// last index.
pub async fn reclaim_kept(
    dir: &Path,
    current: &DiskLog,
    config: LogConfig,
) -> io::Result<KeptReport> {
    let parent = parent_of(dir)?;
    let committed = current.committed().0.checked_sub(1);
    let mut report = KeptReport::default();
    for KeptCopy { path, copy } in kept_copies(dir)? {
        let last = match last_index(&path, config.clone()).await {
            Ok(last) => last,
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "a kept shard copy cannot be read, so it is held"
                );
                report.held.push(HeldCopy {
                    path,
                    copy,
                    last_index: None,
                });
                continue;
            }
        };
        let covered = match (last, committed) {
            (None, _) => true,
            (Some(last), Some(committed)) => committed >= last,
            (Some(_), None) => false,
        };
        if !covered {
            report.held.push(HeldCopy {
                path,
                copy,
                last_index: last,
            });
            continue;
        }
        match crate::incarnation::retire_dir(&parent, &path) {
            Ok(_) => {
                tracing::info!(
                    path = %path.display(),
                    copy = ?copy.map(|copy| format!("{copy:016x}")),
                    last_index = ?last,
                    committed = ?committed,
                    "reclaimed a kept shard copy"
                );
                report.reclaimed.push(path);
            }
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "could not reclaim a kept shard copy"
                );
                report.held.push(HeldCopy {
                    path,
                    copy,
                    last_index: last,
                });
            }
        }
    }
    if !report.reclaimed.is_empty() {
        crate::io::sync_dir(&parent.join(KEPT_DIR))?;
    }
    publish_kept_metrics(&parent);
    Ok(report)
}

/// The last log index the directory holds as a Raft log, if it holds any.
async fn last_index(path: &Path, config: LogConfig) -> io::Result<Option<u64>> {
    let log = DiskLog::open(path, path.display().to_string(), config).map_err(io::Error::other)?;
    let last = crate::raft_log::last_point(&log).await;
    log.close().await.map_err(io::Error::other)?;
    Ok(last?.map(|point| point.index))
}

/// Set the kept-copy gauges from what `parent`'s kept directory holds.
pub(crate) fn publish_kept_metrics(parent: &Path) {
    let kept = parent.join(KEPT_DIR);
    let (mut copies, mut bytes) = (0u64, 0u64);
    if let Ok(entries) = crate::fs::read_dir(&kept) {
        for entry in entries.flatten() {
            copies += 1;
            bytes += size_of(&entry.path());
        }
    }
    metrics::gauge!(metrics_names::RAFT_KEPT_COPIES).set(copies as f64);
    metrics::gauge!(metrics_names::RAFT_KEPT_COPY_BYTES).set(bytes as f64);
}

fn size_of(path: &Path) -> u64 {
    let Ok(meta) = crate::fs::metadata(path) else {
        return 0;
    };
    if !meta.is_dir() {
        return meta.len();
    }
    let Ok(entries) = crate::fs::read_dir(path) else {
        return 0;
    };
    entries.flatten().map(|entry| size_of(&entry.path())).sum()
}

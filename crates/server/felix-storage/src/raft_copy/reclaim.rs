//! Deleting the shard directories rotations moved aside, once the group has
//! them covered.
//!
//! A kept directory may go once the copy now in the shard directory has a
//! committed index at or above the kept log's last index and holds, in its own
//! log, every index the kept directory holds. Every entry the kept directory
//! holds is at or below an index the group has committed, so a committed one
//! is in the group's log, and an uncommitted one was never acknowledged. A
//! snapshot carries no records, so a copy that caught up by snapshot, or
//! purged its log, has committed past the kept directory without holding what
//! is in it. A directory that cannot be read is held, never reclaimed on a
//! guess. See `docs/per-shard-raft.md`, "Reclaiming kept copies".

use std::io;
use std::path::{Path, PathBuf};

use super::judge::{KeptCopy, kept_copies, parent_of};
use crate::disk_log::layout::KEPT_DIR;
use crate::log::LogConfig;
use crate::raft_log::held;
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
    /// Why it was left.
    pub reason: HeldReason,
}

/// Why a kept directory was left in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeldReason {
    /// The copy has not committed up to its last index yet.
    NotCommitted,
    /// The copy's own log starts above where the kept one does, so it does
    /// not hold what the kept one holds, whatever it has committed.
    NotHeld,
    /// Its segments could not be read.
    Unreadable,
    /// It was due, but deleting it failed.
    DeleteFailed,
}

impl KeptReport {
    /// The highest last index among the directories held for want of a
    /// commit: the committed index at which another pass would reclaim them
    /// all. A directory held for another reason is not waited for.
    pub fn wait_for(&self) -> Option<u64> {
        self.held
            .iter()
            .filter(|held| held.reason == HeldReason::NotCommitted)
            .filter_map(|held| held.last_index)
            .max()
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
    let current_first = held::first_index(current)?;
    let mut report = KeptReport::default();
    for KeptCopy { path, copy } in kept_copies(dir)? {
        let read = {
            let (path, spacing) = (path.clone(), config.index_spacing_bytes);
            crate::task::blocking(move || {
                held::read(&path, &path.display().to_string(), spacing)
                    .map_err(crate::StorageError::Io)
            })
            .await
        };
        let kept = match read {
            Ok(kept) => kept,
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
                    reason: HeldReason::Unreadable,
                });
                continue;
            }
        };
        let reason = if kept.entries && current_first > kept.first {
            Some(HeldReason::NotHeld)
        } else {
            match (kept.last, committed) {
                (None, _) => None,
                (Some(last), Some(committed)) if committed >= last => None,
                _ => Some(HeldReason::NotCommitted),
            }
        };
        if let Some(reason) = reason {
            report.held.push(HeldCopy {
                path,
                copy,
                last_index: kept.last,
                reason,
            });
            continue;
        }
        match crate::incarnation::retire_dir(&parent, &path) {
            Ok(_) => {
                tracing::info!(
                    path = %path.display(),
                    copy = ?copy.map(|copy| format!("{copy:016x}")),
                    last_index = ?kept.last,
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
                    last_index: kept.last,
                    reason: HeldReason::DeleteFailed,
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

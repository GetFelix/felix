//! Deleting the shard directories rotations moved aside, once the group has
//! them covered.
//!
//! A kept directory holding records may go once the copy now in the shard
//! directory shows all of this in its own log: its committed index (the log's
//! commit offset, which a snapshot install does not move) is at or above the
//! kept log's last index, its entry at the kept log's last record has the
//! same term as the kept one, its flushed tail is at or above the kept log's
//! last index, and its first index is at or below where the kept log's lowest
//! segment that holds a record starts.
//!
//! Every entry the kept directory holds is at or below an index the group has
//! committed, so a committed one is in the group's log, and an uncommitted
//! one was never acknowledged. Two logs with the same index and term at one
//! entry hold the same entries below it, so the matching term makes the
//! current copy's prefix the kept one's. The flushed tail and first index make
//! the copy hold those entries itself, on disk: a commit index can be saved
//! before its entry is flushed, and a snapshot carries no records. A
//! directory with no records is covered by the committed index alone. One
//! that cannot be read is held, never reclaimed on a guess. See
//! `docs/per-shard-raft.md`, "Reclaiming kept copies".

use std::io;
use std::path::{Path, PathBuf};

use super::judge::{KeptCopy, kept_copies, parent_of};
use crate::disk_log::layout::KEPT_DIR;
use crate::log::LogConfig;
use std::sync::Arc;

use crate::disk_log::WriterAt;
use crate::log::UNKNOWN_WRITER;
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
    /// The copy has not yet committed and flushed up to its last index.
    Behind,
    /// The copy holds a different entry at the kept log's last record, so its
    /// log is another branch. Committing past it does not make it the same.
    Diverged,
    /// The copy's own log starts above its lowest record, so it does not
    /// hold what the kept one holds, whatever it has committed.
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
            .filter(|held| held.reason == HeldReason::Behind)
            .filter_map(|held| held.last_index)
            .max()
    }
}

/// The copy in a shard directory: its log, and a reader for its first index
/// that counts a purge held only in memory.
#[derive(Clone)]
pub struct CurrentCopy {
    log: DiskLog,
    first_index: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl CurrentCopy {
    pub fn new(log: DiskLog, first_index: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        Self { log, first_index }
    }

    /// The committed index the log itself saved.
    pub fn committed(&self) -> Option<u64> {
        self.log.commit_offset().checked_sub(1)
    }

    /// The last index on disk, whether or not the adapter has reported a
    /// flush since it opened.
    pub fn flushed_last(&self) -> Option<u64> {
        self.log.durable_offset().checked_sub(1)
    }
}

/// Delete the directories rotations moved aside from `dir` that `current`
/// covers. `config` sets how the kept logs' segments are scanned.
pub async fn reclaim_kept(
    dir: &Path,
    current: &CurrentCopy,
    config: LogConfig,
) -> io::Result<KeptReport> {
    let parent = parent_of(dir)?;
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
        let committed = current.committed();
        let reason = match (kept.lowest, kept.last, kept.last_record) {
            // No records at all: nothing to lose beyond a purged id.
            (None, last, _) => last
                .filter(|last| !committed.is_some_and(|at| at >= *last))
                .map(|_| HeldReason::Behind),
            (Some(_), Some(last), Some((record, generation))) => {
                let flushed = current.flushed_last();
                if !committed.is_some_and(|at| at >= last) || !flushed.is_some_and(|at| at >= last)
                {
                    Some(HeldReason::Behind)
                } else {
                    match current
                        .log
                        .writer_at(record)
                        .await
                        .map_err(io::Error::other)?
                    {
                        WriterAt::Absent => Some(HeldReason::NotHeld),
                        WriterAt::Unstamped => Some(HeldReason::Diverged),
                        WriterAt::Generation(ours) => (generation != Some(ours)
                            || ours == UNKNOWN_WRITER)
                            .then_some(HeldReason::Diverged)
                            // Read last, so a purge since the term read holds.
                            .or_else(|| {
                                kept.lowest
                                    .filter(|lowest| (current.first_index)() > *lowest)
                                    .map(|_| HeldReason::NotHeld)
                            }),
                    }
                }
            }
            (Some(_), _, _) => Some(HeldReason::Unreadable),
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
                    committed = ?current.committed(),
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

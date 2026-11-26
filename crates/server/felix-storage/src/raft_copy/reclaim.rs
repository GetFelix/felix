//! Deleting the shard directories rotations moved aside, once the group has
//! them covered.
//!
//! A kept directory holding records may go once the copy now in the shard
//! directory shows all of this in its own log: its committed index (the
//! commit offset the log has saved to disk, which a snapshot install does not
//! move and a restart reads back) is at or above the kept log's last index,
//! its entry at the kept log's last record has the same term as the kept one,
//! its flushed tail is at or above the kept log's last index, and its first
//! index is at or below where the kept log's lowest segment that holds a
//! record starts.
//!
//! Every entry the kept directory holds is at or below an index the group has
//! committed, so a committed one is in the group's log, and an uncommitted
//! one was never acknowledged. Two logs with the same index and term at one
//! entry hold the same entries below it, so the matching term makes the
//! current copy's prefix the kept one's. The flushed tail and first index make
//! the copy hold those entries itself, on disk: a commit index can be saved
//! before its entry is flushed, and a snapshot carries no records. A
//! directory with no records is covered by the committed index alone. One
//! that cannot be read is held, never reclaimed on a guess, and so is one
//! reserved past the current copy (`raft_log::reserved`). See
//! `docs/per-shard-raft.md`, "Reclaiming kept copies".

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::judge::{KeptCopy, kept_copies, parent_of};
use crate::disk_log::WriterAt;
use crate::disk_log::layout::KEPT_DIR;
use crate::incarnation::kept_gauges;
use crate::log::{LogConfig, UNKNOWN_WRITER};
use crate::raft_log::{held, reserved};
use crate::{DiskLog, StorageError};

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
    /// Its `raft-reserved` and the current copy's, as the pass read them.
    /// None when the pass did not read them, as for [`HeldReason::Itself`].
    pub reserved: Option<HeldReservations>,
}

/// The two reservations a pass compares before it judges a kept directory's
/// records. With both, an operator can tell whether deleting a damaged kept
/// `raft-reserved` is safe (`docs/storage-format.md`, "`raft-reserved`").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeldReservations {
    /// The kept directory's.
    pub kept: ReservationRead,
    /// The current shard directory's.
    pub current: ReservationRead,
}

/// One directory's `raft-reserved` as a pass read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationRead {
    /// No file: the copy never reserved.
    Absent,
    /// The highest reserved Raft index.
    At(u64),
    /// Present but not readable or not decodable. The value is unknown.
    Unreadable,
}

/// Why a kept directory was left in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeldReason {
    /// The copy has not yet saved a commit and flushed up to its last index.
    Behind,
    /// The copy holds a different entry at the kept log's last record, so its
    /// log is another branch. Committing past it does not make it the same.
    Diverged,
    /// The copy's own log starts above its lowest record, so it does not
    /// hold what the kept one holds, whatever it has committed.
    NotHeld,
    /// It is the copy doing the judging: that copy's directory was rotated
    /// aside since, so it no longer speaks for the shard.
    Itself,
    /// It reserved Raft indexes past the copy's own reservation, so deleting
    /// it would lose how far a re-form has to pad.
    Reserved,
    /// Its segments could not be read.
    Unreadable,
    /// It was due, but deleting it failed.
    DeleteFailed,
}

impl ReservationRead {
    fn of(read: &io::Result<Option<u64>>) -> Self {
        match read {
            Ok(None) => Self::Absent,
            Ok(Some(index)) => Self::At(*index),
            Err(_) => Self::Unreadable,
        }
    }
}

impl fmt::Display for ReservationRead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => f.write_str("absent"),
            Self::At(index) => write!(f, "{index}"),
            Self::Unreadable => f.write_str("unreadable"),
        }
    }
}

impl fmt::Display for HeldReservations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "kept {}, current {}", self.kept, self.current)
    }
}

/// The line an operator reads: the directory, why it is held, and both
/// reservations.
impl fmt::Display for HeldCopy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} held ({:?})", self.path.display(), self.reason)?;
        if let Some(copy) = self.copy {
            write!(f, ", copy {copy:016x}")?;
        }
        if let Some(last) = self.last_index {
            write!(f, ", last index {last}")?;
        }
        match self.reserved {
            Some(reserved) => write!(f, ", raft-reserved {reserved}"),
            None => Ok(()),
        }
    }
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

/// The copy in a shard directory: its log, its copy id, and a reader for its
/// first index that counts a purge held only in memory.
#[derive(Clone)]
pub struct CurrentCopy {
    log: DiskLog,
    copy: Option<u64>,
    first_index: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl CurrentCopy {
    pub fn new(
        log: DiskLog,
        copy: Option<u64>,
        first_index: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            log,
            copy,
            first_index,
        }
    }

    /// The committed index the log has saved to disk. Only that one counts:
    /// a restart reads it back, and the group vouches for this copy's log up
    /// to it, so a kept copy deleted on a commit still in memory could lose
    /// acknowledged entries with the power.
    pub fn committed(&self) -> Option<u64> {
        self.log.saved_commit_offset().checked_sub(1)
    }

    /// The committed index in memory, which the saved one follows.
    pub fn committed_in_memory(&self) -> Option<u64> {
        self.log.commit_offset().checked_sub(1)
    }

    /// The last index on disk, whether or not the adapter has reported a
    /// flush since it opened.
    pub fn flushed_last(&self) -> Option<u64> {
        self.log.durable_offset().checked_sub(1)
    }
}

/// Delete the directories rotations moved aside from `dir` that `current`
/// covers. `config` sets how the kept logs' segments are scanned. A pass
/// deletes nothing once `stopped` answers true, as it does when the group
/// that `current` belongs to has shut down. The filesystem work runs on the
/// blocking pool.
pub async fn reclaim_kept(
    dir: &Path,
    current: &CurrentCopy,
    config: LogConfig,
    stopped: &(dyn Fn() -> bool + Sync),
) -> io::Result<KeptReport> {
    let parent = parent_of(dir)?;
    let listed = {
        let dir = dir.to_path_buf();
        off_runtime(move || kept_copies(&dir)).await?
    };
    let mut report = KeptReport::default();
    let judged = judge(dir, listed, &parent, current, config, stopped, &mut report).await;
    if report.reclaimed.is_empty() {
        kept_gauges::note(&parent);
        judged?;
        return Ok(report);
    }
    // Synced whatever happened after the deletes, so none comes back.
    let kept = parent.join(KEPT_DIR);
    let synced = off_runtime(move || crate::io::sync_dir(&kept)).await;
    kept_gauges::refresh(&parent);
    judged?;
    synced?;
    Ok(report)
}

async fn judge(
    dir: &Path,
    listed: Vec<KeptCopy>,
    parent: &Path,
    current: &CurrentCopy,
    config: LogConfig,
    stopped: &(dyn Fn() -> bool + Sync),
    report: &mut KeptReport,
) -> io::Result<()> {
    for KeptCopy { path, copy } in listed {
        if copy.is_some() && copy == current.copy {
            report.held.push(HeldCopy {
                path,
                copy,
                last_index: None,
                reason: HeldReason::Itself,
                reserved: None,
            });
            continue;
        }
        // A rotation carries every kept reservation into the new copy, so
        // this holds only a directory some other path left uncovered.
        let reads = {
            let (dir, path) = (dir.to_path_buf(), path.clone());
            off_runtime(move || Ok((reserved::load(&path), reserved::load(&dir)))).await
        };
        let (kept_read, current_read) = reads.unwrap_or_else(|err| {
            let again = io::Error::new(err.kind(), err.to_string());
            (Err(err), Err(again))
        });
        let reservations = HeldReservations {
            kept: ReservationRead::of(&kept_read),
            current: ReservationRead::of(&current_read),
        };
        let uncovered = match (kept_read, current_read) {
            (Ok(kept), Ok(current)) => Ok(kept > current),
            (Err(err), _) | (_, Err(err)) => Err(err),
        };
        if !matches!(uncovered, Ok(false)) {
            let held = HeldCopy {
                path,
                copy,
                last_index: None,
                reason: if uncovered.is_ok() {
                    HeldReason::Reserved
                } else {
                    HeldReason::Unreadable
                },
                reserved: Some(reservations),
            };
            match &uncovered {
                // Names the file that failed, which can be the current copy's.
                Err(err) => tracing::warn!(
                    held = %held,
                    error = %err,
                    "a reservation cannot be read, so the kept shard copy is held"
                ),
                Ok(_) => tracing::warn!(
                    held = %held,
                    "a kept shard copy reserved past the current copy, so it is held"
                ),
            }
            report.held.push(held);
            continue;
        }
        let read = {
            let (path, spacing) = (path.clone(), config.index_spacing_bytes);
            off_runtime(move || held::read(&path, &path.display().to_string(), spacing)).await
        };
        let kept = match read {
            Ok(kept) => kept,
            Err(err) => {
                let held = HeldCopy {
                    path,
                    copy,
                    last_index: None,
                    reason: HeldReason::Unreadable,
                    reserved: Some(reservations),
                };
                tracing::warn!(
                    held = %held,
                    error = %err,
                    "a kept shard copy cannot be read, so it is held"
                );
                report.held.push(held);
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
            let held = HeldCopy {
                path,
                copy,
                last_index: kept.last,
                reason,
                reserved: Some(reservations),
            };
            tracing::info!(held = %held, "a kept shard copy is held");
            report.held.push(held);
            continue;
        }
        // Checked as late as possible: once the group stops, another member
        // may own the directory, and this copy's log may be the kept one.
        if stopped() {
            return Ok(());
        }
        let retired = {
            let (parent, path) = (parent.to_path_buf(), path.clone());
            crate::task::blocking(move || crate::incarnation::retire_dir(&parent, &path)).await
        };
        match retired {
            Ok(_) => {
                tracing::info!(
                    path = %path.display(),
                    copy = ?copy.map(|copy| format!("{copy:016x}")),
                    last_index = ?kept.last,
                    committed = ?committed,
                    reserved = %reservations,
                    "reclaimed a kept shard copy"
                );
                report.reclaimed.push(path);
            }
            Err(err) => {
                let held = HeldCopy {
                    path,
                    copy,
                    last_index: kept.last,
                    reason: HeldReason::DeleteFailed,
                    reserved: Some(reservations),
                };
                tracing::warn!(
                    held = %held,
                    error = %err,
                    "could not reclaim a kept shard copy"
                );
                report.held.push(held);
            }
        }
    }
    Ok(())
}

/// Run blocking filesystem work on the blocking pool.
async fn off_runtime<T, F>(work: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    crate::task::blocking(move || work().map_err(StorageError::Io))
        .await
        .map_err(|err| match err {
            StorageError::Io(err) | StorageError::OpenFileLimit(err) => err,
            other => io::Error::other(other),
        })
}

//! Opening a shard directory as a copy: keep its copy id, or rotate it.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::copy_file::{self, CopyFile};
use super::gate::GatePassed;
use super::marks::{CopyMarks, MarkLog};
use super::{VoteMark, log_below};
use crate::disk_log::crash_point;
use crate::disk_log::layout::{KEPT_DIR, kept_name, kept_of};
use crate::log::LogConfig;
use crate::raft_log::state_file;
use crate::{DiskLog, StorageId};

/// Longer shard names do not fit a mark.
const MAX_SHARD_NAME: usize = 4096;

/// A data root that passed the gate, opening shard directories as copies.
pub struct CopyRoot {
    root: PathBuf,
    marks: MarkLog,
    mint: Box<dyn Fn() -> u64 + Send + Sync>,
}

/// A shard directory opened as a copy.
pub struct OpenedCopy {
    /// The copy id this member is, from the directory's copy file.
    pub copy: u64,
    pub log: DiskLog,
    /// The shard's marks, for the Raft log to keep as it moves.
    pub marks: CopyMarks,
    /// Set when the directory held no copy it could keep.
    pub rotation: Option<Rotation>,
}

/// Why a copy was rotated and what became of the old directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rotation {
    pub reason: RotateReason,
    /// The copy id the directory held, if it had one.
    pub replaced: Option<u64>,
    /// Where the old directory was moved, if it held more than a copy file.
    pub kept: Option<PathBuf>,
}

/// Why a directory could not keep its copy id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotateReason {
    /// The directory is missing or empty.
    Empty,
    /// It holds files but no copy file.
    NoCopyFile,
    /// Its copy id was minted under another storage id.
    StorageId,
    /// The root has no marks for the shard.
    NoMarks,
    /// The root's marks belong to another copy id.
    OtherCopy,
    /// Its vote is below the vote mark.
    VoteBelowMark,
    /// Its last log id is below the log mark.
    LogBelowMark,
    /// Its log was poisoned in its last run: a flush failed, or a rollover
    /// or truncation failed partway. The directory can hold less than the
    /// copy reported as written.
    Poisoned,
}

/// A shard directory an earlier rotation moved aside. Its records are
/// kept as plain records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptCopy {
    pub path: PathBuf,
    /// The copy id it held, if it had one.
    pub copy: Option<u64>,
}

impl CopyRoot {
    /// Open the root the gate passed. `mint` makes copy ids; a 0 it returns
    /// is asked again.
    ///
    /// Watches the root for poison ([`crate::run_state::watch_poison`]): if
    /// a copy's poison marker cannot be written, the forgotten run record
    /// still rotates the storage id, and every copy with it, on restart.
    pub fn open(
        root: impl Into<PathBuf>,
        passed: GatePassed,
        mint: impl Fn() -> u64 + Send + Sync + 'static,
    ) -> Self {
        let root = root.into();
        crate::run_state::watch_poison(passed.marks.root());
        Self {
            root,
            marks: passed.marks,
            mint: Box::new(mint),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn storage_id(&self) -> StorageId {
        self.marks.storage_id()
    }

    pub fn mark_log(&self) -> &MarkLog {
        &self.marks
    }

    /// Open `dir` as `shard`'s copy, as a log labelled `label` under
    /// `config`, rotating the copy when the directory cannot keep its id.
    ///
    /// A rotated copy has a new copy id that never voted and an empty log,
    /// with fresh marks synced before this returns. The old directory, if it
    /// held more than a copy file, is moved into `.kept` beside `dir` (see
    /// [`kept_copies`]). A crash part way leaves the old directory, or one this rotates again.
    pub async fn open_copy(
        &self,
        shard: &str,
        dir: &Path,
        label: impl Into<String>,
        config: LogConfig,
    ) -> io::Result<OpenedCopy> {
        if shard.len() > MAX_SHARD_NAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("a shard name of {} bytes is too long", shard.len()),
            ));
        }
        let shard: Arc<str> = Arc::from(shard);
        let label = label.into();
        let (found, reason) = self.judge_files(&shard, dir)?;
        let reason = match (found, reason) {
            (Some(found), None) => {
                let log = open_log(dir, &label, config.clone())?;
                match judge_log(&log, &self.marks, &shard).await {
                    Ok(None) => {
                        return Ok(OpenedCopy {
                            copy: found.copy,
                            marks: self.marks.for_copy(shard, found.copy),
                            log,
                            rotation: None,
                        });
                    }
                    Ok(Some(reason)) => {
                        log.close().await.map_err(io::Error::other)?;
                        reason
                    }
                    Err(err) => {
                        let _ = log.close().await;
                        return Err(err);
                    }
                }
            }
            (_, Some(reason)) => reason,
            (None, None) => unreachable!("a directory with no copy file has a reason"),
        };
        let replaced = copy_file::load(dir).ok().flatten().map(|found| found.copy);
        let (copy, kept) = self.rotate(&shard, dir, replaced).await?;
        tracing::info!(
            %shard,
            dir = %dir.display(),
            ?reason,
            replaced = ?replaced.map(|copy| format!("{copy:016x}")),
            copy = format!("{copy:016x}"),
            "rotated the shard's copy id"
        );
        Ok(OpenedCopy {
            copy,
            log: open_log(dir, &label, config)?,
            marks: self.marks.for_copy(shard, copy),
            rotation: Some(Rotation {
                reason,
                replaced,
                kept,
            }),
        })
    }

    /// The checks that need no log: the copy file and the marks.
    fn judge_files(
        &self,
        shard: &str,
        dir: &Path,
    ) -> io::Result<(Option<CopyFile>, Option<RotateReason>)> {
        if is_empty(dir)? {
            return Ok((None, Some(RotateReason::Empty)));
        }
        let Some(found) = copy_file::load(dir)? else {
            return Ok((None, Some(RotateReason::NoCopyFile)));
        };
        if found.minted != self.storage_id() {
            return Ok((Some(found), Some(RotateReason::StorageId)));
        }
        let reason = match self.marks.marks(shard) {
            None => Some(RotateReason::NoMarks),
            Some(marks) if marks.copy != found.copy => Some(RotateReason::OtherCopy),
            // Before any open of the log, which would cut it back to its
            // last sync. A marker that cannot be read counts the same.
            Some(_) if crate::disk_log::left_poisoned(dir)? => Some(RotateReason::Poisoned),
            Some(_) => None,
        };
        Ok((Some(found), reason))
    }

    /// Move the old directory aside, make a new one with a new copy id,
    /// then write fresh marks. Each step is synced before the next, and a
    /// crash before the marks leaves a directory whose copy the marks do
    /// not name.
    async fn rotate(
        &self,
        shard: &Arc<str>,
        dir: &Path,
        replaced: Option<u64>,
    ) -> io::Result<(u64, Option<PathBuf>)> {
        let parent = parent_of(dir)?;
        let step = || crash_point::reached(&parent);
        step()?;
        let kept = if holds_no_records(dir)? {
            None
        } else {
            let kept = kept_path(dir, &parent, replaced)?;
            crate::fs::rename(dir, &kept)?;
            step()?;
            crate::io::sync_dir(&parent.join(KEPT_DIR))?;
            crate::io::sync_dir(&parent)?;
            crate::incarnation::kept_gauges::refresh(&parent);
            Some(kept)
        };
        step()?;
        crate::fs::create_dir_all(dir)?;
        step()?;
        crate::io::sync_dir(&parent)?;
        let copy = loop {
            let copy = (self.mint)();
            if copy != 0 && Some(copy) != replaced {
                break copy;
            }
        };
        copy_file::store(
            dir,
            &CopyFile {
                copy,
                minted: self.storage_id(),
            },
        )?;
        step()?;
        self.marks.fresh(Arc::clone(shard), copy).await?;
        Ok((copy, kept))
    }
}

/// The directories rotations moved aside from `dir`, by name.
pub fn kept_copies(dir: &Path) -> io::Result<Vec<KeptCopy>> {
    let parent = parent_of(dir)?;
    let Some(name) = dir.file_name().and_then(|name| name.to_str()) else {
        return Ok(Vec::new());
    };
    let entries = match crate::fs::read_dir(parent.join(KEPT_DIR)) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let mut kept = Vec::new();
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some((of, copy)) = file_name.to_str().and_then(kept_of) else {
            continue;
        };
        if of == name {
            kept.push(KeptCopy {
                path: entry.path(),
                copy: Some(copy).filter(|copy| *copy != 0),
            });
        }
    }
    kept.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(kept)
}

/// Whether the log's vote or last log id is below the shard's marks.
async fn judge_log(
    log: &DiskLog,
    marks: &MarkLog,
    shard: &str,
) -> io::Result<Option<RotateReason>> {
    let Some(mark) = marks.marks(shard) else {
        return Ok(Some(RotateReason::NoMarks));
    };
    let (generation, leader, committed) = log.accepted_vote();
    let vote = VoteMark {
        generation,
        leader,
        committed,
    };
    if !vote.at_or_above(&mark.vote) {
        return Ok(Some(RotateReason::VoteBelowMark));
    }
    if log_below(crate::raft_log::last_point(log).await?, mark.log) {
        return Ok(Some(RotateReason::LogBelowMark));
    }
    Ok(None)
}

fn open_log(dir: &Path, label: &str, config: LogConfig) -> io::Result<DiskLog> {
    crate::fs::create_dir_all(dir)?;
    DiskLog::open(dir, label.to_string(), config).map_err(io::Error::other)
}

/// Missing, or holding at most a copy file: what a crash before a new
/// copy's marks leaves. There is nothing in it to keep, and the new copy
/// file replaces the old.
fn holds_no_records(dir: &Path) -> io::Result<bool> {
    let entries = match crate::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(err) => return Err(err),
    };
    let temporary = state_file::temporary(dir, copy_file::FILE_NAME);
    for entry in entries {
        let path = entry?.path();
        if path != dir.join(copy_file::FILE_NAME) && path != temporary {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Missing, or holding nothing.
fn is_empty(dir: &Path) -> io::Result<bool> {
    match crate::fs::read_dir(dir) {
        Ok(mut entries) => Ok(entries.next().is_none()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(err) => Err(err),
    }
}

pub(super) fn parent_of(dir: &Path) -> io::Result<PathBuf> {
    dir.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} has no parent to keep old copies in", dir.display()),
            )
        })
}

/// `<parent>/.kept/<name>.<old copy id>`, with a number after it if that is
/// taken. The directory holding it is made durably before the rename.
fn kept_path(dir: &Path, parent: &Path, replaced: Option<u64>) -> io::Result<PathBuf> {
    let name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "a shard directory needs a name",
            )
        })?;
    let kept = parent.join(KEPT_DIR);
    crate::io::create_dir_all_durable(&kept)?;
    let copy = replaced.unwrap_or(0);
    let mut n = 0u32;
    loop {
        let candidate = kept.join(kept_name(name, copy, n));
        if !crate::fs::try_exists(&candidate)? {
            return Ok(candidate);
        }
        n += 1;
    }
}

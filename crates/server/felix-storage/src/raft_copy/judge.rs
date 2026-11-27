//! Opening a shard directory as a copy: keep its copy id, or rotate it.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::copy_file::{self, CopyFile};
use super::epoch_file::{self, EpochFile};
use super::gate::GatePassed;
use super::marks::{self, CopyMarks, MarkLog};
use super::rotated_by::{self, RotatedBy};
use super::{ShardMarks, VoteMark, log_below};
use crate::disk_log::dir_lock::DirLock;
use crate::disk_log::layout::{KEPT_DIR, kept_name, kept_of};
use crate::disk_log::{crash_point, engine_marker};
use crate::log::LogConfig;
use crate::raft_log::{held, reserved, state_file};
use crate::{DiskLog, StorageId};
use felix_common::random::HashMap;

/// Longer shard names do not fit a mark.
const MAX_SHARD_NAME: usize = 4096;

/// A data root that passed the gate, opening shard directories as copies.
pub struct CopyRoot {
    root: PathBuf,
    marks: MarkLog,
    /// The marks a relaxed crash's rotation set aside, with the storage id
    /// they were written under ([`marks::marks_before`]).
    before: Arc<MarksBefore>,
    mint: Arc<dyn Fn() -> u64 + Send + Sync>,
}

type MarksBefore = Option<(StorageId, HashMap<Arc<str>, ShardMarks>)>;

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
    /// A re-form stopped it in the epoch it is at, and a leader of that
    /// epoch was heard since ([`CopyRoot::rejoin`]).
    Stopped,
}

/// A shard directory an earlier rotation moved aside. Its records are
/// kept as plain records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptCopy {
    pub path: PathBuf,
    /// The copy id it held, if it had one.
    pub copy: Option<u64>,
}

/// What the files in a directory say about the copy it holds.
enum Judged {
    /// It may keep its copy id once its log, open here, is judged too.
    Keep(CopyFile, DiskLog),
    /// It rotates, under the lock it was judged under.
    Rotate(RotateReason, DirLock),
    /// It was minted under the storage id a relaxed crash rotated and meets
    /// the marks the root set aside for it. It rotates, and is kept as a
    /// power cut if its log, open here, still holds this vote.
    CheckVote(VoteMark, DiskLog),
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
        let before = Arc::new(marks::marks_before(passed.marks.root()));
        Self {
            root,
            marks: passed.marks,
            before,
            mint: Arc::new(mint),
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

    /// Whether the root's disk takes writes again, for a member that
    /// stopped on a storage error and is about to open again: a probe file
    /// written, fsynced and removed at the root, then the mark log reopened
    /// if it had failed ([`MarkLog::reopen`]). An error means the disk is
    /// still failing and nothing should open yet.
    pub async fn heal(&self) -> io::Result<()> {
        let root = self.marks.root().to_path_buf();
        felix_common::blocking::spawn(move || probe(&root))
            .await
            .map_err(io::Error::other)??;
        self.marks.reopen().await?;
        Ok(())
    }

    /// Open `dir` as `shard`'s copy, as a log labelled `label` under
    /// `config`, rotating the copy when the directory cannot keep its id.
    ///
    /// A rotated copy has a new copy id that never voted and an empty log,
    /// with fresh marks synced before this returns. The old directory, if it
    /// held more than a copy file, is moved into `.kept` beside `dir` (see
    /// [`kept_copies`]). A crash part way leaves the old directory, or one this rotates again.
    ///
    /// `dir` is locked before anything in it is judged, and the lock passes
    /// through any rotation to the open log. Another open of the copy, in
    /// this process or another, fails with [`io::ErrorKind::ResourceBusy`].
    ///
    /// Every file it touches is read, written and synced on the blocking
    /// pool, so a slow disk holds up only this open.
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
        // Held from the first check through any rotation into the open log,
        // so no other open can change what was judged.
        let lock = {
            let dir = dir.to_path_buf();
            off_runtime(move || lock_dir(&dir)).await?
        };
        self.open_copy_holding(shard, dir, label.into(), config, lock)
            .await
    }

    /// [`Self::open_copy`] under `lock`, the lock on `dir`, which passes to
    /// the open log.
    pub(super) async fn open_copy_holding(
        &self,
        shard: &str,
        dir: &Path,
        label: String,
        config: LogConfig,
        lock: DirLock,
    ) -> io::Result<OpenedCopy> {
        if shard.len() > MAX_SHARD_NAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("a shard name of {} bytes is too long", shard.len()),
            ));
        }
        let shard: Arc<str> = Arc::from(shard);
        let judged = {
            let (marks, before) = (self.marks.clone(), Arc::clone(&self.before));
            let (shard, dir) = (Arc::clone(&shard), dir.to_path_buf());
            let (label, config) = (label.clone(), config.clone());
            off_runtime(move || judge_dir(&marks, &before, &shard, &dir, &label, config, lock))
                .await?
        };
        let mut vote_kept = false;
        let (reason, lock) = match judged {
            Judged::Keep(found, log) => match judge_log(&log, &self.marks, &shard).await {
                Ok(None) => {
                    return Ok(OpenedCopy {
                        copy: found.copy,
                        marks: self.marks.for_copy(shard, found.copy),
                        log,
                        rotation: None,
                    });
                }
                Ok(Some(reason)) => {
                    let kept = log.close_keeping_lock().await;
                    let lock = kept.map_err(io::Error::other)?.ok_or_else(|| {
                        io::Error::other(format!("{}: closed while judged", dir.display()))
                    })?;
                    (reason, lock)
                }
                Err(err) => {
                    let _ = log.close().await;
                    return Err(err);
                }
            },
            Judged::Rotate(reason, lock) => (reason, lock),
            Judged::CheckVote(mark, log) => {
                let (generation, leader, committed) = log.accepted_vote();
                vote_kept = VoteMark {
                    generation,
                    leader,
                    committed,
                }
                .at_or_above(&mark);
                let kept = log.close_keeping_lock().await;
                let lock = kept.map_err(io::Error::other)?.ok_or_else(|| {
                    io::Error::other(format!("{}: closed while judged", dir.display()))
                })?;
                (RotateReason::StorageId, lock)
            }
        };
        self.rotate_and_open(shard, dir, label, config, reason, vote_kept, lock)
            .await
    }

    /// Open `dir`, which a re-form stopped in the epoch it is at, as a new
    /// copy of `shard`: a new copy id that never voted, an empty log, and
    /// the stopped directory kept in `.kept` as any rotation keeps it. Call
    /// it only once a leader is heard committing past its last entry, so
    /// the group went on without the copy; until then a re-run of the
    /// re-form may still need it as a voter. A directory that is not stopped is an error.
    pub async fn rejoin(
        &self,
        shard: &str,
        dir: &Path,
        label: impl Into<String>,
        config: LogConfig,
    ) -> io::Result<OpenedCopy> {
        let lock = {
            let dir = dir.to_path_buf();
            off_runtime(move || {
                // Before the check, so the copy rotated is the one found stopped.
                let lock = lock_dir(&dir)?;
                if super::start::building(&dir)? || super::stop_file::stopped(&dir)?.is_none() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "{}: the copy is not stopped, or a re-form start did not finish",
                            dir.display()
                        ),
                    ));
                }
                Ok(lock)
            })
            .await?
        };
        self.rotate_and_open(
            Arc::from(shard),
            dir,
            label.into(),
            config,
            RotateReason::Stopped,
            false,
            lock,
        )
        .await
    }

    /// `vote_kept` says the copy was judged against the marks a relaxed
    /// crash set aside and still holds its newest vote, so it is kept as a
    /// power cut whatever `reason` says.
    #[allow(clippy::too_many_arguments, reason = "each is one fact about the open")]
    async fn rotate_and_open(
        &self,
        shard: Arc<str>,
        dir: &Path,
        label: String,
        config: LogConfig,
        reason: RotateReason,
        vote_kept: bool,
        lock: DirLock,
    ) -> io::Result<OpenedCopy> {
        let (replaced, copy, kept, lock) = {
            let (shard, dir) = (Arc::clone(&shard), dir.to_path_buf());
            let (mint, storage_id) = (Arc::clone(&self.mint), self.storage_id());
            off_runtime(move || {
                let mut lock = lock;
                let replaced = copy_file::load(&dir).ok().flatten().map(|found| found.copy);
                let (copy, kept) = rotate(
                    &shard, &dir, replaced, reason, vote_kept, &mut lock, &*mint, storage_id,
                )?;
                Ok((replaced, copy, kept, lock))
            })
            .await?
        };
        // Fresh marks last: a crash before them leaves a directory whose
        // copy the marks do not name, which rotates again.
        self.marks.fresh(Arc::clone(&shard), copy).await?;
        tracing::info!(
            %shard,
            dir = %dir.display(),
            ?reason,
            replaced = ?replaced.map(|copy| format!("{copy:016x}")),
            copy = format!("{copy:016x}"),
            "rotated the shard's copy id"
        );
        let log = {
            let dir = dir.to_path_buf();
            off_runtime(move || open_log(&dir, &label, config, lock)).await?
        };
        Ok(OpenedCopy {
            copy,
            log,
            marks: self.marks.for_copy(shard, copy),
            rotation: Some(Rotation {
                reason,
                replaced,
                kept,
            }),
        })
    }
}

/// The highest reservation among the copy in `dir` (its own
/// `raft-reserved` counts, not only the kept copies') and every copy
/// rotations moved aside from it, or `None` when none took one. A re-form
/// starts its epoch above it. A reservation that does not decode is an
/// error, never skipped.
pub fn highest_reserved(dir: &Path) -> io::Result<Option<u64>> {
    let mut highest = reserved::load(dir)?;
    for kept in kept_copies(dir)? {
        highest = highest.max(reserved::load(&kept.path)?);
    }
    Ok(highest)
}

/// The newest `raft-epoch` among the copy in `dir` and every copy
/// rotations moved aside from it: a crash after the move leaves it only in
/// `.kept`. A file that does not decode is an error, never skipped, so a
/// new copy never reads as an older epoch than the one it replaces.
fn carried_epoch(dir: &Path) -> io::Result<Option<EpochFile>> {
    let mut newest = epoch_file::load(dir)?;
    for kept in kept_copies(dir)? {
        if let Some(file) = epoch_file::load(&kept.path)?
            && newest.as_ref().is_none_or(|n| file.epoch() > n.epoch())
        {
            newest = Some(file);
        }
    }
    Ok(newest)
}

/// Whether a group reached an earlier copy of `dir`: one rotations moved
/// aside holds a log entry or a reservation, or a rotation carried a
/// reservation into `dir` and it holds no entry yet. A group formed on the rotated
/// copies alone would start at index 0 and give out offsets the old one
/// acknowledged. A kept copy that cannot be read is an error.
pub fn reached_before(dir: &Path, index_spacing_bytes: u64) -> io::Result<bool> {
    for kept in kept_copies(dir)? {
        let label = kept.path.display().to_string();
        if reserved::load(&kept.path)?.is_some()
            || held::read(&kept.path, &label, index_spacing_bytes)?
                .last
                .is_some()
        {
            return Ok(true);
        }
    }
    // Every copy reserves before its first write, so its own reservation
    // says nothing about an earlier copy, even with no entry after a crash.
    // Entries in a carried copy came from a group it is then a member of.
    if !reserved::carried(dir)? || reserved::load(dir)?.is_none() {
        return Ok(false);
    }
    let label = dir.display().to_string();
    Ok(held::read(dir, &label, index_spacing_bytes)?.last.is_none())
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

/// Judge `dir`, locked by `lock`, by its files, and open its log when they
/// let it keep its copy id.
fn judge_dir(
    marks: &MarkLog,
    before: &MarksBefore,
    shard: &str,
    dir: &Path,
    label: &str,
    config: LogConfig,
    lock: DirLock,
) -> io::Result<Judged> {
    // A re-form start that did not finish has to be run again first.
    if super::start::building(dir)? {
        return Err(io::Error::other(format!(
            "{}: a re-form start did not finish; run it again before opening",
            dir.display()
        )));
    }
    super::start::adopt_pending(dir)?;
    match judge_files(marks, shard, dir)? {
        // Every other reason rotates it anyway, which is the rejoin.
        (Some(_), None) if let Some(stopped) = super::stop_file::stopped(dir)? => {
            Err(io::Error::other(stopped))
        }
        (Some(found), None) => Ok(Judged::Keep(found, open_log(dir, label, config, lock)?)),
        // Before the catch-all below, which would rotate it as a restore.
        (Some(found), Some(RotateReason::StorageId))
            if let Some(mark) = mark_before(before, shard, &found)
                && super::stop_file::stopped(dir)?.is_none()
                && !crate::disk_log::left_poisoned(dir)? =>
        {
            let vote = mark.vote.clone();
            Ok(Judged::CheckVote(vote, open_log(dir, label, config, lock)?))
        }
        (_, Some(reason)) => Ok(Judged::Rotate(reason, lock)),
        (None, None) => unreachable!("a directory with no copy file has a reason"),
    }
}

/// The marks set aside for `found`'s copy of `shard`, when a relaxed
/// crash's rotation kept them for the storage id it was minted under.
fn mark_before<'a>(
    before: &'a MarksBefore,
    shard: &str,
    found: &CopyFile,
) -> Option<&'a ShardMarks> {
    let (id, marks) = before.as_ref()?;
    let mark = marks.get(shard)?;
    (*id == found.minted && mark.copy == found.copy).then_some(mark)
}

/// The checks that need no log: the copy file and the marks.
fn judge_files(
    marks: &MarkLog,
    shard: &str,
    dir: &Path,
) -> io::Result<(Option<CopyFile>, Option<RotateReason>)> {
    if is_empty(dir)? {
        return Ok((None, Some(RotateReason::Empty)));
    }
    let Some(found) = copy_file::load(dir)? else {
        return Ok((None, Some(RotateReason::NoCopyFile)));
    };
    if found.minted != marks.storage_id() {
        return Ok((Some(found), Some(RotateReason::StorageId)));
    }
    let reason = match marks.marks(shard) {
        None => Some(RotateReason::NoMarks),
        Some(marks) if marks.copy != found.copy => Some(RotateReason::OtherCopy),
        // Before any open of the log, which would cut it back to its
        // last sync. A marker that cannot be read counts the same.
        Some(_) if crate::disk_log::left_poisoned(dir)? => Some(RotateReason::Poisoned),
        Some(_) => None,
    };
    Ok((Some(found), reason))
}

/// Move the old directory aside and make a new one with a copy id from
/// `mint`, for the caller to write fresh marks for. Each step is synced
/// before the next.
#[allow(
    clippy::too_many_arguments,
    reason = "each is one fact about the rotation"
)]
fn rotate(
    shard: &Arc<str>,
    dir: &Path,
    replaced: Option<u64>,
    reason: RotateReason,
    vote_kept: bool,
    lock: &mut DirLock,
    mint: &dyn Fn() -> u64,
    storage_id: StorageId,
) -> io::Result<(u64, Option<PathBuf>)> {
    let parent = parent_of(dir)?;
    let step = || crash_point::reached(&parent);
    step()?;
    // Read before anything moves. A crash after the move leaves the
    // old reservation in `.kept`, which the next rotation reads again.
    let carried = highest_reserved(dir)?;
    // The engine marker goes into `.kept` with the directory, and the
    // new directory is as much a Raft member's as the old one.
    let marked = engine_marker::claimed_for_raft(dir, shard).map_err(io::Error::other)?;
    // The new copy is a member of the same group epoch, and votes in it
    // are keyed by the epoch it reads.
    let epoch = carried_epoch(dir)?;
    // Minted first, so the kept copy can name the copy it speaks for.
    let copy = loop {
        let copy = mint();
        if copy != 0 && Some(copy) != replaced {
            break copy;
        }
    };
    let kept = if holds_no_records(dir)? {
        None
    } else {
        let kept = kept_path(dir, &parent, replaced)?;
        crate::fs::rename(dir, &kept)?;
        step()?;
        // Locked at once, and the old one held until it is finished
        // with. Another open that takes the new, empty directory first
        // makes this fail before anything is written to it, which leaves
        // what a crash at the step above leaves.
        crate::fs::create_dir_all(dir)?;
        let _old = lock.follow(dir)?;
        step()?;
        crate::io::sync_dir(&parent.join(KEPT_DIR))?;
        crate::io::sync_dir(&parent)?;
        // After the rename, so a crash leaves a kept copy without it,
        // which reads as a restore, never a live directory with one.
        // A crash before copy_file::store below leaves this naming a copy id
        // that was never written. The retry mints a new id, so a real power cut
        // reads as a restore and only asks for --accept-loss.
        let by = if vote_kept {
            RotatedBy::PowerCut { to: copy }
        } else {
            RotatedBy::of(reason, copy)
        };
        rotated_by::store(&kept, by)?;
        step()?;
        crate::incarnation::kept_gauges::refresh(&parent);
        Some(kept)
    };
    step()?;
    crate::fs::create_dir_all(dir)?;
    step()?;
    crate::io::sync_dir(&parent)?;
    // The poisoned log is in `.kept` or gone; the new copy starts clean.
    crate::disk_log::forget_poisoned_dir(dir);
    if marked && kept.is_some() {
        engine_marker::claim_for_raft_holding(dir, shard, lock).map_err(io::Error::other)?;
    }
    // Before the copy file, so the new copy never votes without it.
    if let Some(epoch) = &epoch {
        epoch_file::store(dir, epoch)?;
        step()?;
    }
    // Synced before the copy file, so a directory with a copy file
    // always carries the reservations it replaced.
    if let Some(index) = carried {
        reserved::mark_carried(dir)?;
        step()?;
        reserved::store(dir, index)?;
        step()?;
    }
    copy_file::store(
        dir,
        &CopyFile {
            copy,
            minted: storage_id,
        },
    )?;
    step()?;
    Ok((copy, kept))
}

fn open_log(dir: &Path, label: &str, config: LogConfig, lock: DirLock) -> io::Result<DiskLog> {
    DiskLog::open_holding(dir, label.to_string(), config, None, lock).map_err(io::Error::other)
}

/// Run `work` on the blocking pool. Opening a copy creates, writes and
/// syncs files and directories, and on a runtime worker a slow disk stalls
/// every task queued behind it.
async fn off_runtime<R: Send + 'static>(
    work: impl FnOnce() -> io::Result<R> + Send + 'static,
) -> io::Result<R> {
    match felix_common::blocking::spawn(work).await {
        Ok(done) => done,
        Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
        Err(err) => Err(io::Error::other(err)),
    }
}

/// Lock `dir`, creating it if it is not there.
pub(super) fn lock_dir(dir: &Path) -> io::Result<DirLock> {
    crate::io::create_dir_all_durable(dir)?;
    DirLock::take(dir)
}

/// Missing, or holding at most a copy file and a reservation: what a crash
/// before a new copy's marks leaves. There is nothing in it to keep: the new
/// copy file replaces the old, and the rotation carries the reservation.
fn holds_no_records(dir: &Path) -> io::Result<bool> {
    let entries = match crate::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(err) => return Err(err),
    };
    let temporary = state_file::temporary(dir, copy_file::FILE_NAME);
    // The epoch file alone is what a crash after a rotation wrote it and
    // before the copy file leaves; the next rotation carries it again.
    let epoch = dir.join(epoch_file::FILE_NAME);
    let epoch_temporary = state_file::temporary(dir, epoch_file::FILE_NAME);
    for entry in entries {
        let path = entry?.path();
        if path != dir.join(copy_file::FILE_NAME)
            && path != temporary
            && path != epoch
            && path != epoch_temporary
            && !reserved::is_reservation(dir, &path)
            && !engine_marker::is_marker(dir, &path)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Missing, or holding nothing but the engine marker a broker writes
/// before a member first opens.
fn is_empty(dir: &Path) -> io::Result<bool> {
    let entries = match crate::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(err) => return Err(err),
    };
    for entry in entries {
        if !engine_marker::is_marker(dir, &entry?.path()) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether `dir` holds a log and no copy file. Only a member opened with no
/// copy id leaves one: a copy file is written before a copy's first
/// segment. [`CopyRoot::open_copy`] would rotate it as it does any
/// directory without a copy file, so a caller whose group's members may
/// all be such directories has to ask this first.
pub fn holds_log_without_copy(dir: &Path) -> io::Result<bool> {
    if copy_file::load(dir)?.is_some() {
        return Ok(false);
    }
    engine_marker::holds_segments(dir).map_err(io::Error::other)
}

/// A directory directly under `root` that holds a log and no copy file: the
/// hybrid engine's data, which no Raft member may open. `None` for a root
/// with none, or that does not exist yet. Directories starting with `.`
/// (kept copies) are not shard directories.
pub fn hybrid_log_under(root: &Path) -> io::Result<Option<PathBuf>> {
    let entries = match crate::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let mut found: Option<PathBuf> = None;
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() || entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let dir = entry.path();
        // The smallest path, so the error names the same directory each run.
        if holds_log_without_copy(&dir)? && found.as_ref().is_none_or(|first| dir < *first) {
            found = Some(dir);
        }
    }
    Ok(found)
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

/// The file [`CopyRoot::heal`] probes the disk with.
const PROBE: &str = ".disk-probe";

fn probe(root: &Path) -> io::Result<()> {
    let path = root.join(PROBE);
    {
        let file = crate::fs::File::create(&path)?;
        crate::io::write_all(&file, b"probe")?;
        crate::io::sync_all(&file)?;
    }
    crate::fs::remove_file(&path)?;
    crate::io::sync_dir(root)
}

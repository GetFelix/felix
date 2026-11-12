//! Bringing a shard's segments back after an unclean shutdown.
//!
//! The contract recovery upholds:
//!
//! * **A torn tail is repaired.** A crash mid-append leaves a partial record at
//!   the end of the last segment. That record was never acknowledged under any
//!   fsync policy, so it is truncated away and the log resumes at the last intact
//!   record.
//! * **Committed data is never silently discarded.** Corruption anywhere that is
//!   not the very end of the newest segment (or of a retired segment whose seal
//!   never finished, see `unsealed_retired`) is an error at startup, naming the
//!   shard, segment and byte position. Losing acknowledged records quietly is
//!   worse than refusing to start.
//! * **Recovery is idempotent.** Opening an already recovered log changes
//!   nothing, so a crash *during* recovery is safe.
//! * **Indexes are derived, never trusted.** A missing, short or mismatched index
//!   is rebuilt from the segment it describes.
//!
//! ## What is validated, and what it costs
//!
//! Fully checksumming every segment at startup is O(bytes on disk) — minutes for
//! a large shard, which is the difference between a rolling restart and an
//! outage. So by default:
//!
//! * The **active** segment is always scanned in full. It is the only one that
//!   can have a torn tail, and it is bounded by `segment_size_bytes`.
//! * **Sealed** segments get their header validated and the records after
//!   their index's last entry checked — bounded by one index interval. Only
//!   that last entry is read here; the index itself is loaded by the first
//!   read that needs it, so an open holds no index in memory. Everything else
//!   is verified lazily, because every read verifies the checksum of every
//!   record it returns.
//!
//! Set `LogConfig::verify_all_on_open` to trade startup time for eager detection
//! of bit rot in cold data.

use std::path::Path;

use super::durable_mark;
use super::now_micros;
use super::offload::Manifest;
use super::segments::SealedEntry;
use crate::io::{create_dir_all_durable, sync_dir};
use crate::log::{LogConfig, Offset, RecordMark, SegmentId};
use crate::segment::known::known_file_name;
use crate::segment::writer::ResumeState;
use crate::segment::{SegmentWriter, index_file_name, parse_segment_file_name, segment_file_name};
use crate::{Result, StorageError, metrics_names};

pub(crate) use self::plan::{
    End, IndexRebuild, RecoveryPlan, Removal, Resume, Step, View, plan_recovery,
};

use self::durable_mark::DurableMark;
// Only the tests reach these through `super::*`.
#[cfg(test)]
use crate::CorruptionKind;
#[cfg(test)]
use crate::segment::{ScanStart, SparseIndex, read_segment_header, scan_segment};

mod plan;

/// The outcome of recovering one shard directory.
#[derive(Debug)]
pub(super) struct Recovered {
    pub sealed: Vec<SealedEntry>,
    pub active: SegmentWriter,
    /// Bytes discarded from a torn tail, for logging and metrics.
    pub truncated_bytes: u64,
    /// Indexes that had to be rebuilt.
    pub index_rebuilds: usize,
    /// Producer marks in the active segment, from its full scan.
    pub active_marks: Vec<(Offset, RecordMark, u64)>,
}

/// Open, validate and repair every segment for one shard: plan what to do,
/// then do it. `felix_storage::inspect` runs the same plan and stops there.
pub(super) fn recover_shard(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    offloaded: &Manifest,
) -> Result<Recovered> {
    let started = std::time::Instant::now();
    let plan = plan_recovery(dir, label, config, offloaded)?;
    let abandoned = plan.abandoned;
    let recovered = apply(dir, label, config, plan)?;

    metrics::histogram!(metrics_names::RECOVERY_DURATION_SECONDS)
        .record(started.elapsed().as_secs_f64());
    if recovered.truncated_bytes > 0 {
        metrics::counter!(metrics_names::RECOVERY_TRUNCATED_BYTES)
            .increment(recovered.truncated_bytes);
    }
    if abandoned > 0 {
        metrics::counter!(metrics_names::RECOVERY_ABANDONED_ROLLS_TOTAL)
            .increment(abandoned as u64);
    }
    if recovered.index_rebuilds > 0 {
        metrics::counter!(metrics_names::RECOVERY_INDEX_REBUILDS_TOTAL)
            .increment(recovered.index_rebuilds as u64);
    }
    Ok(recovered)
}

/// Make the writes `plan` lists, in order, and open the log where it says.
///
/// A plan that refuses still has its steps made first, as startup always has:
/// they are the repairs decided before the damage was found.
fn apply(dir: &Path, label: &str, config: &LogConfig, plan: RecoveryPlan) -> Result<Recovered> {
    // The directory entry itself must be durable, or a crash could lose a shard
    // that already reported successful writes.
    create_dir_all_durable(dir)?;
    for step in plan.steps {
        apply_step(dir, label, step)?;
    }
    match plan.end {
        End::Refuse(detail) => Err(StorageError::Corruption(detail)),
        End::Fresh => Ok(Recovered {
            sealed: Vec::new(),
            // `DiskLog::open_shared` syncs the directory before it returns.
            active: SegmentWriter::create_in_opening_log(
                dir,
                0,
                now_micros(),
                (config.reserve_limit_bytes(), config.index_spacing_bytes),
                config.first_segment_version(),
            )?,
            truncated_bytes: 0,
            index_rebuilds: 0,
            active_marks: Vec::new(),
        }),
        End::Resume(resume) => resume_active(dir, label, config, resume),
    }
}

fn apply_step(dir: &Path, label: &str, step: Step) -> Result<()> {
    match step {
        Step::Remove { id, why } => {
            match why {
                Removal::LostRollRace => tracing::warn!(
                    shard = label,
                    segment = id,
                    "discarding an empty segment left behind by a rollover that lost its race"
                ),
                Removal::HeaderlessPreparation => tracing::warn!(
                    shard = label,
                    segment = id,
                    "discarding a headerless segment left behind by an uninstalled rollover"
                ),
                Removal::EmptyPreparation {
                    base_offset,
                    log_tail,
                } => tracing::warn!(
                    shard = label,
                    segment = id,
                    base_offset,
                    log_tail,
                    "discarding an empty segment left behind by an uninstalled rollover"
                ),
                Removal::BlankFirst => tracing::warn!(
                    shard = label,
                    "discarding a first segment whose creation never finished"
                ),
                Removal::BelowOffloadedGap => tracing::warn!(
                    shard = label,
                    segment = id,
                    "removing a segment below a gap the offload manifest covers"
                ),
            }
            crate::fs::note_torn_tail(&dir.join(segment_file_name(id)));
            remove_segment_files(dir, id)
        }
        Step::SyncDir => Ok(sync_dir(dir)?),
        Step::RebuildIndex {
            id,
            index,
            known,
            why,
        } => {
            // Derived and checked on every open, so a failed write costs
            // only another walk.
            if let Some((known, segment_len)) = known
                && let Err(err) = known.persist(
                    &dir.join(known_file_name(id)),
                    index.base_offset(),
                    segment_len,
                )
            {
                tracing::warn!(shard = label, segment = id, error = %err, "could not write a sealed segment's known generations");
            }
            if why == IndexRebuild::Mismatch {
                tracing::warn!(
                    shard = label,
                    segment = id,
                    "sealed segment index does not match its segment; rebuilding it",
                );
            }
            index.persist(&dir.join(index_file_name(id)))
        }
        Step::CutRetired {
            id,
            valid_bytes,
            drop_next,
        } => {
            tracing::warn!(
                shard = label,
                segment = id,
                valid_bytes,
                kept_next_segment = drop_next.is_none(),
                "repairing the torn tail of a segment whose seal never finished",
            );
            crate::fs::note_torn_tail(&dir.join(segment_file_name(id)));
            // The segment may be appended to again and sealed at the same
            // length, so its old file must not survive to describe it.
            match crate::fs::remove_file(dir.join(known_file_name(id))) {
                Ok(()) => sync_dir(dir)?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
            if let Some(next) = drop_next {
                crate::fs::note_torn_tail(&dir.join(segment_file_name(next)));
                remove_segment_files(dir, next)?;
                sync_dir(dir)?;
            }
            let file = crate::fs::OpenOptions::new()
                .write(true)
                .open(dir.join(segment_file_name(id)))?;
            file.set_len(valid_bytes)?;
            crate::io::sync_data(&file)?;
            Ok(())
        }
    }
}

fn resume_active(dir: &Path, label: &str, config: &LogConfig, resume: Resume) -> Result<Recovered> {
    let Resume {
        sealed,
        active_id,
        active: mut outcome,
        sealed_index_rebuilds,
        generation_floor,
    } = resume;
    let truncated_bytes = outcome
        .torn_tail
        .as_ref()
        .map(|tail| tail.discarded_bytes)
        .unwrap_or(0);
    if let Some(tail) = &outcome.torn_tail {
        tracing::warn!(
            shard = label,
            segment = active_id,
            position = tail.position,
            discarded_bytes = tail.discarded_bytes,
            cause = %tail.cause,
            "repaired a torn tail in the active segment"
        );
        crate::fs::note_torn_tail(&dir.join(segment_file_name(active_id)));
    }

    let active_marks = std::mem::take(&mut outcome.marks);
    // `reopen` applies the truncation and positions the write cursor, which is
    // what makes recovery idempotent: a second open finds nothing to repair.
    let active = SegmentWriter::reopen(
        dir,
        active_id,
        ResumeState {
            base_offset: outcome.header.base_offset,
            valid_bytes: outcome.valid_bytes,
            next_offset: outcome.next_offset,
            record_count: outcome.record_count,
            index: outcome.index,
            version: outcome.header.version,
            generation_floor,
            known: Some(outcome.known),
        },
        config.reserve_limit_bytes(),
        config.index_spacing_bytes,
    )?;

    Ok(Recovered {
        sealed: sealed
            .into_iter()
            .map(|planned| {
                SealedEntry::new(planned.descriptor, planned.holds_marks, planned.holds_trims)
                    .with_last_generation(planned.last_generation)
            })
            .collect(),
        active,
        truncated_bytes,
        // The active segment's index was just rewritten from the scan, so it
        // always counts as a rebuild.
        index_rebuilds: sealed_index_rebuilds + 1,
        active_marks,
    })
}

/// Create a shard's first segment so its log begins at `base_offset`.
///
/// A no-op when the directory already holds segments: a shard that is already
/// here keeps the base recorded in its own first segment, and a restart must
/// not reinterpret it. Only an empty directory is a shard being placed.
///
/// The segment carries `base_offset` in its header, so recovery reads it back
/// without needing to be told again.
pub(super) fn place_empty_shard(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    base_offset: Offset,
) -> Result<bool> {
    create_dir_all_durable(dir)?;
    let mut ids = discover_segment_ids(dir)?;
    let mut view = View::new(dir, label, config, durable_mark::load(dir));
    view.discard_blank_first(&mut ids)?;
    for step in view.steps {
        apply_step(dir, label, step)?;
    }
    if !ids.is_empty() {
        return Ok(false);
    }
    let mut writer = SegmentWriter::create_in_opening_log(
        dir,
        base_offset,
        now_micros(),
        (config.reserve_limit_bytes(), config.index_spacing_bytes),
        config.first_segment_version(),
    )?;
    // The header is flushed before anything can append to it, and its name by
    // `DiskLog::open_shared`'s directory sync: a base offset that did not
    // survive a crash would leave the shard reading back as one starting at
    // zero, which is a hole rather than a shorter log.
    writer.sync()?;
    Ok(true)
}

/// Segment ids present in `dir`, in ascending numeric order.
///
/// Directory iteration order is filesystem-defined and must never be relied on:
/// on some filesystems it is hash order, which would interleave segments and
/// make the log look shuffled.
pub(super) fn discover_segment_ids(dir: &Path) -> Result<Vec<SegmentId>> {
    let mut ids = Vec::new();
    for entry in crate::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(id) = parse_segment_file_name(name) {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    // Two files cannot share an id, but a corrupt listing should not produce a
    // duplicate that later code treats as two segments.
    ids.dedup();
    Ok(ids)
}

/// Start after a log was poisoned in the boot still running. The bytes of
/// the flush that failed can sit in the page cache looking valid, and a sync
/// of them would succeed without writing anything.
///
/// Under `OnCommit` nothing past the last sync was acknowledged, so the log
/// is cut back to the mark the poison recorded, as a power loss would have
/// left it. A relaxed run acknowledged those records, so they are kept and
/// written again for real. The mode is the poisoned run's, from the marker.
/// The small state files are written again too. The marker goes last, so a
/// crash part way repeats all of this.
pub(super) fn after_poison(
    dir: &Path,
    label: &str,
    marker: Option<durable_mark::Poison>,
) -> Result<()> {
    match marker {
        Some(poison) if poison.on_commit => cut_to_mark(dir, label, poison.mark)?,
        Some(poison) => rewrite_past_mark(dir, label, poison.mark)?,
        None => tracing::warn!(
            shard = label,
            "the last run poisoned this log but its marker cannot be read; the log is kept as \
             it is, and bytes the failed flush wrote may not be on the device"
        ),
    }
    for name in STATE_FILES {
        let bytes = match crate::fs::read(dir.join(name)) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        crate::run_state::write_atomically(dir, name, &bytes, true)?;
    }
    match crate::fs::remove_file(dir.join(durable_mark::poison_file_name())) {
        Ok(()) => Ok(sync_dir(dir)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// A step of the cut a test can stop at, as a crash would.
#[cfg(test)]
fn crash_point() -> std::io::Result<()> {
    CUT_STEPS_LEFT.with(|left| match left.get() {
        Some(0) => Err(std::io::Error::other("injected crash")),
        Some(n) => {
            left.set(Some(n - 1));
            Ok(())
        }
        None => Ok(()),
    })
}

#[cfg(not(test))]
#[inline]
fn crash_point() -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
thread_local! {
    /// How many steps of the cut to take before [`crash_point`] fails.
    pub(super) static CUT_STEPS_LEFT: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    /// How many bytes [`rewrite_from`] has written on this thread.
    pub(super) static REWRITTEN_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Files whose last write may have failed, each read back from the page
/// cache as if it were durable.
const STATE_FILES: [&str; 4] = ["ballot", "replica", "replica-set", "epochs"];

/// Drop everything past `mark`: the segments after its own, and its own
/// bytes past `synced_bytes`. A mark naming a segment that is gone, or a
/// segment already that short, leaves the log as it is.
fn cut_to_mark(dir: &Path, label: &str, mark: DurableMark) -> Result<()> {
    let ids = discover_segment_ids(dir)?;
    if !ids.contains(&mark.segment) {
        return Ok(());
    }
    let path = dir.join(segment_file_name(mark.segment));
    let len = crate::fs::metadata(&path)?.len();
    let newer: Vec<SegmentId> = ids.into_iter().filter(|id| *id > mark.segment).collect();
    if newer.is_empty() && len <= mark.synced_bytes {
        return Ok(());
    }
    tracing::warn!(
        shard = label,
        segment = mark.segment,
        synced_bytes = mark.synced_bytes,
        later_segments = newer.len(),
        "the last run poisoned this log; cutting it back to its last synced point"
    );
    // Newest first, so a crash part way leaves a longer log and no gap.
    for id in newer.into_iter().rev() {
        crate::fs::note_torn_tail(&dir.join(segment_file_name(id)));
        remove_segment_files(dir, id)?;
        sync_dir(dir)?;
        crash_point()?;
    }
    if len > mark.synced_bytes {
        crate::fs::note_torn_tail(&path);
        // Cut and possibly appended to again, so these must not outlive it.
        for stale in [known_file_name(mark.segment), index_file_name(mark.segment)] {
            match crate::fs::remove_file(dir.join(stale)) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }
        crash_point()?;
        let file = crate::fs::OpenOptions::new().write(true).open(&path)?;
        file.set_len(mark.synced_bytes)?;
        crate::io::sync_data(&file)?;
        sync_dir(dir)?;
        crash_point()?;
    }
    // A flush in flight at the poison can have moved the mark past the
    // marker's. Left there, it would make damage past the cut look like rot.
    crate::run_state::write_atomically(dir, durable_mark::mark_file_name(), &mark.encode(), true)?;
    Ok(())
}

/// Write the bytes past `mark` again and sync them: the segment the mark is
/// in from its synced point, and every later segment whole. Writing the
/// same bytes makes their pages dirty again, so the sync really writes them.
fn rewrite_past_mark(dir: &Path, label: &str, mark: DurableMark) -> Result<()> {
    let mut rewritten = 0;
    for id in discover_segment_ids(dir)? {
        if id < mark.segment {
            continue;
        }
        let from = if id == mark.segment {
            mark.synced_bytes
        } else {
            0
        };
        rewritten += rewrite_from(&dir.join(segment_file_name(id)), from)?;
    }
    if rewritten > 0 {
        tracing::warn!(
            shard = label,
            segment = mark.segment,
            synced_bytes = mark.synced_bytes,
            rewritten_bytes = rewritten,
            "the last run poisoned this log; writing the bytes past its last synced point again"
        );
    }
    Ok(())
}

/// Read back and write again the bytes of `path` from `from` on, then sync
/// them. Returns how many bytes it wrote.
fn rewrite_from(path: &Path, from: u64) -> Result<u64> {
    let file = crate::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let len = file.metadata()?.len();
    let mut at = from;
    let mut buf = vec![0u8; 1 << 20];
    while at < len {
        let want = usize::try_from(len - at).map_or(buf.len(), |left| left.min(buf.len()));
        let read = crate::io::read_at(&file, &mut buf[..want], at)?;
        if read == 0 {
            break;
        }
        crate::io::write_at(&file, &buf[..read], at)?;
        at += read as u64;
    }
    if at > from {
        crate::io::sync_data(&file)?;
    }
    #[cfg(test)]
    REWRITTEN_BYTES.with(|total| total.set(total.get() + (at - from.min(at))));
    Ok(at.saturating_sub(from))
}

/// Unlink a segment and its index. The caller syncs the directory.
fn remove_segment_files(dir: &Path, id: SegmentId) -> Result<()> {
    for path in [
        dir.join(segment_file_name(id)),
        dir.join(index_file_name(id)),
        dir.join(known_file_name(id)),
    ] {
        match crate::fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(StorageError::Io(err)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

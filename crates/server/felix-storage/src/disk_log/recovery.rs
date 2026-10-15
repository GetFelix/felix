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
use crate::segment::writer::ResumeState;
use crate::segment::{SegmentWriter, index_file_name, parse_segment_file_name, segment_file_name};
use crate::{Result, StorageError, metrics_names};

pub(crate) use self::plan::{
    End, IndexRebuild, RecoveryPlan, Removal, Resume, Step, View, plan_recovery,
};

// Only the tests reach these through `super::*`.
#[cfg(test)]
use self::durable_mark::DurableMark;
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
            active: SegmentWriter::create(
                dir,
                0,
                0,
                now_micros(),
                config.reserve_limit_bytes(),
                config.index_spacing_bytes,
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
            remove_segment_files(dir, id)
        }
        Step::SyncDir => Ok(sync_dir(dir)?),
        Step::RebuildIndex { id, index, why } => {
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
            if let Some(next) = drop_next {
                remove_segment_files(dir, next)?;
                sync_dir(dir)?;
            }
            let file = std::fs::OpenOptions::new()
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
        },
        config.reserve_limit_bytes(),
        config.index_spacing_bytes,
    )?;

    Ok(Recovered {
        sealed: sealed
            .into_iter()
            .map(|planned| SealedEntry::new(planned.descriptor, planned.holds_marks))
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
    let mut writer = SegmentWriter::create(
        dir,
        0,
        base_offset,
        now_micros(),
        config.reserve_limit_bytes(),
        config.index_spacing_bytes,
    )?;
    // Flushed before anything can append to it: a base offset that did not
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
    for entry in std::fs::read_dir(dir)? {
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

/// Unlink a segment and its index. The caller syncs the directory.
fn remove_segment_files(dir: &Path, id: SegmentId) -> Result<()> {
    for path in [
        dir.join(segment_file_name(id)),
        dir.join(index_file_name(id)),
    ] {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(StorageError::Io(err)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

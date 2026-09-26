//! Bringing a shard's segments back after an unclean shutdown.
//!
//! The contract recovery upholds:
//!
//! * **A torn tail is repaired.** A crash mid-append leaves a partial record at
//!   the end of the last segment. That record was never acknowledged under any
//!   fsync policy, so it is truncated away and the log resumes at the last intact
//!   record.
//! * **Committed data is never silently discarded.** Corruption anywhere that is
//!   not the very end of the newest segment is an error at startup, naming the
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
//! * **Sealed** segments get their header validated, their index loaded, and the
//!   records after the last index entry checked — bounded by one index interval.
//!   Everything else is verified lazily, because every read verifies the checksum
//!   of every record it returns.
//!
//! Set `LogConfig::verify_all_on_open` to trade startup time for eager detection
//! of bit rot in cold data.

use std::path::Path;

use super::now_micros;
use super::segments::SealedEntry;
use crate::io::sync_dir;
use crate::log::{LogConfig, Offset, RecordMark, SegmentDescriptor, SegmentId};
use crate::segment::format::SEGMENT_HEADER_LEN;
use crate::segment::writer::ResumeState;
use crate::segment::{
    ScanStart, SegmentReader, SegmentWriter, SparseIndex, index_file_name, parse_segment_file_name,
    read_segment_header, scan_segment, segment_file_name,
};
use crate::{Corruption, CorruptionKind, Result, StorageError, metrics_names};

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
    pub active_marks: Vec<(Offset, RecordMark)>,
}

struct OpenedSealed {
    entry: SealedEntry,
    rebuilt_index: bool,
}

/// Open, validate and repair every segment for one shard.
pub(super) fn recover_shard(dir: &Path, label: &str, config: &LogConfig) -> Result<Recovered> {
    let started = std::time::Instant::now();
    std::fs::create_dir_all(dir)?;
    // The directory entry itself must be durable, or a crash could lose a shard
    // that already reported successful writes.
    if let Some(parent) = dir.parent() {
        sync_dir(parent)?;
    }

    let mut ids = discover_segment_ids(dir)?;
    // Rollover prepares the replacement segment ahead of the swap that installs
    // it, so a crash — or a truncation — can leave one behind that was never
    // used. Drop them before recovery proper, or their base offset reads as a
    // break in the offset chain. See `discard_abandoned_preparations`.
    let abandoned = discard_abandoned_preparations(dir, label, config, &mut ids)?;
    let recovered = match ids.split_last() {
        None => Recovered {
            sealed: Vec::new(),
            active: SegmentWriter::create(
                dir,
                0,
                0,
                now_micros(),
                config.preallocate_bytes(),
                config.index_spacing_bytes,
            )?,
            truncated_bytes: 0,
            index_rebuilds: 0,
            active_marks: Vec::new(),
        },
        Some((active_id, sealed_ids)) => {
            recover_existing(dir, label, config, sealed_ids, *active_id)?
        }
    };

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
    config: &LogConfig,
    base_offset: Offset,
) -> Result<bool> {
    std::fs::create_dir_all(dir)?;
    if let Some(parent) = dir.parent() {
        sync_dir(parent)?;
    }
    if !discover_segment_ids(dir)?.is_empty() {
        return Ok(false);
    }
    let mut writer = SegmentWriter::create(
        dir,
        0,
        base_offset,
        now_micros(),
        config.preallocate_bytes(),
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

/// Remove trailing segments that a rollover created but never installed.
///
/// A background rollover builds its replacement segment — file, header,
/// directory entry, all fsynced — before taking the lock that swaps it in, so
/// that the flushes never land on an append. The consequence is a window in
/// which a replacement exists on disk but is not yet part of the log, and two
/// things can end that window without installing it: a crash, or a truncation
/// that rewinds the tail past the offset the replacement was built for.
///
/// Such a segment is recognisable without any durable roll-intent record,
/// because it is self-describing: it is the newest segment, it holds **zero
/// records**, and its base offset is not where the log actually ends. A segment
/// that holds no records has nothing to lose, so deleting it cannot discard an
/// acknowledged write — which is what makes this rule safe to apply blindly.
///
/// Both directions occur and both are handled:
///
/// * base offset *below* the tail — appends kept landing in the old segment
///   after the replacement was built, which is the normal design of the
///   background roll;
/// * base offset *above* the tail — a truncation rewound the log underneath a
///   replacement that had already been built.
///
/// An empty newest segment whose base offset *does* match the tail is the
/// ordinary state right after a successful roll, and is kept.
fn discard_abandoned_preparations(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    ids: &mut Vec<SegmentId>,
) -> Result<usize> {
    let mut discarded = 0;
    // More than one can accumulate: each crash during a roll can leave its own.
    while ids.len() > 1 {
        let id = *ids.last().expect("non-empty");
        let path = dir.join(segment_file_name(id));

        // A file too short to hold a header is a rollover that was interrupted
        // between creating the segment and writing its header. It has no base
        // offset to compare and, having no header, cannot hold a record either.
        let headerless = std::fs::metadata(&path)?.len() < SEGMENT_HEADER_LEN;
        let base_offset = if headerless {
            None
        } else {
            let outcome = scan_segment(
                &path,
                id,
                label,
                config.index_spacing_bytes,
                ScanStart::Full,
                config.repair_checksum_tail,
            )?;
            if outcome.record_count > 0 {
                break;
            }

            // Empty. Compare its base against where the previous segment ends.
            let previous = *ids.get(ids.len() - 2).expect("len > 1");
            let previous_end = scan_segment(
                &dir.join(segment_file_name(previous)),
                previous,
                label,
                config.index_spacing_bytes,
                ScanStart::Full,
                config.repair_checksum_tail,
            )?
            .next_offset;
            if outcome.header.base_offset == previous_end {
                break;
            }
            Some((outcome.header.base_offset, previous_end))
        };

        match base_offset {
            None => tracing::warn!(
                shard = label,
                segment = id,
                "discarding a headerless segment left behind by an uninstalled rollover"
            ),
            Some((base_offset, log_tail)) => tracing::warn!(
                shard = label,
                segment = id,
                base_offset,
                log_tail,
                "discarding an empty segment left behind by an uninstalled rollover"
            ),
        }
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
        ids.pop();
        discarded += 1;
    }
    if discarded > 0 {
        sync_dir(dir)?;
    }
    Ok(discarded)
}

fn recover_existing(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    sealed_ids: &[SegmentId],
    active_id: SegmentId,
) -> Result<Recovered> {
    let mut sealed = Vec::with_capacity(sealed_ids.len());
    let mut index_rebuilds = 0usize;
    let mut expected_base: Option<Offset> = None;

    for id in sealed_ids {
        let opened = open_sealed(dir, label, config, *id)?;
        if opened.rebuilt_index {
            index_rebuilds += 1;
        }
        // Offsets must be contiguous across segment boundaries. A gap means a
        // segment file was deleted or replaced out from under us.
        if let Some(expected) = expected_base
            && expected != opened.entry.descriptor.base_offset
        {
            return Err(gap_error(
                label,
                *id,
                expected,
                opened.entry.descriptor.base_offset,
            ));
        }
        expected_base = Some(opened.entry.descriptor.last_offset + 1);
        sealed.push(opened.entry);
    }

    // The newest segment is the only one that can have been mid-write when the
    // process died, so it always gets a full scan.
    let active_path = dir.join(segment_file_name(active_id));
    let mut outcome = scan_segment(
        &active_path,
        active_id,
        label,
        config.index_spacing_bytes,
        ScanStart::Full,
        config.repair_checksum_tail,
    )?;
    if let Some(expected) = expected_base
        && expected != outcome.header.base_offset
    {
        return Err(gap_error(
            label,
            active_id,
            expected,
            outcome.header.base_offset,
        ));
    }

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
            holds_marks: outcome.header.holds_marks(),
        },
        config.index_spacing_bytes,
    )?;
    // The index was just rewritten from the scan, so it always counts as a
    // rebuild for the active segment.
    index_rebuilds += 1;

    Ok(Recovered {
        sealed,
        active,
        truncated_bytes,
        index_rebuilds,
        active_marks,
    })
}

/// Validate one sealed segment and prepare it for reads.
fn open_sealed(dir: &Path, label: &str, config: &LogConfig, id: SegmentId) -> Result<OpenedSealed> {
    let path = dir.join(segment_file_name(id));
    let file_len = std::fs::metadata(&path)?.len();

    // The header alone establishes the base offset every other check is
    // relative to, and proves the file is ours before anything else is trusted.
    let header = read_segment_header(&path, id, label)?;
    let base_offset = header.base_offset;

    let loaded = SparseIndex::load(&dir.join(index_file_name(id)), base_offset);
    let mut rebuilt_index = false;

    let resumed = match (loaded, config.verify_all_on_open) {
        (Some(index), false) if !index.is_empty() => {
            // Resume from the last index entry: only the records it does not
            // cover need checking, which is bounded by one index interval.
            let last = index.entries().last().copied().expect("non-empty");
            let resumed = scan_segment(
                &path,
                id,
                label,
                config.index_spacing_bytes,
                ScanStart::Resume {
                    position: last.position,
                    next_offset: last.offset,
                },
                // A sealed segment was synced and trimmed when it was sealed,
                // so nothing in it can be an unfinished write.
                false,
            );
            match resumed {
                Ok(outcome) if outcome.torn_tail.is_none() && outcome.valid_bytes == file_len => {
                    Some((index, outcome))
                }
                // The index sent the scan somewhere that is not a record
                // boundary, or not the one it claims. That may be the index
                // alone, so the full scan below decides: it fails only if the
                // segment itself is damaged.
                Ok(_) | Err(StorageError::Corruption(_)) => {
                    tracing::warn!(
                        shard = label,
                        segment = id,
                        "sealed segment index does not match its segment; rebuilding it",
                    );
                    None
                }
                Err(err) => return Err(err),
            }
        }
        _ => None,
    };
    let (index, outcome) = match resumed {
        Some(resumed) => resumed,
        None => {
            // No usable index, or a full verification was requested: walk the
            // whole segment and rebuild.
            rebuilt_index = true;
            let outcome = scan_segment(
                &path,
                id,
                label,
                config.index_spacing_bytes,
                ScanStart::Full,
                false,
            )?;
            outcome.index.persist(&dir.join(index_file_name(id)))?;
            (outcome.index.clone(), outcome)
        }
    };

    // A sealed segment was synced and trimmed when it was sealed, so damage at
    // its tail is not a torn write — it is data loss in committed bytes.
    if let Some(tail) = outcome.torn_tail {
        return Err(StorageError::Corruption(
            Corruption::new(tail.cause)
                .in_segment(label, id)
                .at_position(tail.position),
        ));
    }
    if outcome.valid_bytes != file_len {
        return Err(StorageError::Corruption(
            Corruption::new(CorruptionKind::Truncated {
                needed: file_len,
                available: outcome.valid_bytes,
            })
            .in_segment(label, id)
            .at_position(outcome.valid_bytes),
        ));
    }

    let descriptor = SegmentDescriptor {
        id,
        base_offset,
        last_offset: outcome.next_offset.saturating_sub(1).max(base_offset),
        size_bytes: file_len,
    };
    Ok(OpenedSealed {
        entry: SealedEntry {
            descriptor,
            index,
            reader: std::sync::Arc::new(SegmentReader::open(&path, id, base_offset)?),
            holds_marks: header.holds_marks(),
        },
        rebuilt_index,
    })
}

fn gap_error(label: &str, id: SegmentId, expected: Offset, found: Offset) -> StorageError {
    StorageError::Corruption(
        Corruption::new(CorruptionKind::OffsetOutOfOrder { expected, found })
            .in_segment(label, id)
            .at_position(0),
    )
}

#[cfg(test)]
mod tests;

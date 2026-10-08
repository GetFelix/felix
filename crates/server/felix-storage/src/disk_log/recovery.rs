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

use super::durable_mark::{self, DurableMark};
use super::now_micros;
use super::offload::Manifest;
use super::segments::SealedEntry;
use crate::io::{create_dir_all_durable, sync_dir};
use crate::log::{LogConfig, Offset, RecordMark, SegmentDescriptor, SegmentId};
use crate::segment::format::SEGMENT_HEADER_LEN;
use crate::segment::writer::ResumeState;
use crate::segment::{
    ScanStart, SegmentWriter, SparseIndex, TailRepair, index_file_name, parse_segment_file_name,
    read_segment_header, scan_segment, scan_segment_with, segment_file_name,
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
    pub active_marks: Vec<(Offset, RecordMark, u64)>,
}

struct OpenedSealed {
    entry: SealedEntry,
    /// Not `last_offset + 1`: a retired segment a crash emptied holds no
    /// records, and its successor must then start at its base.
    next_offset: Offset,
    rebuilt_index: bool,
}

/// Open, validate and repair every segment for one shard.
pub(super) fn recover_shard(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    offloaded: &Manifest,
) -> Result<Recovered> {
    let started = std::time::Instant::now();
    // The directory entry itself must be durable, or a crash could lose a shard
    // that already reported successful writes.
    create_dir_all_durable(dir)?;

    let mut ids = discover_segment_ids(dir)?;
    // Read once, before anything is repaired: it describes the files as the
    // last run left them.
    let mark = durable_mark::load(dir);
    // Rollover prepares the replacement segment ahead of the swap that installs
    // it, so a crash — or a truncation — can leave one behind that was never
    // used. Drop them before recovery proper, or their base offset reads as a
    // break in the offset chain. See `discard_abandoned_preparations`.
    let abandoned = discard_blank_interior(dir, label, config, mark, &mut ids)?
        + discard_abandoned_preparations(dir, label, config, mark, &mut ids)?;
    discard_blank_first(dir, label, mark, &mut ids)?;
    let recovered = match ids.split_last() {
        None => Recovered {
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
        },
        Some((active_id, sealed_ids)) => {
            match recover_existing(dir, label, config, mark, offloaded, sealed_ids, *active_id) {
                Err(StorageError::Corruption(detail)) => {
                    let Some(retired) = unsealed_retired(
                        dir, label, config, mark, sealed_ids, *active_id, &detail,
                    )?
                    else {
                        return Err(StorageError::Corruption(detail));
                    };
                    if !repair_unsealed_retired(dir, label, mark, &retired, *active_id)? {
                        return Err(StorageError::Corruption(detail));
                    }
                    let ids = discover_segment_ids(dir)?;
                    let (active_id, sealed_ids) = ids.split_last().expect("the retired segment");
                    recover_existing(dir, label, config, mark, offloaded, sealed_ids, *active_id)?
                }
                recovered => recovered?,
            }
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
    label: &str,
    config: &LogConfig,
    base_offset: Offset,
) -> Result<bool> {
    create_dir_all_durable(dir)?;
    let mut ids = discover_segment_ids(dir)?;
    discard_blank_first(dir, label, durable_mark::load(dir), &mut ids)?;
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

/// Remove segments inside the chain that never got as far as a header.
///
/// A background rollover that loses the race to an inline one deletes the
/// blank segment it built. The blank's directory entry was synced when it was
/// created, so if the unlink did not reach the disk a power loss brings it
/// back, empty, between two installed segments.
///
/// Shorter than a header, it cannot hold a record. It is dropped only when
/// the segments either side of it still meet exactly, so a real segment that
/// lost its bytes stays the offset gap that `recover_existing` refuses. A
/// blank run with nothing installed after it is left to
/// `discard_abandoned_preparations`.
fn discard_blank_interior(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    mark: Option<DurableMark>,
    ids: &mut Vec<SegmentId>,
) -> Result<usize> {
    let is_blank = |id: SegmentId| -> Result<bool> {
        Ok(std::fs::metadata(dir.join(segment_file_name(id)))?.len() < SEGMENT_HEADER_LEN)
    };
    let mut discarded = 0;
    let mut at = 1;
    while at + 1 < ids.len() {
        if !is_blank(ids[at])? {
            at += 1;
            continue;
        }
        let mut next = at + 1;
        while next < ids.len() && is_blank(ids[next])? {
            next += 1;
        }
        let Some(&following) = ids.get(next) else {
            break;
        };
        let previous = ids[at - 1];
        let previous_end = scan_segment_with(
            &dir.join(segment_file_name(previous)),
            previous,
            label,
            config.index_spacing_bytes,
            ScanStart::Full,
            tail_repair(config, mark, previous),
        )
        .map(|outcome| outcome.next_offset);
        let following_base =
            read_segment_header(&dir.join(segment_file_name(following)), following, label)
                .map(|header| header.base_offset);
        match (previous_end, following_base) {
            (Ok(end), Ok(base)) if end == base => {}
            // Not provably harmless: leave it for the chain checks to report.
            (
                Ok(_) | Err(StorageError::Corruption(_)),
                Ok(_) | Err(StorageError::Corruption(_)),
            ) => {
                at = next;
                continue;
            }
            (Err(err), _) | (_, Err(err)) => return Err(err),
        }
        for id in ids.drain(at..next) {
            tracing::warn!(
                shard = label,
                segment = id,
                "discarding an empty segment left behind by a rollover that lost its race"
            );
            remove_segment_files(dir, id)?;
            discarded += 1;
        }
    }
    if discarded > 0 {
        sync_dir(dir)?;
    }
    Ok(discarded)
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
    mark: Option<DurableMark>,
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
        let headerless = header_never_written(&path)?;
        let base_offset = if headerless {
            None
        } else {
            let outcome = scan_segment_with(
                &path,
                id,
                label,
                config.index_spacing_bytes,
                ScanStart::Full,
                tail_repair(config, mark, id),
            )?;
            if outcome.record_count > 0 {
                break;
            }

            // Empty. Compare its base against where the previous segment ends.
            let previous = *ids.get(ids.len() - 2).expect("len > 1");
            let previous_end = scan_segment_with(
                &dir.join(segment_file_name(previous)),
                previous,
                label,
                config.index_spacing_bytes,
                ScanStart::Full,
                tail_repair(config, mark, previous),
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
        remove_segment_files(dir, id)?;
        ids.pop();
        discarded += 1;
    }
    if discarded > 0 {
        sync_dir(dir)?;
    }
    Ok(discarded)
}

/// The segment a background rollover retired but never finished sealing,
/// when `detail` is damage a crash can leave there.
///
/// The rollover installs the new active segment first and flushes the retired
/// one afterwards. The two files reach the device independently, so a power
/// loss in between can leave the retired segment short -- torn or zero-filled
/// at the end, or cut cleanly back to its last sync -- with the newer segment
/// after it intact. Every flush syncs the retired segment before the active
/// one, so no record past the loss, in either segment, was ever reported
/// durable. Two shapes qualify:
///
/// * a torn tail in the retired segment (`scan_segment` with repair off);
/// * an intact retired segment that ends before the active segment begins,
///   which is the same loss landing on a record boundary.
///
/// Anything else is still corruption.
fn unsealed_retired(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    mark: Option<DurableMark>,
    sealed_ids: &[SegmentId],
    active_id: SegmentId,
    detail: &Corruption,
) -> Result<Option<UnsealedRetired>> {
    let Some(&id) = sealed_ids.last() else {
        return Ok(None);
    };
    let cut_short = match detail.kind {
        CorruptionKind::OffsetOutOfOrder { expected, found } => {
            detail.site.segment == Some(active_id) && found > expected
        }
        _ => false,
    };
    if detail.site.segment != Some(id) && !cut_short {
        return Ok(None);
    }
    match scan_segment_with(
        &dir.join(segment_file_name(id)),
        id,
        label,
        config.index_spacing_bytes,
        ScanStart::Full,
        TailRepair {
            checksum_tail: false,
            unsynced_from: DurableMark::unsynced_from(mark, id),
        },
    ) {
        Ok(outcome) if outcome.torn_tail.is_some() || cut_short => Ok(Some(UnsealedRetired {
            id,
            valid_bytes: outcome.valid_bytes,
            next_offset: outcome.next_offset,
        })),
        Ok(_) | Err(StorageError::Corruption(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

struct UnsealedRetired {
    id: SegmentId,
    valid_bytes: u64,
    next_offset: Offset,
}

/// Cut an unsealed retired segment back to its last intact record. The newer
/// segment is kept only if it starts exactly there; otherwise records were
/// lost in between and nothing after the tear can be kept in order.
///
/// Returns `false`, touching nothing, when the mark says the tear is in bytes
/// that were synced: those records may have been acknowledged, so the damage
/// is corruption, not an unfinished seal. That also protects the active
/// segment, because a mark can only reach it once the retired segment was
/// synced whole.
fn repair_unsealed_retired(
    dir: &Path,
    label: &str,
    mark: Option<DurableMark>,
    retired: &UnsealedRetired,
    active_id: SegmentId,
) -> Result<bool> {
    if retired.valid_bytes < DurableMark::synced_through(mark, retired.id) {
        return Ok(false);
    }
    let active_path = dir.join(segment_file_name(active_id));
    let continues = read_segment_header(&active_path, active_id, label)
        .is_ok_and(|header| header.base_offset == retired.next_offset);
    tracing::warn!(
        shard = label,
        segment = retired.id,
        valid_bytes = retired.valid_bytes,
        kept_next_segment = continues,
        "repairing the torn tail of a segment whose seal never finished",
    );
    if !continues {
        remove_segment_files(dir, active_id)?;
        sync_dir(dir)?;
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join(segment_file_name(retired.id)))?;
    file.set_len(retired.valid_bytes)?;
    crate::io::sync_data(&file)?;
    Ok(true)
}

/// Remove a log's first segment when it is the only one and is blank.
///
/// Its creation failed, on a full disk say, or a crash came before the
/// header. It never held a record, so the log starts again as if new. Only
/// segment 0 qualifies: any later one follows records that were somewhere.
///
/// Blank means no header, nothing but zeros after it, and no durable mark
/// past the header. A zeroed header in front of records, or one the mark
/// says was synced beyond, is damage to acknowledged data and stays fatal.
fn discard_blank_first(
    dir: &Path,
    label: &str,
    mark: Option<DurableMark>,
    ids: &mut Vec<SegmentId>,
) -> Result<()> {
    if ids.as_slice() != [0]
        || DurableMark::synced_through(mark, 0) > SEGMENT_HEADER_LEN
        || !segment_is_blank(&dir.join(segment_file_name(0)))?
    {
        return Ok(());
    }
    tracing::warn!(
        shard = label,
        "discarding a first segment whose creation never finished"
    );
    remove_segment_files(dir, 0)?;
    sync_dir(dir)?;
    ids.clear();
    Ok(())
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

/// A segment whose header never reached the disk: shorter than a header, or
/// a header of zeros. A background rollover writes the header without a flush
/// of its own, and every flush that would cover it syncs the retired segment
/// first, so nothing in such a segment was ever reported durable.
fn header_never_written(path: &Path) -> Result<bool> {
    let file = std::fs::File::open(path)?;
    if file.metadata()?.len() < SEGMENT_HEADER_LEN {
        return Ok(true);
    }
    let mut header = [0u8; SEGMENT_HEADER_LEN as usize];
    let read = crate::io::read_at(&file, &mut header, 0)?;
    Ok(read == header.len() && header.iter().all(|byte| *byte == 0))
}

/// Every byte of the file is zero, or it is shorter than a header.
fn segment_is_blank(path: &Path) -> Result<bool> {
    if !header_never_written(path)? {
        return Ok(false);
    }
    let file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let mut buf = vec![0u8; 64 * 1024];
    let mut offset = SEGMENT_HEADER_LEN;
    while offset < len {
        let want = buf.len().min((len - offset) as usize);
        let read = crate::io::read_at(&file, &mut buf[..want], offset)?;
        if read == 0 {
            break;
        }
        if buf[..read].iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
        offset += read as u64;
    }
    Ok(true)
}

fn recover_existing(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    mark: Option<DurableMark>,
    offloaded: &Manifest,
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
            && !drop_offloaded_below(
                dir,
                label,
                offloaded,
                &mut sealed,
                expected,
                opened.entry.descriptor.base_offset,
            )?
        {
            return Err(gap_error(
                label,
                *id,
                expected,
                opened.entry.descriptor.base_offset,
            ));
        }
        expected_base = Some(opened.next_offset);
        sealed.push(opened.entry);
    }

    // The newest segment is the only one that can have been mid-write when the
    // process died, so it always gets a full scan.
    let active_path = dir.join(segment_file_name(active_id));
    let mut outcome = scan_segment_with(
        &active_path,
        active_id,
        label,
        config.index_spacing_bytes,
        ScanStart::Full,
        tail_repair(config, mark, active_id),
    )?;
    if let Some(expected) = expected_base
        && expected != outcome.header.base_offset
        && !drop_offloaded_below(
            dir,
            label,
            offloaded,
            &mut sealed,
            expected,
            outcome.header.base_offset,
        )?
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
            version: outcome.header.version,
        },
        config.reserve_limit_bytes(),
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

/// Accept a gap at `[expected, found)` that offload explains, by deleting the
/// segments below it.
///
/// Offload copies oldest first and records each copy before its local file
/// goes, so a gap the manifest covers, above segments it also records, is an
/// unlink that landed out of order. Every record below `found` has a verified
/// copy, and the log resumes at `found`. Anything short of that is still the
/// damage `gap_error` reports.
fn drop_offloaded_below(
    dir: &Path,
    label: &str,
    offloaded: &Manifest,
    sealed: &mut Vec<SealedEntry>,
    expected: Offset,
    found: Offset,
) -> Result<bool> {
    if found < expected
        || !offloaded.covers(expected, found)
        || !sealed
            .iter()
            .all(|entry| offloaded.records(&entry.descriptor))
    {
        return Ok(false);
    }
    for entry in sealed.drain(..) {
        tracing::warn!(
            shard = label,
            segment = entry.descriptor.id,
            "removing a segment below a gap the offload manifest covers"
        );
        remove_segment_files(dir, entry.descriptor.id)?;
        sync_dir(dir)?;
    }
    Ok(true)
}

/// Validate one sealed segment and prepare it for reads.
fn open_sealed(dir: &Path, label: &str, config: &LogConfig, id: SegmentId) -> Result<OpenedSealed> {
    let path = dir.join(segment_file_name(id));
    let file_len = std::fs::metadata(&path)?.len();

    // The header alone establishes the base offset every other check is
    // relative to, and proves the file is ours before anything else is trusted.
    let header = read_segment_header(&path, id, label)?;
    let base_offset = header.base_offset;

    let last = SparseIndex::load_last(&dir.join(index_file_name(id)), base_offset);
    let mut rebuilt_index = false;

    let resumed = match (last, config.verify_all_on_open) {
        (Some(last), false) => {
            // Resume from the last index entry: only the records it does not
            // cover need checking, which is bounded by one index interval.
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
                    Some(outcome)
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
    let outcome = match resumed {
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
            outcome
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
        entry: SealedEntry::new(descriptor, header.holds_marks()),
        next_offset: outcome.next_offset,
        rebuilt_index,
    })
}

/// What a scan of segment `id` may repair: the configured checksum rule, and
/// anything past where the durable mark says its synced bytes end.
fn tail_repair(config: &LogConfig, mark: Option<DurableMark>, id: SegmentId) -> TailRepair {
    TailRepair {
        checksum_tail: config.repair_checksum_tail,
        unsynced_from: DurableMark::unsynced_from(mark, id),
    }
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

//! Walking a whole segment at startup, verifying every record.
//!
//! [`scan_segment`] is the only place that decides whether damage is a
//! repairable torn tail or committed data that has rotted. It also rebuilds
//! the segment's sparse index from the records it walks.

use std::fs::File;
use std::path::Path;

use crate::io::read_at;
use crate::log::{Offset, RecordMark, SegmentId};
use crate::segment::cursor::SegmentCursor;
use crate::segment::format::{
    IndexEntry, RECORD_HEADER_LEN, RecordHeader, SEGMENT_HEADER_LEN, SegmentHeader,
    check_offset_continuity, decode_record,
};
use crate::segment::index::SparseIndex;
use crate::{Corruption, CorruptionKind, Result, StorageError};

/// Where a scan begins, and therefore how much work it does.
#[derive(Debug, Clone, Copy)]
pub enum ScanStart {
    /// Validate every record from the head of the segment and rebuild the
    /// sparse index. Cost is proportional to the segment's size.
    Full,
    /// Resume from a record boundary already known to be good — typically the
    /// last sparse index entry. Only the records after it are validated and the
    /// index is *not* rebuilt, so cost is proportional to one index interval.
    Resume { position: u64, next_offset: Offset },
}

/// What a full validating scan of a segment found.
#[derive(Debug, Clone)]
pub struct ScanOutcome {
    pub header: SegmentHeader,
    /// Offset the next appended record will take.
    pub next_offset: Offset,
    /// File length after the last intact record: where appends resume, and
    /// where recovery truncates to if `torn_tail` is set.
    pub valid_bytes: u64,
    pub record_count: u64,
    /// Index rebuilt from the records actually present.
    pub index: SparseIndex,
    /// `Some` when the tail was damaged and must be truncated to `valid_bytes`.
    pub torn_tail: Option<TornTail>,
    /// The producer marks of the records scanned, in order. Collected here so
    /// rebuilding producer state costs no second pass over the segment.
    pub marks: Vec<(Offset, RecordMark)>,
}

impl ScanOutcome {
    /// Offset of the last intact record, or `None` for an empty segment.
    pub fn last_offset(&self) -> Option<Offset> {
        (self.record_count > 0).then(|| self.next_offset - 1)
    }
}

/// Damage found at the end of a segment that recovery is allowed to discard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TornTail {
    /// Byte position of the first unreadable record.
    pub position: u64,
    /// Bytes from `position` to the end of the file, all of which are dropped.
    pub discarded_bytes: u64,
    /// Why the record failed to decode.
    pub cause: CorruptionKind,
}

/// Running state of a scan, so the several exit paths agree on what to report.
struct ScanState {
    header: SegmentHeader,
    next_offset: Offset,
    position: u64,
    record_count: u64,
    index: SparseIndex,
    bytes_since_entry: u64,
    spacing: u64,
    rebuild_index: bool,
    repair_checksum_tail: bool,
    marks: Vec<(Offset, RecordMark)>,
}

impl ScanState {
    fn into_outcome(self, torn_tail: Option<TornTail>) -> ScanOutcome {
        ScanOutcome {
            header: self.header,
            next_offset: self.next_offset,
            valid_bytes: self.position,
            record_count: self.record_count,
            index: self.index,
            torn_tail,
            marks: self.marks,
        }
    }

    fn torn(self, file_len: u64, cause: CorruptionKind) -> ScanOutcome {
        let position = self.position;
        let discarded_bytes = file_len.saturating_sub(position);
        self.into_outcome(Some(TornTail {
            position,
            discarded_bytes,
            cause,
        }))
    }

    /// Turn a mid-scan decode failure into either a repaired tail or a hard
    /// error.
    fn finish_or_fail(
        self,
        file: &File,
        err: Corruption,
        claimed_len: Option<u64>,
        file_len: u64,
        shard_label: &str,
        segment_id: SegmentId,
    ) -> Result<ScanOutcome> {
        if is_repairable_tail(
            &err.kind,
            self.position,
            claimed_len,
            file_len,
            self.repair_checksum_tail,
        ) {
            return Ok(self.torn(file_len, err.kind));
        }
        let damaged_end = self.position + claimed_len.unwrap_or(RECORD_HEADER_LEN);
        if is_zero_filled_tail(file, self.position, damaged_end, file_len)? {
            return Ok(self.torn(file_len, err.kind));
        }
        let position = self.position;
        Err(StorageError::Corruption(
            err.in_segment(shard_label, segment_id)
                .at_position(position),
        ))
    }

    /// Emit an index entry on exactly the same rule `IndexWriter` uses, so a
    /// rebuilt index is byte-identical to one written during append.
    fn observe(&mut self, offset: Offset, position: u64, total_len: u64) {
        if !self.rebuild_index {
            return;
        }
        if self.index.is_empty() || self.bytes_since_entry >= self.spacing {
            self.index.push(IndexEntry { offset, position });
            self.bytes_since_entry = 0;
        }
        self.bytes_since_entry = self.bytes_since_entry.saturating_add(total_len);
    }
}

/// Validate a segment and, for a [`ScanStart::Full`] scan, rebuild its index.
///
/// This is the startup path. It never mutates the file; the caller applies the
/// truncation implied by [`ScanOutcome::torn_tail`].
///
/// `record_count` counts the records this scan validated, which for a
/// [`ScanStart::Resume`] scan is only those after the resume point.
///
/// `repair_checksum_tail` mirrors [`crate::log::LogConfig::repair_checksum_tail`]:
/// when false, only a provably incomplete trailing record is truncated.
pub fn scan_segment(
    path: &Path,
    segment_id: SegmentId,
    shard_label: &str,
    index_spacing_bytes: u64,
    start: ScanStart,
    repair_checksum_tail: bool,
) -> Result<ScanOutcome> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut cursor = SegmentCursor::new(&file);

    let annotate = |err: Corruption, position: u64| -> StorageError {
        StorageError::Corruption(
            err.in_segment(shard_label, segment_id)
                .at_position(position),
        )
    };

    // A segment header is written once, before any record, and is synced with
    // the first append. Damage here is never a repairable tail.
    let header_bytes = cursor.slice_at(0, SEGMENT_HEADER_LEN as usize)?;
    let header = SegmentHeader::decode(header_bytes).map_err(|err| annotate(err, 0))?;

    let (position, next_offset, rebuild_index) = match start {
        ScanStart::Full => (SEGMENT_HEADER_LEN, header.base_offset, true),
        ScanStart::Resume {
            position,
            next_offset,
        } => (position.max(SEGMENT_HEADER_LEN), next_offset, false),
    };

    let mut state = ScanState {
        header,
        next_offset,
        position,
        record_count: 0,
        index: SparseIndex::new(header.base_offset),
        bytes_since_entry: 0,
        // A resume scan leaves the index alone; `u64::MAX` spacing is how it
        // says "emit nothing" without a second code path in `observe`.
        spacing: if rebuild_index {
            index_spacing_bytes.max(1)
        } else {
            u64::MAX
        },
        rebuild_index,
        repair_checksum_tail,
        marks: Vec::new(),
    };

    while state.position < file_len {
        let position = state.position;

        // Read the header first so the payload read is sized before it is
        // attempted; a corrupt length is caught here, not by a huge allocation.
        let header_slice = cursor.slice_at(position, RECORD_HEADER_LEN as usize)?;
        if (header_slice.len() as u64) < RECORD_HEADER_LEN {
            let available = header_slice.len() as u64;
            return Ok(state.torn(
                file_len,
                CorruptionKind::Truncated {
                    needed: RECORD_HEADER_LEN,
                    available,
                },
            ));
        }
        let total_len = match RecordHeader::decode(header_slice) {
            Ok(record_header) => record_header.encoded_len(),
            Err(err) => {
                return state.finish_or_fail(&file, err, None, file_len, shard_label, segment_id);
            }
        };

        let record_slice = cursor.slice_at(position, total_len as usize)?;
        if (record_slice.len() as u64) < total_len {
            let available = record_slice.len() as u64;
            return Ok(state.torn(
                file_len,
                CorruptionKind::Truncated {
                    needed: total_len,
                    available,
                },
            ));
        }

        let decoded = match decode_record(record_slice) {
            Ok((decoded, _)) => decoded,
            Err(err) => {
                return state.finish_or_fail(
                    &file,
                    err,
                    Some(total_len),
                    file_len,
                    shard_label,
                    segment_id,
                );
            }
        };

        if let Err(err) = check_offset_continuity(state.next_offset, decoded.header.offset) {
            return state.finish_or_fail(
                &file,
                err,
                Some(total_len),
                file_len,
                shard_label,
                segment_id,
            );
        }

        state.observe(decoded.header.offset, position, total_len);
        if decoded.mark != RecordMark::None {
            state.marks.push((decoded.header.offset, decoded.mark));
        }
        state.position += total_len;
        state.next_offset = decoded.header.offset + 1;
        state.record_count += 1;
    }

    Ok(state.into_outcome(None))
}

/// Read and validate only a segment's header.
///
/// Cheap enough to call on every segment at startup: it proves the file is ours
/// and yields the base offset that everything else is relative to.
pub fn read_segment_header(
    path: &Path,
    segment_id: SegmentId,
    shard_label: &str,
) -> Result<SegmentHeader> {
    let file = File::open(path)?;
    let mut cursor = SegmentCursor::new(&file);
    let bytes = cursor.slice_at(0, SEGMENT_HEADER_LEN as usize)?;
    SegmentHeader::decode(bytes).map_err(|err| {
        StorageError::Corruption(err.in_segment(shard_label, segment_id).at_position(0))
    })
}

/// Decide whether a decode failure at `position` may be truncated away.
///
/// Repairable means "this damage is confined to the end of the file, so no
/// record beyond it could have been acknowledged". Anything else is committed
/// data that has changed underneath us, and callers must fail loudly rather than
/// silently shorten the log.
fn is_repairable_tail(
    kind: &CorruptionKind,
    position: u64,
    claimed_len: Option<u64>,
    file_len: u64,
    repair_checksum_tail: bool,
) -> bool {
    match kind {
        // The header verified, so `payload_len` is the length the writer
        // actually intended — and the file ends before it. Nothing but an
        // unfinished write produces that, and nothing can have acknowledged a
        // record that was never finished. Provably repairable.
        //
        // Before v2 this was only *probably* true: a rotted length field on a
        // complete, acknowledged record produced the same error, and truncating
        // deleted data the caller had been told was safe. The header checksum
        // is what turned the guess into a decision.
        CorruptionKind::Truncated { .. } => true,
        // The header itself did not verify, so nothing it says can be trusted —
        // including its length. This may be an unfinished write, or a complete
        // record whose header rotted after being acknowledged. The two are
        // indistinguishable from the bytes, so recovery refuses to choose
        // unless an operator has said which risk they prefer.
        CorruptionKind::RecordHeaderChecksum { .. } => {
            repair_checksum_tail && position.saturating_add(RECORD_HEADER_LEN) >= file_len
        }
        // The header verified but the payload did not. The record is complete
        // on disk, so this is rot rather than a torn write, and under
        // `OnCommit` it may already have been acknowledged.
        CorruptionKind::RecordChecksum { .. } | CorruptionKind::OffsetOutOfOrder { .. } => {
            repair_checksum_tail
                && claimed_len.is_some_and(|len| position.saturating_add(len) >= file_len)
        }
        // A verified header carrying an impossible length. The writer rejects
        // oversized records, so this is damage the checksum did not catch;
        // treat it as ambiguous rather than assume a torn write.
        CorruptionKind::RecordTooLarge { payload_len, .. } => {
            repair_checksum_tail
                && position.saturating_add(RECORD_HEADER_LEN + u64::from(*payload_len)) > file_len
        }
        _ => false,
    }
}

/// The granularity a lost write comes back at. Filesystems allocate and drop
/// whole blocks, and every block size is a multiple of this.
const SECTOR_BYTES: u64 = 512;

/// Whether the damage at `position` is a power-loss tail: everything from the
/// damaged record to end of file reads as zeros, or the zeros start at a
/// sector boundary inside the damaged record (which ends at `damaged_end`) and
/// run to end of file.
///
/// After a power loss the file size can survive while the data blocks it
/// covers do not, and those read back as zeros. Nothing after them can have
/// been acknowledged: an fsync that covered a later record covered these
/// bytes too. Zeros followed by anything else are not this case and stay
/// fatal. See `docs/storage-format.md`, "What recovery may repair".
fn is_zero_filled_tail(
    file: &File,
    position: u64,
    damaged_end: u64,
    file_len: u64,
) -> Result<bool> {
    let Some(zeros_from) = trailing_zeros_start(file, position, file_len)? else {
        return Ok(false);
    };
    if zeros_from == position {
        return Ok(true);
    }
    let boundary = zeros_from.next_multiple_of(SECTOR_BYTES);
    Ok(boundary < damaged_end.min(file_len))
}

/// Where the run of zero bytes that ends the file begins, searching no
/// earlier than `from`. `None` when the file does not end in a zero byte.
fn trailing_zeros_start(file: &File, from: u64, file_len: u64) -> Result<Option<u64>> {
    let mut buf = vec![0u8; 64 * 1024];
    let mut last_nonzero: Option<u64> = None;
    let mut at = from;
    while at < file_len {
        let want = buf.len().min((file_len - at) as usize);
        let read = read_at(file, &mut buf[..want], at)?;
        if read == 0 {
            break;
        }
        if let Some(i) = buf[..read].iter().rposition(|byte| *byte != 0) {
            last_nonzero = Some(at + i as u64);
        }
        at += read as u64;
    }
    Ok(match last_nonzero {
        None => Some(from),
        Some(last) if last + 1 < file_len => Some(last + 1),
        Some(_) => None,
    })
}

#[cfg(test)]
mod tests;

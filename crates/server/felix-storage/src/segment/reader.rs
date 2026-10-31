//! Range reads on a healthy segment: seek with the sparse index, then decode
//! forward under a budget.

use std::fs::File;
use std::path::Path;
use std::sync::OnceLock;

use crate::io::read_at;
use crate::log::{LogRecord, Offset, SegmentId};
use crate::segment::cursor::SegmentCursor;
use crate::segment::format::{
    RECORD_HEADER_LEN, RecordHeader, RecordKind, SEGMENT_HEADER_LEN, SegmentHeader, decode_record,
};
use crate::segment::index::SparseIndex;
use crate::{Corruption, Result, StorageError};

/// Whether a read returns trim records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimRecords {
    /// Leave them out, as every reader of a stream's records does.
    Skip,
    /// Return them, for replication and for finding a log's floor.
    Include,
    /// Return only them, for rebuilding a log's head floor at open. Every
    /// other record is skipped by its header.
    Only,
}

/// Read-side handle on one segment.
///
/// Holds a single descriptor and reads through it positionally, so `&self` is
/// enough to serve a read and several can run at once.
#[derive(Debug)]
pub struct SegmentReader {
    id: SegmentId,
    base_offset: Offset,
    file: File,
    /// The header's version, read the first time a trim record is met, so a
    /// segment that holds none never pays for it.
    version: OnceLock<u16>,
}

impl SegmentReader {
    pub fn open(path: &Path, id: SegmentId, base_offset: Offset) -> Result<Self> {
        Ok(Self {
            id,
            base_offset,
            file: File::open(path)?,
            version: OnceLock::new(),
        })
    }

    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    /// Append records from `start_offset` onward into `out`.
    ///
    /// Stops at `valid_bytes` or once `budget` is spent, whichever comes first,
    /// and decrements `budget` by what it produced so a caller reading across
    /// several segments keeps one running allowance.
    ///
    /// At least one record is always returned when one exists at or after
    /// `start_offset`, even if it alone exceeds the byte budget; otherwise a
    /// record larger than the caller's budget could never be read at all.
    pub fn read_from(
        &self,
        index: &SparseIndex,
        start_offset: Offset,
        valid_bytes: u64,
        budget: &mut ReadBudget,
        shard_label: &str,
        out: &mut Vec<LogRecord>,
    ) -> Result<()> {
        self.read_from_position(
            index.seek_position(start_offset),
            start_offset,
            valid_bytes,
            budget,
            shard_label,
            out,
        )
    }

    /// [`SegmentReader::read_from`], starting at a record boundary the caller
    /// already found with the index, so the index need not be held while the
    /// segment is read.
    pub(crate) fn read_from_position(
        &self,
        mut position: u64,
        start_offset: Offset,
        valid_bytes: u64,
        budget: &mut ReadBudget,
        shard_label: &str,
        out: &mut Vec<LogRecord>,
    ) -> Result<()> {
        if budget.is_spent() {
            return Ok(());
        }

        let mut cursor = SegmentCursor::new(&self.file);

        while position < valid_bytes && !budget.is_spent() {
            let header_slice = cursor.slice_at(position, RECORD_HEADER_LEN as usize)?;
            let record_header = RecordHeader::decode(header_slice).map_err(|err| {
                StorageError::Corruption(err.in_segment(shard_label, self.id).at_position(position))
            })?;
            let total_len = record_header.encoded_len();

            // Records before the requested start, and trims the caller does
            // not want, are skipped by arithmetic alone: their payloads are
            // never even read.
            if record_header.kind == RecordKind::Trim {
                self.check_trim(&record_header, position, shard_label)?;
                if budget.trims == TrimRecords::Skip {
                    position += total_len;
                    continue;
                }
            } else if budget.trims == TrimRecords::Only {
                position += total_len;
                continue;
            }
            if record_header.offset < start_offset {
                position += total_len;
                continue;
            }

            // Honour the budget, but never return an empty result for a range
            // that does have data.
            if !budget.admits(record_header.payload_len as usize) {
                break;
            }

            let record_slice = cursor.slice_at(position, total_len as usize)?;
            let (decoded, _) = decode_record(record_slice).map_err(|err| {
                StorageError::Corruption(err.in_segment(shard_label, self.id).at_position(position))
            })?;

            budget.consume(decoded.payload.len());
            out.push(LogRecord {
                offset: decoded.header.offset,
                timestamp_micros: decoded.header.timestamp_micros,
                checksum: decoded.header.checksum,
                payload: decoded.payload,
                mark: decoded.mark,
                publisher: decoded.publisher,
            });
            position += total_len;
        }

        Ok(())
    }

    /// Byte position of the record at `offset`, or `valid_bytes` when every
    /// record is below it.
    ///
    /// Walks record headers from the nearest index entry, so it reads one
    /// index interval plus a header per record, never a payload.
    pub(crate) fn position_of(
        &self,
        index: &SparseIndex,
        offset: Offset,
        valid_bytes: u64,
        shard_label: &str,
    ) -> Result<u64> {
        let mut position = index.seek_position(offset);
        let mut cursor = SegmentCursor::new(&self.file);
        while position < valid_bytes {
            let header_slice = cursor.slice_at(position, RECORD_HEADER_LEN as usize)?;
            let record_header = RecordHeader::decode(header_slice).map_err(|err| {
                StorageError::Corruption(err.in_segment(shard_label, self.id).at_position(position))
            })?;
            if record_header.kind == RecordKind::Trim {
                self.check_trim(&record_header, position, shard_label)?;
            }
            if record_header.offset >= offset {
                return Ok(position);
            }
            position += record_header.encoded_len();
        }
        Ok(valid_bytes)
    }

    /// Refuse a trim record in a segment older than v7, which no build
    /// wrote. Recovery scans only the active segment, so a sealed one can
    /// reach here unchecked.
    #[cold]
    #[inline(never)]
    fn check_trim(&self, header: &RecordHeader, position: u64, shard_label: &str) -> Result<()> {
        let annotate = |err: Corruption, at: u64| {
            StorageError::Corruption(err.in_segment(shard_label, self.id).at_position(at))
        };
        let version = match self.version.get() {
            Some(version) => *version,
            None => {
                let mut buf = [0u8; SEGMENT_HEADER_LEN as usize];
                let read = read_at(&self.file, &mut buf, 0)?;
                let version = SegmentHeader::decode(&buf[..read])
                    .map_err(|err| annotate(err, 0))?
                    .version;
                *self.version.get_or_init(|| version)
            }
        };
        header
            .check_version(version)
            .map_err(|err| annotate(err, position))
    }
}

/// How much a read may produce, carried across every segment it visits.
///
/// One budget spans a whole `read_range`, which is what makes the guarantees
/// global rather than per file: `max_bytes` bounds the response overall, and the
/// "always return at least one record" rule applies once, not once per segment.
#[derive(Debug, Clone, Copy)]
pub struct ReadBudget {
    pub max_bytes: usize,
    pub max_records: usize,
    /// Whether trim records are returned. They cost nothing when skipped.
    pub trims: TrimRecords,
    /// Records produced so far under this budget.
    produced: usize,
}

impl ReadBudget {
    pub fn new(max_bytes: usize, max_records: usize) -> Self {
        Self {
            max_bytes,
            max_records,
            trims: TrimRecords::Skip,
            produced: 0,
        }
    }

    /// This budget, returning trim records as `trims` says.
    pub fn with_trims(self, trims: TrimRecords) -> Self {
        Self { trims, ..self }
    }

    pub fn unbounded() -> Self {
        Self::new(usize::MAX, usize::MAX)
    }

    /// True when no further record may be produced.
    pub fn is_spent(&self) -> bool {
        self.max_records == 0
    }

    pub fn produced(&self) -> usize {
        self.produced
    }

    /// Whether `payload_len` fits, given that an empty response is never a
    /// useful answer for a range that has data.
    fn admits(&self, payload_len: usize) -> bool {
        self.produced == 0 || payload_len <= self.max_bytes
    }

    /// Charge one record against the budget.
    fn consume(&mut self, payload_len: usize) {
        self.max_bytes = self.max_bytes.saturating_sub(payload_len);
        self.max_records = self.max_records.saturating_sub(1);
        self.produced += 1;
    }
}

#[cfg(test)]
mod tests;

//! Decoding the records around an offset, read-only.
//!
//! Seeks with the segment's sparse index, then decodes forward with the same
//! record format code the broker reads with. The index is only a hint: the
//! record it points at is checked before it is used, and a missing, short or
//! stale index costs a longer walk, never a wrong answer.
//!
//! A record that does not verify ends the read instead of failing it. The
//! records before it are returned, with where and why it stopped. Next to a
//! running broker that stop is often an append in flight at the active
//! segment's tail.

use std::fs::File;
use std::path::Path;

use super::super::recovery;
use super::label_of;
use crate::log::{Offset, RecordMark, SegmentId};
use crate::segment::format::{check_offset_continuity, decode_record};
use crate::segment::{
    RECORD_HEADER_LEN, RecordHeader, SEGMENT_HEADER_LEN, ScanStart, SegmentHeader, SparseIndex,
    TailRepair, index_file_name, read_segment_header, scan_segment_with, segment_file_name,
};
use crate::{Corruption, CorruptionKind, Result, StorageError};

/// The most records a read returns on each side of the offset.
pub const MAX_AROUND: u64 = 1000;

/// The most payload bytes a read returns for one record.
pub const PAYLOAD_PREVIEW_BYTES: usize = 64 * 1024;

/// Which records to decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordsQuery {
    pub offset: Offset,
    /// Records wanted on each side of `offset`. Capped at [`MAX_AROUND`].
    pub around: u64,
    /// Return payload bytes, cut at [`PAYLOAD_PREVIEW_BYTES`] each.
    pub payload: bool,
}

/// The records found, and what ended the read.
#[derive(Debug, Clone)]
pub struct RecordsReport {
    /// The offset the oldest segment on disk starts at. `None` when the shard
    /// has no readable segment.
    pub log_start: Option<Offset>,
    /// The offset after the last record, when the read got that far. A read
    /// that stopped at damage or at the end of the window does not know it.
    pub log_end: Option<Offset>,
    /// Records that verified, in offset order.
    pub records: Vec<RecordView>,
    /// The first record that did not verify, if the read reached one.
    pub stop: Option<Stop>,
    pub offset: OffsetStatus,
    /// Where the walk began.
    pub seek: Seek,
}

/// One record that passed its checksum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordView {
    pub segment: SegmentId,
    /// Byte position of the record in its segment.
    pub position: u64,
    pub offset: Offset,
    pub timestamp_micros: u64,
    pub mark: RecordMark,
    /// Payload size, without the publisher.
    pub payload_len: u64,
    pub checksum: u32,
    /// The principal that published the record, when it was stored with one.
    pub publisher: Option<Vec<u8>>,
    /// The payload cut at [`PAYLOAD_PREVIEW_BYTES`], only when asked for.
    pub payload: Option<Vec<u8>>,
}

/// Where a read stopped on a record that does not verify.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stop {
    pub segment: SegmentId,
    pub position: u64,
    /// The offset the record should have: the one after the last that
    /// verified, or the segment's base.
    pub expected_offset: Offset,
    pub cause: CorruptionKind,
    /// The damage runs to the end of the file in a shape only an unfinished
    /// write leaves. Otherwise it could be a complete record that rotted.
    pub tail: bool,
}

/// Whether the requested offset was read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetStatus {
    /// Its record is in the report.
    Found,
    /// Before the oldest segment on disk.
    BelowStart { log_start: Offset },
    /// At or past the end of the log.
    PastTail { log_end: Offset },
    /// The read stopped at damage before reaching it.
    Unreached,
    /// The shard has no segments.
    Empty,
    /// Inside the log's range, yet no record carries it.
    Missing,
}

/// Where the walk began.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seek {
    pub segment: SegmentId,
    pub position: u64,
    /// The segment's index gave the start. Otherwise the walk began at the
    /// segment's first record.
    pub via_index: bool,
}

/// Decode the records within `query.around` of `query.offset` in the shard in
/// `dir`. Nothing is written.
pub fn read_records(dir: &Path, query: &RecordsQuery) -> Result<RecordsReport> {
    let label = label_of(dir);
    let around = query.around.min(MAX_AROUND);
    let segments = list_segments(dir, &label)?;
    let log_start = segments
        .iter()
        .find_map(|segment| segment.header.as_ref().ok())
        .map(|header| header.base_offset);
    let Some(log_start) = log_start else {
        return Ok(RecordsReport {
            log_start: None,
            log_end: None,
            records: Vec::new(),
            stop: None,
            offset: OffsetStatus::Empty,
            seek: Seek {
                segment: 0,
                position: SEGMENT_HEADER_LEN,
                via_index: false,
            },
        });
    };

    let lo = query.offset.saturating_sub(around).max(log_start);
    let hi = query.offset.saturating_add(around);
    let first = segments
        .iter()
        .rposition(|segment| {
            segment
                .header
                .as_ref()
                .is_ok_and(|header| header.base_offset <= lo)
        })
        .unwrap_or(0);

    let mut walk = Walk {
        dir,
        label: &label,
        query,
        lo,
        hi,
        records: Vec::new(),
        expected: None,
        stop: None,
        window_done: false,
        seek: None,
    };
    for segment in &segments[first..] {
        walk.segment(segment)?;
        if walk.stop.is_some() || walk.window_done {
            break;
        }
    }
    let log_end =
        (walk.stop.is_none() && !walk.window_done).then(|| walk.expected.unwrap_or(log_start));
    let offset = status(query.offset, log_start, log_end, &walk);
    Ok(RecordsReport {
        log_start: Some(log_start),
        log_end,
        records: walk.records,
        stop: walk.stop,
        offset,
        seek: walk.seek.unwrap_or(Seek {
            segment: segments[first].id,
            position: SEGMENT_HEADER_LEN,
            via_index: false,
        }),
    })
}

fn status(offset: Offset, log_start: Offset, log_end: Option<Offset>, walk: &Walk) -> OffsetStatus {
    if walk.records.iter().any(|record| record.offset == offset) {
        OffsetStatus::Found
    } else if offset < log_start {
        OffsetStatus::BelowStart { log_start }
    } else if let Some(log_end) = log_end.filter(|end| offset >= *end) {
        OffsetStatus::PastTail { log_end }
    } else if walk.stop.is_some() {
        OffsetStatus::Unreached
    } else {
        OffsetStatus::Missing
    }
}

/// A segment file, with its header if that decodes.
struct SegmentFile {
    id: SegmentId,
    header: std::result::Result<SegmentHeader, Corruption>,
}

fn list_segments(dir: &Path, label: &str) -> Result<Vec<SegmentFile>> {
    let mut segments = Vec::new();
    for id in recovery::discover_segment_ids(dir)? {
        let header = match read_segment_header(&dir.join(segment_file_name(id)), id, label) {
            Ok(header) => Ok(header),
            Err(StorageError::Corruption(detail)) => Err(detail),
            Err(err) => return Err(err),
        };
        segments.push(SegmentFile { id, header });
    }
    Ok(segments)
}

/// State carried from one segment to the next.
struct Walk<'a> {
    dir: &'a Path,
    label: &'a str,
    query: &'a RecordsQuery,
    /// The first offset wanted, and the last.
    lo: Offset,
    hi: Offset,
    records: Vec<RecordView>,
    /// The offset the next record must carry. `None` before the first.
    expected: Option<Offset>,
    stop: Option<Stop>,
    /// A record past `hi` was met.
    window_done: bool,
    seek: Option<Seek>,
}

impl Walk<'_> {
    fn segment(&mut self, segment: &SegmentFile) -> Result<()> {
        let id = segment.id;
        if self.expected.is_some_and(|expected| expected > self.hi) {
            self.window_done = true;
            return Ok(());
        }
        let header = match &segment.header {
            Ok(header) => *header,
            Err(detail) => {
                self.stop = Some(Stop {
                    segment: id,
                    position: 0,
                    expected_offset: self.expected.unwrap_or(0),
                    cause: detail.kind.clone(),
                    tail: false,
                });
                return Ok(());
            }
        };
        let path = self.dir.join(segment_file_name(id));
        let file = File::open(&path)?;
        let file_len = file.metadata()?.len();

        let (mut position, mut expected, via_index) = match self.expected {
            // Later segments are read from their first record.
            Some(_) => (SEGMENT_HEADER_LEN, header.base_offset, false),
            None => self.seek_in(&file, id, &header),
        };
        if self.seek.is_none() {
            self.seek = Some(Seek {
                segment: id,
                position,
                via_index,
            });
        }
        // A segment that holds records must pick up where the last one ended.
        if let Some(previous) = self.expected
            && file_len > SEGMENT_HEADER_LEN
            && previous != header.base_offset
        {
            return self.fail(
                id,
                &path,
                SEGMENT_HEADER_LEN,
                previous,
                CorruptionKind::OffsetOutOfOrder {
                    expected: previous,
                    found: header.base_offset,
                },
            );
        }

        let mut buf = Vec::new();
        while position < file_len {
            // Past the window nothing is read, so damage there is not reported.
            if expected > self.hi {
                self.window_done = true;
                self.expected = Some(expected);
                return Ok(());
            }
            buf.resize(RECORD_HEADER_LEN as usize, 0);
            let read = read_at(&file, &mut buf, position)?;
            if (read as u64) < RECORD_HEADER_LEN {
                let cause = CorruptionKind::Truncated {
                    needed: RECORD_HEADER_LEN,
                    available: read as u64,
                };
                return self.fail(id, &path, position, expected, cause);
            }
            let record_header = match RecordHeader::decode_in(&buf, header.version) {
                Ok(record_header) => record_header,
                Err(err) => return self.fail(id, &path, position, expected, err.kind),
            };
            if let Err(err) = check_offset_continuity(expected, record_header.offset) {
                return self.fail(id, &path, position, expected, err.kind);
            }
            let total = record_header.encoded_len();
            if record_header.offset > self.hi {
                self.window_done = true;
                self.expected = Some(expected);
                return Ok(());
            }
            if record_header.offset < self.lo {
                // Before the window, so only its header is read.
                position += total;
                expected += 1;
                continue;
            }

            buf.resize(total as usize, 0);
            let read = read_at(&file, &mut buf, position)?;
            if (read as u64) < total {
                let cause = CorruptionKind::Truncated {
                    needed: total,
                    available: read as u64,
                };
                return self.fail(id, &path, position, expected, cause);
            }
            let decoded = match decode_record(&buf) {
                Ok((decoded, _)) => decoded,
                Err(err) => return self.fail(id, &path, position, expected, err.kind),
            };
            self.records.push(RecordView {
                segment: id,
                position,
                offset: decoded.header.offset,
                timestamp_micros: decoded.header.timestamp_micros,
                mark: decoded.mark,
                payload_len: decoded.payload.len() as u64,
                checksum: decoded.header.checksum,
                publisher: decoded.publisher.as_deref().map(<[u8]>::to_vec),
                payload: self.query.payload.then(|| {
                    let keep = decoded.payload.len().min(PAYLOAD_PREVIEW_BYTES);
                    decoded.payload[..keep].to_vec()
                }),
            });
            position += total;
            expected += 1;
        }
        self.expected = Some(expected);
        Ok(())
    }

    /// Where to start in the segment holding `lo`: the index entry at or
    /// before it, if the record there checks out, else the first record.
    fn seek_in(&self, file: &File, id: SegmentId, header: &SegmentHeader) -> (u64, Offset, bool) {
        let from_start = (SEGMENT_HEADER_LEN, header.base_offset, false);
        let Some(index) =
            SparseIndex::load(&self.dir.join(index_file_name(id)), header.base_offset)
        else {
            return from_start;
        };
        let position = index.seek_position(self.lo);
        if position <= SEGMENT_HEADER_LEN {
            return from_start;
        }
        // The index is derived data, so the record it points at has to show
        // it is a record boundary at an offset the window can start from.
        let mut buf = [0u8; RECORD_HEADER_LEN as usize];
        let found = read_at(file, &mut buf, position).ok() == Some(buf.len());
        match RecordHeader::decode_in(&buf, header.version) {
            Ok(record) if found && (header.base_offset..=self.lo).contains(&record.offset) => {
                (position, record.offset, true)
            }
            _ => from_start,
        }
    }

    /// End the read at the record at `position`, and say whether the damage
    /// is a torn tail. Judged by the scan startup uses, from the record on,
    /// with nothing repaired.
    fn fail(
        &mut self,
        id: SegmentId,
        path: &Path,
        position: u64,
        expected: Offset,
        cause: CorruptionKind,
    ) -> Result<()> {
        let rescan = scan_segment_with(
            path,
            id,
            self.label,
            u64::MAX,
            ScanStart::Resume {
                position,
                next_offset: expected,
            },
            TailRepair::default(),
        );
        // A scan that finds nothing wrong lost a race with the writer: the
        // record was unfinished when it was read.
        let tail = match rescan {
            Ok(_) => true,
            Err(StorageError::Corruption(_)) => false,
            Err(err) => return Err(err),
        };
        self.stop = Some(Stop {
            segment: id,
            position,
            expected_offset: expected,
            cause,
            tail,
        });
        Ok(())
    }
}

fn read_at(file: &File, buf: &mut [u8], position: u64) -> Result<usize> {
    Ok(crate::io::read_at(file, buf, position)?)
}

#[cfg(test)]
mod tests;

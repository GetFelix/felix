//! Decoding the records around an offset, read-only.
//!
//! Seeks with the segment's sparse index, then decodes forward with the same
//! record format code the broker reads with. The index is only a hint: the
//! record it points at is checked before it is used, and a missing, short or
//! stale index costs a longer walk, never a wrong answer.
//!
//! A record that does not verify ends the read instead of failing it. The
//! records before it are returned, with where and why it stopped. Whether that
//! is a torn tail or damage comes from the plan startup runs, which also
//! decides which segments count: a rollover's leftovers are not read. Next to
//! a running broker a stop at the active segment's tail is often an append in
//! flight.

use std::fs::File;
use std::path::Path;

use super::super::{offload, recovery};
use super::{Action, Startup, label_of, verdict};
use crate::log::{LogConfig, Offset, RecordMark, SegmentId};
use crate::segment::format::{check_offset_continuity, decode_record};
use crate::segment::{
    RECORD_HEADER_LEN, RecordHeader, SEGMENT_HEADER_LEN, SegmentHeader, SparseIndex,
    index_file_name, read_segment_header, segment_file_name,
};
use crate::{Corruption, CorruptionKind, Result, StorageError};

/// The most records a read returns on each side of the offset.
pub const MAX_AROUND: u64 = 1000;

/// The same cap when payloads are returned, so a window of large records stays
/// within a few megabytes.
pub const MAX_AROUND_WITH_PAYLOAD: u64 = 100;

/// The most payload bytes a read returns for one record.
pub const PAYLOAD_PREVIEW_BYTES: usize = 64 * 1024;

/// Which records to decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordsQuery {
    pub offset: Offset,
    /// Records wanted on each side of `offset`. Capped at [`MAX_AROUND`], or
    /// at [`MAX_AROUND_WITH_PAYLOAD`] with `payload`.
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
    /// What startup would make of the shard, from the plan it runs before it
    /// writes anything. The segments read are the ones left after that plan's
    /// removals, so a rollover's leftovers are not mistaken for data.
    pub startup: Startup,
    pub actions: Vec<Action>,
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
    /// Startup would cut the file here, as a torn tail. Otherwise it would
    /// refuse the shard, or not look at this record at all.
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
/// `dir`, with startup's verdict for `config`. Only `index_spacing_bytes`,
/// `repair_checksum_tail` and `verify_all_on_open` affect the result. Nothing
/// is written.
pub fn read_records(dir: &Path, query: &RecordsQuery, config: &LogConfig) -> Result<RecordsReport> {
    retrying(dir, || read_once(dir, query, config))
}

/// The first record that does not verify, reading the shard from its start.
/// `None` when every record does.
pub fn first_damage(dir: &Path, config: &LogConfig) -> Result<Option<Stop>> {
    retrying(dir, || {
        let label = label_of(dir);
        let (_, actions) = plan(dir, &label, config)?;
        let segments = list_segments(dir, &label, &actions)?;
        let mut walk = Walk::new(
            dir,
            &actions,
            segments.last().map_or(0, |segment| segment.id),
            false,
            0,
            u64::MAX,
        );
        for segment in &segments {
            walk.segment(segment)?;
            if walk.stop.is_some() {
                break;
            }
        }
        Ok(walk.stop)
    })
}

/// Run `read` again once if a file vanished under it, which is retention
/// deleting a segment on a running broker.
fn retrying<T>(dir: &Path, read: impl Fn() -> Result<T>) -> Result<T> {
    match read() {
        Err(StorageError::Io(err))
            if err.kind() == std::io::ErrorKind::NotFound && dir.is_dir() =>
        {
            read()
        }
        other => other,
    }
}

fn read_once(dir: &Path, query: &RecordsQuery, config: &LogConfig) -> Result<RecordsReport> {
    let label = label_of(dir);
    let around = query.around.min(if query.payload {
        MAX_AROUND_WITH_PAYLOAD
    } else {
        MAX_AROUND
    });
    let (startup, actions) = plan(dir, &label, config)?;
    let segments = list_segments(dir, &label, &actions)?;
    let seek = |segment: SegmentId| Seek {
        segment,
        position: SEGMENT_HEADER_LEN,
        via_index: false,
    };
    if segments.is_empty() {
        return Ok(RecordsReport {
            log_start: None,
            log_end: None,
            records: Vec::new(),
            stop: None,
            offset: OffsetStatus::Empty,
            seek: seek(0),
            startup,
            actions,
        });
    }
    // The oldest segment's header says where the log starts. When it is the
    // one that does not decode, the read stops there, so the damage is named
    // and not mistaken for an offset below the start.
    let log_start = segments[0]
        .header
        .as_ref()
        .ok()
        .map(|header| header.base_offset);
    let lo = query
        .offset
        .saturating_sub(around)
        .max(log_start.unwrap_or(0));
    let hi = query.offset.saturating_add(around);
    let first = if log_start.is_none() {
        0
    } else {
        segments
            .iter()
            .rposition(|segment| {
                segment
                    .header
                    .as_ref()
                    .is_ok_and(|header| header.base_offset <= lo)
            })
            .unwrap_or(0)
    };

    let mut walk = Walk::new(dir, &actions, segments[segments.len() - 1].id, true, lo, hi);
    walk.payload = query.payload;
    for segment in &segments[first..] {
        walk.segment(segment)?;
        if walk.stop.is_some() || walk.window_done {
            break;
        }
    }
    let log_end = (walk.stop.is_none() && !walk.window_done)
        .then(|| walk.expected.or(log_start).unwrap_or(0));
    let offset = status(query.offset, log_start, log_end, &walk);
    Ok(RecordsReport {
        log_start,
        log_end,
        records: walk.records,
        stop: walk.stop,
        offset,
        seek: walk.seek.unwrap_or(seek(segments[first].id)),
        startup,
        actions,
    })
}

/// Startup's plan for the shard, as a verdict and the writes it would make.
fn plan(dir: &Path, label: &str, config: &LogConfig) -> Result<(Startup, Vec<Action>)> {
    let offloaded = offload::manifest::load(dir)?;
    let plan = recovery::plan_recovery(dir, label, config, &offloaded)?;
    Ok(verdict(&plan))
}

fn status(
    offset: Offset,
    log_start: Option<Offset>,
    log_end: Option<Offset>,
    walk: &Walk,
) -> OffsetStatus {
    if walk.records.iter().any(|record| record.offset == offset) {
        OffsetStatus::Found
    } else if let Some(log_start) = log_start.filter(|start| offset < *start) {
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

/// The segments startup would keep: those on disk less the ones its plan
/// removes (rollover leftovers, and what a cut retired segment orphans).
fn list_segments(dir: &Path, label: &str, actions: &[Action]) -> Result<Vec<SegmentFile>> {
    let removed = |id: SegmentId| {
        actions.iter().any(|action| match action {
            Action::RemoveSegment { segment, .. } => *segment == id,
            Action::CutRetired { drops_next, .. } => *drops_next == Some(id),
            _ => false,
        })
    };
    let mut segments = Vec::new();
    for id in recovery::discover_segment_ids(dir)? {
        if removed(id) {
            continue;
        }
        let header = match read_segment_header(&dir.join(segment_file_name(id)), id, label) {
            Ok(header) => Ok(header),
            Err(StorageError::Corruption(detail)) => Err(detail),
            Err(err) => return Err(err),
        };
        segments.push(SegmentFile { id, header });
    }
    Ok(segments)
}

/// Whether startup cuts a retired segment at `position` and keeps the segment
/// after it.
fn retired_cut_continues(actions: &[Action], id: SegmentId, position: u64) -> bool {
    actions.iter().any(|action| {
        matches!(action, Action::CutRetired { segment, valid_bytes, drops_next: None }
            if *segment == id && *valid_bytes == position)
    })
}

/// Whether startup cuts segment `id` at `position`: a torn tail of the active
/// segment, or an unsealed retired segment back to its last intact record.
fn startup_cuts(actions: &[Action], id: SegmentId, position: u64) -> bool {
    actions.iter().any(|action| match action {
        Action::TruncateTail {
            segment,
            position: at,
            ..
        } => *segment == id && *at == position,
        Action::CutRetired {
            segment,
            valid_bytes,
            ..
        } => *segment == id && *valid_bytes == position,
        _ => false,
    })
}

/// State carried from one segment to the next.
struct Walk<'a> {
    dir: &'a Path,
    actions: &'a [Action],
    /// The newest segment, the only one a write can still be finishing.
    newest: SegmentId,
    /// Whether to keep the records, or only check them.
    keep: bool,
    payload: bool,
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

impl<'a> Walk<'a> {
    fn new(
        dir: &'a Path,
        actions: &'a [Action],
        newest: SegmentId,
        keep: bool,
        lo: Offset,
        hi: Offset,
    ) -> Self {
        Self {
            dir,
            actions,
            newest,
            keep,
            payload: false,
            lo,
            hi,
            records: Vec::new(),
            expected: None,
            stop: None,
            window_done: false,
            seek: None,
        }
    }

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
            && previous != header.base_offset
        {
            return self.fail(
                id,
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
                return self.fail(id, position, expected, cause);
            }
            let record_header = match RecordHeader::decode_in(&buf, header.version) {
                Ok(record_header) => record_header,
                Err(err) => return self.fail(id, position, expected, err.kind),
            };
            if let Err(err) = check_offset_continuity(expected, record_header.offset) {
                return self.fail(id, position, expected, err.kind);
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
                return self.fail(id, position, expected, cause);
            }
            let decoded = match decode_record(&buf) {
                Ok((decoded, _)) => decoded,
                Err(err) => return self.fail(id, position, expected, err.kind),
            };
            if self.keep {
                self.records.push(RecordView {
                    segment: id,
                    position,
                    offset: decoded.header.offset,
                    timestamp_micros: decoded.header.timestamp_micros,
                    mark: decoded.mark,
                    payload_len: decoded.payload.len() as u64,
                    checksum: decoded.header.checksum,
                    publisher: decoded.publisher.as_deref().map(<[u8]>::to_vec),
                    payload: self.payload.then(|| {
                        let keep = decoded.payload.len().min(PAYLOAD_PREVIEW_BYTES);
                        decoded.payload[..keep].to_vec()
                    }),
                });
            }
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

    /// End the read at the record at `position`, unless startup cuts it and
    /// goes on. Whether that is a torn tail
    /// is startup's call, made by the plan, so it cannot disagree with
    /// `inspect segments`. A record cut short at the end of the newest
    /// segment counts as one too: startup would cut it, and it can have
    /// appeared since the plan was made.
    fn fail(
        &mut self,
        id: SegmentId,
        position: u64,
        expected: Offset,
        cause: CorruptionKind,
    ) -> Result<()> {
        // Startup cuts a retired segment's torn bytes and carries on into the
        // segment after it, so the read does the same.
        if retired_cut_continues(self.actions, id, position) {
            self.expected = Some(expected);
            return Ok(());
        }
        let unfinished = id == self.newest && matches!(cause, CorruptionKind::Truncated { .. });
        self.stop = Some(Stop {
            segment: id,
            position,
            expected_offset: expected,
            cause,
            tail: unfinished || startup_cuts(self.actions, id, position),
        });
        Ok(())
    }
}

fn read_at(file: &File, buf: &mut [u8], position: u64) -> Result<usize> {
    Ok(crate::io::read_at(file, buf, position)?)
}

#[cfg(test)]
mod tests;

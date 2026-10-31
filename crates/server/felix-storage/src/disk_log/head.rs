//! The head floor a log's trim records set, and the `head` file that spares
//! an open from scanning for them.
//!
//! The floor is derived, never stored as the truth. Each trim record the log
//! holds is one [`HeadEntry`]; the floor is the largest floor among the
//! entries below the apply bound (durable, and committed on a log held at its
//! commit offset). The `head` file is a snapshot of the entries below an
//! offset, written at a roll like the producer snapshot. It is checked
//! against the records it names at open, and a missing, short, stale or
//! corrupt one is rebuilt by reading the trim records themselves. See
//! `docs/durable-storage.md`, "Conditional head trim".

use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::segments::SegmentSet;
use crate::log::{LogRecord, Offset, RecordMark};
use crate::segment::ReadBudget;
use crate::segment::TrimRecords;

/// `"FLHD"`, so a stray file in a shard directory is not mistaken for one.
const HEAD_MAGIC: u32 = 0x464C_4844;
const HEAD_VERSION: u16 = 1;
/// magic(4) + version(2) + as_of(8) + count(4)
const HEAD_HEADER_LEN: usize = 18;
/// offset(8) + floor(8) + headers length(4)
const HEAD_ENTRY_LEN: usize = 20;
/// crc32 over everything before it.
const HEAD_CRC_LEN: usize = 4;

/// One trim record the log holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HeadEntry {
    pub(super) offset: Offset,
    pub(super) floor: Offset,
    pub(super) headers: Bytes,
}

impl HeadEntry {
    /// The entry for `record`, if it is a trim.
    pub(super) fn of(record: &LogRecord) -> Option<Self> {
        match record.mark {
            RecordMark::Trim { floor } => Some(Self {
                offset: record.offset,
                floor,
                headers: record.payload.clone(),
            }),
            _ => None,
        }
    }
}

/// Every trim record a log holds at or above its base, and which of them
/// apply.
#[derive(Debug, Default, Clone)]
pub(super) struct Head {
    /// Ascending by offset.
    entries: Vec<HeadEntry>,
    /// How many entries, from the first, are below the apply bound. The
    /// bound only rises between truncations, so this is a prefix.
    applied: usize,
    /// The first applied entry with the largest floor: the one that set it.
    setter: Option<usize>,
}

impl Head {
    pub(super) fn new(entries: Vec<HeadEntry>) -> Self {
        debug_assert!(entries.is_sorted_by(|a, b| a.offset < b.offset));
        Self {
            entries,
            applied: 0,
            setter: None,
        }
    }

    /// Reads below this are refused. Zero when no trim applies.
    pub(super) fn floor(&self) -> Offset {
        self.setter().map_or(0, |entry| entry.floor)
    }

    /// The applied entry that set the floor.
    pub(super) fn setter(&self) -> Option<&HeadEntry> {
        self.setter.map(|index| &self.entries[index])
    }

    pub(super) fn entries(&self) -> &[HeadEntry] {
        &self.entries
    }

    /// Whether an entry waits for the apply bound to pass it.
    pub(super) fn has_pending(&self) -> bool {
        self.applied < self.entries.len()
    }

    /// Note trim records just written, above every entry held.
    pub(super) fn observe(&mut self, entries: impl IntoIterator<Item = HeadEntry>) {
        for entry in entries {
            debug_assert!(
                self.entries
                    .last()
                    .is_none_or(|last| last.offset < entry.offset)
            );
            self.entries.push(entry);
        }
    }

    /// Apply every entry below `bound`.
    pub(super) fn apply(&mut self, bound: Offset) {
        while let Some(entry) = self.entries.get(self.applied)
            && entry.offset < bound
        {
            // Strictly above, so the setter is the first entry with the floor.
            if entry.floor > self.floor() {
                self.setter = Some(self.applied);
            }
            self.applied += 1;
        }
    }

    /// Forget the entries at or after `offset`, for a truncation.
    pub(super) fn truncate_from(&mut self, offset: Offset) {
        let keep = self.entries.partition_point(|entry| entry.offset < offset);
        self.entries.truncate(keep);
        self.applied = self.applied.min(keep);
        self.recompute();
    }

    /// Forget the entries below `base`, whose records were deleted.
    pub(super) fn prune_below(&mut self, base: Offset) {
        let gone = self.entries.partition_point(|entry| entry.offset < base);
        if gone == 0 {
            return;
        }
        self.entries.drain(..gone);
        self.applied = self.applied.saturating_sub(gone);
        self.recompute();
    }

    fn recompute(&mut self) {
        self.setter = None;
        for index in 0..self.applied {
            if self.entries[index].floor > self.floor() {
                self.setter = Some(index);
            }
        }
    }

    /// Sealed segments wholly below this may be deleted: the floor, but
    /// never past the record that set it, so the floor can always be read
    /// back from the log. `None` when no trim applies.
    pub(super) fn delete_bound(&self) -> Option<Offset> {
        self.setter().map(|entry| entry.floor.min(entry.offset))
    }
}

/// The entries `segments` holds, from the `head` file in `dir` where it can
/// be trusted and from the trim records themselves otherwise. Also returns
/// whether sealed segments had to be read, in which case the caller saves a
/// fresh file.
///
/// `active_has_trims` is whether recovery's scan of the active segment met a
/// trim; when it did not, the active segment is not read again.
pub(super) fn rebuild(
    dir: &Path,
    segments: &SegmentSet,
    active_has_trims: bool,
) -> crate::Result<(Head, bool)> {
    let base = segments.base_offset();
    let tail = segments.tail_offset();
    let active_base = segments.active().base_offset();
    let saved = load(dir).filter(|(as_of, entries)| {
        *as_of <= tail
            && entries.iter().all(|entry| entry.offset < *as_of)
            && verified(segments, entries, base)
    });
    let (mut entries, from) = match saved {
        Some((as_of, entries)) => (
            entries
                .into_iter()
                .filter(|entry| entry.offset >= base)
                .collect::<Vec<_>>(),
            as_of.max(base),
        ),
        None => (Vec::new(), segments.first_trimmable_offset().max(base)),
    };
    let to = if active_has_trims { tail } else { active_base };
    let read_sealed = from < active_base;
    if from < to {
        entries.extend(
            segments
                .trim_records(from, to)?
                .iter()
                .filter_map(HeadEntry::of),
        );
    }
    Ok((Head::new(entries), read_sealed))
}

/// Whether every saved entry at or above `base` is the trim record the log
/// holds at its offset. A truncation removes the file before anything is
/// written past the cut, so this catches what that cannot: a file damaged
/// in a way its checksum missed, or copied beside the wrong log.
fn verified(segments: &SegmentSet, entries: &[HeadEntry], base: Offset) -> bool {
    let ascending = entries.is_sorted_by(|a, b| a.offset < b.offset);
    ascending
        && entries
            .iter()
            .filter(|entry| entry.offset >= base)
            .all(|entry| {
                let budget = ReadBudget::new(0, 1).with_trims(TrimRecords::Include);
                segments.read(entry.offset, budget).is_ok_and(|records| {
                    records
                        .first()
                        .and_then(HeadEntry::of)
                        .is_some_and(|found| found == *entry)
                })
            })
}

pub(super) fn head_file_name() -> &'static str {
    "head"
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(head_file_name())
}

pub(super) fn encode(as_of: Offset, entries: &[HeadEntry]) -> Vec<u8> {
    let headers: usize = entries.iter().map(|entry| entry.headers.len()).sum();
    let mut out = Vec::with_capacity(
        HEAD_HEADER_LEN + entries.len() * HEAD_ENTRY_LEN + headers + HEAD_CRC_LEN,
    );
    out.extend_from_slice(&HEAD_MAGIC.to_be_bytes());
    out.extend_from_slice(&HEAD_VERSION.to_be_bytes());
    out.extend_from_slice(&as_of.to_be_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for entry in entries {
        out.extend_from_slice(&entry.offset.to_be_bytes());
        out.extend_from_slice(&entry.floor.to_be_bytes());
        out.extend_from_slice(&(entry.headers.len() as u32).to_be_bytes());
        out.extend_from_slice(&entry.headers);
    }
    let crc = crate::segment::format::crc32(&[&out]);
    out.extend_from_slice(&crc.to_be_bytes());
    out
}

/// The entries and the offset they are as of, or `None` for anything that
/// does not decode in full.
pub(super) fn decode(bytes: &[u8]) -> Option<(Offset, Vec<HeadEntry>)> {
    let (body, crc) = bytes.split_last_chunk::<HEAD_CRC_LEN>()?;
    if crate::segment::format::crc32(&[body]) != u32::from_be_bytes(*crc) {
        return None;
    }
    let mut cursor = Cursor(body);
    if cursor.u32()? != HEAD_MAGIC || cursor.u16()? != HEAD_VERSION {
        return None;
    }
    let as_of = cursor.u64()?;
    let count = cursor.u32()? as usize;
    let mut entries = Vec::with_capacity(count.min(body.len() / HEAD_ENTRY_LEN));
    for _ in 0..count {
        let offset = cursor.u64()?;
        let floor = cursor.u64()?;
        let len = cursor.u32()? as usize;
        let headers = Bytes::copy_from_slice(cursor.take(len)?);
        entries.push(HeadEntry {
            offset,
            floor,
            headers,
        });
    }
    cursor.0.is_empty().then_some((as_of, entries))
}

struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let (taken, rest) = self.0.split_at_checked(len)?;
        self.0 = rest;
        Some(taken)
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }
}

/// The saved entries, or `None` when there is no usable file. Unreadable is
/// the same as absent: the log has the answer.
fn load(dir: &Path) -> Option<(Offset, Vec<HeadEntry>)> {
    let bytes = std::fs::read(path_in(dir)).ok()?;
    let decoded = decode(&bytes);
    if decoded.is_none() {
        tracing::warn!(
            dir = %dir.display(),
            "the head file is unreadable; rebuilding the head floor from the log",
        );
    }
    decoded
}

/// Save the entries below `as_of`, through a temporary and a rename so a
/// reader never sees half a file.
///
/// Not flushed. A file lost to a crash costs a longer open, and any file
/// that survives describes records still in the log, because a truncation
/// removes it durably before anything is written past the cut.
pub(super) fn store(dir: &Path, as_of: Offset, entries: &[HeadEntry]) -> std::io::Result<()> {
    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, encode(as_of, entries))?;
    std::fs::rename(&temporary, &path)
}

/// Remove the file, when the records it describes are no longer the log's.
/// The caller syncs the directory.
pub(super) fn discard(dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path_in(dir)) {
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => Err(err),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests;

//! The sparse offset index that sits beside each segment.
//!
//! One entry every `index_spacing_bytes` of segment data, mapping a logical
//! offset to the byte position where that record starts. A read binary-searches
//! the entries to find a floor, then scans forward a bounded distance — so
//! locating an offset costs O(log n) plus at most one spacing interval of I/O,
//! rather than a scan from the head of the log.
//!
//! Index files are an accelerator and are never trusted on their own: entries are
//! only ever used as a *starting position* for a scan that re-validates real
//! records, and any index that fails to load is rebuilt from its segment.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use crate::Result;
use crate::log::Offset;
use crate::segment::format::{
    INDEX_ENTRY_LEN, INDEX_HEADER_LEN, IndexEntry, IndexHeader, SEGMENT_HEADER_LEN,
};

/// The in-memory form of a segment's sparse index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseIndex {
    base_offset: Offset,
    entries: Vec<IndexEntry>,
}

impl SparseIndex {
    pub fn new(base_offset: Offset) -> Self {
        Self {
            base_offset,
            entries: Vec::new(),
        }
    }

    /// Load an index file, returning `None` when it is absent or unusable.
    ///
    /// A `None` here is not an error: the caller rebuilds from the segment,
    /// which is the same work a first-ever open would do.
    pub fn load(path: &Path, base_offset: Offset) -> Option<Self> {
        let mut buf = Vec::new();
        File::open(path).ok()?.read_to_end(&mut buf).ok()?;
        let header = IndexHeader::decode(&buf).ok()?;
        if header.base_offset != base_offset {
            // Index belongs to a different segment generation; rebuild.
            return None;
        }

        let mut index = Self::new(base_offset);
        let mut at = INDEX_HEADER_LEN as usize;
        // A torn final entry is expected after a crash — stop at the last whole
        // one instead of discarding the file.
        while at + (INDEX_ENTRY_LEN as usize) <= buf.len() {
            let entry = IndexEntry::decode(&buf[at..]).ok()?;
            index.push(entry);
            at += INDEX_ENTRY_LEN as usize;
        }
        Some(index)
    }

    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Record that `offset` begins at byte `position`.
    ///
    /// Entries must ascend in both offset and position; anything else is
    /// dropped rather than corrupting the search invariant. Both halves matter,
    /// and only the offset was checked: an index file is bytes on disk like any
    /// other, so a corrupt or hostile one can pair ascending offsets with a
    /// position of zero, and [`SparseIndex::seek_position`] would hand that
    /// back as somewhere to decode forward from — inside the segment header,
    /// where the next thing read is a header field interpreted as a record.
    ///
    /// A real index cannot contain such an entry: records occupy distinct
    /// ascending byte ranges after the header, so position ascends exactly when
    /// offset does. Dropping the entry keeps the promise `seek_position` makes
    /// while leaving everything before it usable, which is the same bargain the
    /// torn-tail case already strikes.
    pub fn push(&mut self, entry: IndexEntry) {
        if entry.position < SEGMENT_HEADER_LEN {
            return;
        }
        if let Some(last) = self.entries.last()
            && (entry.offset <= last.offset || entry.position <= last.position)
        {
            return;
        }
        self.entries.push(entry);
    }

    /// Byte position to start scanning from when looking for `offset`.
    ///
    /// Returns the position of the greatest indexed offset that is `<= offset`,
    /// falling back to the first record in the segment when the index has no
    /// entry that early. The result is always a valid record boundary, so the
    /// caller can decode forward from it.
    pub fn seek_position(&self, offset: Offset) -> u64 {
        match self.entries.binary_search_by(|e| e.offset.cmp(&offset)) {
            Ok(idx) => self.entries[idx].position,
            // `Err(idx)` is the insertion point, so `idx - 1` is the floor.
            Err(0) => SEGMENT_HEADER_LEN,
            Err(idx) => self.entries[idx - 1].position,
        }
    }

    /// Write the whole index out, replacing whatever was there.
    ///
    /// Used after a rebuild. Incremental appends during normal operation go
    /// through [`IndexWriter`].
    pub fn persist(&self, path: &Path) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(
            &IndexHeader {
                base_offset: self.base_offset,
            }
            .encode(),
        )?;
        for entry in &self.entries {
            writer.write_all(&entry.encode())?;
        }
        writer.flush()?;
        crate::io::sync_all(&writer.into_inner().map_err(|e| e.into_error())?)?;
        Ok(())
    }
}

/// Appends index entries to an index file as its segment grows.
///
/// The writer decides *when* to emit an entry (every `spacing_bytes` of segment
/// data, plus one for the segment's first record); [`SparseIndex`] decides how
/// entries are searched.
#[derive(Debug)]
pub struct IndexWriter {
    file: File,
    index: SparseIndex,
    spacing_bytes: u64,
    /// Segment bytes written since the last entry was emitted.
    bytes_since_entry: u64,
}

impl IndexWriter {
    /// Open an index for a segment that has just been created.
    ///
    /// Skips the fsync that [`Self::open`] performs, because there is nothing
    /// yet to make durable: the file holds a header and no entries. That
    /// matters because this runs while installing a rollover, under the lock
    /// appends contend on, where an fsync costs milliseconds and would put back
    /// exactly the stall that preparing the segment ahead of time removes.
    ///
    /// Safe for the same reason a stale index is safe anywhere: indexes are
    /// derived data, rebuilt from the segment whenever they are missing, short
    /// or inconsistent. Losing this write costs a rebuild, never a record.
    pub fn create(path: &Path, index: SparseIndex, spacing_bytes: u64) -> Result<Self> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.write_all(
            &IndexHeader {
                base_offset: index.base_offset(),
            }
            .encode(),
        )?;
        drop(file);
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Self {
            file,
            index,
            spacing_bytes: spacing_bytes.max(1),
            bytes_since_entry: 0,
        })
    }

    /// Open `path` for appending, seeding in-memory state from `index`.
    pub fn open(path: &Path, index: SparseIndex) -> Result<Self> {
        // The index was just rebuilt or loaded, so rewrite it whole and append
        // from there. This is what makes a stale index self-correcting.
        index.persist(path)?;
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Self {
            file,
            index,
            // Replaced by `with_spacing`; a zero here would emit an entry per
            // record, so start from the documented default instead.
            spacing_bytes: 4 * 1024,
            bytes_since_entry: 0,
        })
    }

    pub fn with_spacing(mut self, spacing_bytes: u64) -> Self {
        // Zero would mean "index every record", which defeats the point of a
        // sparse index and unbounds the file's size.
        self.spacing_bytes = spacing_bytes.max(1);
        self
    }

    pub fn index(&self) -> &SparseIndex {
        &self.index
    }

    /// Offer a record boundary to the index.
    ///
    /// Emits an entry when the spacing threshold has been crossed, otherwise
    /// just accumulates. `record_len` is the record's full on-disk size.
    pub fn observe_record(&mut self, offset: Offset, position: u64, record_len: u64) -> Result<()> {
        let first_entry = self.index.is_empty();
        if first_entry || self.bytes_since_entry >= self.spacing_bytes {
            let entry = IndexEntry { offset, position };
            self.index.push(entry);
            self.file.write_all(&entry.encode())?;
            self.bytes_since_entry = 0;
        }
        self.bytes_since_entry = self.bytes_since_entry.saturating_add(record_len);
        Ok(())
    }

    /// Flush buffered entries to the OS. Not an fsync: the index is rebuildable,
    /// so paying for a second device sync per append would buy nothing.
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    /// Durably persist the index. Used when sealing a segment, where the extra
    /// sync is amortised over the whole segment.
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        crate::io::sync_all(&self.file)?;
        Ok(())
    }

    pub fn into_index(self) -> SparseIndex {
        self.index
    }
}

#[cfg(test)]
mod tests;

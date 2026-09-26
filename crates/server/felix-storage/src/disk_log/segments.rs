//! The set of segments that make up one shard's log: many sealed, exactly one
//! active.
//!
//! This is where rollover, offset-to-segment routing and truncation live. It is
//! entirely synchronous and holds no locks of its own — `DiskLog` owns the lock
//! and calls in.

mod rollover;
mod truncation;

#[cfg(test)]
mod test_support;

pub(super) use rollover::RollOutcome;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::io::read_at;
use crate::log::{AppendRecord, LogConfig, LogRecord, Offset, SegmentDescriptor, SegmentId};
use crate::segment::{
    ReadBudget, SegmentReader, SegmentWriter, SparseIndex, index_file_name, segment_file_name,
};
use crate::{Result, StorageError, metrics_names};

/// Everything on disk for one shard.
#[derive(Debug)]
pub(super) struct SegmentSet {
    dir: PathBuf,
    label: String,
    config: LogConfig,
    /// Ordered by `base_offset`, oldest first.
    sealed: Vec<SealedEntry>,
    active: SegmentWriter,
    /// Read handle on the active segment. Recreated on every roll.
    active_reader: Arc<SegmentReader>,
    /// Bumped by every truncation and reset. A read planned under the lock and
    /// run without it compares this afterwards: a cut in between can put new
    /// records at the byte positions the read was walking.
    generation: u64,
    /// Next free segment id.
    ///
    /// Atomic because `roll_plan` runs under a *read* lock — it has to, so
    /// appends continue while a replacement is built — and two plans racing on
    /// the same id would both try to create the same file, with the loser
    /// failing on `create_new`.
    next_segment_id: AtomicU64,
}

impl SegmentSet {
    /// Take ownership of an already recovered set of segments.
    pub(super) fn new(
        dir: PathBuf,
        label: String,
        config: LogConfig,
        sealed: Vec<SealedEntry>,
        active: SegmentWriter,
    ) -> Result<Self> {
        let active_reader = Arc::new(SegmentReader::open(
            active.path(),
            active.id(),
            active.base_offset(),
        )?);
        let next_segment_id = AtomicU64::new(active.id() + 1);
        metrics::gauge!(metrics_names::SEGMENT_COUNT).set((sealed.len() + 1) as f64);
        Ok(Self {
            dir,
            label,
            config,
            sealed,
            active,
            active_reader,
            generation: 0,
            next_segment_id,
        })
    }

    /// Offset the next appended record will take.
    pub(super) fn tail_offset(&self) -> Offset {
        self.active.next_offset()
    }

    /// The first offset a producer mark can be at: the base of the oldest v3
    /// segment. Everything below it was written by a build without marks.
    pub(super) fn first_markable_offset(&self) -> Offset {
        self.sealed
            .iter()
            .find(|entry| entry.holds_marks)
            .map(|entry| entry.descriptor.base_offset)
            .unwrap_or_else(|| self.active.base_offset())
    }

    /// Oldest offset still readable. Rises only when segments are deleted.
    pub(super) fn base_offset(&self) -> Offset {
        self.sealed
            .first()
            .map(|entry| entry.descriptor.base_offset)
            .unwrap_or_else(|| self.active.base_offset())
    }

    pub(super) fn active(&self) -> &SegmentWriter {
        &self.active
    }

    pub(super) fn active_mut(&mut self) -> &mut SegmentWriter {
        &mut self.active
    }

    /// Every segment, oldest first.
    pub(super) fn descriptors(&self) -> Vec<SegmentDescriptor> {
        self.sealed
            .iter()
            .map(|entry| entry.descriptor.clone())
            .chain(std::iter::once(self.active.descriptor()))
            .collect()
    }

    /// Append a batch, rolling to a new segment first if it would not fit.
    ///
    /// A batch is never split across segments: offsets stay contiguous either
    /// way, but keeping a batch whole means one `write` call and one index
    /// update per append regardless of where the boundary falls.
    pub(super) fn append(&mut self, records: &[AppendRecord]) -> Result<(Offset, Offset)> {
        // An empty active segment must accept the batch even when it is
        // oversized — otherwise a record larger than `segment_size_bytes` could
        // never be written at all. Such a record gets a segment to itself and
        // the next append rolls again.
        //
        // Normally `DiskLog::append` has already rolled off-thread by this
        // point; this is the fallback for a roll that became necessary in the
        // window since that check, and for callers that drive `SegmentSet`
        // directly.
        if self.would_roll(records) {
            self.roll()?;
        }
        self.active.append(records)
    }

    /// Read records from `start` onward, spending at most `budget`.
    ///
    /// Walks segments in offset order, so results are strictly ascending with no
    /// duplicates and no gaps inside the data that is present.
    pub(super) fn read(&self, start: Offset, mut budget: ReadBudget) -> Result<Vec<LogRecord>> {
        self.read_plan(start)
            .execute(start, &mut budget, &self.label)
    }

    /// Everything a read from `start` needs, copied out so the read itself can
    /// run without the lock: appends must not wait behind a cold `pread`.
    pub(super) fn read_plan(&self, start: Offset) -> ReadPlan {
        let mut spans = Vec::new();
        if start < self.tail_offset() {
            // Segments entirely below the requested start are skipped.
            let first = self
                .sealed
                .partition_point(|entry| entry.next_offset() <= start);
            for entry in &self.sealed[first..] {
                spans.push(ReadSpan {
                    reader: Arc::clone(&entry.reader),
                    position: entry.index.seek_position(start),
                    valid_bytes: entry.descriptor.size_bytes,
                });
            }
            if self.active.next_offset() > start {
                spans.push(ReadSpan {
                    reader: Arc::clone(&self.active_reader),
                    position: self.active.index().seek_position(start),
                    valid_bytes: self.active.size_bytes(),
                });
            }
        }
        ReadPlan {
            generation: self.generation,
            spans,
        }
    }

    /// Changes whenever a truncation or reset may have rewritten bytes a
    /// planned read could be walking.
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    /// Seal the active segment and report a verifiable summary of it.
    pub(super) fn seal_active(&mut self) -> Result<(SegmentDescriptor, u64)> {
        let descriptor = self.active.seal()?;
        let checksum = checksum_file(self.active.path())?;
        Ok((descriptor, checksum))
    }

    /// Delete whole sealed segments from the head until the configured
    /// retention bounds are satisfied.
    ///
    /// Head-only and whole-segment: partial segments are never rewritten, which
    /// is what lets recovery keep trusting "valid bytes end at EOF". The active
    /// segment is never a candidate, so a log always retains at least the
    /// records written since the last roll.
    ///
    /// Advancing `base_offset` is a side effect of removing the head entry, and
    /// both happen under the caller's write lock — so a reader either sees a
    /// segment and can read it, or sees a raised base offset and gets
    /// `Trimmed`. It never sees a descriptor whose file is gone.
    pub(super) fn enforce_retention(&mut self, now_micros: u64) -> Result<RetentionOutcome> {
        let max_bytes = self.config.retention_bytes;
        let max_age_micros = self
            .config
            .retention_age
            .map(|age| age.as_micros().min(u128::from(u64::MAX)) as u64);
        let mut outcome = RetentionOutcome::default();
        if max_bytes.is_none() && max_age_micros.is_none() {
            outcome.base_offset = self.base_offset();
            return Ok(outcome);
        }

        let mut total_bytes: u64 = self
            .sealed
            .iter()
            .map(|entry| entry.descriptor.size_bytes)
            .sum::<u64>()
            + self.active.size_bytes();

        while let Some(head) = self.sealed.first() {
            let id = head.descriptor.id;
            let size = head.descriptor.size_bytes;

            let over_size = max_bytes.is_some_and(|max| total_bytes > max);
            let too_old = match max_age_micros {
                // The newest record decides: a segment is only expired once
                // every record in it is, so nothing younger than the bound goes.
                Some(max_age) => match self.newest_timestamp(head)? {
                    Some(newest) => now_micros.saturating_sub(newest) > max_age,
                    // A sealed segment always holds a record; treat an
                    // unreadable timestamp as "keep" rather than deleting on
                    // missing evidence.
                    None => false,
                },
                None => false,
            };
            if !over_size && !too_old {
                break;
            }

            // Drop the entry first: it owns the reader's descriptor, and
            // closing before unlinking keeps this correct on platforms that
            // refuse to remove an open file.
            self.sealed.remove(0);
            self.remove_segment_files(id)?;
            total_bytes = total_bytes.saturating_sub(size);
            outcome.segments_deleted += 1;
            outcome.bytes_reclaimed += size;
        }

        outcome.base_offset = self.base_offset();
        if outcome.segments_deleted > 0 {
            metrics::counter!(metrics_names::RETENTION_SEGMENTS_DELETED_TOTAL)
                .increment(outcome.segments_deleted as u64);
            metrics::counter!(metrics_names::RETENTION_BYTES_RECLAIMED_TOTAL)
                .increment(outcome.bytes_reclaimed);
            metrics::gauge!(metrics_names::SEGMENT_COUNT).set((self.sealed.len() + 1) as f64);
        }
        metrics::gauge!(metrics_names::RETENTION_BASE_OFFSET).set(outcome.base_offset as f64);
        Ok(outcome)
    }

    /// Timestamp of the newest record in a sealed segment.
    fn newest_timestamp(&self, entry: &SealedEntry) -> Result<Option<u64>> {
        let mut out = Vec::new();
        let mut budget = ReadBudget::new(usize::MAX, 1);
        entry.reader.read_from(
            &entry.index,
            entry.descriptor.last_offset,
            entry.descriptor.size_bytes,
            &mut budget,
            &self.label,
            &mut out,
        )?;
        Ok(out.first().map(|record| record.timestamp_micros))
    }

    fn bump_next_segment_id(&self, at_least: SegmentId) {
        self.next_segment_id.fetch_max(at_least, Ordering::AcqRel);
    }

    fn remove_segment_files(&self, id: SegmentId) -> Result<()> {
        for path in [
            self.dir.join(segment_file_name(id)),
            self.dir.join(index_file_name(id)),
        ] {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(StorageError::Io(err)),
            }
        }
        Ok(())
    }
}

/// A read planned under the segment lock, to run without it.
#[derive(Debug)]
pub(super) struct ReadPlan {
    pub(super) generation: u64,
    spans: Vec<ReadSpan>,
}

#[derive(Debug)]
struct ReadSpan {
    reader: Arc<SegmentReader>,
    /// A record boundary at or before the read's start, from the index.
    position: u64,
    /// Bytes of the segment that held records when the plan was made.
    valid_bytes: u64,
}

impl ReadPlan {
    pub(super) fn execute(
        &self,
        start: Offset,
        budget: &mut ReadBudget,
        label: &str,
    ) -> Result<Vec<LogRecord>> {
        let mut out = Vec::new();
        for span in &self.spans {
            if budget.is_spent() {
                break;
            }
            span.reader.read_from_position(
                span.position,
                start,
                span.valid_bytes,
                budget,
                label,
                &mut out,
            )?;
        }
        Ok(out)
    }
}

/// A finished segment: immutable bytes plus the index needed to seek into them.
#[derive(Debug)]
pub(super) struct SealedEntry {
    pub descriptor: SegmentDescriptor,
    pub index: SparseIndex,
    pub reader: Arc<SegmentReader>,
    /// A v3 segment, which may hold producer marks. A v2 one cannot, so a
    /// rebuild of producer state never has to read it.
    pub holds_marks: bool,
}

impl SealedEntry {
    /// Offset one past the last record, matching `SegmentWriter::next_offset`.
    fn next_offset(&self) -> Offset {
        // A sealed segment always holds at least one record, so `last_offset`
        // is real rather than the empty-segment placeholder.
        self.descriptor.last_offset + 1
    }
}

/// What one retention pass reclaimed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionOutcome {
    pub segments_deleted: usize,
    pub bytes_reclaimed: u64,
    /// Oldest offset still readable after the pass.
    pub base_offset: Offset,
}

/// CRC-32 of an entire file, streamed in fixed chunks.
pub(super) fn checksum_file(path: &Path) -> Result<u64> {
    let file = std::fs::File::open(path)?;
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = vec![0u8; 64 * 1024];
    let mut position = 0u64;
    loop {
        let read = read_at(&file, &mut buf, position)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        position += read as u64;
    }
    Ok(u64::from(hasher.finalize()))
}

#[cfg(test)]
mod tests;

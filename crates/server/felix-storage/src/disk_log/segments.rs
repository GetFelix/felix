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

pub(super) use rollover::{PreparedSegment, RollOutcome};
#[cfg(all(test, target_os = "linux"))]
pub(super) use truncation::stop_after_unlinks;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) use super::sealed::SealedEntry;
use super::sealed::{SealedFiles, SealedLocator};
use crate::io::read_at;
use crate::log::{AppendRecord, LogConfig, LogRecord, Offset, SegmentDescriptor, SegmentId};
use crate::segment::writer::StagedAppend;
use crate::segment::{
    ReadBudget, SegmentReader, SegmentWriter, index_file_name, segment_file_name,
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
    /// Where the sealed segments' files and indexes are opened, shared with
    /// every other log under the same root.
    files: Arc<SealedFiles>,
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
    /// Set by `DiskLog::close`. Checked by everything that changes the files,
    /// under the write lock those changes take, so once it is set and the lock
    /// released nothing more is written here and another log may open them.
    closed: bool,
}

impl SegmentSet {
    /// Take ownership of an already recovered set of segments.
    pub(super) fn new(
        dir: PathBuf,
        label: String,
        config: LogConfig,
        sealed: Vec<SealedEntry>,
        active: SegmentWriter,
        files: Arc<SealedFiles>,
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
            files,
            active,
            active_reader,
            generation: 0,
            next_segment_id,
            closed: false,
        })
    }

    #[cfg(test)]
    pub(super) fn open_sealed_segments(&self) -> usize {
        self.files.len()
    }

    /// Refuse every later change to the files, and let go of the sealed
    /// segments' cached handles.
    pub(super) fn close(&mut self) {
        self.closed = true;
        for entry in &self.sealed {
            self.files.remove(entry.key());
        }
    }

    /// Fails once the log is closed. Every path that writes, renames or
    /// deletes a file checks this under the write lock first.
    pub(super) fn check_open(&self) -> Result<()> {
        if self.closed {
            return Err(StorageError::Closed(self.label.clone()));
        }
        Ok(())
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

    /// Append a batch, rolling to a new segment first if it would not fit, for
    /// tests that drive the set directly. The log stages and writes in two
    /// steps instead; see `append`.
    ///
    /// A batch is never split across segments: offsets stay contiguous either
    /// way, but keeping a batch whole means one `write` call and one index
    /// update per append regardless of where the boundary falls. An empty
    /// active segment accepts the batch even when it is oversized, or a record
    /// larger than `segment_size_bytes` could never be written at all.
    #[cfg(test)]
    pub(super) fn append(&mut self, records: &[AppendRecord]) -> Result<(Offset, Offset)> {
        self.check_open()?;
        if self.would_roll_within(records, false) {
            self.roll_for(records)?;
        }
        self.active.append(records)
    }

    /// Encode `records` at the end of the active segment without writing
    /// them. See [`SegmentWriter::stage`]; the caller has already rolled if
    /// [`Self::would_roll_within`] said to.
    pub(super) fn stage(&mut self, records: &[AppendRecord]) -> Result<StagedAppend> {
        self.check_open()?;
        self.active.stage(records)
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
                spans.push(ReadSpan::Sealed(
                    entry.locator(&self.dir, self.config.index_spacing_bytes),
                ));
            }
            if self.active.next_offset() > start {
                spans.push(ReadSpan::Active {
                    reader: Arc::clone(&self.active_reader),
                    position: self.active.index().seek_position(start),
                    valid_bytes: self.active.size_bytes(),
                });
            }
        }
        ReadPlan {
            generation: self.generation,
            spans,
            files: Arc::clone(&self.files),
        }
    }

    /// Changes whenever a truncation or reset may have rewritten bytes a
    /// planned read could be walking.
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    /// Seal the active segment and report a verifiable summary of it.
    pub(super) fn seal_active(&mut self) -> Result<(SegmentDescriptor, u64)> {
        self.check_open()?;
        let descriptor = self.active.seal()?;
        let checksum = checksum_file(self.active.path())?;
        Ok((descriptor, checksum))
    }

    /// The sealed segments retention may look at, oldest first, and the log's
    /// size. Copied out so the pass can read timestamps without the lock.
    pub(super) fn retention_plan(&self) -> RetentionPlan {
        RetentionPlan {
            total_bytes: self
                .sealed
                .iter()
                .map(|entry| entry.descriptor.size_bytes)
                .sum::<u64>()
                + self.active.size_bytes(),
            candidates: self
                .sealed
                .iter()
                .map(|entry| entry.locator(&self.dir, self.config.index_spacing_bytes))
                .collect(),
            files: Arc::clone(&self.files),
        }
    }

    /// Drop `ids` from the head of the list, stopping at the first that is no
    /// longer there: something else changed the log since they were chosen.
    /// Returns what was dropped; the caller deletes the files.
    ///
    /// Advancing `base_offset` is a side effect of removing the head entry,
    /// and this runs under the caller's write lock, so a reader either sees a
    /// segment and can read it, or sees a raised base offset and gets
    /// `Trimmed`. A read planned before this is redone because the generation
    /// moves, so none trusts a file about to be deleted.
    pub(super) fn remove_head(&mut self, ids: &[SegmentId]) -> Result<Vec<SegmentDescriptor>> {
        self.check_open()?;
        let mut removed = Vec::new();
        for id in ids {
            if self.sealed.first().map(|entry| entry.descriptor.id) != Some(*id) {
                break;
            }
            let entry = self.sealed.remove(0);
            self.forget(&entry);
            removed.push(entry.descriptor);
        }
        if !removed.is_empty() {
            self.generation += 1;
            metrics::gauge!(metrics_names::SEGMENT_COUNT).set((self.sealed.len() + 1) as f64);
        }
        Ok(removed)
    }

    /// Drop a sealed entry's cached handle. Every path that takes an entry
    /// out of the list calls this.
    pub(super) fn forget(&self, entry: &SealedEntry) {
        self.files.remove(entry.key());
    }

    fn bump_next_segment_id(&self, at_least: SegmentId) {
        self.next_segment_id.fetch_max(at_least, Ordering::AcqRel);
    }

    fn remove_segment_files(&self, id: SegmentId) -> Result<()> {
        remove_segment_files(&self.dir, id)
    }
}

/// A read planned under the segment lock, to run without it.
#[derive(Debug)]
pub(super) struct ReadPlan {
    pub(super) generation: u64,
    spans: Vec<ReadSpan>,
    files: Arc<SealedFiles>,
}

#[derive(Debug)]
enum ReadSpan {
    /// Opened, and its index consulted, only when the read reaches it.
    Sealed(SealedLocator),
    Active {
        reader: Arc<SegmentReader>,
        /// A record boundary at or before the read's start, from the index.
        position: u64,
        /// Bytes of the segment that held records when the plan was made.
        valid_bytes: u64,
    },
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
            match span {
                ReadSpan::Sealed(locator) => {
                    let handle = locator.open(&self.files, label)?;
                    handle.reader.read_from(
                        &handle.index,
                        start,
                        locator.valid_bytes(),
                        budget,
                        label,
                        &mut out,
                    )?;
                }
                ReadSpan::Active {
                    reader,
                    position,
                    valid_bytes,
                } => reader.read_from_position(
                    *position,
                    start,
                    *valid_bytes,
                    budget,
                    label,
                    &mut out,
                )?,
            }
        }
        Ok(out)
    }
}

/// The sealed segments a retention pass may delete, captured under the lock.
#[derive(Debug)]
pub(super) struct RetentionPlan {
    total_bytes: u64,
    candidates: Vec<SealedLocator>,
    files: Arc<SealedFiles>,
}

impl RetentionPlan {
    /// The head segments the configured bounds say to delete, oldest first.
    ///
    /// Head-only and whole-segment: partial segments are never rewritten,
    /// which is what lets recovery keep trusting "valid bytes end at EOF". The
    /// active segment is never a candidate, so a log always retains at least
    /// the records written since the last roll. Runs without the lock: an age
    /// check reads the newest record of each candidate, which may be cold.
    pub(super) fn choose(
        &self,
        config: &LogConfig,
        now_micros: u64,
        label: &str,
    ) -> Result<Vec<SegmentId>> {
        let max_bytes = config.retention_bytes;
        let max_age_micros = config
            .retention_age
            .map(|age| age.as_micros().min(u128::from(u64::MAX)) as u64);
        let mut total_bytes = self.total_bytes;
        let mut chosen = Vec::new();
        for candidate in &self.candidates {
            let over_size = max_bytes.is_some_and(|max| total_bytes > max);
            let too_old = match max_age_micros {
                // The newest record decides: a segment is only expired once
                // every record in it is, so nothing younger than the bound goes.
                Some(max_age) => match candidate.newest_timestamp(&self.files, label)? {
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
            total_bytes = total_bytes.saturating_sub(candidate.valid_bytes());
            chosen.push(candidate.id());
        }
        Ok(chosen)
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

/// Delete a segment and its index. Either may already be gone.
pub(super) fn remove_segment_files(dir: &Path, id: SegmentId) -> Result<()> {
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

//! The append side of a single segment.
//!
//! A writer owns one data file plus its index and knows nothing about rollover,
//! retention, or fsync policy — it exposes `append` and `sync` and lets
//! `crate::disk_log` decide when to call them.
//!
//! Performance shape, and why:
//!
//! * **One `write` per batch, not per record.** Records are encoded into a
//!   reusable staging buffer and handed to the kernel in a single call. Syscall
//!   count is the thing that scales with batch size otherwise, and it dominates
//!   at small payloads.
//! * **`write` and `sync` are separate.** A `write` lands in the page cache and
//!   is cheap; only `sync` touches the device. Keeping them apart is what lets
//!   the log amortise one device flush across many appends (see
//!   `disk_log::sync`), which is the single largest lever on durable throughput.
//! * **Blocks are reserved up front.** See `crate::io::preallocate`.
//! * **The staging buffer is never freed.** Steady-state appends do no
//!   allocation at all beyond growing it once to the high-water batch size.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::Result;
use crate::io::{preallocate, sync_data, sync_dir, write_segment_at};
use crate::log::{AppendRecord, Offset, RecordMark, SegmentDescriptor, SegmentId};
use crate::segment::format::{
    BASELINE_VERSION, FORMAT_VERSION, MAX_PAYLOAD_BYTES, SEGMENT_HEADER_LEN, SegmentHeader,
    encode_record,
};
use crate::segment::index::{IndexPlan, IndexWriter, SparseIndex};
use crate::segment::{index_file_name, segment_file_name};
use crate::{StorageError, metrics_names};

/// The active segment: the only file in a shard that accepts writes.
#[derive(Debug)]
pub struct SegmentWriter {
    id: SegmentId,
    base_offset: Offset,
    path: PathBuf,
    file: File,
    index: IndexWriter,
    /// Bytes handed to the kernel, i.e. the file's logical length.
    size_bytes: u64,
    /// Bytes known to be on stable storage.
    synced_bytes: u64,
    /// Offset the next appended record will take.
    next_offset: Offset,
    record_count: u64,
    /// Reused across appends so steady state allocates nothing.
    staging: Vec<u8>,
    /// A duplicate descriptor for work that runs without the lock that guards
    /// `file`: flushes, and a staged batch's write.
    sync_handle: Arc<File>,
    /// Set when this writer can no longer make truthful claims about the
    /// segment: an append that could not be rolled back, so its real length is
    /// unknown, or a failed sync.
    ///
    /// A failed sync counts because Linux may drop the dirty pages it could not
    /// write, and the *next* fsync then returns success having flushed nothing.
    /// Carrying on would report durability for bytes that are gone.
    poisoned: bool,
    /// Test-only: make the next `sync` take the failure path.
    #[cfg(test)]
    fail_next_sync: bool,
    /// Set when an index write has failed. Purely informational: the index is
    /// rebuilt from the segment on the next open, so the log stays correct.
    index_degraded: bool,
    /// The layout version in the segment header. A segment reopened from an
    /// older build is rolled before a record it cannot hold goes in: that
    /// build reading it would take the new flag bit for a bad length.
    version: u16,
}

impl SegmentWriter {
    /// Create a brand new segment starting at `base_offset`.
    pub fn create(
        dir: &Path,
        id: SegmentId,
        base_offset: Offset,
        created_at_micros: u64,
        preallocate_bytes: u64,
        index_spacing_bytes: u64,
    ) -> Result<Self> {
        Self::create_at_version(
            dir,
            id,
            base_offset,
            created_at_micros,
            preallocate_bytes,
            index_spacing_bytes,
            BASELINE_VERSION,
        )
    }

    /// [`Self::create`] with the header at `version`.
    pub(crate) fn create_at_version(
        dir: &Path,
        id: SegmentId,
        base_offset: Offset,
        created_at_micros: u64,
        preallocate_bytes: u64,
        index_spacing_bytes: u64,
        version: u16,
    ) -> Result<Self> {
        let mut writer = BlankSegment::create(dir, id, preallocate_bytes, index_spacing_bytes)?
            .activate(base_offset, created_at_micros, version)?;
        // The header must be durable before any record claims to live here. The
        // directory entry already is: `BlankSegment::create` synced it.
        sync_data(&writer.file)?;
        writer.mark_synced(SEGMENT_HEADER_LEN);
        Ok(writer)
    }

    /// Reopen an existing, already validated segment for further appends.
    ///
    /// Trusts `resume` and does not re-validate.
    pub fn reopen(
        dir: &Path,
        id: SegmentId,
        resume: ResumeState,
        index_spacing_bytes: u64,
    ) -> Result<Self> {
        let ResumeState {
            base_offset,
            valid_bytes,
            next_offset,
            record_count,
            index,
            version,
        } = resume;
        let path = dir.join(segment_file_name(id));
        let file = OpenOptions::new().write(true).open(&path)?;
        // Recovery has already decided where valid data ends; make the file
        // agree so an append cannot land after a hole. Appends are positioned
        // writes at `size_bytes`, so the cursor does not matter.
        file.set_len(valid_bytes)?;
        sync_data(&file)?;

        let index = IndexWriter::open(&dir.join(index_file_name(id)), index)?
            .with_spacing(index_spacing_bytes);
        let sync_handle = Arc::new(file.try_clone()?);

        Ok(Self {
            id,
            base_offset,
            path,
            file,
            index,
            size_bytes: valid_bytes,
            synced_bytes: valid_bytes,
            next_offset,
            record_count,
            staging: Vec::new(),
            sync_handle,
            poisoned: false,
            #[cfg(test)]
            fail_next_sync: false,
            index_degraded: false,
            version,
        })
    }

    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    pub fn next_offset(&self) -> Offset {
        self.next_offset
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Whether a record with a producer mark may be appended here.
    pub fn holds_marks(&self) -> bool {
        self.version >= 3
    }

    /// Whether a generation-start record may be appended here.
    pub fn holds_generation_starts(&self) -> bool {
        self.version >= 4
    }

    /// Whether a commit record may be appended here.
    pub fn holds_commits(&self) -> bool {
        self.version >= 5
    }

    /// The layout version in the segment header.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// The version a segment following this one is written at so that it
    /// holds `records`: never below this one's, v5 when `records` include a
    /// commit record, v4 when they include a generation-start record, and
    /// [`BASELINE_VERSION`] otherwise. A log moves up only when it has to, so
    /// a build that predates a version reads it until then.
    pub fn successor_version(&self, records: &[AppendRecord]) -> u16 {
        let needed = records
            .iter()
            .map(|record| match record.mark {
                RecordMark::Commit => FORMAT_VERSION,
                RecordMark::GenerationStart => 4,
                _ => BASELINE_VERSION,
            })
            .max()
            .unwrap_or(BASELINE_VERSION);
        needed.max(self.version).max(BASELINE_VERSION)
    }

    /// Whether every record in `records` may be appended here.
    pub fn holds(&self, records: &[AppendRecord]) -> bool {
        records.iter().all(|record| match record.mark {
            RecordMark::None => true,
            RecordMark::Opens(_) | RecordMark::Continues => self.holds_marks(),
            RecordMark::GenerationStart => self.holds_generation_starts(),
            RecordMark::Commit => self.holds_commits(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn index(&self) -> &SparseIndex {
        self.index.index()
    }

    #[cfg(test)]
    pub(crate) fn fail_next_sync(&mut self) {
        self.fail_next_sync = true;
    }

    /// True once a failed sync or an unrecoverable write has left this
    /// segment's on-disk state unknown.
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// True when every byte written so far is on stable storage.
    pub fn is_synced(&self) -> bool {
        self.synced_bytes >= self.size_bytes
    }

    /// Bytes written but not yet synced — the exposure window of the current
    /// fsync policy.
    pub fn unsynced_bytes(&self) -> u64 {
        self.size_bytes.saturating_sub(self.synced_bytes)
    }

    pub fn descriptor(&self) -> SegmentDescriptor {
        SegmentDescriptor {
            id: self.id,
            base_offset: self.base_offset,
            // An empty segment has no last offset; report the base so the range
            // is empty rather than wrapping below zero.
            last_offset: self.next_offset.saturating_sub(1).max(self.base_offset),
            size_bytes: self.size_bytes,
        }
    }

    /// A second descriptor for the same file, for flushing off the write path.
    pub fn sync_handle(&self) -> Arc<File> {
        Arc::clone(&self.sync_handle)
    }

    /// How large this segment would become if `records` were appended.
    ///
    /// Used by rollover to decide *before* writing, so a batch is never split
    /// across two segments.
    pub fn projected_size(&self, records: &[AppendRecord]) -> u64 {
        records.iter().fold(self.size_bytes, |acc, record| {
            acc + crate::segment::format::record_len(record.payload.len(), &record.mark)
        })
    }

    /// Append a batch, assigning consecutive offsets from `next_offset`.
    ///
    /// The bytes reach the page cache before this returns; they are *not*
    /// durable until [`SegmentWriter::sync`] succeeds. Callers that promise
    /// durability must sequence the two.
    pub fn append(&mut self, records: &[AppendRecord]) -> Result<(Offset, Offset)> {
        let staged = self.stage(records)?;
        if let Err(err) = staged.write() {
            return Err(self.abandon(staged, err));
        }
        let (first_offset, last_offset, index) = self.finish(staged);
        if let Err(err) = index.write() {
            self.index_write_failed(first_offset, &err);
        }
        Ok((first_offset, last_offset))
    }

    /// The first half of [`Self::append`]: encode `records` at the end of the
    /// segment and work out their offsets, changing nothing yet.
    ///
    /// The split lets the write run without the lock that guards this writer,
    /// so a write the kernel stalls does not stall everything else that needs
    /// the lock. That is sound only while nothing else changes this segment
    /// between `stage` and [`Self::finish`] or [`Self::abandon`], which the
    /// caller guarantees by staging from one thread at a time. The index
    /// entries [`Self::finish`] hands back are written after it, so an
    /// abandoned batch leaves none behind.
    pub(crate) fn stage(&mut self, records: &[AppendRecord]) -> Result<StagedAppend> {
        debug_assert!(!records.is_empty());

        if self.poisoned {
            return Err(StorageError::Unsupported(
                "segment writer is poisoned after a failed append could not be rolled back",
            ));
        }

        for record in records {
            if record.payload.len() > MAX_PAYLOAD_BYTES as usize {
                return Err(StorageError::Unsupported(
                    "record payload exceeds the maximum supported size",
                ));
            }
        }
        if !self.holds(records) {
            return Err(StorageError::Unsupported(
                "the record's mark is newer than the segment's version; roll first",
            ));
        }

        let mut bytes = std::mem::take(&mut self.staging);
        bytes.clear();
        // Record boundaries for the index, captured while encoding so the index
        // never needs a second pass over the batch.
        let mut boundaries = Vec::with_capacity(records.len());
        let mut position = self.size_bytes;
        let mut offset = self.next_offset;
        for record in records {
            let written = encode_record(
                &mut bytes,
                offset,
                record.timestamp_micros,
                &record.payload,
                &record.mark,
            );
            boundaries.push((offset, position, written));
            position += written;
            offset += 1;
        }
        Ok(StagedAppend {
            index: self.index.plan(&boundaries),
            bytes,
            position: self.size_bytes,
            file: Arc::clone(&self.sync_handle),
            first_offset: self.next_offset,
            next_offset: offset,
            records: records.len() as u64,
        })
    }

    /// Take a staged batch whose write succeeded into this writer's state.
    /// Returns its first and last offsets, and the index entries it adds for
    /// the caller to write to the index file, with no lock held.
    pub(crate) fn finish(&mut self, staged: StagedAppend) -> (Offset, Offset, IndexPlan) {
        let StagedAppend {
            bytes,
            position,
            index,
            first_offset,
            next_offset,
            records,
            ..
        } = staged;
        debug_assert_eq!(position, self.size_bytes, "staged against another tail");
        self.size_bytes = position + bytes.len() as u64;
        self.next_offset = next_offset;
        self.record_count += records;
        self.index.apply(&index);

        metrics::counter!(metrics_names::APPEND_RECORDS_TOTAL).increment(records);
        metrics::counter!(metrics_names::APPEND_BYTES_TOTAL).increment(bytes.len() as u64);
        metrics::histogram!(metrics_names::APPEND_BATCH_RECORDS).record(records as f64);
        self.staging = bytes;

        (first_offset, next_offset - 1, index)
    }

    /// Report a failed write of the index entries for the batch at
    /// `first_offset`.
    ///
    /// The index is an accelerator, not a record of truth: a missing or stale
    /// one is rebuilt from the segment on open, and every read re-validates
    /// the records it lands on. So an index write that fails must not fail the
    /// append. Returning `Err` would be actively harmful: the data write has
    /// already succeeded and the offsets are already spent, so the error would
    /// look retryable to a caller who cannot retry. Retrying appends the batch
    /// a second time under new offsets, and not retrying leaves a publish
    /// reported as failed that is in fact durably stored.
    pub(crate) fn index_write_failed(&mut self, first_offset: Offset, err: &std::io::Error) {
        if self.index_degraded {
            return;
        }
        self.index_degraded = true;
        tracing::warn!(
            segment = self.id,
            offset = first_offset,
            error = %err,
            "sparse index write failed; the index will be rebuilt on next open"
        );
        metrics::counter!(metrics_names::INDEX_WRITE_FAILURES_TOTAL).increment(1);
    }

    /// Undo a staged batch whose write failed, or that its caller no longer
    /// wants, and return the error to report.
    ///
    /// A failed write can still have landed some of the batch: the write loops
    /// over partial writes, so an error means "some prefix landed", not
    /// "nothing happened". Left alone, those bytes sit past the last record
    /// this writer knows about and the next append lands after the debris,
    /// turning a failed write into interior corruption that recovery must
    /// refuse to start on, or into a duplicate if the caller retries.
    ///
    /// So the file goes back to the last good byte, and no offset is spent. If
    /// that fails there is no way to restore the invariant, and the writer
    /// refuses further appends rather than building on a file whose shape it no
    /// longer knows.
    pub(crate) fn abandon(&mut self, staged: StagedAppend, err: std::io::Error) -> StorageError {
        self.staging = staged.bytes;
        match self.rewind_after_failed_write() {
            Ok(()) => StorageError::Io(err),
            Err(rewind) => rewind,
        }
    }

    /// Flush every written byte to stable storage.
    ///
    /// Cheap and idempotent when nothing has changed since the last call, which
    /// matters because the periodic syncer polls on a timer regardless of load.
    pub fn sync(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(StorageError::Unsupported(
                "segment writer is poisoned; durability cannot be vouched for",
            ));
        }
        if self.is_synced() {
            return Ok(());
        }
        let pending = self.size_bytes;
        let started = std::time::Instant::now();
        #[cfg(test)]
        let result = if std::mem::take(&mut self.fail_next_sync) {
            Err(std::io::Error::other("injected"))
        } else {
            sync_data(&self.file)
        };
        #[cfg(not(test))]
        let result = sync_data(&self.file);
        if let Err(err) = result {
            // Poisoned rather than merely reported: the pages may be gone, and
            // a later sync would return success having flushed nothing.
            self.poisoned = true;
            return Err(StorageError::SyncFailed(err.to_string()));
        }
        // The index is rebuildable, so it gets a flush but not a device sync.
        self.index.flush()?;
        self.synced_bytes = pending;

        metrics::counter!(metrics_names::SYNC_TOTAL).increment(1);
        metrics::histogram!(metrics_names::SYNC_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());
        Ok(())
    }

    /// Record that everything up to `bytes` is now durable.
    ///
    /// The log-level syncer flushes through a cloned descriptor so it can do so
    /// without holding the writer lock; this is how the result gets back.
    pub fn mark_synced(&mut self, bytes: u64) {
        if self.poisoned {
            return;
        }
        self.synced_bytes = self.synced_bytes.max(bytes.min(self.size_bytes));
    }

    /// Finish this segment: sync data and index, then release any preallocated
    /// blocks past the last record so the file on disk is exactly its contents.
    ///
    /// Takes `&mut self` rather than consuming, so the caller can keep the
    /// sealed writer around to serve reads until it swaps in a replacement.
    pub fn seal(&mut self) -> Result<SegmentDescriptor> {
        self.sync()?;
        self.index.sync()?;
        self.file.set_len(self.size_bytes)?;
        if let Err(err) = sync_data(&self.file) {
            self.poisoned = true;
            return Err(StorageError::SyncFailed(err.to_string()));
        }
        Ok(self.descriptor())
    }

    /// Restore the file to the last byte this writer accounts for.
    ///
    /// Called only after a failed append, so that a partial write leaves no
    /// trace and the segment stays exactly as it was before the attempt.
    fn rewind_after_failed_write(&mut self) -> Result<()> {
        let valid = self.size_bytes;
        match self.file.set_len(valid) {
            Ok(()) => Ok(()),
            Err(err) => {
                // The segment's on-disk shape no longer matches this writer's
                // idea of it, and nothing here can reconcile them.
                self.poisoned = true;
                Err(StorageError::Io(std::io::Error::other(format!(
                    "append failed and the segment could not be rewound to {valid}: {err}"
                ))))
            }
        }
    }
}

/// A batch [`SegmentWriter::stage`] has encoded and placed, to be written with
/// no lock held and then finished or abandoned.
#[derive(Debug)]
pub(crate) struct StagedAppend {
    bytes: Vec<u8>,
    /// Where the batch starts in the segment.
    position: u64,
    file: Arc<File>,
    index: IndexPlan,
    first_offset: Offset,
    /// One past the batch's last offset.
    next_offset: Offset,
    records: u64,
}

impl StagedAppend {
    /// Write the batch. One syscall for the batch: syscall count is what
    /// scales with batch size otherwise, and it dominates at small payloads.
    pub(crate) fn write(&self) -> std::io::Result<()> {
        write_segment_at(&self.file, &self.bytes, self.position)
    }
}

/// A segment file that exists on disk but has no header yet, and so does not
/// yet claim a base offset.
///
/// This split is what lets a rollover be prepared without blocking appends.
/// Everything expensive about creating a segment — the file, its preallocated
/// blocks, its index, and the *directory* fsync that makes the entry durable —
/// happens here, with no lock held. What is left for [`Self::activate`] is a
/// 32-byte write into the page cache.
///
/// The reason the header cannot be written up front is that its `base_offset`
/// must be the log's tail *at the moment of the swap*, and the whole point of
/// preparing ahead is that appends keep advancing that tail meanwhile. Writing
/// the header early would pin the segment to an offset the log has already
/// passed, and the swap would have to be abandoned — which is exactly what
/// makes a prepare-ahead scheme with an early header no faster than rolling
/// inline.
///
/// A crash between `create` and `activate` leaves a headerless file. Recovery
/// treats one as an uninstalled rollover and deletes it; it can hold no
/// records, so nothing acknowledged is at stake.
#[derive(Debug)]
pub(crate) struct BlankSegment {
    id: SegmentId,
    path: PathBuf,
    file: File,
    index_path: PathBuf,
    index_spacing_bytes: u64,
}

impl BlankSegment {
    /// Create the file and its index, and make the directory entry durable.
    pub(crate) fn create(
        dir: &Path,
        id: SegmentId,
        preallocate_bytes: u64,
        index_spacing_bytes: u64,
    ) -> Result<Self> {
        let path = dir.join(segment_file_name(id));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        preallocate(&file, preallocate_bytes)?;
        sync_dir(dir)?;
        Ok(Self {
            id,
            path,
            file,
            index_path: dir.join(index_file_name(id)),
            index_spacing_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Write the header and become a writable segment based at `base_offset`.
    ///
    /// One page-cache write and no flush, so this is safe to call under the
    /// lock that appends contend on. The header reaches disk with the first
    /// sync of this segment, which under every fsync policy happens no later
    /// than the acknowledgement of the first record written to it.
    pub(crate) fn activate(
        mut self,
        base_offset: Offset,
        created_at_micros: u64,
        version: u16,
    ) -> Result<SegmentWriter> {
        self.file.write_all(
            &SegmentHeader::at_version(base_offset, created_at_micros, version).encode(),
        )?;
        let index = IndexWriter::create(
            &self.index_path,
            SparseIndex::new(base_offset),
            self.index_spacing_bytes,
        )?;
        let sync_handle = Arc::new(self.file.try_clone()?);
        Ok(SegmentWriter {
            id: self.id,
            base_offset,
            path: self.path,
            file: self.file,
            index,
            size_bytes: SEGMENT_HEADER_LEN,
            synced_bytes: 0,
            next_offset: base_offset,
            record_count: 0,
            staging: Vec::new(),
            sync_handle,
            poisoned: false,
            #[cfg(test)]
            fail_next_sync: false,
            index_degraded: false,
            version,
        })
    }

    /// Delete a blank segment that will never be activated, durably.
    pub(crate) fn discard(self) -> Result<()> {
        let Self {
            path,
            file,
            index_path,
            ..
        } = self;
        drop(file);
        let dir = path.parent().map(Path::to_path_buf);
        for path in [path, index_path] {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(StorageError::Io(err)),
            }
        }
        // `create` synced the directory entry, so an unsynced unlink can come
        // back after a power loss as an empty file in the middle of the chain.
        if let Some(dir) = dir {
            sync_dir(&dir)?;
        }
        Ok(())
    }
}

/// State a recovered segment resumes from, as produced by
/// [`crate::segment::scan_segment`].
///
/// Grouped into one type because the fields are only meaningful together: a
/// `valid_bytes` from one scan paired with a `next_offset` from another would
/// silently corrupt the segment.
#[derive(Debug)]
pub struct ResumeState {
    pub base_offset: Offset,
    pub valid_bytes: u64,
    pub next_offset: Offset,
    pub record_count: u64,
    pub index: SparseIndex,
    /// From the segment header: see [`SegmentWriter::holds`].
    pub version: u16,
}

#[cfg(test)]
mod tests;

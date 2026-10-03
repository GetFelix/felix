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
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::Result;
use crate::io::{preallocate, sync_data, sync_dir};
use crate::log::{AppendRecord, Offset, RecordMark, SegmentDescriptor, SegmentId};
use crate::segment::format::{
    BASELINE_VERSION, FORMAT_VERSION, MAX_PAYLOAD_BYTES, SEGMENT_HEADER_LEN, SegmentHeader,
    encode_record,
};
use crate::segment::index::{IndexWriter, SparseIndex, UnwrittenEntries};
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
        let created = BlankSegment::create(dir, id, preallocate_bytes, index_spacing_bytes)?
            .activate(base_offset, created_at_micros, version)
            .and_then(|mut writer| {
                // The header must be durable before any record claims to live
                // here. The directory entry already is: `BlankSegment::create`
                // synced it.
                sync_data(&writer.file)?;
                writer.mark_synced(SEGMENT_HEADER_LEN);
                Ok(writer)
            });
        if created.is_err() {
            remove_unused_segment(dir, id);
        }
        created
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
        // agree so an append cannot land after a hole, and position the write
        // cursor there — a freshly opened handle starts at zero and would
        // otherwise overwrite the segment header.
        let mut file = file;
        file.set_len(valid_bytes)?;
        file.seek(SeekFrom::Start(valid_bytes))?;
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
        let written = staged.write();
        let (offsets, index) = self.finish(staged, written)?;
        index.write(self.id);
        Ok(offsets)
    }

    /// The first step of [`Self::append`], split out so a log can write a
    /// batch without holding the lock around this writer: encode `records`
    /// for the end of the segment and change nothing. Nothing else may change
    /// this segment until [`Self::finish`].
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
        for (offset, record) in (self.next_offset..).zip(records) {
            let written = encode_record(
                &mut bytes,
                offset,
                record.timestamp_micros,
                &record.payload,
                &record.mark,
            );
            boundaries.push((offset, position, written));
            position += written;
        }
        Ok(StagedAppend {
            bytes,
            boundaries,
            file: Arc::clone(&self.sync_handle),
        })
    }

    /// Take in a batch from [`Self::stage`] once its write has returned
    /// `written`. Returns its first and last offsets, and the index entries
    /// it added, for the caller to write once it has released the lock.
    pub(crate) fn finish(
        &mut self,
        staged: StagedAppend,
        written: std::io::Result<()>,
    ) -> Result<((Offset, Offset), UnwrittenEntries)> {
        let StagedAppend {
            bytes, boundaries, ..
        } = staged;
        let len = bytes.len() as u64;
        self.staging = bytes;

        // A failed write can still have written some of the buffer: `write_all`
        // loops over partial writes, so an error means "some prefix landed",
        // not "nothing happened". Left alone, those bytes sit past the last
        // record this writer knows about, the file cursor points past them, and
        // the next append lands after the debris - turning a failed write into
        // interior corruption that recovery must refuse to start on, or into a
        // duplicate if the caller retries.
        //
        // So a failed append rewinds the file to the last good byte. If the
        // rewind itself fails there is no way to restore the invariant, and the
        // writer refuses further appends rather than building on a file whose
        // shape it no longer knows.
        if let Err(err) = written {
            self.rewind_after_failed_write()?;
            return Err(err.into());
        }

        let first_offset = self.next_offset;
        self.size_bytes += len;
        self.next_offset += boundaries.len() as u64;
        self.record_count += boundaries.len() as u64;
        for &(offset, position, written) in &boundaries {
            self.index.observe_record(offset, position, written);
        }

        metrics::counter!(metrics_names::APPEND_RECORDS_TOTAL).increment(boundaries.len() as u64);
        metrics::counter!(metrics_names::APPEND_BYTES_TOTAL).increment(len);
        metrics::histogram!(metrics_names::APPEND_BATCH_RECORDS).record(boundaries.len() as f64);

        Ok((
            (first_offset, self.next_offset - 1),
            self.index.take_unwritten(),
        ))
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
        let restore = self
            .file
            .set_len(valid)
            .and_then(|()| self.file.seek(SeekFrom::Start(valid)).map(|_| ()));
        match restore {
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

/// A batch [`SegmentWriter::stage`] has encoded, to write and then hand to
/// [`SegmentWriter::finish`].
#[derive(Debug)]
pub(crate) struct StagedAppend {
    bytes: Vec<u8>,
    /// `(offset, position, length)` of each record.
    boundaries: Vec<(Offset, u64, u64)>,
    /// Shares the writer's cursor, which `stage` left at the end of the file.
    file: Arc<File>,
}

impl StagedAppend {
    /// Write the batch, in one syscall: syscall count is what scales with
    /// batch size otherwise, and it dominates at small payloads.
    pub(crate) fn write(&self) -> std::io::Result<()> {
        crate::io::write_all(&self.file, &self.bytes)
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
        if let Err(err) = preallocate(&file, preallocate_bytes).and_then(|()| sync_dir(dir)) {
            // On a full disk the reservation is what fails. A file left behind
            // would be a log's only segment with no header.
            drop(file);
            remove_unused_segment(dir, id);
            return Err(err.into());
        }
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

/// Best effort removal of a segment that never held a record, after its
/// creation failed. One that stays is headerless, and recovery discards it.
fn remove_unused_segment(dir: &Path, id: SegmentId) {
    for path in [
        dir.join(segment_file_name(id)),
        dir.join(index_file_name(id)),
    ] {
        let _ = std::fs::remove_file(path);
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

//! Sealed segments, and the bounded cache of their open files and indexes.
//!
//! A log keeps only a descriptor per sealed segment. The file descriptor and
//! the sparse index a read needs are loaded on first use and held in a
//! [`SealedFiles`] cache shared by every log opened with the same
//! `LogConfig::sealed_cache` (or under one provider), which evicts the
//! least recently used once it holds `LogConfig::max_open_sealed_segments`.
//! Without the bound, memory and descriptors grow with retained data rather
//! than with what is being read: a sparse index is about 1/256 of its segment,
//! and every sealed segment held a descriptor open for the life of the process.
//!
//! Each [`SealedEntry`] has a key no other entry ever reuses, so a read planned
//! before a truncation or a deletion can only ever load into a key nothing will
//! look up again. It cannot put a stale index where a later segment with the
//! same id would find it.

use felix_common::random::HashMap;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::log::{Offset, SegmentDescriptor};
use crate::segment::{
    ReadBudget, SEGMENT_HEADER_LEN, ScanStart, SegmentReader, SparseIndex, TrimRecords,
    index_file_name, scan_segment, segment_file_name,
};
use crate::{Result, metrics_names};

/// Keys for [`SealedEntry`], unique for the life of the process.
static NEXT_KEY: AtomicU64 = AtomicU64::new(1);

/// A finished segment, as the log holds it between reads.
#[derive(Debug)]
pub(crate) struct SealedEntry {
    pub(crate) descriptor: SegmentDescriptor,
    /// A v3 segment, which may hold producer marks. A v2 one cannot, so a
    /// rebuild of producer state never has to read it.
    pub(crate) holds_marks: bool,
    /// A v7 segment, which may hold trim records. An older one cannot, so a
    /// rebuild of the head floor never has to read it.
    pub(crate) holds_trims: bool,
    /// The generation of its last record, `None` before v9: the floor for
    /// the segment after it when a truncation empties that one.
    pub(crate) last_generation: Option<u64>,
    key: u64,
    /// Set once the log lets go of the entry (deleted, truncated, closed).
    /// A read in flight may still open it, and must not cache the handle.
    forgotten: Arc<AtomicBool>,
}

impl SealedEntry {
    pub(crate) fn new(descriptor: SegmentDescriptor, holds_marks: bool, holds_trims: bool) -> Self {
        Self {
            descriptor,
            holds_marks,
            holds_trims,
            last_generation: None,
            key: NEXT_KEY.fetch_add(1, Ordering::Relaxed),
            forgotten: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn with_last_generation(mut self, generation: Option<u64>) -> Self {
        self.last_generation = generation;
        self
    }

    /// Offset one past the last record, matching `SegmentWriter::next_offset`.
    pub(crate) fn next_offset(&self) -> Offset {
        // A sealed segment always holds at least one record, so `last_offset`
        // is real rather than the empty-segment placeholder.
        self.descriptor.last_offset + 1
    }

    /// Everything needed to open this segment later, without the lock that
    /// guards the segment list.
    pub(crate) fn locator(&self, dir: &Path, index_spacing_bytes: u64) -> SealedLocator {
        SealedLocator {
            key: self.key,
            forgotten: Arc::clone(&self.forgotten),
            dir: dir.to_path_buf(),
            descriptor: self.descriptor.clone(),
            index_spacing_bytes,
        }
    }
}

/// Where a sealed segment is, captured under the segment lock so it can be
/// opened after it is released.
#[derive(Debug, Clone)]
pub(crate) struct SealedLocator {
    key: u64,
    forgotten: Arc<AtomicBool>,
    dir: PathBuf,
    descriptor: SegmentDescriptor,
    index_spacing_bytes: u64,
}

impl SealedLocator {
    pub(crate) fn id(&self) -> crate::log::SegmentId {
        self.descriptor.id
    }

    pub(crate) fn valid_bytes(&self) -> u64 {
        self.descriptor.size_bytes
    }

    pub(crate) fn descriptor(&self) -> &SegmentDescriptor {
        &self.descriptor
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.dir.join(segment_file_name(self.descriptor.id))
    }

    /// Timestamp of the segment's first record. Read from just past the
    /// header, so it needs no index.
    pub(crate) fn oldest_timestamp(&self, label: &str) -> Result<Option<u64>> {
        let id = self.descriptor.id;
        let base_offset = self.descriptor.base_offset;
        let reader = SegmentReader::open(&self.path(), id, base_offset)?;
        let mut out = Vec::new();
        // A trim's timestamp is as real as any record's.
        let mut budget = ReadBudget::new(usize::MAX, 1).with_trims(TrimRecords::Include);
        reader.read_from_position(
            SEGMENT_HEADER_LEN,
            base_offset,
            self.valid_bytes(),
            &mut budget,
            label,
            &mut out,
        )?;
        Ok(out.first().map(|record| record.timestamp_micros))
    }

    /// The segment's reader and index, opening them if the cache has neither.
    ///
    /// The open happens outside the cache lock, so a cold segment does not
    /// hold up reads of warm ones. Two readers racing on the same cold segment
    /// may both open it; the first to finish is kept.
    pub(crate) fn open(&self, files: &SealedFiles, label: &str) -> Result<Arc<SealedHandle>> {
        if let Some(handle) = files.get(self.key) {
            return Ok(handle);
        }
        let id = self.descriptor.id;
        let base_offset = self.descriptor.base_offset;
        let path = self.dir.join(segment_file_name(id));
        let reader = SegmentReader::open(&path, id, base_offset)?;
        let index_path = self.dir.join(index_file_name(id));
        let index = match SparseIndex::load(&index_path, base_offset) {
            Some(index) => index,
            None => {
                // Recovery checked the segment at open, so a bad index here is
                // the index's problem. Derived data: rebuild it.
                let outcome = scan_segment(
                    &path,
                    id,
                    label,
                    self.index_spacing_bytes,
                    ScanStart::Full,
                    false,
                )?;
                if let Err(err) = outcome.index.persist(&index_path) {
                    tracing::warn!(shard = label, segment = id, error = %err, "could not rewrite a sealed segment's index");
                }
                if outcome.header.holds_generations()
                    && let Err(err) = outcome.known.persist(
                        &self.dir.join(crate::segment::known::known_file_name(id)),
                        base_offset,
                        outcome.valid_bytes,
                    )
                {
                    tracing::warn!(shard = label, segment = id, error = %err, "could not rewrite a sealed segment's known generations");
                }
                outcome.index
            }
        };
        Ok(files.insert(self.key, &self.forgotten, SealedHandle { reader, index }))
    }

    /// Timestamp of the segment's newest record, for retention by age.
    ///
    /// Uses the cached handle when there is one. Otherwise it opens the file
    /// for this one read and seeks from the index's last entry, rather than
    /// loading a whole index into the cache for a segment that is about to be
    /// deleted.
    pub(crate) fn newest_timestamp(&self, files: &SealedFiles, label: &str) -> Result<Option<u64>> {
        let last_offset = self.descriptor.last_offset;
        let mut out = Vec::new();
        // A trim's timestamp is as real as any record's.
        let mut budget = ReadBudget::new(usize::MAX, 1).with_trims(TrimRecords::Include);
        if let Some(handle) = files.get(self.key) {
            handle.reader.read_from(
                &handle.index,
                last_offset,
                self.valid_bytes(),
                &mut budget,
                label,
                &mut out,
            )?;
        } else {
            let id = self.descriptor.id;
            let base_offset = self.descriptor.base_offset;
            let reader =
                SegmentReader::open(&self.dir.join(segment_file_name(id)), id, base_offset)?;
            let position = SparseIndex::load_last(&self.dir.join(index_file_name(id)), base_offset)
                .filter(|entry| entry.offset <= last_offset)
                .map_or(SEGMENT_HEADER_LEN, |entry| entry.position);
            reader.read_from_position(
                position,
                last_offset,
                self.valid_bytes(),
                &mut budget,
                label,
                &mut out,
            )?;
        }
        Ok(out.first().map(|record| record.timestamp_micros))
    }
}

/// An open sealed segment: its file and its index.
#[derive(Debug)]
pub(crate) struct SealedHandle {
    pub(crate) reader: SegmentReader,
    pub(crate) index: SparseIndex,
}

/// The shared, bounded cache of open sealed segments.
///
/// An evicted handle stays usable by whoever already holds it; the file is
/// closed when the last of them lets go.
#[derive(Debug)]
pub(crate) struct SealedFiles {
    capacity: usize,
    lru: Mutex<Lru>,
}

#[derive(Debug, Default)]
struct Lru {
    entries: HashMap<u64, (Arc<SealedHandle>, u64)>,
    /// Last use to key, oldest first.
    order: BTreeMap<u64, u64>,
    clock: u64,
}

impl SealedFiles {
    /// The cache `config` names, or a new one of its own size.
    pub(crate) fn for_config(config: &crate::log::LogConfig) -> Arc<Self> {
        match &config.sealed_cache {
            Some(shared) => Arc::clone(&shared.0),
            None => Self::new(config.max_open_sealed_segments),
        }
    }

    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity: capacity.max(1),
            lru: Mutex::new(Lru::default()),
        })
    }

    /// How many sealed segments are open right now.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lru.lock().entries.len()
    }

    /// Hand over a handle a writer already has, so the segment it just sealed
    /// is warm for the reader most likely to want it next.
    pub(crate) fn seed(&self, entry: &SealedEntry, handle: SealedHandle) {
        self.insert(entry.key, &entry.forgotten, handle);
    }

    /// Forget one segment: it was deleted, or became the active one again.
    /// The entry can never be cached again, so a read that planned before
    /// this cannot leave a handle on a file the log no longer owns.
    pub(crate) fn forget(&self, entry: &SealedEntry) {
        let mut lru = self.lru.lock();
        entry.forgotten.store(true, Ordering::Release);
        if let Some((_, used)) = lru.entries.remove(&entry.key) {
            lru.order.remove(&used);
            metrics::gauge!(metrics_names::OPEN_SEALED_SEGMENTS).decrement(1.0);
        }
    }

    fn get(&self, key: u64) -> Option<Arc<SealedHandle>> {
        let mut lru = self.lru.lock();
        let lru = &mut *lru;
        let (handle, used) = lru.entries.get_mut(&key)?;
        lru.order.remove(used);
        lru.clock += 1;
        *used = lru.clock;
        lru.order.insert(lru.clock, key);
        Some(Arc::clone(handle))
    }

    fn insert(&self, key: u64, forgotten: &AtomicBool, handle: SealedHandle) -> Arc<SealedHandle> {
        let mut lru = self.lru.lock();
        if let Some((existing, _)) = lru.entries.get(&key) {
            return Arc::clone(existing);
        }
        // Checked under the lock `forget` takes: the reader still gets its
        // handle, but nothing caches it.
        if forgotten.load(Ordering::Acquire) {
            return Arc::new(handle);
        }
        lru.clock += 1;
        let used = lru.clock;
        let handle = Arc::new(handle);
        lru.entries.insert(key, (Arc::clone(&handle), used));
        lru.order.insert(used, key);
        metrics::gauge!(metrics_names::OPEN_SEALED_SEGMENTS).increment(1.0);
        while lru.entries.len() > self.capacity {
            let Some((_, oldest)) = lru.order.pop_first() else {
                break;
            };
            lru.entries.remove(&oldest);
            metrics::gauge!(metrics_names::OPEN_SEALED_SEGMENTS).decrement(1.0);
        }
        handle
    }
}

impl Drop for SealedFiles {
    fn drop(&mut self) {
        let open = self.lru.get_mut().entries.len();
        metrics::gauge!(metrics_names::OPEN_SEALED_SEGMENTS).decrement(open as f64);
    }
}

#[cfg(test)]
mod tests;

//! Cutting a log back: dropping a suffix for replication, or discarding
//! everything to start again at a new base.

use std::sync::Arc;

use super::{SealedEntry, SegmentSet};
use crate::disk_log::now_micros;
use crate::io::sync_dir;
use crate::log::{Offset, SegmentId};
use crate::segment::format::SEGMENT_HEADER_LEN;
use crate::segment::writer::ResumeState;
use crate::segment::{
    ScanStart, SegmentReader, SegmentWriter, SparseIndex, index_file_name, read_segment_header,
    scan_segment, segment_file_name,
};
use crate::{Result, metrics_names};

impl SegmentSet {
    /// Drop every record at or after `offset`.
    ///
    /// Used by replication to discard a divergent suffix. Truncating to at or
    /// beyond the tail is a no-op; truncating below the base offset empties the
    /// log.
    pub(crate) fn truncate(&mut self, offset: Offset) -> Result<()> {
        self.check_open()?;
        if offset >= self.tail_offset() {
            return Ok(());
        }
        self.generation += 1;

        // Whole segments at or after the cut go, the active one first. The
        // newest survivor then becomes the active segment and is cut short.
        if self.active.base_offset() >= offset {
            let active_id = self.active.id();
            let mut doomed = vec![active_id];
            while let Some(entry) = self
                .sealed
                .pop_if(|entry| entry.descriptor.base_offset >= offset)
            {
                self.forget(&entry);
                doomed.push(entry.descriptor.id);
            }
            self.unlink_newest_first(&doomed)?;
            match self.sealed.pop() {
                Some(resume) => self.adopt_sealed_as_active(resume)?,
                None => {
                    // Nothing left at all: restart the log at `offset`.
                    self.replace_active(active_id + 1, offset, SEGMENT_HEADER_LEN, offset, 0)?;
                    return Ok(());
                }
            }
        }
        self.truncate_active_to(offset)
    }

    /// Unlink segments newest first, syncing the directory after each.
    ///
    /// Unsynced, a power loss can keep an older unlink and undo a newer one,
    /// leaving a gap that recovery refuses. In this order it leaves a longer
    /// log ending at a segment boundary. It also makes every unlink durable
    /// before the survivor is cut short, so a crash cannot bring later
    /// segments back next to a shortened predecessor.
    fn unlink_newest_first(&self, ids: &[SegmentId]) -> Result<()> {
        debug_assert!(ids.is_sorted_by(|a, b| a > b), "not newest first: {ids:?}");
        for id in ids {
            #[cfg(all(test, target_os = "linux"))]
            stop_point(&self.dir)?;
            self.remove_segment_files(*id)?;
            sync_dir(&self.dir)?;
        }
        Ok(())
    }

    /// Discard every record and start again, empty, at `base_offset`.
    ///
    /// For a follower rebuilding a shard whose copy is wrong rather than
    /// merely short: nothing here is worth keeping, and the leader's oldest
    /// record is where the new copy begins. Unlike `truncate`, the base may
    /// move in either direction.
    pub(crate) fn reset_to(&mut self, base_offset: Offset) -> Result<()> {
        self.check_open()?;
        self.generation += 1;
        let active_id = self.active.id();
        let mut doomed = vec![active_id];
        while let Some(entry) = self.sealed.pop() {
            self.forget(&entry);
            doomed.push(entry.descriptor.id);
        }
        // All of them before the new segment exists. Its base need not follow
        // the old chain, so an old segment a crash brought back beside it
        // would be a gap. This way a crash leaves a prefix of the old log or
        // the new empty one.
        self.unlink_newest_first(&doomed)?;
        self.replace_active(
            active_id + 1,
            base_offset,
            SEGMENT_HEADER_LEN,
            base_offset,
            0,
        )?;
        sync_dir(&self.dir)?;
        Ok(())
    }

    /// Reopen a sealed segment as the active one so appends resume inside it.
    fn adopt_sealed_as_active(&mut self, entry: SealedEntry) -> Result<()> {
        self.forget(&entry);
        let outcome = scan_segment(
            &self.dir.join(segment_file_name(entry.descriptor.id)),
            entry.descriptor.id,
            &self.label,
            self.config.index_spacing_bytes,
            ScanStart::Full,
            self.config.repair_checksum_tail,
        )?;
        self.active = SegmentWriter::reopen(
            &self.dir,
            entry.descriptor.id,
            ResumeState {
                base_offset: entry.descriptor.base_offset,
                valid_bytes: outcome.valid_bytes,
                next_offset: outcome.next_offset,
                record_count: outcome.record_count,
                index: outcome.index,
                holds_marks: outcome.header.holds_marks(),
            },
            self.config.index_spacing_bytes,
        )?;
        self.active_reader = Arc::new(SegmentReader::open(
            self.active.path(),
            self.active.id(),
            self.active.base_offset(),
        )?);
        Ok(())
    }

    /// Cut the active segment back to `offset`, keeping records below it.
    fn truncate_active_to(&mut self, offset: Offset) -> Result<()> {
        if offset >= self.active.next_offset() {
            return Ok(());
        }

        // Seek with the index and walk headers forward, reading one index
        // interval rather than the whole segment. Offsets are contiguous
        // within a segment, so the count kept follows from the offset.
        let keep_bytes = self.active_reader.position_of(
            self.active.index(),
            offset,
            self.active.size_bytes(),
            &self.label,
        )?;
        let keep_count = offset.saturating_sub(self.active.base_offset());

        let id = self.active.id();
        let base_offset = self.active.base_offset();
        self.replace_active(id, base_offset, keep_bytes, offset, keep_count)
    }

    /// Swap in an active writer over segment `id`, either reopened at
    /// `valid_bytes` or created fresh when the file does not exist.
    fn replace_active(
        &mut self,
        id: SegmentId,
        base_offset: Offset,
        valid_bytes: u64,
        next_offset: Offset,
        record_count: u64,
    ) -> Result<()> {
        let path = self.dir.join(segment_file_name(id));
        self.active = if path.exists() {
            let index = SparseIndex::load(&self.dir.join(index_file_name(id)), base_offset)
                .unwrap_or_else(|| SparseIndex::new(base_offset));
            SegmentWriter::reopen(
                &self.dir,
                id,
                ResumeState {
                    base_offset,
                    valid_bytes,
                    next_offset,
                    record_count,
                    // A truncation invalidates every index entry past the cut;
                    // rebuild from the surviving prefix rather than trusting it.
                    index: rebuild_index_prefix(index, valid_bytes),
                    holds_marks: read_segment_header(&path, id, &self.label)?.holds_marks(),
                },
                self.config.index_spacing_bytes,
            )?
        } else {
            SegmentWriter::create(
                &self.dir,
                id,
                base_offset,
                now_micros(),
                self.config.preallocate_bytes(),
                self.config.index_spacing_bytes,
            )?
        };
        self.active_reader = Arc::new(SegmentReader::open(
            self.active.path(),
            self.active.id(),
            self.active.base_offset(),
        )?);
        self.bump_next_segment_id(id + 1);
        metrics::gauge!(metrics_names::SEGMENT_COUNT).set((self.sealed.len() + 1) as f64);
        Ok(())
    }
}

/// Logs whose next truncation stops after a number of unlinks.
#[cfg(all(test, target_os = "linux"))]
static STOPS: parking_lot::Mutex<Vec<(std::path::PathBuf, usize)>> =
    parking_lot::Mutex::new(Vec::new());

/// Make the next truncation or reset of the log in `dir` fail after
/// `unlinks` unlinks, leaving the directory as a power loss at that moment
/// would find it. Keyed by directory, so parallel tests are unaffected.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn stop_after_unlinks(dir: &std::path::Path, unlinks: usize) {
    STOPS.lock().push((dir.to_path_buf(), unlinks));
}

#[cfg(all(test, target_os = "linux"))]
fn stop_point(dir: &std::path::Path) -> Result<()> {
    let mut stops = STOPS.lock();
    let Some(at) = stops.iter().position(|(stop, _)| stop == dir) else {
        return Ok(());
    };
    if stops[at].1 == 0 {
        stops.remove(at);
        return Err(crate::StorageError::Io(std::io::Error::other(
            "stopped for a simulated power loss",
        )));
    }
    stops[at].1 -= 1;
    Ok(())
}

/// Drop index entries that point past `valid_bytes`.
fn rebuild_index_prefix(index: SparseIndex, valid_bytes: u64) -> SparseIndex {
    let mut rebuilt = SparseIndex::new(index.base_offset());
    for entry in index.entries() {
        if entry.position < valid_bytes {
            rebuilt.push(*entry);
        }
    }
    rebuilt
}

#[cfg(test)]
mod tests;

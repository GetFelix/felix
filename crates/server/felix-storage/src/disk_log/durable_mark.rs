//! How far the active segment is known to be synced.
//!
//! After a power loss the file size can survive while the blocks it covers do
//! not, and those blocks read back as zeros or as whatever the device held
//! before. Recovery cannot tell stale bytes from rot on an acknowledged record
//! by looking at them. It can if it knows where the last successful sync ended:
//! nothing past that point was ever reported durable, so damage there is an
//! unfinished write whatever it looks like.
//!
//! The mark is written after each flush with a positioned write and no sync of
//! its own, because a second device flush per group commit would halve the
//! commit rate. So it can lag after a power loss by however long the
//! filesystem holds a dirty page. It never runs ahead: it is written only
//! after the sync it describes has returned. See `docs/storage-format.md`,
//! "`durable.mark`".

use std::fs::File;
use std::path::Path;

use super::LogInner;
use super::segments::SegmentSet;
use crate::log::SegmentId;
use crate::segment::format::SEGMENT_HEADER_LEN;

/// `"FLSM"`: Felix Segment Mark.
const MARK_MAGIC: u32 = 0x464C_534D;
const MARK_VERSION: u16 = 1;
/// magic(4) + version(2) + reserved(2) + segment(8) + synced(8) + crc(4) +
/// reserved(4). Well inside one sector, so a torn write is unlikely and a
/// failed checksum reads as no mark at all.
pub(super) const MARK_LEN: usize = 32;

/// What the mark file says: segment `segment` was synced through `synced_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableMark {
    pub(crate) segment: SegmentId,
    pub(crate) synced_bytes: u64,
}

impl DurableMark {
    /// Where damage in segment `id` stops being possible rot and becomes an
    /// unfinished write, or `None` when every byte of it was synced (or there
    /// is no mark to say otherwise).
    ///
    /// A segment newer than the mark's was never synced as far as the mark
    /// knows. One older than it was sealed before the mark moved on, and
    /// sealing syncs it whole.
    pub(crate) fn unsynced_from(mark: Option<Self>, id: SegmentId) -> Option<u64> {
        let mark = mark?;
        if id == mark.segment {
            Some(mark.synced_bytes.max(SEGMENT_HEADER_LEN))
        } else if id > mark.segment {
            Some(SEGMENT_HEADER_LEN)
        } else {
            None
        }
    }

    /// How many bytes of segment `id` the mark vouches were synced: all of a
    /// segment older than the mark's, none of one newer, and none at all
    /// without a mark. Recovery must not cut a segment below this.
    pub(crate) fn synced_through(mark: Option<Self>, id: SegmentId) -> u64 {
        match mark {
            Some(mark) if id < mark.segment => u64::MAX,
            Some(mark) if id == mark.segment => mark.synced_bytes,
            _ => 0,
        }
    }

    pub(super) fn encode(&self) -> [u8; MARK_LEN] {
        let mut buf = [0u8; MARK_LEN];
        buf[0..4].copy_from_slice(&MARK_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&MARK_VERSION.to_be_bytes());
        buf[8..16].copy_from_slice(&self.segment.to_be_bytes());
        buf[16..24].copy_from_slice(&self.synced_bytes.to_be_bytes());
        let crc = crc32fast::hash(&buf[0..24]);
        buf[24..28].copy_from_slice(&crc.to_be_bytes());
        buf
    }

    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; MARK_LEN] = bytes.get(..MARK_LEN)?.try_into().ok()?;
        let word = |at: usize| u64::from_be_bytes(bytes[at..at + 8].try_into().expect("8 bytes"));
        if u32::from_be_bytes(bytes[0..4].try_into().ok()?) != MARK_MAGIC
            || u16::from_be_bytes(bytes[4..6].try_into().ok()?) != MARK_VERSION
            || u32::from_be_bytes(bytes[24..28].try_into().ok()?) != crc32fast::hash(&bytes[0..24])
        {
            return None;
        }
        Some(Self {
            segment: word(8),
            synced_bytes: word(16),
        })
    }
}

/// The mark as stored, or `None` when it is absent or unreadable.
///
/// Unreadable is the same as absent, and both mean the strict rules: a shard
/// written before the mark existed has none, and recovery then treats damage
/// exactly as it always has.
pub(crate) fn load(dir: &Path) -> Option<DurableMark> {
    let bytes = std::fs::read(dir.join(mark_file_name())).ok()?;
    DurableMark::decode(&bytes)
}

pub(crate) fn mark_file_name() -> &'static str {
    "durable.mark"
}

/// The open mark file of one log.
#[derive(Debug)]
pub(super) struct MarkFile {
    file: File,
}

impl MarkFile {
    pub(super) fn open(dir: &Path) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(mark_file_name()))?;
        // The repair rule rests on the mark existing after a power loss, and a
        // created file's name is durable only once its directory is synced.
        crate::io::sync_dir(dir)?;
        Ok(Self { file })
    }

    /// Record that `mark` has been synced. Called only after the sync returned.
    pub(super) fn record(&self, mark: DurableMark) -> std::io::Result<()> {
        crate::io::write_at(&self.file, &mark.encode(), 0)
    }

    /// Sync whatever the mark last recorded. For a clean shutdown, so the
    /// next open has an exact mark rather than one a power loss left behind.
    pub(super) fn sync(&self) -> std::io::Result<()> {
        crate::io::sync_data(&self.file)
    }

    /// [`Self::record`], then sync the mark itself. For the rare operations
    /// that move it backwards (truncation, a reset), where a stale mark left
    /// ahead of the log would make recovery refuse a repairable tail.
    pub(super) fn record_durably(&self, mark: DurableMark) -> std::io::Result<()> {
        self.record(mark)?;
        self.sync()
    }
}

impl LogInner {
    /// Record a flush that synced `segment` through `synced_bytes`.
    ///
    /// Best effort: a mark that failed to move is merely behind, which widens
    /// what recovery may repair after a power loss but never loses a record
    /// that the log still has.
    pub(super) fn note_synced(&self, segment: SegmentId, synced_bytes: u64) {
        if let Err(err) = self.mark.record(DurableMark {
            segment,
            synced_bytes,
        }) {
            tracing::warn!(shard = %self.label, error = %err, "could not update the durable mark");
        }
    }

    /// Record that every segment before `active` is sealed, and so synced
    /// whole. Keeps a sealed segment out of the repairable range when the mark
    /// last pointed into it.
    pub(super) fn note_sealed_before(&self, active: SegmentId) {
        self.note_synced(active, SEGMENT_HEADER_LEN);
    }

    /// Move the mark back to the active segment's current end after a
    /// truncation or reset, durably. Called holding the flush lock, with the
    /// active segment just synced.
    pub(super) fn note_rewound(&self, segments: &SegmentSet) -> crate::Result<()> {
        #[cfg(test)]
        if self
            .fail_next_rewind_sync
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(crate::StorageError::SyncFailed(
                "injected fsync failure".to_string(),
            ));
        }
        self.mark.record_durably(DurableMark {
            segment: segments.active().id(),
            synced_bytes: segments.active().size_bytes(),
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

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
//! after the sync it describes has returned. It never moves back either,
//! except on a truncation or reset: a background seal can finish after a
//! flush that covered more, and recording it would leave the mark behind for
//! as long as the shard stays idle. See `docs/storage-format.md`,
//! "`durable.mark`".

use crate::fs::File;
use std::path::Path;

use parking_lot::Mutex;

use super::LogInner;
use super::segments::SegmentSet;
use crate::log::{FsyncMode, SegmentId};
use crate::segment::format::SEGMENT_HEADER_LEN;

/// `"FLSM"`: Felix Segment Mark.
const MARK_MAGIC: u32 = 0x464C_534D;
const MARK_VERSION: u16 = 1;
/// magic(4) + version(2) + mode(1) + reserved(1) + segment(8) + synced(8) +
/// crc(4) + reserved(4). The mode byte is zero in `durable.mark` and names
/// the poisoned run's fsync mode in a poison marker. Well inside one sector, so a torn write is unlikely and a
/// failed checksum reads as no mark at all.
pub(super) const MARK_LEN: usize = 32;

/// What the mark file says: segment `segment` was synced through `synced_bytes`.
///
/// Ordered by segment, then bytes, which is the order the log is synced in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
        self.encode_with_mode(0)
    }

    fn encode_with_mode(&self, mode: u8) -> [u8; MARK_LEN] {
        let mut buf = [0u8; MARK_LEN];
        buf[0..4].copy_from_slice(&MARK_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&MARK_VERSION.to_be_bytes());
        buf[6] = mode;
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
    let bytes = crate::fs::read(dir.join(mark_file_name())).ok()?;
    DurableMark::decode(&bytes)
}

pub(crate) fn mark_file_name() -> &'static str {
    "durable.mark"
}

/// The file a poisoned log leaves behind. It holds the mark as of the
/// poison, in the mark's own encoding.
pub(crate) fn poison_file_name() -> &'static str {
    "poisoned"
}

/// The poison marker's mode byte for each fsync mode. Zero is a marker that
/// does not say, which is read as a relaxed mode.
const POISON_MODE_NONE: u8 = 1;
const POISON_MODE_PERIODIC: u8 = 2;
const POISON_MODE_ON_COMMIT: u8 = 3;

/// What a poison marker says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Poison {
    /// The last point a sync of the poisoned run is known to have reached.
    pub(crate) mark: DurableMark,
    /// Whether the poisoned run acknowledged only synced records. Decided by
    /// that run, not the one reopening the log: a relaxed run acknowledged
    /// records past the mark whatever mode comes next.
    pub(crate) on_commit: bool,
}

impl Poison {
    fn encode(mark: DurableMark, mode: &FsyncMode) -> [u8; MARK_LEN] {
        mark.encode_with_mode(match mode {
            FsyncMode::None => POISON_MODE_NONE,
            FsyncMode::Periodic { .. } => POISON_MODE_PERIODIC,
            FsyncMode::OnCommit => POISON_MODE_ON_COMMIT,
        })
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let mark = DurableMark::decode(bytes)?;
        Some(Self {
            mark,
            on_commit: bytes[6] == POISON_MODE_ON_COMMIT,
        })
    }
}

/// What the poison marker in `dir` says. `None` when there is no marker;
/// `Some(None)` when there is one that cannot be read.
pub(crate) fn load_poison(dir: &Path) -> Option<Option<Poison>> {
    match crate::fs::read(dir.join(poison_file_name())) {
        Ok(bytes) => Some(Poison::decode(&bytes)),
        Err(_) => None,
    }
}

/// The open mark file of one log.
#[derive(Debug)]
pub(super) struct MarkFile {
    file: File,
    dir: std::path::PathBuf,
    /// Held across the write, so two writers cannot land out of order.
    state: Mutex<MarkState>,
    /// Fails the next write of the mark, after its sync succeeded.
    #[cfg(test)]
    pub(super) fail_next_write: std::sync::atomic::AtomicBool,
}

#[derive(Debug, Default)]
struct MarkState {
    /// The furthest mark written since the last rewind.
    last: Option<DurableMark>,
    /// The furthest point a sync reached since the last rewind, kept even
    /// when writing it to the file failed. A poison records this, not
    /// `last`: a cut back to a mark the file lagged at would remove records
    /// that were synced and acknowledged.
    synced: Option<DurableMark>,
    /// How many times the mark has been moved back.
    rewinds: u64,
    /// The thread that wrote it.
    #[cfg(test)]
    writer: Option<String>,
}

impl MarkFile {
    /// Open or create the mark. A created mark's name is durable only once
    /// `dir` is synced, which the caller does before relying on it: the repair
    /// rule rests on the mark existing after a power loss.
    pub(super) fn open(dir: &Path) -> std::io::Result<Self> {
        let file = crate::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(mark_file_name()))?;
        Ok(Self {
            file,
            dir: dir.to_path_buf(),
            state: Mutex::new(MarkState::default()),
            #[cfg(test)]
            fail_next_write: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Leave a marker beside the mark saying the log was poisoned, with the
    /// mark as of now: the last point a sync is known to have reached. Not
    /// synced. A process restart finds it in the page cache, which is the
    /// case it exists for; after a power loss the log is recovered as one
    /// anyway. Syncing could also fail on the device that just failed.
    pub(super) fn record_poison(&self, mode: &FsyncMode) -> std::io::Result<()> {
        use std::io::Write;
        let synced = self.state.lock().synced;
        let bytes = synced.map_or([0u8; MARK_LEN], |mark| Poison::encode(mark, mode));
        let mut file = File::create(self.dir.join(poison_file_name()))?;
        file.write_all(&bytes)
    }

    /// [`MarkFile::record_since`] as of now.
    #[cfg(test)]
    pub(super) fn record(&self, mark: DurableMark) -> std::io::Result<()> {
        self.record_since(mark, self.rewinds())
    }

    /// How many times the mark has been moved back, for
    /// [`MarkFile::record_since`].
    pub(super) fn rewinds(&self) -> u64 {
        self.state.lock().rewinds
    }

    /// Record that `mark` has been synced, unless the mark was moved back
    /// since `rewinds` was read. Called only after the sync returned. A mark
    /// behind the one already recorded is ignored. A flush whose caller
    /// stopped waiting finishes on the flush thread after its lock is gone,
    /// so a truncation can land before it writes: its mark would then point
    /// past the cut.
    pub(super) fn record_since(&self, mark: DurableMark, rewinds: u64) -> std::io::Result<()> {
        let mut state = self.state.lock();
        if state.rewinds != rewinds {
            return Ok(());
        }
        state.synced = state.synced.max(Some(mark));
        if state.last >= Some(mark) {
            return Ok(());
        }
        #[cfg(test)]
        if self
            .fail_next_write
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(std::io::Error::other("injected mark write failure"));
        }
        crate::io::write_at(&self.file, &mark.encode(), 0)?;
        state.last = Some(mark);
        #[cfg(test)]
        {
            state.writer = std::thread::current().name().map(str::to_string);
        }
        Ok(())
    }

    /// The thread that wrote the last mark [`MarkFile::record_since`] recorded.
    #[cfg(test)]
    pub(super) fn writer(&self) -> Option<String> {
        self.state.lock().writer.clone()
    }

    /// Sync whatever the mark last recorded. For a clean shutdown, so the
    /// next open has an exact mark rather than one a power loss left behind.
    pub(super) fn sync(&self) -> std::io::Result<()> {
        crate::io::sync_data(&self.file)
    }

    /// Write `mark` wherever it falls, then sync it. For opening the log and
    /// for the rare operations that move it backwards (truncation, a reset),
    /// where a stale mark left ahead of the log would make recovery refuse a
    /// repairable tail.
    pub(super) fn record_durably(&self, mark: DurableMark) -> std::io::Result<()> {
        let mut state = self.state.lock();
        state.last = None;
        state.rewinds += 1;
        // The caller synced the log through `mark` before calling.
        state.synced = Some(mark);
        crate::io::write_at(&self.file, &mark.encode(), 0)?;
        self.sync()?;
        state.last = Some(mark);
        Ok(())
    }
}

impl LogInner {
    /// Record a flush that synced `segment` through `synced_bytes`.
    ///
    /// Best effort: a mark that failed to move is merely behind, which widens
    /// what recovery may repair after a power loss. A poison records the
    /// synced point held in memory, not the file, so a cut after it never
    /// removes a record this sync covered.
    pub(super) fn note_synced(&self, segment: SegmentId, synced_bytes: u64) {
        self.note_synced_since(segment, synced_bytes, self.mark.rewinds());
    }

    /// [`LogInner::note_synced`] for a flush that read `rewinds` under the
    /// flush lock; dropped if a rewind came since. See [`MarkFile::record_since`].
    pub(super) fn note_synced_since(&self, segment: SegmentId, synced_bytes: u64, rewinds: u64) {
        self.warn_unmarked(self.mark.record_since(
            DurableMark {
                segment,
                synced_bytes,
            },
            rewinds,
        ));
    }

    /// Record that every segment before `active` is sealed, and so synced
    /// whole. Keeps a sealed segment out of the repairable range when the mark
    /// last pointed into it. The caller keeps a rewind out: with the append
    /// lock, or for a background seal, by holding the roll slot.
    pub(super) fn note_sealed_before(&self, active: SegmentId) {
        self.note_synced(active, SEGMENT_HEADER_LEN);
    }

    fn warn_unmarked(&self, written: std::io::Result<()>) {
        if let Err(err) = written {
            tracing::warn!(shard = %self.label, error = %err, "could not update the durable mark");
        }
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

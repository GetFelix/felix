//! The log every store in this crate is built on: offsets, records, and the
//! [`AppendOnlyLog`] trait that [`crate::DiskLog`] implements.

mod config;
#[cfg(test)]
mod tests;

pub use config::{FsyncMode, GenerationGate, LogConfig, OffloadTarget, Retention, RetentionHold};

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use crate::Result;

/// A record's position in its log. Assigned on append, never reused.
pub type Offset = u64;
/// Names one segment file within a shard's log.
pub type SegmentId = u64;
/// The future every [`AppendOnlyLog`] method returns.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An ordered, append-only sequence of records addressed by offset.
pub trait AppendOnlyLog: Send + Sync {
    fn append(&self, records: &[AppendRecord]) -> BoxFuture<'_, Result<AppendResult>>;
    /// Records from `range.start` on, leaving out trim records, and where the
    /// next read continues. The offsets trim records hold are gaps in what
    /// this returns, so a page can be empty below the tail; carry on from
    /// [`ReadPage::next`], never from the last record. A log with a head
    /// floor refuses a start below it with
    /// [`crate::StorageError::Trimmed`].
    fn read_range(&self, range: ReadRange) -> BoxFuture<'_, Result<ReadPage>>;
    /// [`Self::read_range`] with trim records included, for the paths that
    /// must see every record: replication, and finding the trim that set a
    /// log's floor. Ignores the head floor: a follower is sent the records
    /// below it too, and applies the floor itself.
    fn read_range_with_trims(&self, range: ReadRange) -> BoxFuture<'_, Result<Vec<LogRecord>>>;
    fn tail_offset(&self) -> BoxFuture<'_, Result<Offset>>;
    fn truncate(&self, offset: Offset) -> BoxFuture<'_, Result<()>>;
    /// Note that `generation` begins at `start_offset`, and persist it.
    ///
    /// Returns whether anything was recorded: a generation at or below the
    /// newest already held is ignored, since a leader re-reporting its own is
    /// ordinary and an older one is stale.
    ///
    /// Default: not recorded. An in-memory log has no divergence to repair,
    /// because it has no follower.
    fn record_generation(&self, _generation: u64, _start_offset: Offset) -> Result<bool> {
        Ok(false)
    }

    /// The generation history, oldest first.
    fn generations(&self) -> Vec<Epoch> {
        Vec::new()
    }

    /// Where `generation` stops, given `tail`. `None` if it is not in the
    /// history — the case that must refuse rather than guess, because the
    /// answer becomes a truncation point.
    fn generation_end(&self, _generation: u64, _tail: Offset) -> Option<Offset> {
        None
    }
    fn seal(&self) -> BoxFuture<'_, Result<SealedSegment>>;
}

/// Opens the log for a shard.
pub trait LogProvider: Send + Sync {
    type Log: AppendOnlyLog;

    fn open(&self, shard: &ShardKey) -> BoxFuture<'_, Result<Self::Log>>;
}

/// One record to append. Its offset is assigned by the log.
#[derive(Debug, Clone)]
pub struct AppendRecord {
    pub payload: Bytes,
    pub timestamp_micros: u64,
    pub mark: RecordMark,
    /// The principal that published the record, stored with it. At most
    /// [`crate::segment::format::MAX_PUBLISHER_BYTES`]; a record with one
    /// needs storage format v6, so a log moves onto it only when it writes one.
    pub publisher: Option<Bytes>,
    /// The record's key and headers, already encoded. Storage keeps the bytes
    /// as given. A record with them needs storage format v8, so a log moves
    /// onto it only when it writes one. Boxed so a record without them is
    /// no larger than before: records are moved one by one on every append.
    pub headers: Option<Box<Bytes>>,
}

/// Which idempotent producer's batch a record belongs to, if any.
///
/// Stored with the record, so every copy of the log carries it and a replica
/// promoted to leader knows each producer's sequence from the records it
/// holds. See `docs/storage-format.md`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RecordMark {
    #[default]
    None,
    /// The first record of a producer's batch.
    Opens(ProducerBatch),
    /// A later record of the batch the record before it belongs to.
    Continues,
    /// A leader's first record at its generation, written before it serves.
    /// It is what lets the leader's quorum mark cover records it inherited;
    /// no reader outside replication ever sees it. The payload is the
    /// generation, a big-endian `u64`.
    GenerationStart,
    /// An atomic commit: one record whose payload holds an event and the
    /// state updates that go with it, so replication, truncation and the
    /// commit mark take all of it or none. The payload layout is the
    /// broker's; see `docs/atomic-commit.md`.
    Commit,
    /// A head trim, asking that reads below `floor` be refused. Once the
    /// record is durable and, on a log held at its commit offset, committed,
    /// [`crate::DiskLog`] applies the floor and deletes the sealed segments
    /// below it in the background. Storage does not check that `floor` is
    /// at or below the record's own offset; the caller must. The payload is
    /// the trimming caller's headers, in the broker's layout. [`AppendOnlyLog::read_range`] skips the record;
    /// [`AppendOnlyLog::read_range_with_trims`] returns it. Needs a v7
    /// segment, so a log moves onto v7 only when it writes one. See
    /// `docs/storage-format.md`, "Trim records".
    Trim { floor: Offset },
    /// A change to the shard's replica set, committed through the log. The
    /// payload is the engine's. Only a v9 segment holds one, and nothing
    /// writes one yet.
    Membership,
    /// A signed add to a counter, for a cache shard whose counters share its
    /// log. The payload is the counter store's. v9 only, and nothing writes
    /// one yet.
    CounterDelta,
    /// The running sum of every counter, written when counter deltas are
    /// compacted. The payload is the counter store's. v9 only, and nothing
    /// writes one yet.
    CounterCheckpoint,
}

/// An idempotent producer's batch, as its first record describes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatch {
    pub producer_id: u64,
    pub sequence: u64,
    /// Records in the batch, this one included.
    pub len: u32,
}

impl RecordMark {
    /// Whether a client reads the record: an event or cache entry, or a
    /// commit record holding one. Generation-start, trim, membership and
    /// counter records belong to storage and replication, and every client
    /// read skips them. A skipped record keeps its offset, so a reader sees
    /// the gap and can tell it from a drop; see `docs/storage-format.md`,
    /// "Record kinds".
    pub fn is_data(&self) -> bool {
        match self {
            Self::None | Self::Opens(_) | Self::Continues | Self::Commit => true,
            Self::GenerationStart
            | Self::Trim { .. }
            | Self::Membership
            | Self::CounterDelta
            | Self::CounterCheckpoint => false,
        }
    }

    /// Whether only a v9 segment may hold the record.
    pub fn needs_generation(&self) -> bool {
        matches!(
            self,
            Self::Membership | Self::CounterDelta | Self::CounterCheckpoint
        )
    }

    /// Whether the record is a generation-start record rather than a
    /// client's.
    pub fn is_generation_start(&self) -> bool {
        matches!(self, Self::GenerationStart)
    }

    /// Whether the record is an atomic commit.
    pub fn is_commit(&self) -> bool {
        matches!(self, Self::Commit)
    }

    /// Whether the record is a head trim.
    pub fn is_trim(&self) -> bool {
        matches!(self, Self::Trim { .. })
    }

    /// The marks for a batch of `len` records: the first opens it, the rest
    /// continue it.
    pub fn for_batch(producer_id: u64, sequence: u64, len: usize) -> impl Iterator<Item = Self> {
        let opens = Self::Opens(ProducerBatch {
            producer_id,
            sequence,
            len: len as u32,
        });
        std::iter::once(opens).chain(std::iter::repeat_n(Self::Continues, len.saturating_sub(1)))
    }
}

/// A digest of a producer batch's payloads, which is what tells a re-send of
/// a batch apart from a different batch that reuses its sequence.
///
/// CRC-64/XZ over each record's length and bytes, chained record to record in
/// order. It is derived from the payloads a log holds, so every replica
/// computes the same value without it being stored in the records. It is
/// saved in the producer snapshot, so it must never change between builds.
/// See `docs/storage-format.md`, "Producer state".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PayloadDigest(u64);

impl PayloadDigest {
    /// The digest of a batch with no records yet.
    pub(crate) const EMPTY: Self = Self(0);

    /// The digest of a whole batch.
    pub fn of<P: AsRef<[u8]>>(payloads: impl IntoIterator<Item = P>) -> Self {
        payloads.into_iter().fold(Self::EMPTY, |digest, payload| {
            digest.then(record_digest(payload.as_ref()))
        })
    }

    /// This digest extended by one record, given that record's
    /// [`record_digest`].
    pub(crate) fn then(self, record: u64) -> Self {
        let mut digest = CRC64.digest();
        digest.update(&self.0.to_be_bytes());
        digest.update(&record.to_be_bytes());
        Self(digest.finalize())
    }

    pub(crate) fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    pub(crate) fn to_bits(self) -> u64 {
        self.0
    }
}

/// One record's contribution to a [`PayloadDigest`]. Split out so a scan can
/// keep it per record, next to the record's mark, without holding the payload.
pub(crate) fn record_digest(payload: &[u8]) -> u64 {
    let mut digest = CRC64.digest();
    digest.update(&(payload.len() as u64).to_be_bytes());
    digest.update(payload);
    digest.finalize()
}

/// Sliced tables: this runs over every idempotent payload on the append path.
const CRC64: crc::Crc<u64, crc::Table<16>> = crc::Crc::<u64, crc::Table<16>>::new(&crc::CRC_64_XZ);

/// The offsets an append was given, first and last inclusive.
#[derive(Debug, Clone)]
pub struct AppendResult {
    pub first_offset: Offset,
    pub last_offset: Offset,
}

/// Where a read starts and how many payload bytes it may return.
#[derive(Debug, Clone)]
pub struct ReadRange {
    pub start: Offset,
    pub max_bytes: usize,
}

/// What [`AppendOnlyLog::read_range`] returns.
#[derive(Debug, Clone, Default)]
pub struct ReadPage {
    pub records: Vec<LogRecord>,
    /// Where the next read starts: one past the last record returned, or,
    /// for an empty page, the tail the read ran to, past any trim records it
    /// skipped. Never below the read's start.
    pub next: Offset,
}

/// A record read back from the log.
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub offset: Offset,
    pub timestamp_micros: u64,
    pub checksum: u32,
    pub payload: Bytes,
    pub mark: RecordMark,
    /// The principal that published the record, when it was stored with one.
    pub publisher: Option<Bytes>,
    /// The record's encoded key and headers, when it was stored with them.
    /// Boxed for the same reason as [`AppendRecord::headers`].
    pub headers: Option<Box<Bytes>>,
    /// The generation of the leader that wrote the record, kept in its
    /// header from segment format v9. `None` for a record in an older
    /// segment.
    pub generation: Option<u64>,
}

/// The generation a v9 segment stamps on a record whose writer nobody knows:
/// one a sender held in an older segment, where no generation was kept. It
/// is never compared: it neither raises nor breaks the floor. Generation 0 is
/// also a shard's first, which loses nothing: no record precedes it.
pub const UNKNOWN_WRITER: u64 = 0;

/// Which generation wrote the records of a batch, for a log whose segment
/// stamps each record with one (format v9). An older segment ignores it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum WrittenBy {
    /// This log's own leader: the generation it leads at
    /// (`DiskLog::lead_at`), which must be the one the log accepted. Anything
    /// else is refused rather than stamped with a generation that did not
    /// write the record.
    #[default]
    Leader,
    /// Records copied from another log, at the generations that wrote them
    /// there: runs oldest first, each covering its records up to the next
    /// run's start, the first starting at or before the batch. A run at
    /// [`UNKNOWN_WRITER`] covers records whose writer the sender did not know.
    Writers(Vec<Epoch>),
    /// Records copied from a sender that did not say which generations wrote
    /// them. A v9 segment refuses them.
    Unknown,
}

/// One leadership generation, and the offset its first record took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Epoch {
    pub generation: u64,
    pub start_offset: Offset,
}

/// A segment that will not be written again, with a checksum of its bytes.
#[derive(Debug, Clone)]
pub struct SealedSegment {
    pub descriptor: SegmentDescriptor,
    pub checksum: u64,
}

/// The offset and byte range one segment covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDescriptor {
    pub id: SegmentId,
    pub base_offset: Offset,
    pub last_offset: Offset,
    pub size_bytes: u64,
    /// The layout version in the segment header, which sets the record header size.
    pub version: u16,
}

/// Names one shard's log. Caches and counters reuse it, with the cache or
/// scope name in `stream`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardKey {
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
}

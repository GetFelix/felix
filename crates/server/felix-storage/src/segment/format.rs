//! On-disk encoding for durable log segments.
//!
//! The format is deliberately independent of `felix-wire`: network framing is
//! allowed to change shape for latency reasons, while bytes already on disk must
//! stay readable by every later build. What the two share is the convention —
//! big-endian integers, a magic/version prefix, an explicit length — not code.
//!
//! The authoritative description lives in `docs/storage-format.md`; this module
//! is the implementation of that document and the two must move together.

use bytes::Bytes;

use crate::log::{Offset, ProducerBatch, RecordMark};
use crate::{Corruption, CorruptionKind};

/// `"FLSG"` — **F**e**L**ix **S**e**g**ment. Identifies a segment data file.
///
/// Deliberately distinct from `felix-wire`'s frame magic `"FLX1"`: storage bytes
/// and network bytes are separate formats with separate versioning, and either
/// turning up where the other belongs should be rejected on the first four bytes
/// rather than misparsed.
pub const SEGMENT_MAGIC: u32 = 0x464C_5347;
/// `"FLSI"` — **F**e**L**ix **S**egment **I**ndex. Identifies a sparse index file.
pub const INDEX_MAGIC: u32 = 0x464C_5349;
/// Version of both the segment and index layouts described here.
///
/// v2 added a record header checksum. v1 segments are rejected on open with
/// `CorruptionKind::SegmentVersion`, naming the version found — the format was
/// only ever written by unreleased builds, so the migration path is to discard
/// the data directory rather than to carry a second decoder. Failing loudly is
/// the point: a v1 record read as v2 would misparse every field.
///
/// v3 lets a record carry a producer mark (see [`RecordHeader`]). A v2 segment
/// reads the same under v3, so both are accepted; a v2 build refuses a v3
/// segment instead of reading a mark's flag bits as an impossible length.
///
/// v4 adds the generation-start record, a third flag bit. A v3 build refuses a
/// v4 segment for the same reason a v2 build refuses a v3 one, so a segment is
/// written at v4 only to hold that record ([`BASELINE_VERSION`]).
///
/// v5 adds the commit record (bit 28), for the same reason and under the same
/// rule: a segment is written at v5 only to hold one.
///
/// v6 lets a record carry its publisher (bit 27), again written only to hold
/// one.
///
/// v7 adds the trim record. The four kind bits were all taken, so its code is
/// two of them at once (bits 29 and 28), which every older build refuses as
/// `RecordFlags`. A segment is written at v7 only to hold one.
///
/// v8 lets a record carry a key and headers (bit 26), written only to hold
/// one.
///
/// v9 adds a generation to every record header, so the header is 36 bytes
/// instead of 28. An older build refuses the segment by its version. It also
/// adds three kinds, each a pair or triple of kind bits no older segment may
/// hold: membership, counter delta and counter checkpoint. Nothing writes v9
/// yet: a log moves onto it only once the fleet can read it.
pub const FORMAT_VERSION: u16 = 9;

/// The first version whose records carry a generation.
pub const GENERATION_VERSION: u16 = 9;

/// The first version whose records may carry a key and headers.
pub const HEADERS_VERSION: u16 = 8;

/// The first version whose segments may hold a trim record.
pub const TRIM_VERSION: u16 = 7;

/// The first version whose records may carry a publisher.
pub const PUBLISHER_VERSION: u16 = 6;

/// The longest publisher a record carries. Its length is stored in one byte.
pub const MAX_PUBLISHER_BYTES: usize = u8::MAX as usize;

/// The version a new segment and every index is written at.
///
/// A segment is written at v4 only when a generation-start record is about to
/// go into it, and after that one the log's later segments stay at v4. The
/// record is written only once the fleet finalized `generation_start`, so
/// until then a v3 build can still read everything this build wrote. The
/// index layout has not changed since v2.
pub const BASELINE_VERSION: u16 = 3;

/// The oldest segment and index version this build reads.
pub const OLDEST_READABLE_VERSION: u16 = 2;

/// Bytes occupied by a segment file header.
pub const SEGMENT_HEADER_LEN: u64 = 32;
/// Bytes occupied by a record header before v9, excluding its payload.
pub const RECORD_HEADER_LEN: u64 = 28;
/// Bytes occupied by a v9 record header: [`RECORD_HEADER_LEN`] plus the
/// generation.
pub const GENERATION_HEADER_LEN: u64 = RECORD_HEADER_LEN + 8;
/// Bytes a record opening a producer batch carries after its header.
pub const PRODUCER_TAG_LEN: u64 = 20;
/// Bytes a trim record carries after its header: the floor.
pub const TRIM_TAG_LEN: u64 = 8;
/// Bytes occupied by an index file header.
pub const INDEX_HEADER_LEN: u64 = 24;
/// Bytes occupied by a single sparse index entry.
pub const INDEX_ENTRY_LEN: u64 = 16;

/// Ceiling on a single record's payload, enforced before any allocation is made
/// on behalf of a length field read from disk. A corrupt `payload_len` is
/// otherwise a request to allocate up to 4 GiB.
pub const MAX_PAYLOAD_BYTES: u32 = 64 * 1024 * 1024;

/// Decode results carry a [`Corruption`] rather than an I/O error: these
/// functions operate on bytes already in memory.
pub type DecodeResult<T> = std::result::Result<T, Corruption>;

/// The fixed prefix of a segment data file.
///
/// ```text
///  0   4  magic              u32  "FLSG"
///  4   2  version            u16
///  6   2  flags              u16
///  8   8  base_offset        u64  logical offset of the segment's first record
/// 16   8  created_at_micros  u64
/// 24   4  header_crc         u32  crc32 over bytes 0..24
/// 28   4  reserved           u32  must be zero
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub base_offset: Offset,
    pub created_at_micros: u64,
    pub flags: u16,
    /// The layout version the segment was written with. Only a v3 segment
    /// may hold producer marks.
    pub version: u16,
}

impl SegmentHeader {
    /// A header at [`BASELINE_VERSION`].
    pub fn new(base_offset: Offset, created_at_micros: u64) -> Self {
        Self::at_version(base_offset, created_at_micros, BASELINE_VERSION)
    }

    pub fn at_version(base_offset: Offset, created_at_micros: u64, version: u16) -> Self {
        Self {
            base_offset,
            created_at_micros,
            flags: 0,
            version,
        }
    }

    /// Whether records with producer marks may be written to this segment.
    pub fn holds_marks(&self) -> bool {
        self.version >= 3
    }

    /// Whether a generation-start record may be written to this segment.
    pub fn holds_generation_starts(&self) -> bool {
        self.version >= 4
    }

    /// Whether a commit record may be written to this segment.
    pub fn holds_commits(&self) -> bool {
        self.version >= 5
    }

    /// Whether a record carrying its publisher may be written to this segment.
    pub fn holds_publishers(&self) -> bool {
        self.version >= PUBLISHER_VERSION
    }

    /// Whether a trim record may be written to this segment.
    pub fn holds_trims(&self) -> bool {
        self.version >= TRIM_VERSION
    }

    /// Whether this segment's records carry a generation.
    pub fn holds_generations(&self) -> bool {
        self.version >= GENERATION_VERSION
    }

    pub fn encode(&self) -> [u8; SEGMENT_HEADER_LEN as usize] {
        let mut buf = [0u8; SEGMENT_HEADER_LEN as usize];
        buf[0..4].copy_from_slice(&SEGMENT_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&self.version.to_be_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_be_bytes());
        buf[8..16].copy_from_slice(&self.base_offset.to_be_bytes());
        buf[16..24].copy_from_slice(&self.created_at_micros.to_be_bytes());
        let checksum = crc32(&[&buf[0..24]]);
        buf[24..28].copy_from_slice(&checksum.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> DecodeResult<Self> {
        if (buf.len() as u64) < SEGMENT_HEADER_LEN {
            return Err(truncated(SEGMENT_HEADER_LEN, buf.len() as u64));
        }
        let magic = read_u32(buf, 0);
        if magic != SEGMENT_MAGIC {
            return Err(Corruption::new(CorruptionKind::SegmentMagic {
                found: magic,
            }));
        }
        let version = read_u16(buf, 4);
        if !(OLDEST_READABLE_VERSION..=FORMAT_VERSION).contains(&version) {
            return Err(Corruption::new(CorruptionKind::SegmentVersion {
                found: version,
            }));
        }
        let flags = read_u16(buf, 6);
        if flags != 0 {
            return Err(Corruption::new(CorruptionKind::SegmentFlags {
                found: flags,
            }));
        }
        let expected = read_u32(buf, 24);
        let found = crc32(&[&buf[0..24]]);
        if expected != found {
            return Err(Corruption::new(CorruptionKind::SegmentHeaderChecksum {
                expected,
                found,
            }));
        }
        Ok(Self {
            base_offset: read_u64(buf, 8),
            created_at_micros: read_u64(buf, 16),
            flags,
            version,
        })
    }
}

/// The fixed-size prefix of a record.
///
/// ```text
///  0   4  len_and_flags      u32  payload length, and two flags in the top bits
///  4   8  offset             u64  logical offset of this record
/// 12   8  timestamp_micros   u64
/// 20   4  header_crc         u32  crc32 over bytes 0..20
/// 24   4  checksum           u32  crc32 over bytes 0..24, the tag, and the payload
/// 28  20  producer tag            only when the record opens a producer batch:
///                                 producer_id u64, sequence u64, len u32
/// 28   8  trim tag                only on a trim record: floor u64
///     n   body: with bit 26 a u32 length and the key-and-headers block,
///             then the payload, then with bit 27 the publisher and its u8 length
/// ```
///
/// From v9 the generation follows the timestamp, inside the header checksum,
/// and everything after it moves up 8 bytes:
///
/// ```text
/// 20   8  generation         u64  generation (Raft term) of the leader that wrote it
/// 28   4  header_crc         u32  crc32 over bytes 0..28
/// 32   4  checksum           u32  crc32 over bytes 0..32, the tag, and the payload
/// 36      tag and body, as above
/// ```
///
/// Bit 31 of `len_and_flags` says the record opens an idempotent producer's
/// batch and the tag follows; bit 30 says it continues the batch the record
/// before it belongs to. Bit 29 marks a generation-start record and
/// bit 28 a commit record (see `RecordMark::Commit`). Bits 29 and 28 together
/// mark a trim record (see `RecordMark::Trim`). From v9, bits 30 and 29 mark
/// a membership record, bits 30 and 28 a counter delta, and bits 30, 29 and 28
/// a counter checkpoint. Bit 27 is not a kind:
/// it says the body ends with the principal that published the record,
/// followed by that principal's length in one byte. Bit 26 is not a kind
/// either: it says the body starts with the record's key and headers. Bodies
/// are capped at 2^26 bytes, so the length reaches bit 26 only for a body of
/// exactly 2^26, which every older build could write: bit 26 with no length
/// bits under it is that body, never headers. The length word is
/// covered by the header checksum, so a reader can walk to the next record
/// from the header alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    /// The body's length: the payload, plus the publisher and its length byte
    /// when [`Self::has_publisher`].
    pub payload_len: u32,
    pub offset: Offset,
    pub timestamp_micros: u64,
    /// CRC-32 over bytes `0..20`. Validated before `payload_len` is used for
    /// anything, which is what lets recovery tell an unfinished write from a
    /// rotted length field.
    pub header_crc: u32,
    pub checksum: u32,
    /// The record's kind, from the kind bits.
    pub kind: RecordKind,
    /// The body ends with the record's publisher.
    pub has_publisher: bool,
    /// The body starts with the record's key and headers.
    pub has_headers: bool,
    /// The generation of the leader that wrote the record. `None` before v9,
    /// whose records do not carry one.
    pub generation: Option<u64>,
}

/// What a record's kind bits say it is. See [`RecordMark`] for what each
/// kind means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    /// No kind bit: an ordinary record.
    Plain,
    /// Bit 31: opens a producer batch, and the 20-byte producer tag follows.
    Opens,
    /// Bit 30: continues the producer batch before it.
    Continues,
    /// Bit 29: a leader's generation-start record.
    GenerationStart,
    /// Bit 28: an atomic commit.
    Commit,
    /// Bits 29 and 28: a head trim, and the 8-byte floor follows.
    Trim,
    /// Bits 30 and 29, from v9: a replica-set change.
    Membership,
    /// Bits 30 and 28, from v9: a counter add.
    CounterDelta,
    /// Bits 30, 29 and 28, from v9: every counter's running sum.
    CounterCheckpoint,
}

impl RecordKind {
    /// Bytes between the record header and the body.
    pub fn tag_len(self) -> u64 {
        match self {
            Self::Opens => PRODUCER_TAG_LEN,
            Self::Trim => TRIM_TAG_LEN,
            Self::Plain
            | Self::Continues
            | Self::GenerationStart
            | Self::Commit
            | Self::Membership
            | Self::CounterDelta
            | Self::CounterCheckpoint => 0,
        }
    }
}

const FLAG_OPENS_BATCH: u32 = 1 << 31;
const FLAG_CONTINUES_BATCH: u32 = 1 << 30;
const FLAG_GENERATION_START: u32 = 1 << 29;
const FLAG_COMMIT: u32 = 1 << 28;
/// The one two-bit kind. Only a v7 segment may hold it.
const FLAG_TRIM: u32 = FLAG_GENERATION_START | FLAG_COMMIT;
/// The v9 kinds. Bit 30 alone is a continuing record, so every older build
/// refuses these as `RecordFlags`.
const FLAG_MEMBERSHIP: u32 = FLAG_CONTINUES_BATCH | FLAG_GENERATION_START;
const FLAG_COUNTER_DELTA: u32 = FLAG_CONTINUES_BATCH | FLAG_COMMIT;
const FLAG_COUNTER_CHECKPOINT: u32 = FLAG_CONTINUES_BATCH | FLAG_GENERATION_START | FLAG_COMMIT;
const KIND_MASK: u32 =
    FLAG_OPENS_BATCH | FLAG_CONTINUES_BATCH | FLAG_GENERATION_START | FLAG_COMMIT;
const FLAG_PUBLISHER: u32 = 1 << 27;
const FLAG_MASK: u32 = KIND_MASK | FLAG_PUBLISHER;
/// Also the 2^26 bit of a body's length; see [`RecordHeader`].
const FLAG_HEADERS: u32 = 1 << 26;
/// The length a body's key-and-headers block is prefixed with.
pub const HEADERS_LEN_BYTES: usize = 4;

/// Bytes a record body takes: the key-and-headers block with its length, the
/// payload, and the publisher with its length byte.
pub fn body_len(payload_len: usize, headers: Option<&[u8]>, publisher: Option<&[u8]>) -> usize {
    headers.map_or(0, |headers| HEADERS_LEN_BYTES + headers.len())
        + payload_len
        + publisher.map_or(0, |publisher| publisher.len() + 1)
}

/// Whether a body this long may be written. A record with a key and headers
/// stops one byte short of [`MAX_PAYLOAD_BYTES`], so its length never reads as
/// the legacy 2^26 body.
pub fn body_fits(body_len: usize, has_headers: bool) -> bool {
    body_len <= MAX_PAYLOAD_BYTES as usize - usize::from(has_headers)
}

/// Bytes a record header takes in a segment at `version`.
pub fn record_header_len(version: u16) -> u64 {
    if version >= GENERATION_VERSION {
        GENERATION_HEADER_LEN
    } else {
        RECORD_HEADER_LEN
    }
}

/// Bytes a record with this body and mark takes on disk before v9. See
/// [`body_len`].
pub fn record_len(body_len: usize, mark: &RecordMark) -> u64 {
    record_len_in(body_len, mark, HEADERS_VERSION)
}

/// [`record_len`] for a segment at `version`.
pub fn record_len_in(body_len: usize, mark: &RecordMark, version: u16) -> u64 {
    let tag = match mark {
        RecordMark::Opens(_) => PRODUCER_TAG_LEN,
        RecordMark::Trim { .. } => TRIM_TAG_LEN,
        RecordMark::None
        | RecordMark::Continues
        | RecordMark::GenerationStart
        | RecordMark::Commit
        | RecordMark::Membership
        | RecordMark::CounterDelta
        | RecordMark::CounterCheckpoint => 0,
    };
    record_header_len(version) + tag + body_len as u64
}

impl RecordHeader {
    /// Total bytes this record occupies on disk, header included.
    pub fn encoded_len(&self) -> u64 {
        self.header_len() + self.kind.tag_len() + u64::from(self.payload_len)
    }

    /// Bytes the fixed header takes: more from v9, which adds the generation.
    pub fn header_len(&self) -> u64 {
        if self.generation.is_some() {
            GENERATION_HEADER_LEN
        } else {
            RECORD_HEADER_LEN
        }
    }

    /// Where the payload starts, relative to the record.
    fn payload_start(&self) -> usize {
        (self.encoded_len() - u64::from(self.payload_len)) as usize
    }

    /// [`Self::decode_in`] for a segment at v8, the newest version whose
    /// records carry no generation.
    pub fn decode(buf: &[u8]) -> DecodeResult<Self> {
        Self::decode_in(buf, HEADERS_VERSION)
    }

    /// Decode a record header from a segment at `version`. A trim's bits in
    /// a segment older than v7 are `RecordFlags`, judged before the length,
    /// exactly as an older build judged them.
    pub fn decode_in(buf: &[u8], version: u16) -> DecodeResult<Self> {
        let header_len = record_header_len(version);
        if (buf.len() as u64) < header_len {
            return Err(truncated(header_len, buf.len() as u64));
        }
        // The header checksum is verified first, before any field is used.
        // Everything downstream — the payload length, the offset, how far to
        // step to the next record — is only meaningful once the header is known
        // to be intact.
        let crc_at = header_len as usize - 8;
        let header_crc = read_u32(buf, crc_at);
        let found = crc32(&[&buf[0..crc_at]]);
        if header_crc != found {
            return Err(Corruption::new(CorruptionKind::RecordHeaderChecksum {
                expected: header_crc,
                found,
            }));
        }
        let len_and_flags = read_u32(buf, 0);
        // At most one bit: a record is one kind. The trim code, from v7, and
        // the v9 codes are the exceptions.
        let kind = match len_and_flags & KIND_MASK {
            0 => RecordKind::Plain,
            FLAG_OPENS_BATCH => RecordKind::Opens,
            FLAG_CONTINUES_BATCH => RecordKind::Continues,
            FLAG_GENERATION_START => RecordKind::GenerationStart,
            FLAG_COMMIT => RecordKind::Commit,
            FLAG_TRIM if version >= TRIM_VERSION => RecordKind::Trim,
            FLAG_MEMBERSHIP if version >= GENERATION_VERSION => RecordKind::Membership,
            FLAG_COUNTER_DELTA if version >= GENERATION_VERSION => RecordKind::CounterDelta,
            FLAG_COUNTER_CHECKPOINT if version >= GENERATION_VERSION => {
                RecordKind::CounterCheckpoint
            }
            _ => {
                return Err(Corruption::new(CorruptionKind::RecordFlags {
                    found: len_and_flags,
                }));
            }
        };
        // Before v8 bit 26 is only ever length, exactly as an older build read
        // it. From v8, bit 26 alone is still the one body length older builds
        // could set it with.
        let has_headers = version >= HEADERS_VERSION
            && len_and_flags & FLAG_HEADERS != 0
            && len_and_flags & (FLAG_HEADERS - 1) != 0;
        let payload_len = len_and_flags & !FLAG_MASK & !if has_headers { FLAG_HEADERS } else { 0 };
        if payload_len > MAX_PAYLOAD_BYTES {
            return Err(Corruption::new(CorruptionKind::RecordTooLarge {
                payload_len,
                limit: MAX_PAYLOAD_BYTES,
            }));
        }
        Ok(Self {
            payload_len,
            offset: read_u64(buf, 4),
            timestamp_micros: read_u64(buf, 12),
            header_crc,
            checksum: read_u32(buf, crc_at + 4),
            kind,
            has_publisher: len_and_flags & FLAG_PUBLISHER != 0,
            has_headers,
            generation: (version >= GENERATION_VERSION).then(|| read_u64(buf, 20)),
        })
    }

    /// Refuse a record whose kind a segment at `version` cannot hold.
    ///
    /// Only the multi-bit kinds and headers are checked, each the way an
    /// older build judged it: before v7 the trim's bits, and before v9 the
    /// membership and counter bits, are `RecordFlags` in any segment, and
    /// before v8 bit 26 over a non-zero length is `RecordTooLarge`.
    pub fn check_version(&self, version: u16) -> DecodeResult<()> {
        let needs = match self.kind {
            RecordKind::Trim => TRIM_VERSION,
            RecordKind::Membership | RecordKind::CounterDelta | RecordKind::CounterCheckpoint => {
                GENERATION_VERSION
            }
            _ => 0,
        };
        if version < needs {
            return Err(Corruption::new(CorruptionKind::RecordFlags {
                found: self.len_and_flags(),
            }));
        }
        if self.has_headers && version < HEADERS_VERSION {
            return Err(Corruption::new(CorruptionKind::RecordTooLarge {
                payload_len: self.payload_len | FLAG_HEADERS,
                limit: MAX_PAYLOAD_BYTES,
            }));
        }
        Ok(())
    }

    /// The length word as stored.
    fn len_and_flags(&self) -> u32 {
        let kind = match self.kind {
            RecordKind::Plain => 0,
            RecordKind::Opens => FLAG_OPENS_BATCH,
            RecordKind::Continues => FLAG_CONTINUES_BATCH,
            RecordKind::GenerationStart => FLAG_GENERATION_START,
            RecordKind::Commit => FLAG_COMMIT,
            RecordKind::Trim => FLAG_TRIM,
            RecordKind::Membership => FLAG_MEMBERSHIP,
            RecordKind::CounterDelta => FLAG_COUNTER_DELTA,
            RecordKind::CounterCheckpoint => FLAG_COUNTER_CHECKPOINT,
        };
        let flag = |set: bool, bit: u32| if set { bit } else { 0 };
        self.payload_len
            | kind
            | flag(self.has_publisher, FLAG_PUBLISHER)
            | flag(self.has_headers, FLAG_HEADERS)
    }
}

/// Serialize one record into `out` in the layout before v9, returning the
/// bytes appended.
///
/// Callers batch many of these into a single buffer so that one `write_all`
/// covers a whole append. A `publisher` is at most [`MAX_PUBLISHER_BYTES`],
/// and the body fits by [`body_fits`]. `headers` is the record's key and
/// headers, already encoded; storage does not look inside it.
pub fn encode_record(
    out: &mut Vec<u8>,
    offset: Offset,
    timestamp_micros: u64,
    payload: &[u8],
    headers: Option<&[u8]>,
    publisher: Option<&[u8]>,
    mark: &RecordMark,
) -> u64 {
    encode_record_at(
        out,
        None,
        offset,
        timestamp_micros,
        payload,
        headers,
        publisher,
        mark,
    )
}

/// [`encode_record`] with the record's generation: `Some` for a v9 segment,
/// `None` for an older one.
#[allow(clippy::too_many_arguments)]
pub fn encode_record_at(
    out: &mut Vec<u8>,
    generation: Option<u64>,
    offset: Offset,
    timestamp_micros: u64,
    payload: &[u8],
    headers: Option<&[u8]>,
    publisher: Option<&[u8]>,
    mark: &RecordMark,
) -> u64 {
    let body_len = body_len(payload.len(), headers, publisher);
    debug_assert!(body_fits(body_len, headers.is_some()));
    debug_assert!(publisher.is_none_or(|publisher| publisher.len() <= MAX_PUBLISHER_BYTES));
    let start = out.len();
    let kind = match mark {
        RecordMark::None => 0,
        RecordMark::Opens(_) => FLAG_OPENS_BATCH,
        RecordMark::Continues => FLAG_CONTINUES_BATCH,
        RecordMark::GenerationStart => FLAG_GENERATION_START,
        RecordMark::Commit => FLAG_COMMIT,
        RecordMark::Trim { .. } => FLAG_TRIM,
        RecordMark::Membership => FLAG_MEMBERSHIP,
        RecordMark::CounterDelta => FLAG_COUNTER_DELTA,
        RecordMark::CounterCheckpoint => FLAG_COUNTER_CHECKPOINT,
    };
    let flag = |set: bool, bit: u32| if set { bit } else { 0 };
    let flags =
        kind | flag(publisher.is_some(), FLAG_PUBLISHER) | flag(headers.is_some(), FLAG_HEADERS);
    out.extend_from_slice(&(body_len as u32 | flags).to_be_bytes());
    out.extend_from_slice(&offset.to_be_bytes());
    out.extend_from_slice(&timestamp_micros.to_be_bytes());
    if let Some(generation) = generation {
        out.extend_from_slice(&generation.to_be_bytes());
    }
    let crc_at = out.len() - start;
    // Header checksum first, so a reader can trust `payload_len` without having
    // read the payload it describes.
    let header_crc = crc32(&[&out[start..start + crc_at]]);
    out.extend_from_slice(&header_crc.to_be_bytes());
    let mut tag = [0u8; PRODUCER_TAG_LEN as usize];
    let tag: &[u8] = match mark {
        RecordMark::Opens(batch) => {
            tag[0..8].copy_from_slice(&batch.producer_id.to_be_bytes());
            tag[8..16].copy_from_slice(&batch.sequence.to_be_bytes());
            tag[16..20].copy_from_slice(&batch.len.to_be_bytes());
            &tag
        }
        RecordMark::Trim { floor } => {
            tag[0..8].copy_from_slice(&floor.to_be_bytes());
            &tag[..TRIM_TAG_LEN as usize]
        }
        RecordMark::None
        | RecordMark::Continues
        | RecordMark::GenerationStart
        | RecordMark::Commit
        | RecordMark::Membership
        | RecordMark::CounterDelta
        | RecordMark::CounterCheckpoint => &[],
    };
    let (publisher, publisher_len): (&[u8], &[u8]) = match publisher {
        Some(publisher) => (publisher, &[publisher.len() as u8][..]),
        None => (&[], &[]),
    };
    // Two more checksum parts cost every record, so only a record with
    // headers pays for them.
    let checksum = match headers {
        None => crc32(&[
            &out[start..start + crc_at + 4],
            tag,
            payload,
            publisher,
            publisher_len,
        ]),
        Some(headers) => crc32(&[
            &out[start..start + crc_at + 4],
            tag,
            &(headers.len() as u32).to_be_bytes(),
            headers,
            payload,
            publisher,
            publisher_len,
        ]),
    };
    out.extend_from_slice(&checksum.to_be_bytes());
    out.extend_from_slice(tag);
    if let Some(headers) = headers {
        out.extend_from_slice(&(headers.len() as u32).to_be_bytes());
        out.extend_from_slice(headers);
    }
    out.extend_from_slice(payload);
    out.extend_from_slice(publisher);
    out.extend_from_slice(publisher_len);
    (out.len() - start) as u64
}

/// A decoded record together with the bytes it consumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRecord {
    pub header: RecordHeader,
    pub payload: Bytes,
    /// The principal that published the record, when it was stored with one.
    pub publisher: Option<Bytes>,
    /// The record's encoded key and headers, when it was stored with them.
    pub headers: Option<Bytes>,
    pub mark: RecordMark,
}

/// Decode the record at the front of `buf`, from a v8 segment, verifying its
/// checksum.
///
/// Returns the record and the number of bytes consumed. A `Truncated` error
/// means "ask for more bytes"; every other error means the bytes present are
/// not a valid record.
pub fn decode_record(buf: &[u8]) -> DecodeResult<(DecodedRecord, u64)> {
    decode_record_after(RecordHeader::decode(buf)?, buf)
}

/// [`decode_record`] for a segment at `version`.
pub fn decode_record_in(buf: &[u8], version: u16) -> DecodeResult<(DecodedRecord, u64)> {
    decode_record_after(RecordHeader::decode_in(buf, version)?, buf)
}

/// [`decode_record`] for a caller that already decoded the record's header
/// from the front of `buf`, so the read path checks it once, not twice.
pub(crate) fn decode_record_after(
    header: RecordHeader,
    buf: &[u8],
) -> DecodeResult<(DecodedRecord, u64)> {
    let total = header.encoded_len();
    if (buf.len() as u64) < total {
        return Err(truncated(total, buf.len() as u64));
    }
    let payload_start = header.payload_start();
    let header_len = header.header_len() as usize;
    let tag = &buf[header_len..payload_start];
    let body = &buf[payload_start..total as usize];
    let found = crc32(&[&buf[0..header_len - 4], tag, body]);
    if found != header.checksum {
        return Err(Corruption::new(CorruptionKind::RecordChecksum {
            expected: header.checksum,
            found,
        }));
    }
    let (payload, publisher) = if header.has_publisher {
        // The checksum held, so a length that does not fit is not a torn
        // write: the record was written wrong.
        let misfit = || {
            Corruption::new(CorruptionKind::RecordPublisher {
                body_len: header.payload_len,
            })
        };
        let (&len, rest) = body.split_last().ok_or_else(misfit)?;
        let split = rest
            .len()
            .checked_sub(usize::from(len))
            .ok_or_else(misfit)?;
        let (payload, publisher) = rest.split_at(split);
        (payload, Some(Bytes::copy_from_slice(publisher)))
    } else {
        (body, None)
    };
    let (payload, headers) = if header.has_headers {
        let misfit = || {
            Corruption::new(CorruptionKind::RecordHeaders {
                body_len: header.payload_len,
            })
        };
        let (len, rest) = payload
            .split_first_chunk::<HEADERS_LEN_BYTES>()
            .ok_or_else(misfit)?;
        let len = u32::from_be_bytes(*len) as usize;
        if len > rest.len() {
            return Err(misfit());
        }
        let (headers, payload) = rest.split_at(len);
        (payload, Some(Bytes::copy_from_slice(headers)))
    } else {
        (payload, None)
    };
    let mark = match header.kind {
        RecordKind::Plain => RecordMark::None,
        RecordKind::Opens => RecordMark::Opens(ProducerBatch {
            producer_id: read_u64(tag, 0),
            sequence: read_u64(tag, 8),
            len: read_u32(tag, 16),
        }),
        RecordKind::Continues => RecordMark::Continues,
        RecordKind::GenerationStart => RecordMark::GenerationStart,
        RecordKind::Commit => RecordMark::Commit,
        RecordKind::Trim => RecordMark::Trim {
            floor: read_u64(tag, 0),
        },
        RecordKind::Membership => RecordMark::Membership,
        RecordKind::CounterDelta => RecordMark::CounterDelta,
        RecordKind::CounterCheckpoint => RecordMark::CounterCheckpoint,
    };
    Ok((
        DecodedRecord {
            header,
            payload: Bytes::copy_from_slice(payload),
            publisher,
            headers,
            mark,
        },
        total,
    ))
}

/// Header of a sparse index file.
///
/// ```text
///  0   4  magic         u32  "FLSI"
///  4   2  version       u16
///  6   2  flags         u16
///  8   8  base_offset   u64  must match the segment it describes
/// 16   8  reserved      u64
/// ```
///
/// Index files carry no checksums. They are a pure accelerator: every entry is
/// verified against the segment on use, and a missing or stale index is rebuilt
/// from the segment rather than trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexHeader {
    pub base_offset: Offset,
}

impl IndexHeader {
    pub fn encode(&self) -> [u8; INDEX_HEADER_LEN as usize] {
        let mut buf = [0u8; INDEX_HEADER_LEN as usize];
        buf[0..4].copy_from_slice(&INDEX_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&BASELINE_VERSION.to_be_bytes());
        buf[8..16].copy_from_slice(&self.base_offset.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> DecodeResult<Self> {
        if (buf.len() as u64) < INDEX_HEADER_LEN {
            return Err(truncated(INDEX_HEADER_LEN, buf.len() as u64));
        }
        let magic = read_u32(buf, 0);
        if magic != INDEX_MAGIC {
            return Err(Corruption::new(CorruptionKind::IndexMagic { found: magic }));
        }
        // The index layout did not change in v3, so a v2 index stays usable.
        let version = read_u16(buf, 4);
        if !(OLDEST_READABLE_VERSION..=FORMAT_VERSION).contains(&version) {
            return Err(Corruption::new(CorruptionKind::IndexVersion {
                found: version,
            }));
        }
        Ok(Self {
            base_offset: read_u64(buf, 8),
        })
    }
}

/// One sparse index entry: `offset` starts a record at byte `position`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub offset: Offset,
    pub position: u64,
}

impl IndexEntry {
    pub fn encode(&self) -> [u8; INDEX_ENTRY_LEN as usize] {
        let mut buf = [0u8; INDEX_ENTRY_LEN as usize];
        buf[0..8].copy_from_slice(&self.offset.to_be_bytes());
        buf[8..16].copy_from_slice(&self.position.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> DecodeResult<Self> {
        if (buf.len() as u64) < INDEX_ENTRY_LEN {
            return Err(truncated(INDEX_ENTRY_LEN, buf.len() as u64));
        }
        Ok(Self {
            offset: read_u64(buf, 0),
            position: read_u64(buf, 8),
        })
    }
}

/// Verify that `found` continues the sequence at `expected`.
pub fn check_offset_continuity(expected: Offset, found: Offset) -> DecodeResult<()> {
    if expected == found {
        Ok(())
    } else {
        Err(Corruption::new(CorruptionKind::OffsetOutOfOrder {
            expected,
            found,
        }))
    }
}

pub(crate) fn crc32(parts: &[&[u8]]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize()
}

fn truncated(needed: u64, available: u64) -> Corruption {
    Corruption::new(CorruptionKind::Truncated { needed, available })
}

fn read_u16(buf: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([buf[at], buf[at + 1]])
}

fn read_u32(buf: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([buf[at], buf[at + 1], buf[at + 2], buf[at + 3]])
}

fn read_u64(buf: &[u8], at: usize) -> u64 {
    u64::from_be_bytes([
        buf[at],
        buf[at + 1],
        buf[at + 2],
        buf[at + 3],
        buf[at + 4],
        buf[at + 5],
        buf[at + 6],
        buf[at + 7],
    ])
}

#[cfg(test)]
mod tests;

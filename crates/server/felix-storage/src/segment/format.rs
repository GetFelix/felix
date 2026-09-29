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
pub const FORMAT_VERSION: u16 = 5;

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
/// Bytes occupied by a record header, excluding its payload.
pub const RECORD_HEADER_LEN: u64 = 28;
/// Bytes a record opening a producer batch carries after its header.
pub const PRODUCER_TAG_LEN: u64 = 20;
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
///     n   payload
/// ```
///
/// Bit 31 of `len_and_flags` says the record opens an idempotent producer's
/// batch and the tag follows; bit 30 says it continues the batch the record
/// before it belongs to. Bit 29 marks a generation-start record and
/// bit 28 a commit record (see `RecordMark::Commit`). Payloads are capped well below 2^28, so the length
/// never reaches them. The length word is covered by the header checksum, so
/// a reader can walk to the next record from the header alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub payload_len: u32,
    pub offset: Offset,
    pub timestamp_micros: u64,
    /// CRC-32 over bytes `0..20`. Validated before `payload_len` is used for
    /// anything, which is what lets recovery tell an unfinished write from a
    /// rotted length field.
    pub header_crc: u32,
    pub checksum: u32,
    /// The record opens a producer batch; the tag follows the header.
    pub opens_batch: bool,
    /// The record continues the producer batch before it.
    pub continues_batch: bool,
    /// The record is a leader's generation-start record.
    pub generation_start: bool,
    /// The record is an atomic commit.
    pub commit: bool,
}

const FLAG_OPENS_BATCH: u32 = 1 << 31;
const FLAG_CONTINUES_BATCH: u32 = 1 << 30;
const FLAG_GENERATION_START: u32 = 1 << 29;
const FLAG_COMMIT: u32 = 1 << 28;
const FLAG_MASK: u32 =
    FLAG_OPENS_BATCH | FLAG_CONTINUES_BATCH | FLAG_GENERATION_START | FLAG_COMMIT;

/// Bytes a record with this payload and mark takes on disk.
pub fn record_len(payload_len: usize, mark: &RecordMark) -> u64 {
    let tag = match mark {
        RecordMark::Opens(_) => PRODUCER_TAG_LEN,
        RecordMark::None
        | RecordMark::Continues
        | RecordMark::GenerationStart
        | RecordMark::Commit => 0,
    };
    RECORD_HEADER_LEN + tag + payload_len as u64
}

impl RecordHeader {
    /// Total bytes this record occupies on disk, header included.
    pub fn encoded_len(&self) -> u64 {
        let tag = if self.opens_batch {
            PRODUCER_TAG_LEN
        } else {
            0
        };
        RECORD_HEADER_LEN + tag + u64::from(self.payload_len)
    }

    /// Where the payload starts, relative to the record.
    fn payload_start(&self) -> usize {
        (self.encoded_len() - u64::from(self.payload_len)) as usize
    }

    pub fn decode(buf: &[u8]) -> DecodeResult<Self> {
        if (buf.len() as u64) < RECORD_HEADER_LEN {
            return Err(truncated(RECORD_HEADER_LEN, buf.len() as u64));
        }
        // The header checksum is verified first, before any field is used.
        // Everything downstream — the payload length, the offset, how far to
        // step to the next record — is only meaningful once the header is known
        // to be intact.
        let header_crc = read_u32(buf, 20);
        let found = crc32(&[&buf[0..20]]);
        if header_crc != found {
            return Err(Corruption::new(CorruptionKind::RecordHeaderChecksum {
                expected: header_crc,
                found,
            }));
        }
        let len_and_flags = read_u32(buf, 0);
        let flags = len_and_flags & FLAG_MASK;
        // At most one bit: a record is one kind.
        if flags.count_ones() > 1 {
            return Err(Corruption::new(CorruptionKind::RecordFlags {
                found: len_and_flags,
            }));
        }
        let payload_len = len_and_flags & !FLAG_MASK;
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
            checksum: read_u32(buf, 24),
            opens_batch: flags == FLAG_OPENS_BATCH,
            continues_batch: flags == FLAG_CONTINUES_BATCH,
            generation_start: flags == FLAG_GENERATION_START,
            commit: flags == FLAG_COMMIT,
        })
    }
}

/// Serialize one record into `out`, returning the bytes appended.
///
/// Callers batch many of these into a single buffer so that one `write_all`
/// covers a whole append.
pub fn encode_record(
    out: &mut Vec<u8>,
    offset: Offset,
    timestamp_micros: u64,
    payload: &[u8],
    mark: &RecordMark,
) -> u64 {
    debug_assert!(payload.len() <= MAX_PAYLOAD_BYTES as usize);
    let start = out.len();
    let flags = match mark {
        RecordMark::None => 0,
        RecordMark::Opens(_) => FLAG_OPENS_BATCH,
        RecordMark::Continues => FLAG_CONTINUES_BATCH,
        RecordMark::GenerationStart => FLAG_GENERATION_START,
        RecordMark::Commit => FLAG_COMMIT,
    };
    out.extend_from_slice(&(payload.len() as u32 | flags).to_be_bytes());
    out.extend_from_slice(&offset.to_be_bytes());
    out.extend_from_slice(&timestamp_micros.to_be_bytes());
    // Header checksum first, so a reader can trust `payload_len` without having
    // read the payload it describes.
    let header_crc = crc32(&[&out[start..start + 20]]);
    out.extend_from_slice(&header_crc.to_be_bytes());
    let mut tag = [0u8; PRODUCER_TAG_LEN as usize];
    let tag: &[u8] = match mark {
        RecordMark::Opens(batch) => {
            tag[0..8].copy_from_slice(&batch.producer_id.to_be_bytes());
            tag[8..16].copy_from_slice(&batch.sequence.to_be_bytes());
            tag[16..20].copy_from_slice(&batch.len.to_be_bytes());
            &tag
        }
        RecordMark::None
        | RecordMark::Continues
        | RecordMark::GenerationStart
        | RecordMark::Commit => &[],
    };
    let checksum = crc32(&[&out[start..start + 24], tag, payload]);
    out.extend_from_slice(&checksum.to_be_bytes());
    out.extend_from_slice(tag);
    out.extend_from_slice(payload);
    (out.len() - start) as u64
}

/// A decoded record together with the bytes it consumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRecord {
    pub header: RecordHeader,
    pub payload: Bytes,
    pub mark: RecordMark,
}

/// Decode the record at the front of `buf`, verifying its checksum.
///
/// Returns the record and the number of bytes consumed. A `Truncated` error
/// means "ask for more bytes"; every other error means the bytes present are
/// not a valid record.
pub fn decode_record(buf: &[u8]) -> DecodeResult<(DecodedRecord, u64)> {
    let header = RecordHeader::decode(buf)?;
    let total = header.encoded_len();
    if (buf.len() as u64) < total {
        return Err(truncated(total, buf.len() as u64));
    }
    let payload_start = header.payload_start();
    let tag = &buf[RECORD_HEADER_LEN as usize..payload_start];
    let payload = &buf[payload_start..total as usize];
    let found = crc32(&[&buf[0..24], tag, payload]);
    if found != header.checksum {
        return Err(Corruption::new(CorruptionKind::RecordChecksum {
            expected: header.checksum,
            found,
        }));
    }
    let mark = if header.opens_batch {
        RecordMark::Opens(ProducerBatch {
            producer_id: read_u64(tag, 0),
            sequence: read_u64(tag, 8),
            len: read_u32(tag, 16),
        })
    } else if header.continues_batch {
        RecordMark::Continues
    } else if header.generation_start {
        RecordMark::GenerationStart
    } else if header.commit {
        RecordMark::Commit
    } else {
        RecordMark::None
    };
    Ok((
        DecodedRecord {
            header,
            payload: Bytes::copy_from_slice(payload),
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

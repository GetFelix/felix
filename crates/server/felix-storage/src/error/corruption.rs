//! What went wrong with bytes on disk, and where.
//!
//! Every decoder in the crate reports a [`CorruptionKind`] naming the exact
//! invariant that failed. The low-level ones only know byte positions, so the
//! caller that knows the shard and segment adds them before the error leaves
//! the crate.

use std::fmt;

use crate::log::{Offset, SegmentId};

/// A [`CorruptionKind`] plus the location it was found at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Corruption {
    pub kind: CorruptionKind,
    pub site: CorruptionSite,
}

impl Corruption {
    pub fn new(kind: CorruptionKind) -> Self {
        Self {
            kind,
            site: CorruptionSite::default(),
        }
    }

    /// Attach the shard/segment the decoder had no way to know about.
    pub fn in_segment(mut self, shard: impl fmt::Display, segment: SegmentId) -> Self {
        self.site.shard = Some(shard.to_string());
        self.site.segment = Some(segment);
        self
    }

    /// Attach the shard, for a decoder that knows which log it was reading but
    /// not which segment within it.
    pub fn in_shard(mut self, shard: impl fmt::Display) -> Self {
        self.site.shard = Some(shard.to_string());
        self
    }

    /// Attach the byte position, unless a nested call already recorded a more
    /// specific one.
    pub fn at_position(mut self, position: u64) -> Self {
        self.site.position.get_or_insert(position);
        self
    }

    /// True when the failure is consistent with a write that was interrupted
    /// part-way, which is what recovery is allowed to truncate.
    pub fn is_truncation(&self) -> bool {
        matches!(self.kind, CorruptionKind::Truncated { .. })
    }
}

impl fmt::Display for Corruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        let mut sep = " (";
        if let Some(shard) = &self.site.shard {
            write!(f, "{sep}shard={shard}")?;
            sep = ", ";
        }
        if let Some(segment) = self.site.segment {
            write!(f, "{sep}segment={segment}")?;
            sep = ", ";
        }
        if let Some(position) = self.site.position {
            write!(f, "{sep}position={position}")?;
            sep = ", ";
        }
        if sep == ", " {
            write!(f, ")")?;
        }
        Ok(())
    }
}

/// A typed decode failure. Every variant names the specific invariant that was
/// violated so a failure is actionable without a hex dump.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorruptionKind {
    SegmentMagic {
        found: u32,
    },
    SegmentVersion {
        found: u16,
    },
    /// Unknown flag bits are rejected rather than masked off: a bit we do not
    /// understand may change how the records behind it are laid out.
    SegmentFlags {
        found: u16,
    },
    SegmentHeaderChecksum {
        expected: u32,
        found: u32,
    },
    IndexMagic {
        found: u32,
    },
    IndexVersion {
        found: u16,
    },
    /// A header was cut short. Distinct from a record checksum failure because a
    /// truncated *tail* is the expected outcome of a crash mid-append, and
    /// recovery repairs it instead of refusing to start.
    Truncated {
        needed: u64,
        available: u64,
    },
    RecordChecksum {
        expected: u32,
        found: u32,
    },
    /// The record header did not verify against its own checksum, so
    /// `payload_len` cannot be trusted.
    ///
    /// This is what makes a torn write distinguishable from bit rot. A header
    /// that verifies means the length is real, so a payload short of it was
    /// provably never finished; a header that does not verify could be a
    /// complete, acknowledged record whose length field rotted, and recovery
    /// must not guess.
    RecordHeaderChecksum {
        expected: u32,
        found: u32,
    },
    RecordTooLarge {
        payload_len: u32,
        limit: u32,
    },
    /// A record both opening a producer batch and continuing one.
    RecordFlags {
        found: u32,
    },
    /// A record whose publisher length runs past the start of its body. The
    /// checksum held, so the record was written this way.
    RecordPublisher {
        body_len: u32,
    },
    /// Offsets must ascend by exactly one across a segment; a gap means a record
    /// was lost or the file was spliced.
    OffsetOutOfOrder {
        expected: Offset,
        found: Offset,
    },
    /// A cache record was shorter than its own fixed header.
    CacheRecordTooShort {
        len: usize,
        header_len: usize,
    },
    /// Written by a build whose cache record layout this one does not know.
    ///
    /// Refused rather than read with the layout this build happens to have: a
    /// later version may have moved a field, and guessing produces a plausible
    /// wrong value where an error would have been honest.
    CacheRecordVersion {
        found: u8,
        expected: u8,
    },
    CacheRecordOp {
        found: u8,
    },
    /// The record claims a key longer than the bytes it carries.
    CacheRecordKeyLength {
        claimed: usize,
        available: usize,
    },
    CacheRecordKeyNotUtf8,
    /// A counter record that is not the shape this build writes: too short,
    /// a version or op it does not know, a key length that overruns the
    /// record, or a key that is not UTF-8. One variant for all five because a
    /// counter folded over a misread record is wrong forever after, and every
    /// one of them is answered the same way — refuse the log.
    CounterRecord {
        detail: &'static str,
        found: u64,
    },
}

impl fmt::Display for CorruptionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorruptionKind::SegmentMagic { found } => {
                write!(f, "bad segment magic {found:#010x}")
            }
            CorruptionKind::SegmentVersion { found } => {
                write!(f, "unsupported segment version {found}")
            }
            CorruptionKind::SegmentFlags { found } => {
                write!(f, "unknown segment flags {found:#06x}")
            }
            CorruptionKind::SegmentHeaderChecksum { expected, found } => write!(
                f,
                "segment header checksum mismatch (expected {expected:#010x}, found {found:#010x})"
            ),
            CorruptionKind::IndexMagic { found } => write!(f, "bad index magic {found:#010x}"),
            CorruptionKind::IndexVersion { found } => {
                write!(f, "unsupported index version {found}")
            }
            CorruptionKind::Truncated { needed, available } => {
                write!(f, "truncated: needed {needed} bytes, {available} available")
            }
            CorruptionKind::RecordChecksum { expected, found } => write!(
                f,
                "record checksum mismatch (expected {expected:#010x}, found {found:#010x})"
            ),
            CorruptionKind::RecordFlags { found } => {
                write!(f, "impossible record flags {found:#010x}")
            }
            CorruptionKind::RecordPublisher { body_len } => {
                write!(f, "record publisher does not fit its {body_len}-byte body")
            }
            CorruptionKind::RecordHeaderChecksum { expected, found } => write!(
                f,
                "record header checksum mismatch (expected {expected:#010x}, found {found:#010x})"
            ),
            CorruptionKind::RecordTooLarge { payload_len, limit } => {
                write!(f, "record payload {payload_len} exceeds limit {limit}")
            }
            CorruptionKind::OffsetOutOfOrder { expected, found } => {
                write!(
                    f,
                    "offset out of order (expected {expected}, found {found})"
                )
            }
            CorruptionKind::CacheRecordTooShort { len, header_len } => write!(
                f,
                "cache record is {len} bytes, shorter than its {header_len}-byte header"
            ),
            CorruptionKind::CacheRecordVersion { found, expected } => write!(
                f,
                "cache record version {found} is not readable by this build (reads up to {expected})"
            ),
            CorruptionKind::CacheRecordOp { found } => {
                write!(f, "unknown cache record op {found}")
            }
            CorruptionKind::CacheRecordKeyLength { claimed, available } => write!(
                f,
                "cache record claims a {claimed}-byte key but carries {available} bytes after it"
            ),
            CorruptionKind::CacheRecordKeyNotUtf8 => {
                write!(f, "cache record key is not valid UTF-8")
            }
            CorruptionKind::CounterRecord { detail, found } => {
                write!(f, "counter record {detail} ({found})")
            }
        }
    }
}

/// Where a corruption was found, for operator-facing diagnostics.
///
/// Recovery reports the shard and segment it was reading; the low-level decoder
/// only knows byte positions, so it leaves those `None` and lets the caller fill
/// them in with [`Corruption::in_segment`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CorruptionSite {
    pub shard: Option<String>,
    pub segment: Option<SegmentId>,
    /// Byte position within the segment file where decoding failed.
    pub position: Option<u64>,
}

#[cfg(test)]
mod tests;

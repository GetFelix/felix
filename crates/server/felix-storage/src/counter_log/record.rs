//! The on-disk shape of one counter record.
//!
//! Durable, so it is explicit and versioned rather than whatever a serialiser
//! happened to emit — the same discipline as the cache record, for the same
//! reason: a reader years from now has only these bytes, and a misread record
//! is worse than an unreadable one.
use bytes::{BufMut, Bytes, BytesMut};

use crate::{Corruption, CorruptionKind};

/// The only version this build writes, and the only one it reads.
pub(super) const VERSION: u8 = 1;

const OP_DELTA: u8 = 0;
const OP_CHECKPOINT: u8 = 1;

const OP_AT: usize = 1;
const VALUE_AT: usize = 2;
const KEY_LEN_AT: usize = 10;
/// Version, op, value, key length. Every record carries all four.
pub(super) const HEADER_LEN: usize = 14;

/// What one record says happened to one counter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CounterOp {
    /// Add this much — negative to subtract. The record is the *change*, and
    /// the sum is a fold over the log, which is what makes a counter a log
    /// semantic rather than a read-modify-write on shared state.
    Delta { key: String, delta: i64 },
    /// The folded sum as of this record. What compaction writes when it
    /// collapses a counter's applied deltas: replay resumes the fold from
    /// here, so the history can be reclaimed without the sum changing.
    Checkpoint { key: String, sum: i64 },
}

impl CounterOp {
    pub(crate) fn encode(&self) -> Bytes {
        let (op, key, value) = match self {
            CounterOp::Delta { key, delta } => (OP_DELTA, key, *delta),
            CounterOp::Checkpoint { key, sum } => (OP_CHECKPOINT, key, *sum),
        };
        let mut buf = BytesMut::with_capacity(HEADER_LEN + key.len());
        buf.put_u8(VERSION);
        buf.put_u8(op);
        buf.put_i64_le(value);
        buf.put_u32_le(key.len() as u32);
        buf.put_slice(key.as_bytes());
        buf.freeze()
    }

    /// Decode one record read back from the log.
    ///
    /// Every failure means the bytes are not what this build can read, and
    /// none are recoverable by guessing — a counter folded over a misread
    /// record is wrong forever after.
    pub(crate) fn decode(payload: &Bytes) -> Result<Self, Corruption> {
        if payload.len() < HEADER_LEN {
            return Err(Corruption::new(CorruptionKind::CounterRecord {
                detail: "is shorter than its fixed header",
                found: payload.len() as u64,
            }));
        }
        let version = payload[0];
        if version != VERSION {
            return Err(Corruption::new(CorruptionKind::CounterRecord {
                detail: "version is not readable by this build",
                found: version as u64,
            }));
        }
        let op = payload[OP_AT];
        let value = i64::from_le_bytes(
            payload[VALUE_AT..VALUE_AT + 8]
                .try_into()
                .expect("eight bytes"),
        );
        let key_len = u32::from_le_bytes(
            payload[KEY_LEN_AT..KEY_LEN_AT + 4]
                .try_into()
                .expect("four bytes"),
        ) as usize;
        if payload.len() != HEADER_LEN + key_len {
            return Err(Corruption::new(CorruptionKind::CounterRecord {
                detail: "key length does not match the record",
                found: key_len as u64,
            }));
        }
        let key = std::str::from_utf8(&payload[HEADER_LEN..HEADER_LEN + key_len])
            .map_err(|_| {
                Corruption::new(CorruptionKind::CounterRecord {
                    detail: "key is not valid UTF-8",
                    found: key_len as u64,
                })
            })?
            .to_string();
        match op {
            OP_DELTA => Ok(CounterOp::Delta { key, delta: value }),
            OP_CHECKPOINT => Ok(CounterOp::Checkpoint { key, sum: value }),
            other => Err(Corruption::new(CorruptionKind::CounterRecord {
                detail: "op is not one this build knows",
                found: other as u64,
            })),
        }
    }
}

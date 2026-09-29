//! The payload of a commit record.
//!
//! ```text
//! u8   version (1)
//! u32  event length, then the event bytes
//! u32  operation count, then each operation:
//!        u8   kind: 0 put, 1 delete
//!        u32  key length, then the key (UTF-8)
//!        u32  value length, then the value     (put only)
//! ```
//!
//! Big-endian, like the segment format around it. The log's checksums cover
//! these bytes; the decoder still checks every length, because a payload is
//! also what a peer ships.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{BrokerError, Result};

const VERSION: u8 = 1;
const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;

/// An event and the state updates committed with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    /// What stream readers and consumer groups see at the record's offset.
    pub event: Bytes,
    /// Applied to the shard's state, in order, at the same offset.
    pub ops: Vec<StateOp>,
}

/// One change to a stream shard's keyed state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateOp {
    Put { key: String, value: Bytes },
    Delete { key: String },
}

impl StateOp {
    pub fn key(&self) -> &str {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

impl CommitRecord {
    pub fn encode(&self) -> Bytes {
        let ops_len: usize = self
            .ops
            .iter()
            .map(|op| {
                9 + op.key().len()
                    + match op {
                        StateOp::Put { value, .. } => 4 + value.len(),
                        StateOp::Delete { .. } => 0,
                    }
            })
            .sum();
        let mut out = BytesMut::with_capacity(9 + self.event.len() + ops_len);
        out.put_u8(VERSION);
        out.put_u32(self.event.len() as u32);
        out.put_slice(&self.event);
        out.put_u32(self.ops.len() as u32);
        for op in &self.ops {
            match op {
                StateOp::Put { key, value } => {
                    out.put_u8(OP_PUT);
                    out.put_u32(key.len() as u32);
                    out.put_slice(key.as_bytes());
                    out.put_u32(value.len() as u32);
                    out.put_slice(value);
                }
                StateOp::Delete { key } => {
                    out.put_u8(OP_DELETE);
                    out.put_u32(key.len() as u32);
                    out.put_slice(key.as_bytes());
                }
            }
        }
        out.freeze()
    }

    /// Decode a payload written by [`Self::encode`]. The event and values
    /// share `payload`'s buffer rather than being copied.
    pub fn decode(payload: &Bytes) -> Result<Self> {
        let mut buf = payload.clone();
        let version = take_u8(&mut buf)?;
        if version != VERSION {
            return Err(malformed(format!("unknown version {version}")));
        }
        let event = take_bytes(&mut buf)?;
        let count = take_u32(&mut buf)? as usize;
        // Each operation takes at least nine bytes, so a count past that is a
        // lie told before any allocation.
        if count > buf.remaining() / 9 {
            return Err(malformed(format!(
                "{count} operations in {} bytes",
                buf.remaining()
            )));
        }
        let mut ops = Vec::with_capacity(count);
        for _ in 0..count {
            let kind = take_u8(&mut buf)?;
            let key = take_bytes(&mut buf)?;
            let key = String::from_utf8(key.to_vec())
                .map_err(|_| malformed("a key is not UTF-8".to_owned()))?;
            ops.push(match kind {
                OP_PUT => StateOp::Put {
                    key,
                    value: take_bytes(&mut buf)?,
                },
                OP_DELETE => StateOp::Delete { key },
                other => return Err(malformed(format!("unknown operation {other}"))),
            });
        }
        if buf.has_remaining() {
            return Err(malformed(format!("{} trailing bytes", buf.remaining())));
        }
        Ok(Self { event, ops })
    }
}

fn take_u8(buf: &mut Bytes) -> Result<u8> {
    if buf.remaining() < 1 {
        return Err(malformed("truncated".to_owned()));
    }
    Ok(buf.get_u8())
}

fn take_u32(buf: &mut Bytes) -> Result<u32> {
    if buf.remaining() < 4 {
        return Err(malformed("truncated".to_owned()));
    }
    Ok(buf.get_u32())
}

fn take_bytes(buf: &mut Bytes) -> Result<Bytes> {
    let len = take_u32(buf)? as usize;
    if buf.remaining() < len {
        return Err(malformed("truncated".to_owned()));
    }
    Ok(buf.split_to(len))
}

fn malformed(reason: String) -> BrokerError {
    BrokerError::MalformedCommit(reason)
}

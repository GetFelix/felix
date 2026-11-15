//! A consumer group's write on a Raft stream shard, as the payload of a
//! group record (`RecordMark::Group`).
//!
//! ```text
//! u8   version (1)
//! u8   op: 0 advance, 1 dead letter, 2 redrive, 3 finish redrive,
//!          4 discard, 5 seek, 6 delete
//! u32  group name length, then the name (UTF-8)
//! creation token                                    (ops 0 to 4)
//!   u64  term
//!   u64  index
//!   u8   by: 0 hydrate, 1 seek
//! u64  offset: the advance's or seek's target, or the record (ops 0 to 5)
//! u8   flags: bit 0 if_new                          (op 5 only)
//! ```
//!
//! Big-endian, like the segment format around it. A group's writes are
//! entries in the shard's own Raft log, so every member folds the same
//! writes in the same order. The creation token names the incarnation of
//! the group a write was decided for, so a write from before a seek or a
//! delete of that group is dropped rather than applied to its successor.
//! A seek's own token is its entry's log id, so it carries none.
//! `docs/formal/FelixRaftGroups.tla` models the rules.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{BrokerError, Result};

const VERSION: u8 = 1;
const OP_ADVANCE: u8 = 0;
const OP_DEAD_LETTER: u8 = 1;
const OP_REDRIVE: u8 = 2;
const OP_FINISH_REDRIVE: u8 = 3;
const OP_DISCARD: u8 = 4;
const OP_SEEK: u8 = 5;
const OP_DELETE: u8 = 6;
const BY_HYDRATE: u8 = 0;
const BY_SEEK: u8 = 1;
const IF_NEW: u8 = 1;

/// One consumer group's write, as one Raft entry of a stream shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupEntry {
    pub group: String,
    pub op: GroupOp,
}

/// What a [`GroupEntry`] does to its group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupOp {
    /// A run of settled records closed: the cursor moves up to `to`, never
    /// back. A group's first entry is an advance to where it starts.
    Advance { cre: CreationToken, to: u64 },
    /// `offset` ran out of attempts. Written before the advance over it.
    DeadLetter { cre: CreationToken, offset: u64 },
    /// A dead letter is owed to the group again.
    Redrive { cre: CreationToken, offset: u64 },
    /// A redriven record was settled.
    FinishRedrive { cre: CreationToken, offset: u64 },
    /// A dead letter is dropped without being redelivered.
    Discard { cre: CreationToken, offset: u64 },
    /// The cursor is set to `to`, backwards or forwards, and the group
    /// starts a new incarnation whose token is this entry's log id.
    /// `if_new` was decided by the leader before it proposed the entry.
    Seek { to: u64, if_new: bool },
    /// The group and its dead letters are gone.
    Delete,
}

/// The incarnation of a group a write was decided for: the term and index
/// of the seek that created or last reset the group, or, for a group created
/// by its first write, the term its tracker was made in and the index it
/// hydrated at. `by` keeps a seek at one index apart from a hydrate at the
/// same index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CreationToken {
    pub term: u64,
    pub index: u64,
    pub by: CreatedBy,
}

/// How a group's incarnation started. See [`CreationToken`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CreatedBy {
    Hydrate,
    Seek,
}

impl GroupOp {
    /// The creation token the write carries. A seek and a delete carry none.
    pub fn creation(&self) -> Option<CreationToken> {
        match *self {
            Self::Advance { cre, .. }
            | Self::DeadLetter { cre, .. }
            | Self::Redrive { cre, .. }
            | Self::FinishRedrive { cre, .. }
            | Self::Discard { cre, .. } => Some(cre),
            Self::Seek { .. } | Self::Delete => None,
        }
    }

    fn code(&self) -> u8 {
        match self {
            Self::Advance { .. } => OP_ADVANCE,
            Self::DeadLetter { .. } => OP_DEAD_LETTER,
            Self::Redrive { .. } => OP_REDRIVE,
            Self::FinishRedrive { .. } => OP_FINISH_REDRIVE,
            Self::Discard { .. } => OP_DISCARD,
            Self::Seek { .. } => OP_SEEK,
            Self::Delete => OP_DELETE,
        }
    }

    fn offset(&self) -> Option<u64> {
        match *self {
            Self::Advance { to, .. } | Self::Seek { to, .. } => Some(to),
            Self::DeadLetter { offset, .. }
            | Self::Redrive { offset, .. }
            | Self::FinishRedrive { offset, .. }
            | Self::Discard { offset, .. } => Some(offset),
            Self::Delete => None,
        }
    }
}

impl GroupEntry {
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(2 + 4 + self.group.len() + 17 + 8 + 1);
        out.put_u8(VERSION);
        out.put_u8(self.op.code());
        out.put_u32(self.group.len() as u32);
        out.put_slice(self.group.as_bytes());
        if let Some(cre) = self.op.creation() {
            out.put_u64(cre.term);
            out.put_u64(cre.index);
            out.put_u8(match cre.by {
                CreatedBy::Hydrate => BY_HYDRATE,
                CreatedBy::Seek => BY_SEEK,
            });
        }
        if let Some(offset) = self.op.offset() {
            out.put_u64(offset);
        }
        if let GroupOp::Seek { if_new, .. } = self.op {
            out.put_u8(if if_new { IF_NEW } else { 0 });
        }
        out.freeze()
    }

    /// Decode a payload written by [`Self::encode`]. Anything this build
    /// does not know is refused, never skipped: a member that applied less
    /// than the others would hold a different group table.
    pub fn decode(payload: &[u8]) -> Result<Self> {
        let mut buf = payload;
        let version = take_u8(&mut buf)?;
        if version != VERSION {
            return Err(malformed(format!("unknown version {version}")));
        }
        let code = take_u8(&mut buf)?;
        let len = take_u32(&mut buf)? as usize;
        if buf.remaining() < len {
            return Err(malformed("truncated".to_owned()));
        }
        let group = std::str::from_utf8(&buf[..len])
            .map_err(|_| malformed("the group name is not UTF-8".to_owned()))?
            .to_owned();
        buf.advance(len);
        let op = match code {
            OP_ADVANCE | OP_DEAD_LETTER | OP_REDRIVE | OP_FINISH_REDRIVE | OP_DISCARD => {
                let cre = CreationToken {
                    term: take_u64(&mut buf)?,
                    index: take_u64(&mut buf)?,
                    by: match take_u8(&mut buf)? {
                        BY_HYDRATE => CreatedBy::Hydrate,
                        BY_SEEK => CreatedBy::Seek,
                        other => return Err(malformed(format!("unknown creation {other}"))),
                    },
                };
                let offset = take_u64(&mut buf)?;
                match code {
                    OP_ADVANCE => GroupOp::Advance { cre, to: offset },
                    OP_DEAD_LETTER => GroupOp::DeadLetter { cre, offset },
                    OP_REDRIVE => GroupOp::Redrive { cre, offset },
                    OP_FINISH_REDRIVE => GroupOp::FinishRedrive { cre, offset },
                    _ => GroupOp::Discard { cre, offset },
                }
            }
            OP_SEEK => {
                let to = take_u64(&mut buf)?;
                let if_new = match take_u8(&mut buf)? {
                    0 => false,
                    IF_NEW => true,
                    other => return Err(malformed(format!("unknown seek flags {other:#x}"))),
                };
                GroupOp::Seek { to, if_new }
            }
            OP_DELETE => GroupOp::Delete,
            other => return Err(malformed(format!("unknown operation {other}"))),
        };
        if buf.has_remaining() {
            return Err(malformed(format!("{} trailing bytes", buf.remaining())));
        }
        Ok(Self { group, op })
    }
}

fn take_u8(buf: &mut &[u8]) -> Result<u8> {
    if buf.remaining() < 1 {
        return Err(malformed("truncated".to_owned()));
    }
    Ok(buf.get_u8())
}

fn take_u32(buf: &mut &[u8]) -> Result<u32> {
    if buf.remaining() < 4 {
        return Err(malformed("truncated".to_owned()));
    }
    Ok(buf.get_u32())
}

fn take_u64(buf: &mut &[u8]) -> Result<u64> {
    if buf.remaining() < 8 {
        return Err(malformed("truncated".to_owned()));
    }
    Ok(buf.get_u64())
}

fn malformed(reason: String) -> BrokerError {
    BrokerError::MalformedGroupEntry(reason)
}

#[cfg(test)]
mod tests;

//! A consumer group's write on a Raft stream shard, as the payload of a
//! group record (`RecordMark::Group`).
//!
//! ```text
//! u8   version (1)
//! u8   op: 0 advance, 1 dead letter, 2 redrive, 3 finish redrive,
//!          4 discard, 5 seek, 6 delete, 7 claim
//! u32  group name length, then the name (UTF-8, never empty)
//! creation token                                    (ops 0 to 4, 7)
//!   u64  term
//!   u64  index
//!   u8   by: 0 hydrate, 1 seek
//! u64  offset: the advance's or seek's target, or the record (ops 0 to 5)
//! u8   flags: bit 0 if_new                          (op 5 only)
//! claim                                             (op 7 only)
//!   u64  visibility in milliseconds
//!   u8   1 if a named member holds it, else 0
//!   u32  principal length, principal, u32 name length, name   (if named)
//!   u32  records, then per record ascending: u64 offset, u32 attempts
//! ```
//!
//! Big-endian, like the segment format around it. A group's writes are
//! entries in the shard's own Raft log, so every member folds the same
//! writes in the same order. The creation token names the incarnation of
//! the group a write was decided for, so a write from before a seek or a
//! delete of that group is dropped rather than applied to its successor.
//! A seek's own token is its entry's log id, so it carries none.
//! `docs/formal/FelixRaftGroups.tla` models the rules, except discard (a
//! dead letter dropped), claims, and `if_new`, which the leader decides before it
//! proposes and the fold does not read.

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
const OP_CLAIM: u8 = 7;
const BY_HYDRATE: u8 = 0;
const BY_SEEK: u8 = 1;
const IF_NEW: u8 = 1;

/// One consumer group's write, as one Raft entry of a stream shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupEntry {
    /// The group's name, unique within the shard's stream.
    pub group: String,
    pub op: GroupOp,
}

/// What a [`GroupEntry`] does to its group.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Records handed out, or a claim extended: each is held, at its
    /// attempt count, for `visibility_ms` from when a leader learns of it.
    /// Replaces any claim the table holds on those records.
    Claim { cre: CreationToken, claims: Claims },
}

/// What one [`GroupOp::Claim`] holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    pub holder: Option<ClaimHolder>,
    pub visibility_ms: u64,
    /// `(offset, attempts)`, strictly ascending by offset.
    pub records: Vec<(u64, u32)>,
}

/// The named member a claim was handed to: the principal it authenticated
/// as and the name it chose.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClaimHolder {
    pub principal: String,
    pub name: String,
}

/// The incarnation of a group a write was decided for: the term and index
/// of the seek that created or last reset the group, or, for a group created
/// by its first write, the term its tracker was made in and the index it
/// hydrated at. `by` keeps a seek at one index apart from a hydrate at the
/// same index. Tokens are compared for equality; the delete watermark
/// compares `index` alone, so there is no ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CreationToken {
    pub term: u64,
    pub index: u64,
    pub by: CreatedBy,
}

/// How a group's incarnation started. See [`CreationToken`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
            | Self::Discard { cre, .. }
            | Self::Claim { cre, .. } => Some(cre),
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
            Self::Claim { .. } => OP_CLAIM,
        }
    }

    fn offset(&self) -> Option<u64> {
        match *self {
            Self::Advance { to, .. } | Self::Seek { to, .. } => Some(to),
            Self::DeadLetter { offset, .. }
            | Self::Redrive { offset, .. }
            | Self::FinishRedrive { offset, .. }
            | Self::Discard { offset, .. } => Some(offset),
            Self::Delete | Self::Claim { .. } => None,
        }
    }
}

impl GroupEntry {
    /// The payload of the entry's group record, in the layout above.
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
        if let GroupOp::Claim { claims, .. } = &self.op {
            out.put_u64(claims.visibility_ms);
            match &claims.holder {
                Some(holder) => {
                    out.put_u8(1);
                    for part in [&holder.principal, &holder.name] {
                        out.put_u32(part.len() as u32);
                        out.put_slice(part.as_bytes());
                    }
                }
                None => out.put_u8(0),
            }
            out.put_u32(claims.records.len() as u32);
            for &(offset, attempts) in &claims.records {
                out.put_u64(offset);
                out.put_u32(attempts);
            }
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
        // Every group op names a group, so no proposer writes an empty name.
        if len == 0 {
            return Err(malformed("the group name is empty".to_owned()));
        }
        if buf.remaining() < len {
            return Err(malformed("truncated".to_owned()));
        }
        let group = std::str::from_utf8(&buf[..len])
            .map_err(|_| malformed("the group name is not UTF-8".to_owned()))?
            .to_owned();
        buf.advance(len);
        let op = match code {
            OP_CLAIM => {
                let cre = take_creation(&mut buf)?;
                let visibility_ms = take_u64(&mut buf)?;
                let holder = match take_u8(&mut buf)? {
                    0 => None,
                    1 => Some(ClaimHolder {
                        principal: take_str(&mut buf)?,
                        name: take_str(&mut buf)?,
                    }),
                    other => return Err(malformed(format!("unknown holder flag {other}"))),
                };
                let count = take_u32(&mut buf)?;
                let mut records = Vec::new();
                for _ in 0..count {
                    let record = (take_u64(&mut buf)?, take_u32(&mut buf)?);
                    if records
                        .last()
                        .is_some_and(|last: &(u64, u32)| last.0 >= record.0)
                    {
                        return Err(malformed("claimed offsets out of order".to_owned()));
                    }
                    records.push(record);
                }
                GroupOp::Claim {
                    cre,
                    claims: Claims {
                        holder,
                        visibility_ms,
                        records,
                    },
                }
            }
            OP_ADVANCE | OP_DEAD_LETTER | OP_REDRIVE | OP_FINISH_REDRIVE | OP_DISCARD => {
                let cre = take_creation(&mut buf)?;
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

fn take_creation(buf: &mut &[u8]) -> Result<CreationToken> {
    Ok(CreationToken {
        term: take_u64(buf)?,
        index: take_u64(buf)?,
        by: match take_u8(buf)? {
            BY_HYDRATE => CreatedBy::Hydrate,
            BY_SEEK => CreatedBy::Seek,
            other => return Err(malformed(format!("unknown creation {other}"))),
        },
    })
}

fn take_str(buf: &mut &[u8]) -> Result<String> {
    let len = take_u32(buf)? as usize;
    if buf.remaining() < len {
        return Err(malformed("truncated".to_owned()));
    }
    let text = std::str::from_utf8(&buf[..len])
        .map_err(|_| malformed("a claim holder is not UTF-8".to_owned()))?
        .to_owned();
    buf.advance(len);
    Ok(text)
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

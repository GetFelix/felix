//! The frozen 12-byte header every internal frame starts with, and the kind
//! registry it carries.
//!
//! The header layout never changes; new messages arrive as new [`Kind`]s.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

/// `FLXI`. Distinct from the client protocol's `FLX1` so neither side can
/// decode the other's frames.
pub const INTERNAL_MAGIC: u32 = 0x464C_5849;

/// This protocol's version, independent of the client protocol's.
///
/// Additive change happens by adding a [`Kind`]. An older peer steps over a
/// frame whose kind it does not know — the frozen header says how long the body
/// is — and answers [`ErrorCode::UnsupportedKind`] against the correlation id
/// every body starts with, so the stream survives and the sender is told
/// plainly. This version exists for the change that cannot cover: the header,
/// or an existing body layout.
///
/// [`ErrorCode::UnsupportedKind`]: super::ErrorCode::UnsupportedKind
pub const INTERNAL_VERSION: u16 = 1;

/// Largest body this protocol will decode, before any allocation is sized from
/// a peer-provided number.
pub const MAX_BODY_BYTES: u32 = 64 * 1024 * 1024;

/// Fixed-size header preceding every internal body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InternalHeader {
    pub kind: Kind,
    pub length: u32,
}

impl InternalHeader {
    /// Encoded length in bytes.
    pub const LEN: usize = 12;

    /// Append the header to `buf`.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32(INTERNAL_MAGIC);
        buf.put_u16(INTERNAL_VERSION);
        buf.put_u16(self.kind as u16);
        buf.put_u32(self.length);
    }

    /// Parse a header, refusing an unknown kind or a body over
    /// [`MAX_BODY_BYTES`].
    pub fn decode(buf: &Bytes) -> Result<Self> {
        if buf.len() < Self::LEN {
            return Err(Error::Incomplete);
        }
        let mut head = buf.slice(0..Self::LEN);
        if head.get_u32() != INTERNAL_MAGIC {
            return Err(Error::InvalidMagic);
        }
        let version = head.get_u16();
        if version != INTERNAL_VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        let kind = Kind::from_u16(head.get_u16())?;
        let length = head.get_u32();
        if length > MAX_BODY_BYTES {
            return Err(Error::FrameTooLarge);
        }
        Ok(Self { kind, length })
    }
}

/// Message discriminant.
///
/// A discriminant rather than flag bits: the client protocol uses bits because
/// they modify one payload layout, whereas here each kind *is* a layout, so an
/// enum makes "unknown kind" one unambiguous check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum Kind {
    ForwardPublish = 1,
    ForwardPublishOk = 2,
    ForwardPublishError = 3,
    NotLeader = 4,
    Hello = 5,
    HelloOk = 6,
    ReplicateRecords = 7,
    ReplicateOk = 8,
    ReplicateError = 9,
    ReplicateBootstrap = 10,
    ForwardCacheOp = 11,
    ForwardCacheOk = 12,
    ForwardCacheError = 13,
    ReplicateCacheRecords = 14,
    ReplicateCacheBootstrap = 15,
    ReplicateGroupRecords = 16,
    ReplicateGroupBootstrap = 17,
    ReplicateDeadLetterRecords = 18,
    ReplicateDeadLetterBootstrap = 19,
    ReplicateCounterRecords = 20,
    ReplicateCounterBootstrap = 21,
    /// `ForwardPublish` plus the publisher's credential. A separate kind
    /// because existing body layouts are frozen: an owner that predates it
    /// refuses the kind, and one that knows it refuses the old kind instead,
    /// since a forward with no credential is the hole this closes.
    AuthorizedForwardPublish = 22,
    /// `ForwardCacheOp` plus the caller's credential, by the same reasoning.
    AuthorizedForwardCacheOp = 23,
    /// The leader tells a halted follower to discard its copy of a shard's log
    /// and start again from the leader's oldest record.
    ReplicateRebuild = 24,
    /// `ReplicateRecords` whose records carry idempotent producers' marks.
    /// A separate kind so an unmarked batch stays byte for byte what an older
    /// follower reads, and a marked one is refused by it rather than stored
    /// without the marks.
    ReplicateMarkedRecords = 25,
    /// Any of a shard's logs' records, with their marks, the log they belong
    /// to, and the leader's commit offset. Sent only when there is a commit
    /// offset to carry; without one a batch travels as its log's own kind, so
    /// an older follower reads exactly what it always did, and it refuses
    /// this kind rather than storing records without learning the offset.
    ReplicateCommittedRecords = 26,
}

impl Kind {
    /// An unknown kind is rejected, never skipped: the kind selects how to read
    /// the body, so ignoring one means confidently misparsing it.
    pub fn from_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Kind::ForwardPublish),
            2 => Ok(Kind::ForwardPublishOk),
            3 => Ok(Kind::ForwardPublishError),
            4 => Ok(Kind::NotLeader),
            5 => Ok(Kind::Hello),
            6 => Ok(Kind::HelloOk),
            7 => Ok(Kind::ReplicateRecords),
            8 => Ok(Kind::ReplicateOk),
            9 => Ok(Kind::ReplicateError),
            10 => Ok(Kind::ReplicateBootstrap),
            11 => Ok(Kind::ForwardCacheOp),
            12 => Ok(Kind::ForwardCacheOk),
            13 => Ok(Kind::ForwardCacheError),
            14 => Ok(Kind::ReplicateCacheRecords),
            15 => Ok(Kind::ReplicateCacheBootstrap),
            16 => Ok(Kind::ReplicateGroupRecords),
            17 => Ok(Kind::ReplicateGroupBootstrap),
            18 => Ok(Kind::ReplicateDeadLetterRecords),
            19 => Ok(Kind::ReplicateDeadLetterBootstrap),
            20 => Ok(Kind::ReplicateCounterRecords),
            21 => Ok(Kind::ReplicateCounterBootstrap),
            22 => Ok(Kind::AuthorizedForwardPublish),
            23 => Ok(Kind::AuthorizedForwardCacheOp),
            24 => Ok(Kind::ReplicateRebuild),
            25 => Ok(Kind::ReplicateMarkedRecords),
            26 => Ok(Kind::ReplicateCommittedRecords),
            other => Err(Error::UnsupportedInternalKind(other)),
        }
    }
}

/// The frame envelope, decoded without resolving the kind.
///
/// [`InternalHeader::decode`] refuses a kind it does not know, which is right
/// for deciding how to read a body and wrong for deciding whether to keep the
/// stream. This says only what the frozen header says: the framing is ours, the
/// version is one we speak, and the body is this many bytes — enough to step
/// over a frame this build cannot interpret and answer for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameEnvelope {
    /// The raw discriminant, whether or not [`Kind`] knows it.
    pub kind: u16,
    pub length: u32,
}

impl FrameEnvelope {
    /// Read the envelope, checking only the magic and version.
    pub fn decode(buf: &Bytes) -> Result<Self> {
        if buf.len() < InternalHeader::LEN {
            return Err(Error::Incomplete);
        }
        let mut head = buf.slice(0..InternalHeader::LEN);
        if head.get_u32() != INTERNAL_MAGIC {
            return Err(Error::InvalidMagic);
        }
        let version = head.get_u16();
        if version != INTERNAL_VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        Ok(Self {
            kind: head.get_u16(),
            length: head.get_u32(),
        })
    }
}

/// Every body begins with its correlation id, and nothing may be added before
/// it.
///
/// That is what lets a responder answer a frame whose *kind* it does not know:
/// without the correlation id the refusal could not be matched to the request,
/// and the only remaining option would be to drop the connection.
/// `every_body_begins_with_its_correlation_id` holds every kind to it.
pub fn correlation_id_in(body: &[u8]) -> Option<u64> {
    body.get(..8)
        .map(|head| u64::from_be_bytes(head.try_into().expect("eight bytes")))
}

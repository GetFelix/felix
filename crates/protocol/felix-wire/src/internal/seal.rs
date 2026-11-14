//! The seal that moves a shard from the hybrid protocol to Raft: the seal's
//! fence, a follower's acknowledgement of the seal's record, and the sealing
//! copy's id on what it ships while the seal holds.
//!
//! See "Moving a shard from the hybrid to Raft" in `docs/per-shard-raft.md`.
//! Every message here is sent only to a peer that offered
//! [`PeerCapabilities::SWITCH_SEAL`](super::PeerCapabilities::SWITCH_SEAL).

use bytes::Bytes;

use super::{Fence, FenceOk, InternalMessage, ShardRef};

/// A [`Fence`] that also asks the replica to promise the seal: from here
/// on it takes the shard's records only from the copy `sealer`, refuses
/// ballots and rebuilds, and acknowledges nothing else of the shard until a
/// later generation replaces the promise. Only on the shard's own log, and
/// never with election fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealFence {
    pub fence: Fence,
    /// The seal leader's storage identity, its copy id.
    pub sealer: [u8; 16],
}

/// The replica took the seal's fence and promised it. `ok` is the answer a
/// fence gets, never with an attestation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealFenceOk {
    pub ok: FenceOk,
    pub starts: SealStarts,
}

/// Where a copy's three logs start: the shard's own, its dead letters' and
/// its group cursors'. The seal's digests start at the highest of each among
/// the copies that answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SealStarts {
    pub log: u64,
    pub dead_letters: u64,
    pub cursors: u64,
}

/// The seal leader sends a follower the seal's record at `offset`, `N`. The
/// follower checks its copy against it, appends it, and answers
/// [`SealAckOk`] once it is on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealAck {
    pub correlation_id: u64,
    pub shard: ShardRef,
    /// The copy that sealed, which the follower's promise must name.
    pub sealer: [u8; 16],
    pub offset: u64,
    pub timestamp_micros: u64,
    /// The record's payload, which names the base.
    pub payload: Bytes,
}

/// The follower holds the seal's record on disk, as the copy `copy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealAckOk {
    pub correlation_id: u64,
    pub copy: [u8; 16],
}

/// A request from the copy `copy`: the seal leader's batches while its seal
/// holds, so a follower that promised the seal can tell them from another
/// copy's under the same node id, such as the same broker after a wipe.
/// Travels as an envelope around the request, outermost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromCopy {
    pub copy: [u8; 16],
    pub message: Box<InternalMessage>,
}

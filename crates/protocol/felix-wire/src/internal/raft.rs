//! Raft messages between the members of a per-shard group.
//!
//! The wire protocol carries them without reading them: the body after the
//! correlation id is laid out by the replication crate, which owns the Raft
//! runtime. See "Raft messages" in `docs/internal-protocol.md`.

use bytes::Bytes;

/// One Raft request for one member of one group. Sent only to a peer that
/// offered [`PeerCapabilities::RAFT`](super::PeerCapabilities::RAFT).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftCall {
    pub correlation_id: u64,
    pub body: Bytes,
}

/// The answer to a [`RaftCall`]: the member's reply, or why there is none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftAnswer {
    pub correlation_id: u64,
    pub body: Bytes,
}

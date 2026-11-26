//! The exchange that opens every peer connection.

use super::PeerCapabilities;

/// The first message on a peer connection, naming who is calling.
///
/// Sent before any request so a version or identity mismatch is found while the
/// connection is being established rather than on the first forwarded publish,
/// which would otherwise have to be failed and retried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub correlation_id: u64,
    /// The caller's cluster identity, as the catalog knows it.
    pub node_id: String,
    /// What the caller can do. `None` sends the original `Hello`, byte for
    /// byte; `Some` sends `HelloCapable`, which a peer that predates it
    /// refuses as an unknown kind, and the caller then says `Hello` instead.
    pub capabilities: Option<PeerCapabilities>,
}

/// The responder accepted the handshake and names itself.
///
/// The caller checks this against the node id it dialled. An address the
/// catalog has since reassigned answers with a different id, which is a
/// connection to the wrong broker regardless of whether it would have served
/// the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloOk {
    pub correlation_id: u64,
    pub node_id: String,
    /// What the responder can do, answered only to a `HelloCapable`: a
    /// plain `Hello` gets the original `HelloOk`.
    pub capabilities: Option<PeerCapabilities>,
    /// The responder's storage identity: travels as 16 bytes after the
    /// capability bits, present exactly when those bits include
    /// [`PeerCapabilities::STORAGE_IDENTITY`].
    pub identity: Option<[u8; 16]>,
    /// The responder's node incarnation, when the capability bits include
    /// [`PeerCapabilities::NODE_INCARNATION`]: a byte after the identity, 1
    /// followed by the incarnation as 8 bytes, or 0 alone for a responder
    /// that has not registered yet. Must be `None` without the bit.
    pub incarnation: Option<u64>,
    /// The Raft body layouts the responder reads and writes, when the
    /// capability bits include [`PeerCapabilities::RAFT_LAYOUTS`]: two bytes
    /// after the incarnation, lowest then highest. Must be `None` without
    /// the bit.
    pub raft_layouts: Option<RaftLayouts>,
}

/// An inclusive range of Raft body layouts, as a peer states it in its
/// handshake. Never empty and never includes 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RaftLayouts {
    lowest: u8,
    highest: u8,
}

impl RaftLayouts {
    /// `None` for an empty range or one that starts at 0, which no build
    /// writes.
    pub const fn new(lowest: u8, highest: u8) -> Option<Self> {
        if lowest == 0 || lowest > highest {
            return None;
        }
        Some(Self { lowest, highest })
    }

    pub const fn lowest(self) -> u8 {
        self.lowest
    }

    pub const fn highest(self) -> u8 {
        self.highest
    }

    pub const fn contains(self, layout: u8) -> bool {
        self.lowest <= layout && layout <= self.highest
    }

    /// The highest layout both ranges hold, or `None` when they share none.
    pub fn common(self, other: Self) -> Option<u8> {
        let highest = self.highest.min(other.highest);
        (self.lowest.max(other.lowest) <= highest).then_some(highest)
    }
}

impl std::fmt::Display for RaftLayouts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.lowest == self.highest {
            write!(f, "{}", self.lowest)
        } else {
            write!(f, "{}..={}", self.lowest, self.highest)
        }
    }
}

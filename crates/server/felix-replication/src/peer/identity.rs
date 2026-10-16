//! Who answered on a peer connection: the copy of the data and the process
//! that holds it, as the connection's handshake gave them.

use felix_storage::StorageId;

/// The storage identity and node incarnation a peer gave in a connection's
/// handshake.
///
/// A connection can outlive the process it was opened to, so this describes
/// the process that was there when it opened, not necessarily the one there
/// now. A wiped data directory changes the storage identity; any restart
/// that registers again changes the incarnation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct PeerIdentity {
    /// `None` from a peer that does not send one.
    pub storage: Option<StorageId>,
    pub incarnation: Incarnation,
}

impl PeerIdentity {
    /// A peer that gave only a storage identity.
    pub fn storage(storage: StorageId) -> Self {
        Self {
            storage: Some(storage),
            incarnation: Incarnation::Unsent,
        }
    }
}

/// The node incarnation a peer gave in a connection's handshake.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Incarnation {
    /// The peer does not send one: it predates
    /// [`PeerCapabilities::NODE_INCARNATION`], or this side did not ask.
    ///
    /// [`PeerCapabilities::NODE_INCARNATION`]: felix_wire::internal::PeerCapabilities::NODE_INCARNATION
    #[default]
    Unsent,
    /// The peer sends one but had not registered when the connection opened.
    Unregistered,
    /// Registered at this incarnation when the connection opened.
    At(u64),
}

impl Incarnation {
    /// What a handshake answer says, given whether it carried the bit.
    pub(crate) fn from_handshake(sent: bool, incarnation: Option<u64>) -> Self {
        match (sent, incarnation) {
            (false, _) => Self::Unsent,
            (true, None) => Self::Unregistered,
            (true, Some(at)) => Self::At(at),
        }
    }
}

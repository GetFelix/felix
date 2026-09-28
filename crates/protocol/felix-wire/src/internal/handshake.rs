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
}

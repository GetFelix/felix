//! What each peer said it can do, from whichever handshake last told us.
//!
//! Filled from both directions: the pool records what a peer answered when
//! this broker dialled it, and the listener records what a peer offered when
//! it dialled this broker. A leader deciding whether its replicas can be
//! fenced may never have dialled the one that just lost the shard, but that
//! one has been shipping to it.

use std::collections::HashMap;
use std::sync::Arc;

use felix_wire::internal::PeerCapabilities;
use parking_lot::RwLock;

/// Capabilities by node id. Cheap to clone; clones share the map.
#[derive(Debug, Clone, Default)]
pub struct KnownCapabilities {
    peers: Arc<RwLock<HashMap<String, PeerCapabilities>>>,
}

impl KnownCapabilities {
    /// Record what `node_id` said in its latest handshake, replacing what it
    /// said before: a peer restarted on an older build has lost what it had.
    pub fn record(&self, node_id: &str, capabilities: PeerCapabilities) {
        self.peers.write().insert(node_id.to_string(), capabilities);
    }

    /// What `node_id` last said, or `None` if no handshake with it has
    /// completed since this broker started.
    pub fn get(&self, node_id: &str) -> Option<PeerCapabilities> {
        self.peers.read().get(node_id).copied()
    }
}

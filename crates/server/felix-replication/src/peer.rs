//! The broker-internal transport: how brokers reach each other.
//!
//! [`PeerServer`] accepts peer connections; [`PeerPool`] makes them. Both speak
//! the protocol in `felix_wire::internal`, on their own listener, with their own
//! ALPN and their own configuration — see `docs/internal-protocol.md` for the
//! wire contract and the module docs here for what the transport guarantees.
//!
//! The transport is complete on its own terms: a request sent through the pool
//! always terminates, and an unhealthy peer cannot consume this broker. What
//! travels over it belongs elsewhere: forwarding in the broker service's
//! `serving::forward`, and replication in the rest of this crate.

pub(crate) mod capabilities;
pub(crate) mod codec;
pub mod config;
pub mod identity;
pub mod metrics;
pub mod pool;
pub mod server;
pub mod tls;

mod partition;

pub use capabilities::KnownCapabilities;
pub use config::PeerTransportConfig;
pub(crate) use identity::{Incarnation, PeerIdentity};
pub(crate) use partition::PartitionInjector;
pub use pool::{PeerError, PeerPool, PeerRequester};
pub use server::{PeerRequestHandler, PeerServer, RaftHandler, UnavailableHandler};

/// The second byte of a Raft body for an append that carries entries (see
/// `raft/wire.rs`). The pool sends one on a replication stream, so a
/// heartbeat or vote on the control stream never waits behind its records.
pub(crate) const RAFT_APPEND_ENTRIES: u8 = 2;

/// The longest a listener works on one Raft message, whatever its sender
/// names, so a peer cannot pin a permit with a long deadline.
pub(crate) const RAFT_MAX_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

/// How long the sender of a Raft body waits for its answer: a `u32` of
/// milliseconds in bytes 2..6 (see `raft/wire.rs`), held to
/// [`RAFT_MAX_DEADLINE`]. A body too short to name one gets the longest.
pub(crate) fn raft_deadline(body: &[u8]) -> std::time::Duration {
    body.get(2..6)
        .and_then(|bytes| bytes.try_into().ok())
        .map(|bytes| std::time::Duration::from_millis(u64::from(u32::from_be_bytes(bytes))))
        .map_or(RAFT_MAX_DEADLINE, |named| named.min(RAFT_MAX_DEADLINE))
}

#[cfg(test)]
mod tests;

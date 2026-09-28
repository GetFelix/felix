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

pub mod codec;
pub mod config;
pub mod metrics;
pub mod pool;
pub mod server;
pub mod tls;

mod partition;

pub use config::PeerTransportConfig;
pub use partition::PartitionInjector;
pub use pool::{PeerError, PeerPool, PeerRequester};
pub use server::{PeerRequestHandler, PeerServer, UnavailableHandler};

#[cfg(test)]
mod tests;

//! The QUIC transport every Felix connection runs over.
//!
//! **Start at [`QuicServer`] and [`QuicClient`].** A server accepts
//! connections and a client makes them; both hand back a [`QuicConnection`],
//! which is what the broker and the client SDK open streams on.
//! [`TransportConfig`] carries the tuning — congestion window, path MTU,
//! socket buffers, stream limits.
//!
//! This crate deliberately knows nothing about Felix messages. Framing and
//! message types live in `felix-wire`; this layer moves bytes and manages
//! connection and stream lifetime, so the protocol can change without
//! touching transport tuning and the reverse.
//!
//! One sizing rule is load-bearing enough to state here: a server endpoint
//! multiplexes every connection and drives traffic both ways, so it gets a
//! runtime to itself while client endpoints share the rest. Putting a client
//! endpoint on the server's runtime measured 5-6x slower, because the two
//! halves of a request/response ping-pong then serialize on one thread. See
//! [`plan_server_endpoints`].

mod alpn;
mod client;
mod config;
mod connection;
mod io_runtime;
mod server;
mod socket;

pub use alpn::alpn_optional_server_config;
pub use client::QuicClient;
pub use config::TransportConfig;
pub use connection::{ConnectionId, ConnectionInfo, QuicConnection};
pub use io_runtime::{plan_server_endpoints, required_io_runtime_threads};
pub use server::{IncomingConnection, QuicServer};

#[cfg(test)]
mod tests;

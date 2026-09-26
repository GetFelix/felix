//! Belonging to a cluster.
//!
//! [`membership`] registers this node and heartbeats; [`credential`] holds the
//! token it presents and refreshes it before expiry. [`catalog_sync`] keeps the
//! metadata catalog in step with the control plane, [`node_catalog`] turns node
//! ids into addresses, and [`client_endpoints`] is what this broker tells a
//! client about where to connect. [`lease`] is the authority to serve at all:
//! renewed by the same heartbeat, checked cheaply on admission and against the
//! clock again before any record is committed.

pub mod catalog_sync;
pub mod client_endpoints;
pub(crate) mod controlplane_http;
pub mod credential;
pub mod lease;
pub mod membership;
pub mod node_catalog;

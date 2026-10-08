//! The HTTP API through the in-process router, over the in-memory store.
//!
//! One binary because the suites share their request and credential helpers
//! and none of them touches process-wide state.
mod auth;
mod common;
mod coverage;
mod node_admin;
mod node_heartbeat;
mod node_write_auth;
mod pagination;
mod resource_auth;
mod resources;
mod smoke;

//! Publish to a broker in-process, without a connection.
//!
//! The same fence, append and quorum wait a QUIC publish goes through; see
//! `quic::handlers::publish::direct`.

pub use super::quic::handlers::publish::direct::{
    DirectPublishError, LocalTarget, Refusal, Resolution, publish_direct, resolve_publish,
};

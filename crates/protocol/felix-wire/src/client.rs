//! The client protocol (`FLX1`): what a client and a broker say to each other.
//!
//! Every frame is a [`frame::FrameHeader`] and a payload. Most payloads are a
//! JSON [`message::Message`]; a frame [`flags`] bit selects one of the
//! [`binary`] layouts instead for the hot paths. [`features`] names the
//! optional requests a broker serves, which is negotiated separately from the
//! flags.

pub mod binary;
pub(crate) mod error_code;
pub(crate) mod features;
pub(crate) mod flags;
pub(crate) mod frame;
pub(crate) mod message;
pub(crate) mod record_headers;
pub mod text;

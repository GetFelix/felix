//! JSON round trips for every message, and the byte-compatibility each
//! optional field promises an older peer.

mod auth;
mod cache;
mod codec;
mod commit;
mod idempotent;
mod stream;
mod topology;
mod unsupported;

//! The small things every Felix crate agrees on.
//!
//! Deliberately thin. Something belongs here only when two crates that do not
//! depend on each other must agree on it exactly — a type crossing a process
//! boundary, or a name an operator types.
//!
//! - [`membership`] — the broker-to-control-plane shapes. These cross a
//!   process boundary as JSON, so a field renamed on one side and not the
//!   other is a silent mismatch; sharing the types puts that back in the
//!   compiler's hands.
//! - [`env_registry`] — every `FELIX_*` variable the workspace reads, so a
//!   mistyped one is reported instead of silently taking a default.
//! - [`lifecycle`] — start-up, readiness and bounded drain, shared by both
//!   service binaries. Feature-gated behind `lifecycle` so a library that
//!   never runs a process does not pull in tokio.
//! - [`tls`] — certificate and key files that are re-read when they change,
//!   shared by the broker's listeners and the control plane's. Behind the
//!   `tls` feature.
//! - [`ids`] and [`Error`] — the region id and its parse error.

pub mod env_registry;
mod error;
pub mod ids;
// Feature-gated so that library consumers which never run a process
// (felix-router) do not pull in tokio.
#[cfg(feature = "lifecycle")]
pub mod lifecycle;
// Not gated: they are serde types, and the two ends need them whether or not
// either runs a process.
pub mod membership;
#[cfg(feature = "tls")]
pub mod tls;

pub use error::{Error, Result};

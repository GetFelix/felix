//! The HTTP client every control-plane call goes through.
//!
//! Every request gets a deadline. A control plane that accepts the connection
//! and then stalls would otherwise hang the caller forever: a stuck heartbeat
//! lets the lease lapse with no retry ever made, and a stuck replica report
//! blocks the reporter that every `Quorum` publish waits on.
//!
//! The deadlines are sized against the broker's lease, which is 0.75 of the
//! control plane's expiry window (15 s by default, so about 11 s). A caller
//! that needs a tighter bound sets its own per request, as the heartbeat does,
//! and so does one that must wait longer, as the assignment long-poll does.
use std::time::Duration;

use anyhow::{Context, Result};

/// How long to wait for a TCP (and TLS) connection to the control plane.
///
/// Well under a heartbeat retry, so an unreachable address fails fast enough
/// to be retried before the lease runs out.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// The default deadline for a whole request, response body included.
///
/// Under the default lease with room for a retry, and long enough for a
/// catalog snapshot from a busy control plane.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// A client with [`CONNECT_TIMEOUT`] and [`REQUEST_TIMEOUT`] applied, trusting
/// the configured control-plane CA.
pub(crate) fn build() -> Result<reqwest::Client> {
    crate::cluster::controlplane_http::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("build the control-plane HTTP client")
}

#[cfg(test)]
mod tests;

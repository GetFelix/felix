//! What a client connection may cost the broker before it has authenticated.
//!
//! Authentication is per stream, so until a stream's `Auth` succeeds anything
//! it sends is from an unknown peer. Three limits keep that cheap: frames are
//! capped at `preauth_max_frame_bytes`, only `preauth_max_streams_per_conn`
//! streams per connection read at once, and a connection that authenticates
//! nothing within `auth_timeout_ms` is closed. The broker-wide connection cap
//! is [`ConnectionLimit`].

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::config::BrokerConfig;

/// QUIC application close code for a connection that did not authenticate in
/// time. `0` is a deliberate shutdown.
pub(crate) const AUTH_TIMEOUT_CLOSE_CODE: u32 = 1;

/// One connection's pre-auth state, shared by its streams.
pub(crate) struct PreAuthGate {
    authenticated: watch::Sender<bool>,
    streams: Arc<Semaphore>,
    max_frame_bytes: usize,
}

impl PreAuthGate {
    pub(crate) fn new(config: &BrokerConfig) -> Self {
        Self {
            authenticated: watch::Sender::new(false),
            streams: Arc::new(Semaphore::new(config.preauth_max_streams_per_conn.max(1))),
            max_frame_bytes: config.preauth_max_frame_bytes.min(config.max_frame_bytes),
        }
    }

    /// Wait for a slot to read an unauthenticated stream. Held until the
    /// stream authenticates or ends.
    pub(crate) async fn admit_stream(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.streams)
            .acquire_owned()
            .await
            .expect("pre-auth semaphore is never closed")
    }

    /// The largest frame a stream may send, given whether it has
    /// authenticated.
    pub(crate) fn frame_cap(&self, stream_authenticated: bool, max_frame_bytes: usize) -> usize {
        if stream_authenticated {
            max_frame_bytes
        } else {
            self.max_frame_bytes
        }
    }

    /// Record that a stream on this connection authenticated, which lifts the
    /// connection's deadline.
    pub(crate) fn mark_authenticated(&self) {
        self.authenticated.send_replace(true);
    }

    /// Resolves once any stream on this connection has authenticated.
    pub(crate) async fn authenticated(&self) {
        let mut rx = self.authenticated.subscribe();
        // The sender lives as long as `self`, so this cannot fail.
        let _ = rx.wait_for(|authenticated| *authenticated).await;
    }
}

/// How long a connection has to authenticate; `None` when disabled.
pub(crate) fn auth_timeout(config: &BrokerConfig) -> Option<Duration> {
    (config.auth_timeout_ms > 0).then(|| Duration::from_millis(config.auth_timeout_ms))
}

/// The broker-wide cap on client connections, shared by every client
/// listener.
#[derive(Clone)]
pub struct ConnectionLimit {
    permits: Arc<Semaphore>,
}

impl ConnectionLimit {
    pub fn new(max_connections: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_connections.max(1))),
        }
    }

    /// A slot for one connection, or `None` at the cap. Refused rather than
    /// queued: a client told no can go elsewhere, one left waiting cannot
    /// tell a full broker from a stuck one.
    pub(crate) fn try_admit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.permits).try_acquire_owned().ok()
    }
}

#[cfg(test)]
mod tests;

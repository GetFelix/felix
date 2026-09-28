//! Per-peer bookkeeping: which connections exist, which one a request uses,
//! when a refused peer may be dialled again, and what the reaper may close.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;

use super::connection::PeerConnection;
use super::{PeerError, PeerPool};
use crate::peer::metrics;

/// One peer broker, and this broker's connections to it.
pub(super) struct Peer {
    pub(super) node_id: String,
    /// Async because establishing is held across a dial: concurrent requests to
    /// a cold peer must produce one connection attempt, not one each.
    pub(super) state: tokio::sync::Mutex<PeerState>,
    pub(super) inflight: Arc<Semaphore>,
}

impl Peer {
    pub(super) fn new(node_id: String, max_inflight: usize) -> Self {
        Self {
            node_id,
            state: tokio::sync::Mutex::new(PeerState::default()),
            inflight: Arc::new(Semaphore::new(max_inflight)),
        }
    }

    /// A live connection to this peer, dialling if there is not one.
    pub(super) async fn connection(
        &self,
        pool: &PeerPool,
        addr: SocketAddr,
    ) -> std::result::Result<Arc<PeerConnection>, PeerError> {
        let mut state = self.state.lock().await;
        state.drop_dead();

        if let Some(connection) = state.next(pool.config.conns_per_peer) {
            return Ok(connection);
        }

        if let Some(remaining) = state.backoff_remaining() {
            return Err(PeerError::Unavailable {
                node_id: self.node_id.clone(),
                detail: format!(
                    "reconnecting in {remaining:?} after {} failed attempts",
                    state.attempts
                ),
            });
        }

        if state.attempts > 0 {
            metrics::record_reconnect();
        }
        match pool.dial(&self.node_id, addr).await {
            Ok(connection) => {
                state.attempts = 0;
                state.backoff_until = None;
                state.connections.push(Arc::clone(&connection));
                Ok(connection)
            }
            Err(err) => {
                let delay = pool.config.reconnect_delay(state.attempts);
                state.attempts = state.attempts.saturating_add(1);
                state.backoff_until = Some(Instant::now() + delay);
                Err(err)
            }
        }
    }
}

/// What the pool needs of a connection in order to manage one.
///
/// Named as a trait so the pooling policy below — growth before reuse, backoff,
/// idle eviction — can be tested without a QUIC endpoint behind every
/// connection.
pub(super) trait Pooled {
    fn is_live(&self) -> bool;
    fn has_pending(&self) -> bool;
    fn idle_for(&self) -> Duration;
    fn close(&self, reason: &str);
}

pub(super) struct PeerState<C = PeerConnection> {
    pub(super) connections: Vec<Arc<C>>,
    pub(super) cursor: usize,
    pub(super) attempts: u32,
    pub(super) backoff_until: Option<Instant>,
}

impl<C> Default for PeerState<C> {
    fn default() -> Self {
        Self {
            connections: Vec::new(),
            cursor: 0,
            attempts: 0,
            backoff_until: None,
        }
    }
}

impl<C: Pooled> PeerState<C> {
    pub(super) fn drop_dead(&mut self) {
        self.connections.retain(|connection| connection.is_live());
    }

    pub(super) fn live_connections(&self) -> usize {
        self.connections
            .iter()
            .filter(|connection| connection.is_live())
            .count()
    }

    pub(super) fn in_backoff(&self) -> bool {
        self.backoff_remaining().is_some()
    }

    pub(super) fn backoff_remaining(&self) -> Option<Duration> {
        let until = self.backoff_until?;
        until.checked_duration_since(Instant::now())
    }

    /// The next connection to use, or `None` if the pool is not yet full.
    ///
    /// Growing to the configured size takes precedence over reuse: a pool of one
    /// connection would otherwise never reach two, since the first is always
    /// available.
    pub(super) fn next(&mut self, target: usize) -> Option<Arc<C>> {
        // Also guards the remainder below: a target of zero would otherwise
        // divide by an empty pool's length, panicking the forwarding path.
        if self.connections.is_empty() || self.connections.len() < target {
            return None;
        }
        let connection = self.connections.get(self.cursor % self.connections.len())?;
        self.cursor = self.cursor.wrapping_add(1);
        Some(Arc::clone(connection))
    }

    pub(super) fn evict_idle(&mut self, idle_timeout: Duration) {
        self.connections.retain(|connection| {
            // A connection with work outstanding is not idle no matter how long
            // ago it was handed out.
            if connection.idle_for() < idle_timeout || connection.has_pending() {
                return true;
            }
            connection.close("idle");
            false
        });
    }

    pub(super) fn close_all(&mut self, reason: &str) {
        for connection in self.connections.drain(..) {
            connection.close(reason);
        }
    }
}

#[cfg(test)]
mod tests;

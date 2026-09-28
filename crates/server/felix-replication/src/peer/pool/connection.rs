//! One connection to a peer: its streams, the requests waiting on it, and
//! failing every one of them the moment it drops.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use felix_transport::QuicConnection;
use felix_wire::internal::{Hello, InternalMessage};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::PeerError;
use super::state::Pooled;
use crate::peer::codec::{Incoming, read_frame};
use crate::peer::metrics;

/// One established connection to a peer, with its multiplexed streams.
pub(super) struct PeerConnection {
    pub(super) node_id: String,
    pub(super) connection: QuicConnection,
    pub(super) lanes: Vec<mpsc::Sender<Bytes>>,
    pub(super) cursor: AtomicUsize,
    pub(super) pending: Arc<Mutex<Pending>>,
    pub(super) next_correlation: AtomicU64,
    pub(super) last_used: Mutex<Instant>,
    pub(super) tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// Waiters for responses that have not arrived.
///
/// `closed` is what makes a lost connection fail its waiters rather than leave
/// them to time out, and it also closes the window where a request registers
/// itself just after the connection died.
#[derive(Default)]
pub(super) struct Pending {
    pub(super) waiters: HashMap<u64, oneshot::Sender<InternalMessage>>,
    pub(super) closed: bool,
}

impl PeerConnection {
    /// Open the request streams and exchange the handshake.
    pub(super) async fn establish(
        connection: QuicConnection,
        node_id: String,
        local_node_id: String,
        streams: usize,
        handshake_timeout: Duration,
    ) -> Result<Arc<Self>> {
        let pending = Arc::new(Mutex::new(Pending::default()));
        let mut lanes = Vec::with_capacity(streams);
        let mut tasks = Vec::new();

        for _ in 0..streams {
            let (mut send, mut recv) = connection.open_bi().await.context("open peer stream")?;
            let (tx, mut rx) = mpsc::channel::<Bytes>(64);

            tasks.push(connection.spawn_pump(async move {
                while let Some(frame) = rx.recv().await {
                    if send.write_all(&frame).await.is_err() {
                        // The reader half sees the same failure and fails every
                        // waiter, so there is nothing to report here.
                        break;
                    }
                }
                let _ = send.finish();
            }));

            let responses = Arc::clone(&pending);
            tasks.push(connection.spawn_pump(async move {
                loop {
                    match read_frame(&mut recv).await {
                        Ok(Incoming::Message(message)) => {
                            let waiter = responses.lock().waiters.remove(&message.correlation_id());
                            // A response with no waiter is one whose request
                            // already timed out. Dropping it is correct; the
                            // caller has been told the request failed.
                            if let Some(waiter) = waiter {
                                let _ = waiter.send(message);
                            }
                        }
                        // A *response* kind this build does not know. Nothing
                        // here sends a request whose answer it cannot read, so
                        // this means the peer is from a later build that
                        // answers differently — the waiter is left to time out
                        // rather than given something that cannot be parsed,
                        // and the lane survives for every other request on it.
                        Ok(Incoming::UnknownKind { kind, .. }) => {
                            tracing::debug!(
                                kind,
                                "peer answered with a kind this build does not know"
                            );
                        }
                        Ok(Incoming::Eof) => break,
                        Err(err) => {
                            tracing::debug!(error = %err, "peer response stream ended");
                            break;
                        }
                    }
                }
            }));

            lanes.push(tx);
        }

        let peer = Arc::new(Self {
            node_id: node_id.clone(),
            connection,
            lanes,
            cursor: AtomicUsize::new(0),
            pending: Arc::clone(&pending),
            next_correlation: AtomicU64::new(1),
            last_used: Mutex::new(Instant::now()),
            tasks: Mutex::new(tasks),
        });

        // One watcher fails every waiter the moment the connection drops, which
        // is what makes "no forwarded publish is left pending" hold.
        //
        // It takes the pending map and a connection handle rather than the
        // `PeerConnection` itself: that would be a cycle through `tasks`, and the
        // connection would never drop.
        let watched = peer.connection.clone();
        let waiters = Arc::clone(&pending);
        let watched_id = node_id.clone();
        let watcher = peer.connection.spawn_pump(async move {
            let reason = watched.closed().await;
            tracing::debug!(peer = %watched_id, %reason, "peer connection closed");
            metrics::record_connection_loss(close_reason_label(&reason));
            let mut waiters = waiters.lock();
            waiters.closed = true;
            waiters.waiters.clear();
        });
        peer.tasks.lock().push(watcher);

        peer.handshake(local_node_id, handshake_timeout).await?;
        Ok(peer)
    }

    /// Exchange `Hello`, and check the peer is who the catalog said.
    pub(super) async fn handshake(&self, local_node_id: String, timeout: Duration) -> Result<()> {
        let hello = InternalMessage::Hello(Hello {
            correlation_id: self.next_correlation.fetch_add(1, Ordering::Relaxed),
            node_id: local_node_id,
        });
        let shutdown = CancellationToken::new();
        let response = tokio::time::timeout(timeout, self.request(hello, &shutdown))
            .await
            .map_err(|_| anyhow!("no handshake response within {timeout:?}"))?
            .map_err(|err| anyhow!("{err}"))?;

        match response {
            InternalMessage::HelloOk(ok) if ok.node_id == self.node_id => Ok(()),
            InternalMessage::HelloOk(ok) => {
                self.close("peer identity mismatch");
                bail!(
                    "dialled {} but the broker there is {}",
                    self.node_id,
                    ok.node_id
                )
            }
            other => {
                self.close("unexpected handshake response");
                bail!("expected HelloOk, got {:?}", other.kind())
            }
        }
    }

    /// Send one request and wait for its terminal response.
    pub(super) async fn request(
        &self,
        message: InternalMessage,
        shutdown: &CancellationToken,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let correlation_id = match &message {
            // The handshake picks its own id before the connection is usable.
            InternalMessage::Hello(hello) => hello.correlation_id,
            _ => self.next_correlation.fetch_add(1, Ordering::Relaxed),
        };
        let message = with_correlation(message, correlation_id);
        let frame = message.encode().map_err(|err| PeerError::Handshake {
            node_id: self.node_id.clone(),
            detail: err.to_string(),
        })?;

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock();
            if pending.closed {
                return Err(PeerError::Disconnected {
                    node_id: self.node_id.clone(),
                });
            }
            pending.waiters.insert(correlation_id, tx);
        }
        *self.last_used.lock() = Instant::now();

        let lane = self.cursor.fetch_add(1, Ordering::Relaxed) % self.lanes.len();
        if self.lanes[lane].send(frame).await.is_err() {
            self.pending.lock().waiters.remove(&correlation_id);
            return Err(PeerError::Disconnected {
                node_id: self.node_id.clone(),
            });
        }

        tokio::select! {
            response = rx => match response {
                Ok(message) => Ok(message),
                // The sender is dropped by `fail_all`, so this is the
                // connection dropping rather than a lost response.
                Err(_) => Err(PeerError::Disconnected { node_id: self.node_id.clone() }),
            },
            _ = shutdown.cancelled() => {
                self.pending.lock().waiters.remove(&correlation_id);
                Err(PeerError::ShuttingDown)
            }
        }
    }

    pub(super) fn is_live(&self) -> bool {
        self.connection.close_reason().is_none() && !self.pending.lock().closed
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.lock().waiters.is_empty()
    }

    pub(super) fn idle_for(&self) -> Duration {
        self.last_used.lock().elapsed()
    }

    /// Fail every waiter and refuse new ones.
    pub(super) fn fail_all(&self) {
        let mut pending = self.pending.lock();
        pending.closed = true;
        // Dropping each sender is the signal; the waiting side reads a dropped
        // sender as `Disconnected`.
        pending.waiters.clear();
    }

    pub(super) fn close(&self, reason: &str) {
        self.fail_all();
        self.connection.close(0u32.into(), reason.as_bytes());
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }
    }
}

impl Pooled for PeerConnection {
    fn is_live(&self) -> bool {
        PeerConnection::is_live(self)
    }

    fn has_pending(&self) -> bool {
        PeerConnection::has_pending(self)
    }

    fn idle_for(&self) -> Duration {
        PeerConnection::idle_for(self)
    }

    fn close(&self, reason: &str) {
        PeerConnection::close(self, reason);
    }
}

impl Drop for PeerConnection {
    fn drop(&mut self) {
        // Tasks hold `SendStream`/`RecvStream` handles, so leaving them running
        // would keep the connection alive after the pool has forgotten it.
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }
    }
}

/// Stamp the pool's correlation id onto a caller-supplied message.
///
/// Ids are the connection's to assign — a caller cannot know what is already in
/// flight on the stream its request lands on.
pub(super) fn with_correlation(message: InternalMessage, correlation_id: u64) -> InternalMessage {
    use felix_wire::internal::*;
    match message {
        InternalMessage::ForwardPublish(m) => InternalMessage::ForwardPublish(ForwardPublish {
            correlation_id,
            ..m
        }),
        InternalMessage::ForwardPublishOk(m) => {
            InternalMessage::ForwardPublishOk(ForwardPublishOk {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ForwardPublishError(m) => {
            InternalMessage::ForwardPublishError(ForwardPublishError {
                correlation_id,
                ..m
            })
        }
        InternalMessage::NotLeader(m) => InternalMessage::NotLeader(NotLeader {
            correlation_id,
            ..m
        }),
        InternalMessage::ForwardCacheOp(m) => InternalMessage::ForwardCacheOp(ForwardCacheOp {
            correlation_id,
            ..m
        }),
        InternalMessage::ForwardCacheOk(m) => InternalMessage::ForwardCacheOk(ForwardCacheOk {
            correlation_id,
            ..m
        }),
        InternalMessage::ForwardCacheError(m) => {
            InternalMessage::ForwardCacheError(ForwardCacheError {
                correlation_id,
                ..m
            })
        }
        InternalMessage::Hello(m) => InternalMessage::Hello(Hello {
            correlation_id,
            ..m
        }),
        InternalMessage::HelloOk(m) => InternalMessage::HelloOk(HelloOk {
            correlation_id,
            ..m
        }),
        InternalMessage::ReplicateGroupRecords(m) => {
            InternalMessage::ReplicateGroupRecords(ReplicateRecords {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateRebuild(m) => {
            InternalMessage::ReplicateRebuild(ReplicateRebuild {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateGroupBootstrap(m) => {
            InternalMessage::ReplicateGroupBootstrap(ReplicateBootstrap {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateDeadLetterRecords(m) => {
            InternalMessage::ReplicateDeadLetterRecords(ReplicateRecords {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateDeadLetterBootstrap(m) => {
            InternalMessage::ReplicateDeadLetterBootstrap(ReplicateBootstrap {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateCounterRecords(m) => {
            InternalMessage::ReplicateCounterRecords(ReplicateRecords {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateCounterBootstrap(m) => {
            InternalMessage::ReplicateCounterBootstrap(ReplicateBootstrap {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateCacheRecords(m) => {
            InternalMessage::ReplicateCacheRecords(ReplicateRecords {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateCacheBootstrap(m) => {
            InternalMessage::ReplicateCacheBootstrap(ReplicateBootstrap {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateRecords(m) => {
            InternalMessage::ReplicateRecords(ReplicateRecords {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateMarkedRecords(m) => {
            InternalMessage::ReplicateMarkedRecords(ReplicateRecords {
                correlation_id,
                ..m
            })
        }
        InternalMessage::ReplicateOk(m) => InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id,
            ..m
        }),
        InternalMessage::ReplicateError(m) => InternalMessage::ReplicateError(ReplicateError {
            correlation_id,
            ..m
        }),
        InternalMessage::ReplicateBootstrap(m) => {
            InternalMessage::ReplicateBootstrap(ReplicateBootstrap {
                correlation_id,
                ..m
            })
        }
    }
}

pub(super) fn close_reason_label(reason: &quinn::ConnectionError) -> &'static str {
    match reason {
        quinn::ConnectionError::TimedOut => "timeout",
        quinn::ConnectionError::ApplicationClosed(_) => "closed_by_peer",
        quinn::ConnectionError::LocallyClosed => "closed_locally",
        quinn::ConnectionError::ConnectionClosed(_) => "transport_error",
        quinn::ConnectionError::Reset => "reset",
        _ => "other",
    }
}

#[cfg(test)]
mod tests;

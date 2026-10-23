//! One connection to a peer: its streams, the requests waiting on it, and
//! failing every one of them the moment it drops.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use felix_storage::StorageId;
use felix_transport::QuicConnection;
use felix_wire::internal::{ErrorCode, Hello, InternalMessage, PeerCapabilities};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::PeerError;
use super::state::Pooled;
use crate::peer::codec::{Incoming, read_frame};
use crate::peer::identity::{Incarnation, PeerIdentity};
use crate::peer::metrics;

/// One established connection to a peer, with its multiplexed streams.
pub(super) struct PeerConnection {
    pub(super) node_id: String,
    pub(super) connection: QuicConnection,
    pub(super) lanes: Vec<Lane>,
    /// Streams in each of the forward and replication classes.
    pub(super) per_class: usize,
    pub(super) forward_cursor: AtomicUsize,
    pub(super) replication_cursor: AtomicUsize,
    pub(super) pending: Arc<Mutex<Pending>>,
    pub(super) next_correlation: AtomicU64,
    pub(super) last_used: Mutex<Instant>,
    pub(super) tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// What the peer said it can do in the handshake. `NONE` until then, and
    /// for a peer that predates the bits.
    pub(super) capabilities: Mutex<PeerCapabilities>,
    /// The peer's storage identity and incarnation from the handshake.
    pub(super) identity: Mutex<PeerIdentity>,
}

/// Consecutive timeouts on one stream after which it is replaced. One can be
/// a slow handler; two in a row with nothing answered between them is a
/// stream that is not being served.
const RETIRE_AFTER_TIMEOUTS: u32 = 2;

/// One request stream, replaced when it stops being served.
pub(super) struct Lane {
    stream: Mutex<Arc<LaneStream>>,
    /// Held while a replacement is opened, so concurrent timeouts open one.
    replacing: tokio::sync::Mutex<()>,
}

impl Lane {
    fn current(&self) -> Arc<LaneStream> {
        Arc::clone(&self.stream.lock())
    }
}

/// The QUIC stream a lane currently writes to.
pub(super) struct LaneStream {
    tx: mpsc::Sender<Bytes>,
    /// Timeouts since this stream last carried an answer.
    unanswered: AtomicU32,
    /// Set when either half has ended; nothing sent on it can be answered.
    ended: AtomicBool,
}

impl LaneStream {
    fn dead(&self) -> bool {
        self.ended.load(Ordering::Relaxed)
    }
}

/// Open one request stream, with its writer and the reader that hands answers
/// to their waiters.
async fn open_stream(
    connection: &QuicConnection,
    pending: &Arc<Mutex<Pending>>,
) -> Result<(Arc<LaneStream>, [tokio::task::JoinHandle<()>; 2])> {
    let (mut send, mut recv) = connection.open_bi().await.context("open peer stream")?;
    let (tx, mut rx) = mpsc::channel::<Bytes>(64);
    let stream = Arc::new(LaneStream {
        tx,
        unanswered: AtomicU32::new(0),
        ended: AtomicBool::new(false),
    });

    // Both halves hold the stream weakly: it owns the writer's sender, so a
    // strong reference would keep the writer waiting on itself forever.
    let ended = Arc::downgrade(&stream);
    let writer = connection.spawn_pump(async move {
        while let Some(frame) = rx.recv().await {
            if send.write_all(&frame).await.is_err() {
                break;
            }
        }
        if let Some(lane) = ended.upgrade() {
            lane.ended.store(true, Ordering::Relaxed);
        }
        let _ = send.finish();
    });

    let responses = Arc::clone(pending);
    let lane = Arc::downgrade(&stream);
    let reader = connection.spawn_pump(async move {
        loop {
            match read_frame(&mut recv).await {
                Ok(Incoming::Message(message)) => {
                    if let Some(lane) = lane.upgrade() {
                        lane.unanswered.store(0, Ordering::Relaxed);
                    }
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
                    tracing::debug!(kind, "peer answered with a kind this build does not know");
                }
                Ok(Incoming::Eof) => break,
                Err(err) => {
                    tracing::debug!(error = %err, "peer response stream ended");
                    break;
                }
            }
        }
        if let Some(lane) = lane.upgrade() {
            lane.ended.store(true, Ordering::Relaxed);
        }
    });
    Ok((stream, [writer, reader]))
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
    ///
    /// `1 + 2 * streams` streams, one class each (see [`LaneClass`]). A peer
    /// that predates concurrent forwards answers a stream's requests one at a
    /// time, and a forwarded `Quorum` publish can hold a stream for its whole
    /// quorum wait. That wait needs this broker's replication to the peer, so
    /// replication sharing a stream with forwards can deadlock two brokers
    /// that forward to each other (#1458).
    pub(super) async fn establish(
        connection: QuicConnection,
        node_id: String,
        local_node_id: String,
        local_capabilities: PeerCapabilities,
        streams: usize,
        handshake_timeout: Duration,
    ) -> Result<Arc<Self>> {
        let pending = Arc::new(Mutex::new(Pending::default()));
        let streams = streams.max(1);
        let mut lanes = Vec::with_capacity(1 + 2 * streams);
        let mut tasks = Vec::new();

        for _ in 0..1 + 2 * streams {
            let (stream, opened) = open_stream(&connection, &pending).await?;
            tasks.extend(opened);
            lanes.push(Lane {
                stream: Mutex::new(stream),
                replacing: tokio::sync::Mutex::new(()),
            });
        }

        let peer = Arc::new(Self {
            node_id: node_id.clone(),
            connection,
            lanes,
            per_class: streams,
            forward_cursor: AtomicUsize::new(0),
            replication_cursor: AtomicUsize::new(0),
            pending: Arc::clone(&pending),
            next_correlation: AtomicU64::new(1),
            last_used: Mutex::new(Instant::now()),
            tasks: Mutex::new(tasks),
            capabilities: Mutex::new(PeerCapabilities::NONE),
            identity: Mutex::new(PeerIdentity::default()),
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

        peer.handshake(local_node_id, local_capabilities, handshake_timeout)
            .await?;
        Ok(peer)
    }

    /// Exchange `Hello`, and check the peer is who the catalog said.
    ///
    /// Offers this build's capabilities first. A peer that predates them
    /// refuses the kind, and is greeted again the way it always was: it then
    /// has none, so nothing that needs one is ever sent to it.
    pub(super) async fn handshake(
        &self,
        local_node_id: String,
        local_capabilities: PeerCapabilities,
        timeout: Duration,
    ) -> Result<()> {
        let offered = self.hello(local_node_id.clone(), Some(local_capabilities), timeout);
        let response = match offered.await? {
            InternalMessage::ForwardPublishError(err) if err.code == ErrorCode::UnsupportedKind => {
                self.hello(local_node_id, None, timeout).await?
            }
            other => other,
        };

        match response {
            InternalMessage::HelloOk(ok) if ok.node_id == self.node_id => {
                let capabilities = ok.capabilities.unwrap_or(PeerCapabilities::NONE);
                *self.capabilities.lock() = capabilities;
                *self.identity.lock() = PeerIdentity {
                    storage: ok.identity.map(StorageId::from_bytes),
                    incarnation: Incarnation::from_handshake(
                        capabilities.contains(PeerCapabilities::NODE_INCARNATION),
                        ok.incarnation,
                    ),
                };
                Ok(())
            }
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

    async fn hello(
        &self,
        node_id: String,
        capabilities: Option<PeerCapabilities>,
        timeout: Duration,
    ) -> Result<InternalMessage> {
        let hello = InternalMessage::Hello(Hello {
            correlation_id: self.next_correlation.fetch_add(1, Ordering::Relaxed),
            node_id,
            capabilities,
        });
        let shutdown = CancellationToken::new();
        self.request(hello, timeout, &shutdown)
            .await
            .map_err(|err| match err {
                PeerError::Timeout { .. } => anyhow!("no handshake response within {timeout:?}"),
                err => anyhow!("{err}"),
            })
    }

    /// What the peer said it can do.
    pub(super) fn capabilities(&self) -> PeerCapabilities {
        *self.capabilities.lock()
    }

    /// The peer's storage identity and incarnation from the handshake.
    pub(super) fn identity(&self) -> PeerIdentity {
        *self.identity.lock()
    }

    /// Send one request and wait up to `timeout` for its terminal response.
    pub(super) async fn request(
        &self,
        message: InternalMessage,
        timeout: Duration,
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

        let lane = match LaneClass::of(&message) {
            LaneClass::Control => 0,
            LaneClass::Forward => {
                1 + self.forward_cursor.fetch_add(1, Ordering::Relaxed) % self.per_class
            }
            LaneClass::Replication => {
                1 + self.per_class
                    + self.replication_cursor.fetch_add(1, Ordering::Relaxed) % self.per_class
            }
        };
        let stream = self.lanes[lane].current();
        if stream.dead() || stream.tx.send(frame).await.is_err() {
            self.pending.lock().waiters.remove(&correlation_id);
            self.replace_lane(lane, &stream, timeout).await;
            return Err(PeerError::Disconnected {
                node_id: self.node_id.clone(),
            });
        }

        tokio::select! {
            response = tokio::time::timeout(timeout, rx) => match response {
                Ok(Ok(message)) => Ok(message),
                // The sender is dropped by `fail_all`, so this is the
                // connection dropping rather than a lost response.
                Ok(Err(_)) => Err(PeerError::Disconnected { node_id: self.node_id.clone() }),
                Err(_) => {
                    self.pending.lock().waiters.remove(&correlation_id);
                    let unanswered = stream.unanswered.fetch_add(1, Ordering::Relaxed) + 1;
                    if unanswered >= RETIRE_AFTER_TIMEOUTS {
                        self.replace_lane(lane, &stream, timeout).await;
                    }
                    Err(PeerError::Timeout { node_id: self.node_id.clone(), timeout })
                }
            },
            _ = shutdown.cancelled() => {
                self.pending.lock().waiters.remove(&correlation_id);
                Err(PeerError::ShuttingDown)
            }
        }
    }

    /// Swap `lane`'s stream for a fresh one, if it is still `stale`.
    ///
    /// A stream whose peer stopped answering stays wedged for as long as the
    /// connection lives: a peer serves a stream one request at a time, so one
    /// handler that never returns holds every request behind it. The old
    /// stream is finished and left to drain; answers that still arrive on it
    /// reach their waiters if they have any.
    async fn replace_lane(&self, lane: usize, stale: &Arc<LaneStream>, within: Duration) {
        let slot = &self.lanes[lane];
        let _replacing = slot.replacing.lock().await;
        if !Arc::ptr_eq(&slot.current(), stale) || !self.is_live() {
            return;
        }
        // Bounded: opening waits for stream credit, which a peer still holding
        // the old streams may be slow to give back.
        let opened = tokio::time::timeout(within, open_stream(&self.connection, &self.pending))
            .await
            .unwrap_or_else(|_| Err(anyhow!("no stream credit within {within:?}")));
        match opened {
            Ok((fresh, opened)) => {
                *slot.stream.lock() = fresh;
                let mut tasks = self.tasks.lock();
                tasks.retain(|task| !task.is_finished());
                tasks.extend(opened);
                metrics::record_lane_replaced();
                tracing::warn!(peer = %self.node_id, lane, "replaced a peer stream that stopped answering");
            }
            Err(err) => {
                tracing::debug!(peer = %self.node_id, error = %err, "could not replace a peer stream");
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

/// Which of a connection's streams a request goes on.
///
/// Forwards wait on a quorum, and a quorum waits on replication, so the two
/// never share a stream. Nothing relies on a stream's order for anything but
/// framing: the replication driver keeps at most one exchange outstanding per
/// follower and log, so round-robin within a class cannot reorder a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaneClass {
    /// The handshake, the promotion fence and pings: one stream nothing slow
    /// shares. A ping queued behind a forward would read as a dead leader.
    Control,
    /// Forwarded publishes and cache operations.
    Forward,
    /// Everything else: records, bootstraps and rebuilds for every log.
    Replication,
}

impl LaneClass {
    pub(crate) fn of(message: &InternalMessage) -> Self {
        match message {
            InternalMessage::Hello(_)
            | InternalMessage::Fence(_)
            | InternalMessage::ReplicateFetch(_)
            | InternalMessage::Ping(_) => Self::Control,
            InternalMessage::ForwardPublish(_) | InternalMessage::ForwardCacheOp(_) => {
                Self::Forward
            }
            _ => Self::Replication,
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
        InternalMessage::Fence(m) => InternalMessage::Fence(Fence {
            correlation_id,
            ..m
        }),
        InternalMessage::FenceOk(m) => InternalMessage::FenceOk(FenceOk {
            correlation_id,
            ..m
        }),
        InternalMessage::ReplicateFetch(m) => InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id,
            ..m
        }),
        InternalMessage::Ping(_) => InternalMessage::Ping(Ping { correlation_id }),
        InternalMessage::Pong(_) => InternalMessage::Pong(Pong { correlation_id }),
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

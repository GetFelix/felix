//! The connections a client holds to one broker, and which of them a new
//! stream goes on.
//!
//! Streams are multiplexed: every stream this client opens to the broker is
//! placed on the least-loaded live connection, and a connection is added only
//! when every existing one is saturated -- past its stream budget, or out of
//! the stream credit the broker granted (QUIC `MAX_STREAMS`). The ceiling
//! bounds how far that goes; at the ceiling a new stream waits for whichever
//! connection gets credit back first, as QUIC itself would.
//!
//! What a stream carries is its owner's business: a publish writer, a cache
//! worker and a subscription all hold a stream and its lease the same way.
//!
//! A connection that dies takes only its own streams with it. It is dropped
//! from the set the next time a stream is placed, and its slot is refilled
//! when a stream needs the room.

use std::future::poll_fn;
use std::net::SocketAddr;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};

use anyhow::{Context, Result};
use felix_transport::{QuicClient, QuicConnection};
use quinn::{RecvStream, SendStream};
use tokio::sync::{Semaphore, mpsc};
use tracing::debug;

use super::{
    Credentials, EventRouterCommand, Negotiated, listener_targets, spawn_conn_stats_logger,
    spawn_event_router_with_config,
};

/// The connections to one broker, grown on demand up to a ceiling.
pub(crate) struct NodeConnections {
    endpoint: QuicClient,
    server_name: String,
    credentials: Arc<Credentials>,
    role: &'static str,
    ceiling: usize,
    streams_per_conn: usize,
    max_frame_bytes: usize,
    event_router_max_pending: usize,
    state: Mutex<State>,
    /// One connect at a time, so a burst of streams that all find the set
    /// saturated adds one connection rather than one each.
    growing: tokio::sync::Mutex<()>,
}

impl NodeConnections {
    /// A set with no connections yet; the first stream opens the first one,
    /// at `dialled`.
    pub(crate) fn new(
        endpoint: QuicClient,
        dialled: SocketAddr,
        server_name: &str,
        credentials: Arc<Credentials>,
        limits: NodeLimits,
    ) -> Self {
        Self {
            endpoint,
            server_name: server_name.to_string(),
            credentials,
            role: limits.role,
            ceiling: limits.ceiling.max(1),
            streams_per_conn: limits.streams_per_conn.max(1),
            max_frame_bytes: limits.max_frame_bytes,
            event_router_max_pending: limits.event_router_max_pending,
            state: Mutex::new(State {
                targets: vec![dialled],
                listeners: Vec::new(),
                links: Vec::new(),
                grown: 0,
            }),
            growing: tokio::sync::Mutex::new(()),
        }
    }

    /// Live connections in the set.
    pub(crate) fn connection_count(&self) -> usize {
        let mut state = self.state.lock().expect("node state");
        state.prune();
        state.links.len()
    }

    /// Every address a connection in this set has dialled, in first-use order.
    pub(crate) fn listeners(&self) -> Vec<SocketAddr> {
        self.state.lock().expect("node state").listeners.clone()
    }

    /// Spread later connections across the listener ports the broker
    /// reported. Only ports: see [`listener_targets`].
    pub(crate) fn learn_listeners(&self, dialled: SocketAddr, ports: &[u16]) {
        let targets = listener_targets(dialled, ports);
        if targets.len() > 1 {
            debug!(
                role = self.role,
                listeners = targets.len(),
                "spreading connections across listeners"
            );
        }
        self.state.lock().expect("node state").targets = targets;
    }

    /// Open connections until there are `count`, up to the ceiling.
    ///
    /// For a set whose connections should exist before any stream needs
    /// them. `announce` authenticates each one up front, because the broker
    /// closes a connection that authenticates nothing within its timeout and
    /// such a connection may wait a long time for its first stream.
    pub(crate) async fn fill(&self, count: usize, announce: bool) -> Result<()> {
        let _growing = self.growing.lock().await;
        while self.connection_count() < count.min(self.ceiling) {
            let link = self.connect().await?;
            if announce {
                self.credentials.announce(&link.connection).await?;
            }
        }
        Ok(())
    }

    /// Open and authenticate a stream on the connection with the most room,
    /// adding a connection first if every one is saturated.
    ///
    /// The lease counts the stream against its connection until dropped, so
    /// whoever owns the stream should hold it for as long as the stream is in
    /// use.
    pub(crate) async fn open(&self) -> Result<OpenedStream> {
        let (link, first) = self.place().await?;
        let lease = StreamLease::new(link);
        let (send, recv, negotiated) = self
            .credentials
            .open(lease.connection(), first, self.max_frame_bytes)
            .await?;
        Ok(OpenedStream {
            send,
            recv,
            negotiated,
            lease,
        })
    }

    /// Pick a connection and open a raw stream on it.
    async fn place(&self) -> Result<(Arc<Link>, (SendStream, RecvStream))> {
        loop {
            let (candidates, can_grow, grown) = {
                let mut state = self.state.lock().expect("node state");
                state.prune();
                let mut candidates = state.links.clone();
                candidates.sort_by_key(|link| link.load());
                (candidates, state.links.len() < self.ceiling, state.grown)
            };
            for link in &candidates {
                if link.load() >= self.streams_per_conn {
                    break;
                }
                match try_open_bi(&link.connection) {
                    Probe::Opened(pair) => return Ok((Arc::clone(link), pair)),
                    // Out of the broker's stream credit, or already closed:
                    // either way this one has no room right now.
                    Probe::Saturated | Probe::Closed => continue,
                }
            }
            if can_grow {
                match self.grow(grown).await {
                    Ok(()) => continue,
                    // Somewhere to queue beats failing the stream outright.
                    Err(err) if !candidates.is_empty() => {
                        debug!(role = self.role, error = %err, "could not add a connection; queueing on an existing one");
                    }
                    Err(err) => return Err(err),
                }
            }
            // At the ceiling, and every connection is saturated: wait for
            // whichever gets credit back first, as QUIC itself would.
            match first_to_open(&candidates).await {
                Some((index, pair)) => return Ok((Arc::clone(&candidates[index]), pair)),
                // All of them closed while waiting; place again.
                None if !candidates.is_empty() => continue,
                None => anyhow::bail!("no connection to the broker"),
            }
        }
    }

    /// Add one connection, unless another caller added one since `seen`.
    async fn grow(&self, seen: u64) -> Result<()> {
        let _growing = self.growing.lock().await;
        if self.state.lock().expect("node state").grown != seen {
            return Ok(());
        }
        self.connect().await.map(|_| ())
    }

    /// Callers hold `growing`, which is what keeps the set under its ceiling.
    async fn connect(&self) -> Result<Arc<Link>> {
        let (slot, target) = {
            let mut state = self.state.lock().expect("node state");
            state.prune();
            anyhow::ensure!(
                state.links.len() < self.ceiling,
                "already at {} connections to the broker",
                self.ceiling
            );
            let slot = (0..)
                .find(|slot| state.links.iter().all(|link| link.slot != *slot))
                .expect("a free slot");
            let target = state.targets[slot % state.targets.len()];
            (slot, target)
        };
        let connection = self
            .endpoint
            .connect(target, &self.server_name)
            .await
            .with_context(|| format!("connect to {target}"))?;
        debug!(role = self.role, slot, %target, "client established connection");
        spawn_conn_stats_logger(&connection, self.role);
        let router = spawn_event_router_with_config(
            connection.clone(),
            self.event_router_max_pending,
            self.max_frame_bytes,
        );
        let link = Arc::new(Link {
            slot,
            connection,
            router,
            streams: AtomicUsize::new(0),
            publish_window: std::sync::OnceLock::new(),
        });
        let mut state = self.state.lock().expect("node state");
        if !state.listeners.contains(&target) {
            state.listeners.push(target);
        }
        state.links.push(Arc::clone(&link));
        state.grown += 1;
        Ok(link)
    }
}

/// How a [`NodeConnections`] grows.
#[derive(Clone, Copy)]
pub(crate) struct NodeLimits {
    /// Named in logs and connection stats.
    pub(crate) role: &'static str,
    /// Most connections the set will hold.
    pub(crate) ceiling: usize,
    /// Streams a connection carries before another is opened beside it.
    pub(crate) streams_per_conn: usize,
    pub(crate) max_frame_bytes: usize,
    pub(crate) event_router_max_pending: usize,
}

/// An authenticated stream, and the lease that counts it against its
/// connection.
pub(crate) struct OpenedStream {
    pub(crate) send: SendStream,
    pub(crate) recv: RecvStream,
    pub(crate) negotiated: Negotiated,
    pub(crate) lease: StreamLease,
}

/// One stream's claim on a connection. Dropping it frees the room.
pub(crate) struct StreamLease {
    link: Arc<Link>,
}

impl StreamLease {
    fn new(link: Arc<Link>) -> Self {
        link.streams.fetch_add(1, Ordering::Relaxed);
        Self { link }
    }

    pub(crate) fn connection(&self) -> &QuicConnection {
        &self.link.connection
    }

    /// The router that hands this connection's server-opened event streams
    /// to the subscriptions waiting for them.
    pub(crate) fn router(&self) -> &mpsc::Sender<EventRouterCommand> {
        &self.link.router
    }

    /// The connection's publish window, sized by the first stream to ask.
    /// Every stream on a connection negotiates with the same broker, so they
    /// all ask for the same size.
    pub(crate) fn publish_window(&self, size: u32) -> Arc<Semaphore> {
        Arc::clone(
            self.link
                .publish_window
                .get_or_init(|| Arc::new(Semaphore::new(size as usize))),
        )
    }

    /// The connection's position in the set, stable while it lives and below
    /// the ceiling. Used to label per-connection metrics.
    pub(crate) fn slot(&self) -> usize {
        self.link.slot
    }
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        self.link.streams.fetch_sub(1, Ordering::Relaxed);
    }
}

struct State {
    /// Where new connections go, spread across the broker's listeners.
    targets: Vec<SocketAddr>,
    listeners: Vec<SocketAddr>,
    links: Vec<Arc<Link>>,
    /// Connections added so far, so a grower can tell whether someone beat it.
    grown: u64,
}

impl State {
    fn prune(&mut self) {
        self.links
            .retain(|link| link.connection.close_reason().is_none());
    }
}

/// One connection in the set.
struct Link {
    slot: usize,
    connection: QuicConnection,
    router: mpsc::Sender<EventRouterCommand>,
    streams: AtomicUsize,
    /// One permit per acknowledged publish the connection has unanswered,
    /// shared by every publish stream on it, because the broker counts the
    /// window per connection.
    publish_window: std::sync::OnceLock<Arc<Semaphore>>,
}

impl Link {
    fn load(&self) -> usize {
        self.streams.load(Ordering::Relaxed)
    }
}

enum Probe {
    Opened((SendStream, RecvStream)),
    Saturated,
    Closed,
}

/// Open a stream only if the broker's credit allows it right now.
///
/// quinn's `open_bi` completes at once when a stream id is free and waits for
/// the peer's `MAX_STREAMS` otherwise, so one poll tells the two apart without
/// a timer. Dropping the pending future releases nothing, because nothing was
/// taken.
fn try_open_bi(connection: &QuicConnection) -> Probe {
    let mut open = pin!(connection.open_bi());
    match open
        .as_mut()
        .poll(&mut TaskContext::from_waker(Waker::noop()))
    {
        Poll::Ready(Ok(pair)) => Probe::Opened(pair),
        Poll::Ready(Err(_)) => Probe::Closed,
        Poll::Pending => Probe::Saturated,
    }
}

/// Wait for the first of `links` to open a stream, or `None` once every one
/// has closed. Only the winner's stream is taken: the others' opens are
/// dropped while still pending, which claims nothing.
async fn first_to_open(links: &[Arc<Link>]) -> Option<(usize, (SendStream, RecvStream))> {
    type Open<'a> = Pin<Box<dyn Future<Output = Result<(SendStream, RecvStream)>> + Send + 'a>>;
    let mut opens: Vec<Option<Open<'_>>> = links
        .iter()
        .map(|link| Some(Box::pin(link.connection.open_bi()) as Open<'_>))
        .collect();
    poll_fn(|cx| {
        let mut pending = false;
        for (index, slot) in opens.iter_mut().enumerate() {
            let Some(open) = slot else { continue };
            match open.as_mut().poll(cx) {
                Poll::Ready(Ok(pair)) => return Poll::Ready(Some((index, pair))),
                Poll::Ready(Err(_)) => *slot = None,
                Poll::Pending => pending = true,
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    })
    .await
}

#[cfg(test)]
mod tests;

//! The peer link: QUIC datagrams to one broker's internal listener.
//!
//! The broker advertises this proxy's address instead of its own, so every
//! peer dials through it. Each distinct source gets a session with its own
//! upstream socket, the way a NAT would, so replies find their way back to
//! whoever sent the request. Datagrams are dropped or held one at a time,
//! which is what a lossy or slow network does to QUIC; the transport's own
//! retransmission and idle timeout decide what a broker sees.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::net::UdpSocket;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::owners::Resolver;
use super::rules::{Rules, Verdict};
use crate::fault::Endpoint;

/// Large enough for any UDP datagram, so nothing is truncated on the way.
const MAX_DATAGRAM: usize = 65_536;

/// Each proxy socket's send and receive buffer.
///
/// Brokers treat loopback as a path with a guaranteed 16 KB MTU, so they never
/// fall back to smaller packets. macOS refuses a UDP send larger than the
/// socket's send buffer, 9216 bytes by default, so a proxy on default buffers
/// dropped every full-size datagram and a large replication batch never
/// arrived.
const SOCKET_BUFFER: usize = 4 * 1024 * 1024;

/// How often a source nobody could attribute is looked up again.
const RERESOLVE_AFTER: Duration = Duration::from_millis(500);

/// A datagram and when it may leave.
type Queued = (Instant, Bytes);

/// A UDP proxy in front of one endpoint.
pub(crate) struct UdpProxy {
    addr: SocketAddr,
    upstream: watch::Sender<SocketAddr>,
}

impl UdpProxy {
    /// Listen on a fresh loopback port and forward to `upstream`, which is
    /// `target`'s real address.
    pub(crate) fn start(
        handle: &Handle,
        target: Endpoint,
        upstream: SocketAddr,
        rules: Arc<Rules>,
        resolve: Resolver,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let socket = bind_loopback().context("bind UDP proxy listener")?;
        let addr = socket.local_addr().context("read UDP proxy address")?;
        let (upstream_tx, upstream_rx) = watch::channel(upstream);
        let context = Arc::new(Shared {
            target,
            upstream: upstream_rx,
            rules,
            resolve,
            shutdown,
        });
        handle.spawn(serve(socket, context));
        Ok(Self {
            addr,
            upstream: upstream_tx,
        })
    }

    /// Where peers should be told this endpoint is.
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Forward to a new address from now on: the broker behind this proxy
    /// was started again on fresh ports.
    pub(crate) fn set_upstream(&self, upstream: SocketAddr) {
        self.upstream.send_replace(upstream);
    }
}

/// What every session of one proxy shares.
struct Shared {
    target: Endpoint,
    upstream: watch::Receiver<SocketAddr>,
    rules: Arc<Rules>,
    resolve: Resolver,
    shutdown: CancellationToken,
}

/// One source's traffic through the proxy.
struct Session {
    sender: Arc<Sender>,
    to_upstream: mpsc::UnboundedSender<Queued>,
}

/// Who a session's source is, looked up again while nobody knows.
struct Sender {
    source: SocketAddr,
    known: Mutex<(Option<Endpoint>, std::time::Instant)>,
}

impl Sender {
    fn endpoint(&self, resolve: &Resolver) -> Option<Endpoint> {
        let mut known = self.known.lock().expect("sender lock");
        if known.0.is_none() && known.1.elapsed() >= RERESOLVE_AFTER {
            *known = (resolve(self.source), std::time::Instant::now());
        }
        known.0.clone()
    }
}

async fn serve(socket: std::net::UdpSocket, context: Arc<Shared>) {
    let Ok(listener) = UdpSocket::from_std(socket) else {
        return;
    };
    let listener = Arc::new(listener);
    let mut sessions: HashMap<SocketAddr, Session> = HashMap::new();
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let (len, source) = tokio::select! {
            _ = context.shutdown.cancelled() => return,
            received = listener.recv_from(&mut buf) => match received {
                Ok(received) => received,
                // A reset from an earlier send, on platforms that report one.
                Err(_) => continue,
            },
        };
        let session = match sessions.get(&source) {
            Some(session) => session,
            None => match open(source, &listener, &context).await {
                Ok(session) => sessions.entry(source).or_insert(session),
                Err(err) => {
                    tracing::warn!(%source, error = %err, "UDP proxy could not open a session");
                    continue;
                }
            },
        };
        let from = session.sender.endpoint(&context.resolve);
        if let Verdict::Deliver(delay) = context.rules.verdict(from.as_ref(), Some(&context.target))
        {
            let datagram = Bytes::copy_from_slice(&buf[..len]);
            let _ = session.to_upstream.send((Instant::now() + delay, datagram));
        }
    }
}

/// A non-blocking loopback UDP socket with [`SOCKET_BUFFER`] each way.
pub(super) fn bind_loopback() -> Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .context("create UDP socket")?;
    socket
        .set_send_buffer_size(SOCKET_BUFFER)
        .context("size UDP send buffer")?;
    socket
        .set_recv_buffer_size(SOCKET_BUFFER)
        .context("size UDP receive buffer")?;
    let loopback: SocketAddr = ([127, 0, 0, 1], 0).into();
    socket.bind(&loopback.into()).context("bind UDP socket")?;
    socket
        .set_nonblocking(true)
        .context("make UDP socket non-blocking")?;
    Ok(socket.into())
}

/// Start a session for `source`: its own upstream socket, a queue each way,
/// and a reader for replies.
async fn open(
    source: SocketAddr,
    listener: &Arc<UdpSocket>,
    context: &Arc<Shared>,
) -> Result<Session> {
    let upstream = Arc::new(
        UdpSocket::from_std(bind_loopback().context("bind UDP proxy upstream socket")?)
            .context("register UDP proxy upstream socket")?,
    );
    let sender = Arc::new(Sender {
        source,
        known: Mutex::new(((context.resolve)(source), std::time::Instant::now())),
    });

    let (to_upstream, queue) = mpsc::unbounded_channel();
    {
        let upstream = Arc::clone(&upstream);
        let address = context.upstream.clone();
        tokio::spawn(deliver(queue, context.shutdown.clone(), move |datagram| {
            let upstream = Arc::clone(&upstream);
            let to = *address.borrow();
            async move {
                if let Err(err) = upstream.send_to(&datagram, to).await {
                    tracing::warn!(%to, len = datagram.len(), error = %err, "UDP proxy could not forward a datagram");
                }
            }
        }));
    }

    let (to_source, queue) = mpsc::unbounded_channel();
    {
        let listener = Arc::clone(listener);
        tokio::spawn(deliver(queue, context.shutdown.clone(), move |datagram| {
            let listener = Arc::clone(&listener);
            async move {
                if let Err(err) = listener.send_to(&datagram, source).await {
                    tracing::warn!(%source, len = datagram.len(), error = %err, "UDP proxy could not return a datagram");
                }
            }
        }));
    }

    tokio::spawn(replies(
        upstream,
        Arc::clone(&sender),
        to_source,
        Arc::clone(context),
    ));

    Ok(Session {
        sender,
        to_upstream,
    })
}

/// Read what the target sends back to this session's source, and queue it
/// under the verdict for that direction.
async fn replies(
    upstream: Arc<UdpSocket>,
    sender: Arc<Sender>,
    to_source: mpsc::UnboundedSender<Queued>,
    context: Arc<Shared>,
) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let len = tokio::select! {
            _ = context.shutdown.cancelled() => return,
            received = upstream.recv_from(&mut buf) => match received {
                Ok((len, _)) => len,
                Err(_) => continue,
            },
        };
        let to = sender.endpoint(&context.resolve);
        if let Verdict::Deliver(delay) = context.rules.verdict(Some(&context.target), to.as_ref()) {
            let datagram = Bytes::copy_from_slice(&buf[..len]);
            if to_source.send((Instant::now() + delay, datagram)).is_err() {
                return;
            }
        }
    }
}

/// Send each queued datagram once its time comes, in order.
async fn deliver<F, Fut>(
    mut queue: mpsc::UnboundedReceiver<Queued>,
    shutdown: CancellationToken,
    mut send: F,
) where
    F: FnMut(Bytes) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        let (due, datagram) = tokio::select! {
            _ = shutdown.cancelled() => return,
            next = queue.recv() => match next {
                Some(next) => next,
                None => return,
            },
        };
        tokio::time::sleep_until(due).await;
        send(datagram).await;
    }
}

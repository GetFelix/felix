//! The control-plane link: HTTP over TCP from one broker.
//!
//! One proxy per broker, so the listening port says which broker a connection
//! belongs to and nothing has to be looked up. TCP delivers a stream, not
//! packets, so the faults are the stream-level equivalents: a dropped
//! direction is a black hole (bytes are accepted and never arrive, and the
//! sender learns nothing until its own timeout), and a delayed one holds
//! each chunk. A connection that lost bytes cannot carry a valid stream
//! again, so it is closed once the link heals; its client reconnects.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::rules::{Rules, Verdict};
use crate::fault::Endpoint;

/// A TCP proxy between one client endpoint and one server endpoint.
pub(crate) struct TcpProxy {
    addr: SocketAddr,
}

impl TcpProxy {
    /// Listen on a fresh loopback port and forward each connection to
    /// `upstream`, where `server` listens. Everything arriving here is from
    /// `client`.
    pub(crate) fn start(
        handle: &Handle,
        client: Endpoint,
        server: Endpoint,
        upstream: SocketAddr,
        rules: Arc<Rules>,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").context("bind TCP proxy listener")?;
        listener
            .set_nonblocking(true)
            .context("make TCP proxy non-blocking")?;
        let addr = listener.local_addr().context("read TCP proxy address")?;
        let link = Arc::new(Link {
            client,
            server,
            upstream,
            rules,
            shutdown,
        });
        handle.spawn(serve(listener, link));
        Ok(Self { addr })
    }

    /// Where the client should connect instead of the server.
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }
}

struct Link {
    client: Endpoint,
    server: Endpoint,
    upstream: SocketAddr,
    rules: Arc<Rules>,
    shutdown: CancellationToken,
}

async fn serve(listener: std::net::TcpListener, link: Arc<Link>) {
    let Ok(listener) = TcpListener::from_std(listener) else {
        return;
    };
    loop {
        let inbound = tokio::select! {
            _ = link.shutdown.cancelled() => return,
            accepted = listener.accept() => match accepted {
                Ok((inbound, _)) => inbound,
                Err(_) => continue,
            },
        };
        tokio::spawn(connection(inbound, Arc::clone(&link)));
    }
}

/// Carry one connection until either side closes it, the link heals after
/// losing bytes, or the proxy stops.
async fn connection(inbound: TcpStream, link: Arc<Link>) {
    // A server that is down is refused here, as the client would have been.
    let Ok(outbound) = TcpStream::connect(link.upstream).await else {
        return;
    };
    let _ = inbound.set_nodelay(true);
    let _ = outbound.set_nodelay(true);
    let closed = link.shutdown.child_token();
    let lost_bytes = AtomicBool::new(false);
    let (client_read, client_write) = inbound.into_split();
    let (server_read, server_write) = outbound.into_split();

    let up = pump(
        client_read,
        server_write,
        (&link.client, &link.server),
        &link.rules,
        &closed,
        &lost_bytes,
    );
    let down = pump(
        server_read,
        client_write,
        (&link.server, &link.client),
        &link.rules,
        &closed,
        &lost_bytes,
    );
    let healed = async {
        let mut changes = link.rules.subscribe();
        while changes.changed().await.is_ok() {
            if lost_bytes.load(Ordering::Acquire) && !link.rules.severed(&link.client, &link.server)
            {
                return;
            }
        }
        std::future::pending::<()>().await;
    };
    tokio::select! {
        _ = async { tokio::join!(up, down) } => {}
        _ = healed => {}
        _ = closed.cancelled() => {}
    }
    closed.cancel();
}

/// Copy one direction, under that direction's verdict for every chunk.
async fn pump(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    (from, to): (&Endpoint, &Endpoint),
    rules: &Rules,
    closed: &CancellationToken,
    lost_bytes: &AtomicBool,
) {
    let (queue, mut queued) = mpsc::unbounded_channel::<(Instant, Bytes)>();
    let read = async move {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let len = tokio::select! {
                _ = closed.cancelled() => return,
                read = reader.read(&mut buf) => match read {
                    Ok(0) => return,
                    Ok(len) => len,
                    Err(_) => {
                        closed.cancel();
                        return;
                    }
                },
            };
            match rules.verdict(Some(from), Some(to)) {
                Verdict::Drop => lost_bytes.store(true, Ordering::Release),
                Verdict::Deliver(delay) => {
                    let chunk = Bytes::copy_from_slice(&buf[..len]);
                    if queue.send((Instant::now() + delay, chunk)).is_err() {
                        return;
                    }
                }
            }
        }
    };
    let write = async move {
        while let Some((due, chunk)) = queued.recv().await {
            tokio::time::sleep_until(due).await;
            if writer.write_all(&chunk).await.is_err() {
                closed.cancel();
                return;
            }
        }
        // The reader saw end of stream: pass the half-close on.
        let _ = writer.shutdown().await;
    };
    tokio::join!(read, write);
}

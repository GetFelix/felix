//! An established connection: [`QuicConnection`] and the identity it is
//! logged under.

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use quinn::{Connection, RecvStream, SendStream};

/// Active QUIC connection wrapper with convenience helpers.
///
/// ```no_run
/// use felix_transport::QuicConnection;
///
/// async fn open_streams(connection: QuicConnection) -> anyhow::Result<()> {
///     let (_send, _recv) = connection.open_bi().await?;
///     let _send_only = connection.open_uni().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct QuicConnection {
    inner: Connection,
    // Stable id and peer metadata for tracing.
    info: ConnectionInfo,
    // The I/O runtime this connection's quinn drivers run on, if isolated.
    io_handle: Option<tokio::runtime::Handle>,
}

impl QuicConnection {
    pub(crate) fn new(connection: Connection, io_handle: Option<tokio::runtime::Handle>) -> Self {
        // Not quinn's `stable_id`: that is an address, reused once a
        // connection is freed, and a consumer group tells a restarted member's
        // connection from its predecessor's by this id.
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let info = ConnectionInfo {
            id: ConnectionId(NEXT_ID.fetch_add(1, Ordering::Relaxed)),
            peer_addr: connection.remote_address(),
        };
        Self {
            inner: connection,
            info,
            io_handle,
        }
    }

    /// The id and peer address this connection is logged under.
    pub fn info(&self) -> &ConnectionInfo {
        &self.info
    }

    /// Spawn a task colocated with this connection's quinn drivers.
    ///
    /// For pump tasks woken by the transport per datagram or per write (stream
    /// readers, connection writers): same-thread wakeups are task switches
    /// instead of cross-core kernel round trips, and that latency is the
    /// pipeline's clock under smooth arrival. Falls back to `tokio::spawn`
    /// when driver isolation is disabled.
    pub fn spawn_pump<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        static COLOCATE: OnceLock<bool> = OnceLock::new();
        let colocate = *COLOCATE.get_or_init(|| {
            std::env::var("FELIX_PUMP_COLOCATE")
                .map(|value| value != "0")
                .unwrap_or(true)
        });
        match (&self.io_handle, colocate) {
            (Some(handle), true) => handle.spawn(future),
            _ => tokio::spawn(future),
        }
    }

    /// quinn's counters for this connection.
    pub fn stats(&self) -> quinn::ConnectionStats {
        self.inner.stats()
    }

    /// The certificate chain the peer presented, leaf first, when the
    /// endpoint's TLS config asked for one.
    pub fn peer_certificates(&self) -> Option<Vec<rustls::pki_types::CertificateDer<'static>>> {
        self.inner
            .peer_identity()?
            .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
            .ok()
            .map(|certs| *certs)
    }

    /// The ALPN protocol the handshake settled on, if any.
    ///
    /// `None` means the peer offered no ALPN, which TLS treats as success. An
    /// endpoint that uses ALPN to separate roles must therefore check this
    /// rather than assume the handshake did it — see the broker's internal
    /// listener.
    pub fn negotiated_protocol(&self) -> Option<Vec<u8>> {
        self.inner
            .handshake_data()?
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .ok()?
            .protocol
    }

    /// Resolve when the connection closes, with the reason.
    ///
    /// Lets one task own detection of a lost connection, so every request
    /// waiting on it is failed at the moment it drops rather than at its own
    /// timeout.
    pub async fn closed(&self) -> quinn::ConnectionError {
        self.inner.closed().await
    }

    /// Why the connection closed, or `None` while it is still live. Lets a
    /// task holding a clone notice the close and exit instead of keeping the
    /// handle alive forever.
    pub fn close_reason(&self) -> Option<quinn::ConnectionError> {
        self.inner.close_reason()
    }

    /// Close the connection, telling the peer why.
    ///
    /// Distinct from dropping the connection: a close sends CONNECTION_CLOSE
    /// immediately, so the peer learns this was deliberate rather than waiting
    /// for an idle timeout to decide the server vanished. That distinction is
    /// what makes a graceful drain observable from the client side.
    ///
    /// ```no_run
    /// use felix_transport::QuicConnection;
    ///
    /// fn shutdown(connection: &QuicConnection) {
    ///     connection.close(0u32.into(), b"shutting down");
    /// }
    /// ```
    pub fn close(&self, code: quinn::VarInt, reason: &[u8]) {
        self.inner.close(code, reason);
    }

    /// Open a bidirectional stream to the peer.
    ///
    /// ```no_run
    /// use felix_transport::QuicConnection;
    ///
    /// async fn open(connection: QuicConnection) -> anyhow::Result<()> {
    ///     let (_send, _recv) = connection.open_bi().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        self.inner.open_bi().await.context("open bidi stream")
    }

    /// Open a unidirectional send stream to the peer.
    ///
    /// ```no_run
    /// use felix_transport::QuicConnection;
    ///
    /// async fn open(connection: QuicConnection) -> anyhow::Result<()> {
    ///     let _send = connection.open_uni().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn open_uni(&self) -> Result<SendStream> {
        self.inner.open_uni().await.context("open uni stream")
    }

    /// Accept the next bidirectional stream from the peer.
    ///
    /// ```no_run
    /// use felix_transport::QuicConnection;
    ///
    /// async fn accept(connection: QuicConnection) -> anyhow::Result<()> {
    ///     let (_send, _recv) = connection.accept_bi().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream)> {
        self.inner.accept_bi().await.context("accept bidi stream")
    }

    /// Accept the next unidirectional receive stream from the peer.
    ///
    /// ```no_run
    /// use felix_transport::QuicConnection;
    ///
    /// async fn accept(connection: QuicConnection) -> anyhow::Result<()> {
    ///     let _recv = connection.accept_uni().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn accept_uni(&self) -> Result<RecvStream> {
        self.inner.accept_uni().await.context("accept uni stream")
    }
}

/// Metadata about a live QUIC connection.
///
/// ```
/// use felix_transport::{ConnectionId, ConnectionInfo};
/// use std::net::SocketAddr;
///
/// let info = ConnectionInfo {
///     id: ConnectionId(42),
///     peer_addr: "127.0.0.1:4433".parse::<SocketAddr>().expect("addr"),
/// };
/// assert_eq!(info.id.0, 42);
/// ```
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub id: ConnectionId,
    pub peer_addr: SocketAddr,
}

/// Identifies a connection within this process. Assigned in the order
/// connections are made, starting at 1, and never reused.
///
/// ```
/// use felix_transport::ConnectionId;
///
/// let id = ConnectionId(7);
/// assert_eq!(id.0, 7);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

#[cfg(test)]
mod tests;

//! The accepting side: [`QuicServer`].

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use quinn::{Endpoint, ServerConfig};

use crate::config::TransportConfig;
use crate::connection::QuicConnection;
use crate::io_runtime::{EndpointRole, quinn_runtime};
use crate::socket::{effective_udp_buffer_bytes, warn_if_udp_buffers_were_clamped};

/// QUIC server endpoint wrapper.
///
/// ```no_run
/// use felix_transport::{QuicServer, TransportConfig};
/// use quinn::ServerConfig;
/// use std::net::SocketAddr;
///
/// fn server_config() -> ServerConfig {
///     // Provide a real TLS config when wiring this up in a service.
///     unimplemented!()
/// }
///
/// let bind: SocketAddr = "127.0.0.1:0".parse().expect("addr");
/// let transport = TransportConfig::default();
/// let _server = QuicServer::bind(bind, server_config(), transport).expect("bind");
/// ```
#[derive(Debug)]
pub struct QuicServer {
    endpoint: Endpoint,
    // Retain for debugging/metrics; Quinn owns the active config.
    _transport: TransportConfig,
    // Variant of the server config used for loopback peers; see
    // [`TransportConfig::loopback_initial_mtu`].
    loopback_config: Option<Arc<ServerConfig>>,
    // I/O runtime for this endpoint's quinn drivers (None when isolation is
    // disabled); handed to each connection for pump colocation.
    io_handle: Option<tokio::runtime::Handle>,
}

impl QuicServer {
    /// Bind a server endpoint to `addr`, with `transport`'s tuning applied to
    /// `server_config`.
    pub fn bind(
        addr: SocketAddr,
        mut server_config: ServerConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        // Apply transport defaults before binding the endpoint. The socket is
        // bound first: the loopback MTU guarantee depends on the buffer sizes
        // the OS actually granted it.
        let socket = transport.bind_udp_socket(addr)?;
        let quinn_transport = transport.quinn_transport_config();
        let loopback_config = transport
            .loopback_initial_mtu({
                warn_if_udp_buffers_were_clamped(
                    &socket,
                    transport
                        .udp_recv_buffer_bytes
                        .min(transport.udp_send_buffer_bytes),
                );
                effective_udp_buffer_bytes(&socket)
            })
            .map(|mtu| {
                let mut config = server_config.clone();
                config
                    .transport_config(Arc::new(transport.quinn_transport_config_for_loopback(mtu)));
                Arc::new(config)
            });
        server_config.transport_config(Arc::new(quinn_transport));
        let (runtime, io_handle) = quinn_runtime(EndpointRole::Server);
        let endpoint = Endpoint::new(
            transport.quinn_endpoint_config(),
            Some(server_config),
            socket,
            runtime,
        )
        .context("bind QUIC server")?;
        Ok(Self {
            endpoint,
            _transport: transport,
            loopback_config,
            io_handle,
        })
    }

    /// Wait for the next client to connect and finish its handshake.
    ///
    /// The handshake runs inline, so a peer that stalls it stalls this call.
    /// A server that must keep accepting while handshakes are in flight
    /// should use [`QuicServer::accept_incoming`] and finish each one in its
    /// own task.
    pub async fn accept(&self) -> Result<QuicConnection> {
        self.accept_incoming()
            .await
            .ok_or_else(|| anyhow!("no incoming QUIC connections"))?
            .accept()
            .await
    }

    /// Wait for the next connection attempt, before its handshake.
    ///
    /// `None` once the endpoint is closed. The caller decides whether to
    /// [`accept`](IncomingConnection::accept) or
    /// [`refuse`](IncomingConnection::refuse) it; refusing costs one packet
    /// and no connection state, which is what makes a connection cap cheap to
    /// enforce.
    pub async fn accept_incoming(&self) -> Option<IncomingConnection> {
        let incoming = self.endpoint.accept().await?;
        Some(IncomingConnection {
            incoming,
            loopback_config: self.loopback_config.clone(),
            io_handle: self.io_handle.clone(),
        })
    }

    /// The address the endpoint is bound to, including the port the OS chose
    /// for a bind to port 0.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.endpoint
            .local_addr()
            .context("read QUIC local address")
    }
}

/// A connection attempt that has not been handshaken yet.
#[derive(Debug)]
pub struct IncomingConnection {
    incoming: quinn::Incoming,
    loopback_config: Option<Arc<ServerConfig>>,
    io_handle: Option<tokio::runtime::Handle>,
}

impl IncomingConnection {
    /// The address the attempt came from. Not validated: before the
    /// handshake it may be spoofed.
    pub fn remote_address(&self) -> SocketAddr {
        self.incoming.remote_address()
    }

    /// Turn the attempt away with `CONNECTION_REFUSED`.
    pub fn refuse(self) {
        self.incoming.refuse();
    }

    /// Complete the handshake.
    pub async fn accept(self) -> Result<QuicConnection> {
        let connection = match &self.loopback_config {
            Some(config) if self.incoming.remote_address().ip().is_loopback() => self
                .incoming
                .accept_with(Arc::clone(config))
                .context("accept loopback QUIC connection")?
                .await
                .context("accept QUIC connection")?,
            _ => self.incoming.await.context("accept QUIC connection")?,
        };
        Ok(QuicConnection::new(connection, self.io_handle))
    }
}

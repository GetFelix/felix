//! The connecting side: [`QuicClient`].

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use quinn::{ClientConfig, Endpoint};

use crate::config::TransportConfig;
use crate::connection::PumpRuntimes;
use crate::connection::QuicConnection;
use crate::io_runtime::{EndpointRole, EndpointRuntime, quinn_runtime};
use crate::socket::{effective_udp_buffer_bytes, warn_if_udp_buffers_were_clamped};

/// QUIC client endpoint wrapper.
///
/// ```no_run
/// use felix_transport::{QuicClient, TransportConfig};
/// use quinn::ClientConfig;
/// use std::net::SocketAddr;
///
/// fn client_config() -> ClientConfig {
///     // Provide a real TLS config when wiring this up in a service.
///     unimplemented!()
/// }
///
/// let bind: SocketAddr = "0.0.0.0:0".parse().expect("addr");
/// let transport = TransportConfig::default();
/// let _client = QuicClient::bind(bind, client_config(), transport).expect("bind");
/// ```
#[derive(Debug)]
pub struct QuicClient {
    endpoint: Endpoint,
    // Retain for debugging/metrics; Quinn owns the active config.
    _transport: TransportConfig,
    // Variant of the client config used when connecting to a loopback peer;
    // see [`TransportConfig::loopback_initial_mtu`].
    loopback_config: Option<ClientConfig>,
    // See `QuicServer::pumps`.
    pumps: PumpRuntimes,
}

impl QuicClient {
    /// Bind a client endpoint to `addr`, with `transport`'s tuning applied to
    /// `client_config`.
    pub fn bind(
        addr: SocketAddr,
        mut client_config: ClientConfig,
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
                let mut config = client_config.clone();
                config
                    .transport_config(Arc::new(transport.quinn_transport_config_for_loopback(mtu)));
                config
            });
        client_config.transport_config(Arc::new(quinn_transport));
        let EndpointRuntime {
            quinn: runtime,
            io,
            app,
        } = quinn_runtime(EndpointRole::Client);
        let mut endpoint = Endpoint::new(transport.quinn_endpoint_config(), None, socket, runtime)
            .context("bind QUIC client")?;
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            endpoint,
            _transport: transport,
            loopback_config,
            pumps: PumpRuntimes { io, app },
        })
    }

    /// Connect to `addr` and complete the handshake, verifying the server as
    /// `server_name`.
    pub async fn connect(&self, addr: SocketAddr, server_name: &str) -> Result<QuicConnection> {
        let connecting = match &self.loopback_config {
            Some(config) if addr.ip().is_loopback() => self
                .endpoint
                .connect_with(config.clone(), addr, server_name)
                .context("initiate loopback QUIC connection")?,
            _ => self
                .endpoint
                .connect(addr, server_name)
                .context("initiate QUIC connection")?,
        };
        let connection = connecting.await.context("establish QUIC connection")?;
        Ok(QuicConnection::new(connection, self.pumps.clone()))
    }
}

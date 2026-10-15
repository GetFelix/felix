//! How a client learns that the server forgot its connection.
//!
//! quinn's idle timeout sends nothing. If only the client's datagrams are
//! lost, the server times out while its keep-alives still reach the client,
//! and the client's next packet draws a stateless reset (issue #725).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use quinn::ConnectionError;
use tokio::net::UdpSocket;
use tokio::time::timeout;

use super::{make_client_config, make_server_config};
use crate::{QuicClient, QuicConnection, QuicServer, TransportConfig};

const WAIT: Duration = Duration::from_secs(5);

/// Forwards one client's datagrams to `route` (or drops them while it is
/// `None`) and sends whatever comes back to the client.
struct Relay {
    addr: SocketAddr,
    route: Arc<Mutex<Option<SocketAddr>>>,
    replies: Arc<Mutex<HashMap<SocketAddr, usize>>>,
}

impl Relay {
    fn start(upstream: SocketAddr) -> Result<Self> {
        // Felix's own socket setup, so full-size loopback datagrams fit the
        // send buffer.
        let transport = TransportConfig::default();
        let front = UdpSocket::from_std(transport.bind_udp_socket("127.0.0.1:0".parse()?)?)?;
        let back = UdpSocket::from_std(transport.bind_udp_socket("127.0.0.1:0".parse()?)?)?;
        let relay = Self {
            addr: front.local_addr()?,
            route: Arc::new(Mutex::new(Some(upstream))),
            replies: Arc::default(),
        };
        let route = Arc::clone(&relay.route);
        let replies = Arc::clone(&relay.replies);
        tokio::spawn(async move {
            let mut client = None;
            let mut up = vec![0u8; 65_536];
            let mut down = vec![0u8; 65_536];
            loop {
                tokio::select! {
                    received = front.recv_from(&mut up) => {
                        let Ok((len, from)) = received else { continue };
                        client = Some(from);
                        let to = *route.lock().expect("route");
                        if let Some(to) = to {
                            let _ = back.send_to(&up[..len], to).await;
                        }
                    }
                    received = back.recv_from(&mut down) => {
                        let Ok((len, from)) = received else { continue };
                        *replies.lock().expect("replies").entry(from).or_default() += 1;
                        if let Some(client) = client {
                            let _ = front.send_to(&down[..len], client).await;
                        }
                    }
                }
            }
        });
        Ok(relay)
    }

    fn route(&self, to: Option<SocketAddr>) {
        *self.route.lock().expect("route") = to;
    }

    fn replies_from(&self, from: SocketAddr) -> usize {
        self.replies
            .lock()
            .expect("replies")
            .get(&from)
            .copied()
            .unwrap_or(0)
    }
}

struct Link {
    server: QuicServer,
    server_side: QuicConnection,
    _client: QuicClient,
    client_side: QuicConnection,
    relay: Relay,
}

async fn connect_through_relay() -> Result<Link> {
    let transport = TransportConfig {
        max_idle_timeout: Some(Duration::from_millis(300)),
        keep_alive_interval: Some(Duration::from_millis(75)),
        ..TransportConfig::default()
    };
    let (server_config, cert) = make_server_config()?;
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let relay = Relay::start(server.local_addr()?)?;
    let client = QuicClient::bind("127.0.0.1:0".parse()?, make_client_config(cert)?, transport)?;
    let (server_side, client_side) =
        tokio::try_join!(server.accept(), client.connect(relay.addr, "localhost"))?;
    Ok(Link {
        server,
        server_side,
        _client: client,
        client_side,
        relay,
    })
}

#[tokio::test]
async fn a_silent_server_idle_timeout_reaches_the_client_as_a_reset() -> Result<()> {
    let link = connect_through_relay().await?;

    link.relay.route(None);
    let server_reason = timeout(WAIT, link.server_side.closed()).await?;
    assert!(
        matches!(server_reason, ConnectionError::TimedOut),
        "{server_reason:?}"
    );
    assert!(
        link.client_side.close_reason().is_none(),
        "the client gave up before the server: {:?}",
        link.client_side.close_reason(),
    );

    link.relay.route(Some(link.server.local_addr()?));
    let mut send = link.client_side.open_uni().await?;
    // Buffered locally, so this can succeed; the reset arrives in reply.
    let _ = send.write_all(b"after the timeout").await;
    let client_reason = timeout(WAIT, link.client_side.closed()).await?;
    assert!(
        matches!(client_reason, ConnectionError::Reset),
        "{client_reason:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_reset_under_another_endpoints_key_is_ignored() -> Result<()> {
    let link = connect_through_relay().await?;
    // A Felix listener drops a packet whose CID it did not issue without
    // answering. A decoy that accepts any CID answers with a stateless reset
    // under its own key instead, which is the worse case.
    let mut config = quinn::EndpointConfig::default();
    config.cid_generator(|| Box::new(quinn_proto::RandomConnectionIdGenerator::new(8)));
    let socket = TransportConfig::default().bind_udp_socket("127.0.0.1:0".parse()?)?;
    let decoy = quinn::Endpoint::new(config, None, socket, Arc::new(quinn::TokioRuntime))?;
    let decoy_addr = decoy.local_addr()?;

    link.relay.route(Some(decoy_addr));
    let mut send = link.client_side.open_uni().await?;
    send.write_all(b"misrouted").await?;
    send.finish()?;
    timeout(WAIT, async {
        while link.relay.replies_from(decoy_addr) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    link.relay.route(Some(link.server.local_addr()?));
    let mut recv = timeout(WAIT, link.server_side.accept_uni()).await??;
    assert_eq!(recv.read_to_end(64).await?, b"misrouted");
    assert!(
        link.client_side.close_reason().is_none(),
        "{:?}",
        link.client_side.close_reason(),
    );
    Ok(())
}

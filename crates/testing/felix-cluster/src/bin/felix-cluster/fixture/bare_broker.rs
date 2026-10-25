//! A broker that answers `auth` and advertises no features at all.
//!
//! A real broker advertises `FEATURE_ATOMIC_COMMIT` whether or not its fleet
//! has finalized the feature, so no real broker lets a client prove it holds
//! back `commit` and `state_get` from a broker that did not advertise them.
//! This one does, and counts the commits and state reads it was sent so a
//! suite can check none arrived.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_wire::{Frame, FrameHeader, Message};
use quinn::{ReadExactError, RecvStream, SendStream};

/// Never read past this, so a confused peer cannot make the fixture allocate.
const MAX_FRAME: usize = 1 << 20;

pub(crate) struct BareBroker {
    pub(crate) addr: SocketAddr,
    /// Certificate to add to the trust bundle, PEM.
    pub(crate) cert_pem: String,
    /// Commits and state reads received.
    pub(crate) requests: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl BareBroker {
    pub(crate) fn start() -> Result<Self> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .context("certificate for the bare broker")?;
        let cert_pem = cert.cert.pem();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        // No ALPN, like the client-facing role of a real broker.
        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("bare broker protocol versions")?
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key.into())
        .context("bare broker certificate")?;
        let config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).context("bare broker crypto")?,
        ));
        // The transport a real broker listens with, so a client's handshake
        // tuning (initial MTU, stream limits) meets what it expects.
        let server = felix_transport::QuicServer::bind(
            "127.0.0.1:0".parse()?,
            config,
            felix_transport::TransportConfig::default(),
        )
        .context("bind the bare broker")?;
        let addr = server.local_addr()?;
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            while let Some(incoming) = server.accept_incoming().await {
                let counted = Arc::clone(&counted);
                tokio::spawn(async move {
                    let Ok(connection) = incoming.accept().await else {
                        return;
                    };
                    while let Ok((send, recv)) = connection.accept_bi().await {
                        tokio::spawn(serve_stream(send, recv, Arc::clone(&counted)));
                    }
                });
            }
        });
        Ok(Self {
            addr,
            cert_pem,
            requests,
            task,
        })
    }
}

impl Drop for BareBroker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_stream(
    mut send: SendStream,
    mut recv: RecvStream,
    requests: Arc<AtomicUsize>,
) -> Result<()> {
    while let Some(frame) = read_frame(&mut recv).await? {
        // A frame with flags this reads as a different layout is not a
        // message; the only requests of interest are plain JSON ones.
        let Ok(message) = Message::decode(frame) else {
            continue;
        };
        match message {
            Message::Auth { .. } => {
                let answer = Message::AuthOk {
                    server_flags: felix_wire::ORIGINAL_V1_FLAGS,
                    server_features: None,
                    server_features_hi: None,
                    listener_ports: None,
                    publish_window: None,
                };
                let frame = answer.encode().context("encode auth_ok")?;
                send.write_all(&frame.encode()).await?;
            }
            Message::Commit { .. } | Message::StateGet { .. } => {
                requests.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
    Ok(())
}

async fn read_frame(recv: &mut RecvStream) -> Result<Option<Frame>> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }
    let header = FrameHeader::decode_allowing_unknown_flags(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;
    anyhow::ensure!(length <= MAX_FRAME, "frame of {length} bytes");
    let mut payload = BytesMut::zeroed(length);
    recv.read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    Ok(Some(Frame {
        header,
        payload: payload.freeze(),
    }))
}

#[cfg(test)]
mod tests;

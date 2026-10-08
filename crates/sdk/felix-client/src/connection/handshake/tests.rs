//! A stream opened after the token expires. Nothing here is specific to
//! subscriptions; `subscribe` is just the call that opens a new stream.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::Message;
use rustls::pki_types::CertificateDer;
use tokio::time::{Duration, timeout};

use crate::frame_io::{read_message, write_message};
use crate::test_support::{build_client_config, build_server_config};
use crate::{Client, RefreshingToken};

/// Stub broker that accepts one token at a time. Switching the accepted token
/// simulates the old one expiring.
struct TokenCheckingServer {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    accepted: Arc<std::sync::Mutex<String>>,
    presented: Arc<std::sync::Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl TokenCheckingServer {
    fn start(accepted: &str) -> Result<Self> {
        let (server_config, cert) = build_server_config()?;
        let server = QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?;
        let addr = server.local_addr()?;
        let accepted = Arc::new(std::sync::Mutex::new(accepted.to_string()));
        let presented = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (server_accepted, server_presented) = (Arc::clone(&accepted), Arc::clone(&presented));
        let task = tokio::spawn(async move {
            while let Ok(connection) = server.accept().await {
                let (accepted, presented) =
                    (Arc::clone(&server_accepted), Arc::clone(&server_presented));
                tokio::spawn(async move {
                    while let Ok((send, recv)) = connection.accept_bi().await {
                        tokio::spawn(Self::serve_stream(
                            connection.clone(),
                            send,
                            recv,
                            Arc::clone(&accepted),
                            Arc::clone(&presented),
                        ));
                    }
                });
            }
        });
        Ok(Self {
            addr,
            cert,
            accepted,
            presented,
            task,
        })
    }

    async fn serve_stream(
        connection: felix_transport::QuicConnection,
        mut send: quinn::SendStream,
        mut recv: quinn::RecvStream,
        accepted: Arc<std::sync::Mutex<String>>,
        presented: Arc<std::sync::Mutex<Vec<String>>>,
    ) -> Result<()> {
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        let Some(Message::Auth { token, .. }) = read_message(&mut recv, &mut scratch).await? else {
            return Ok(());
        };
        presented.lock().unwrap().push(token.clone());
        if token != *accepted.lock().unwrap() {
            write_message(&mut send, Message::error("auth failed")).await?;
            let _ = send.finish();
            return Ok(());
        }
        write_message(&mut send, Message::Ok).await?;
        while let Some(message) = read_message(&mut recv, &mut scratch).await? {
            if let Message::Subscribe { .. } = message {
                write_message(
                    &mut send,
                    Message::Subscribed {
                        subscription_id: 7,
                        start_offset: None,
                        live_offset: None,
                        queue_capacity: None,
                    },
                )
                .await?;
                let mut uni = connection.open_uni().await?;
                write_message(&mut uni, Message::EventStreamHello { subscription_id: 7 }).await?;
            }
        }
        Ok(())
    }

    fn expire_current_token(&self, next: &str) {
        *self.accepted.lock().unwrap() = next.to_string();
    }

    fn presented(&self) -> Vec<String> {
        self.presented.lock().unwrap().clone()
    }
}

impl Drop for TokenCheckingServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// #629: after the token expires, a new subscription should retry with a fresh
/// token instead of failing.
#[tokio::test]
#[serial_test::serial]
async fn a_stream_opened_after_the_token_expires_uses_a_fresh_one() -> Result<()> {
    let server = TokenCheckingServer::start("token-1")?;
    let minted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&minted);
    let mut config = build_client_config(server.cert.clone())?;
    config.auth_token = None;
    config.token_provider = Some(Arc::new(RefreshingToken::new(move || {
        let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        async move { Ok(format!("token-{n}")) }
    })));

    let client = timeout(
        Duration::from_secs(5),
        Client::connect_with_transport(
            server.addr,
            "localhost",
            config,
            TransportConfig::default(),
        ),
    )
    .await??;
    server.expire_current_token("token-2");

    timeout(
        Duration::from_secs(5),
        client.subscribe("t1", "default", "updates"),
    )
    .await
    .context("subscribe timed out")?
    .context("subscribe after the token expired")?;

    let presented = server.presented();
    assert_eq!(
        presented.last().map(String::as_str),
        Some("token-2"),
        "the retried stream should present the fresh token: {presented:?}"
    );
    assert_eq!(
        minted.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "one token for connect, one after the refusal"
    );
    Ok(())
}

/// With a fixed token there's nothing new to try, so the refusal is returned.
#[tokio::test]
#[serial_test::serial]
async fn a_fixed_token_that_is_refused_is_not_retried() -> Result<()> {
    let server = TokenCheckingServer::start("test-token")?;
    let client = timeout(
        Duration::from_secs(5),
        Client::connect_with_transport(
            server.addr,
            "localhost",
            build_client_config(server.cert.clone())?,
            TransportConfig::default(),
        ),
    )
    .await??;
    server.expire_current_token("something-else");
    let before = server.presented().len();

    let err = match timeout(
        Duration::from_secs(5),
        client.subscribe("t1", "default", "updates"),
    )
    .await?
    {
        Ok(_) => anyhow::bail!("subscribe with a refused token succeeded"),
        Err(err) => err,
    };
    assert!(format!("{err:#}").contains("auth rejected"), "{err:#}");
    assert_eq!(server.presented().len(), before + 1);
    Ok(())
}

//! How a connect reports its refusals, and what its timeout covers.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::Message;
use rustls::pki_types::CertificateDer;
use tokio::time::{Duration, timeout};

use crate::frame_io::{read_message, write_message};
use crate::test_support::{build_client_config, build_server_config};
use crate::{Client, FETCH_TIMEOUT, RefreshingToken};

/// A broker that refuses every token, or admits every one.
struct Broker {
    addr: SocketAddr,
    cert: CertificateDer<'static>,
    _task: tokio::task::JoinHandle<()>,
}

impl Broker {
    fn start(admit: bool) -> Result<Self> {
        Self::start_with(admit, build_server_config()?)
    }

    /// Brokers started from the same pair present the same certificate.
    fn start_with(
        admit: bool,
        (server_config, cert): (quinn::ServerConfig, CertificateDer<'static>),
    ) -> Result<Self> {
        let server = QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?;
        let addr = server.local_addr()?;
        let task = tokio::spawn(async move {
            while let Ok(connection) = server.accept().await {
                tokio::spawn(async move {
                    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                        tokio::spawn(async move {
                            let mut scratch = BytesMut::with_capacity(64 * 1024);
                            let Ok(Some(Message::Auth { .. })) =
                                read_message(&mut recv, &mut scratch).await
                            else {
                                return;
                            };
                            let reply = if admit {
                                Message::Ok
                            } else {
                                Message::error("auth failed")
                            };
                            let _ = write_message(&mut send, reply).await;
                            let _ = send.finish();
                            // Hold the stream open so the client reads the reply.
                            let _ = recv.read_to_end(1).await;
                        });
                    }
                });
            }
        });
        Ok(Self {
            addr,
            cert,
            _task: task,
        })
    }
}

/// An address nothing listens on: the dial gets no answer at all.
fn silent_addr() -> Result<SocketAddr> {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
    Ok(socket.local_addr()?)
}

#[tokio::test]
async fn a_token_every_broker_refuses_is_an_auth_error() -> Result<()> {
    let identity = build_server_config()?;
    let a = Broker::start_with(false, identity.clone())?;
    let b = Broker::start_with(false, identity)?;
    let mut config = build_client_config(a.cert.clone())?;
    config.connect_timeout = Duration::from_secs(5);
    let err = Client::connect_any(&[a.addr, b.addr], "localhost", config)
        .await
        .err()
        .expect("every broker refused");
    assert!(
        err.downcast_ref::<crate::connection::AuthRejected>()
            .is_some()
    );
    let text = format!("{err:#}");
    assert!(text.contains("no broker accepted the token"), "{text}");
    assert!(text.contains(&a.addr.to_string()) && text.contains(&b.addr.to_string()));
    Ok(())
}

#[tokio::test]
async fn a_refusal_mixed_with_an_unreachable_broker_stays_a_connection_error() -> Result<()> {
    let refusing = Broker::start(false)?;
    let mut config = build_client_config(refusing.cert.clone())?;
    config.connect_timeout = Duration::from_millis(300);
    let err = Client::connect_any(&[refusing.addr, silent_addr()?], "localhost", config)
        .await
        .err()
        .expect("neither broker admitted the client");
    assert!(
        err.downcast_ref::<crate::connection::AuthRejected>()
            .is_none()
    );
    assert!(
        format!("{err:#}").starts_with("no broker answered"),
        "{err:#}"
    );
    Ok(())
}

/// The timeout covers the handshake, not the provider: a provider slower than
/// `connect_timeout` still connects.
#[tokio::test]
async fn a_slow_token_provider_is_not_cut_off_by_the_connect_timeout() -> Result<()> {
    let broker = Broker::start(true)?;
    let mut config = build_client_config(broker.cert.clone())?;
    // Wide margins either side: a loaded runner must not blur "slower than the
    // timeout" into "too slow to connect at all".
    config.connect_timeout = Duration::from_secs(3);
    config.token_provider = Some(Arc::new(RefreshingToken::new(|| async {
        tokio::time::sleep(Duration::from_secs(4)).await;
        Ok("slow-token".to_string())
    })));
    timeout(
        Duration::from_secs(30),
        Client::connect(broker.addr, "localhost", config),
    )
    .await??;
    Ok(())
}

/// A hung provider that holds a still-valid token connects with it: the
/// provider's own timeout, not a second one around it, decides that.
#[tokio::test(start_paused = true)]
async fn a_hung_refresh_connects_with_the_held_token() {
    use base64::Engine;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let held = format!(
        "{}.{}.sig",
        b64.encode("{}"),
        b64.encode(format!("{{\"exp\":{}}}", now + 4))
    );
    let tokens = RefreshingToken::with_initial(held.clone(), || async {
        std::future::pending::<()>().await;
        Ok(String::new())
    });
    // Refresh falls due at two thirds of the token's life, on the wall clock
    // that paused tokio time does not move.
    std::thread::sleep(Duration::from_millis(2200));
    // The inner timer starts a tick after the outer one would, as it does when
    // the provider does a little work first, so the outer would fire first.
    struct Late(RefreshingToken);
    impl crate::TokenProvider for Late {
        fn token(&self) -> crate::TokenFuture<'_> {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(1)).await;
                self.0.token().await
            })
        }
        fn bounds_its_own_fetch(&self) -> bool {
            self.0.bounds_its_own_fetch()
        }
    }
    let tokens = Late(tokens);
    super::fetch_from(&tokens)
        .await
        .expect("the held token is still valid, so the failed refresh is not an error");
    use crate::TokenProvider;
    assert_eq!(tokens.0.token().await.unwrap(), held);
}

/// A provider that never answers fails at the fetch timeout with its own
/// error, not the connect timeout's.
#[tokio::test(start_paused = true)]
async fn a_silent_token_provider_fails_at_the_fetch_timeout() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    drop(server_config);
    let mut config = build_client_config(cert)?;
    config.token_provider = Some(Arc::new(RefreshingToken::new(|| async {
        std::future::pending::<()>().await;
        Ok(String::new())
    })));
    let started = tokio::time::Instant::now();
    let err = Client::connect(silent_addr()?, "localhost", config)
        .await
        .err()
        .expect("the provider never answered");
    assert!(started.elapsed() >= FETCH_TIMEOUT);
    assert!(started.elapsed() < FETCH_TIMEOUT + Duration::from_secs(5));
    let text = format!("{err:#}");
    assert!(text.contains("the token fetch took longer"), "{text}");
    assert!(!text.contains("timed out"), "{text}");
    Ok(())
}

/// A provider that is not a `RefreshingToken` and never answers is still cut
/// off at the fetch timeout, with the same error.
#[tokio::test(start_paused = true)]
async fn a_custom_provider_that_never_answers_fails_at_the_fetch_timeout() -> Result<()> {
    struct Hung;
    impl crate::TokenProvider for Hung {
        fn token(&self) -> crate::TokenFuture<'_> {
            Box::pin(std::future::pending())
        }
    }
    let (server_config, cert) = build_server_config()?;
    drop(server_config);
    let mut config = build_client_config(cert)?;
    config.token_provider = Some(Arc::new(Hung));
    let started = tokio::time::Instant::now();
    let err = Client::connect(silent_addr()?, "localhost", config)
        .await
        .err()
        .expect("the provider never answered");
    assert_eq!(started.elapsed(), FETCH_TIMEOUT);
    let text = format!("{err:#}");
    assert!(text.contains("the token fetch took longer"), "{text}");
    Ok(())
}

fn refusal(code: felix_wire::ErrorCode) -> anyhow::Error {
    crate::error::refused("auth rejected", "no".to_string(), Some(code), None, None)
        .context(crate::connection::AuthRejected::for_test())
}

/// Only an unauthenticated or forbidden refusal is an auth failure; an
/// overloaded broker in the set keeps the connection-style error.
#[test]
fn only_auth_refusals_make_the_set_an_auth_error() {
    use felix_wire::ErrorCode;
    let a: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let b: SocketAddr = "127.0.0.1:2".parse().unwrap();
    let mixed = super::no_broker_answered(vec![
        (a, refusal(ErrorCode::Overloaded)),
        (b, refusal(ErrorCode::Unauthenticated)),
    ]);
    assert!(
        format!("{mixed:#}").starts_with("no broker answered"),
        "{mixed:#}"
    );
    let auth = super::no_broker_answered(vec![
        (a, refusal(ErrorCode::Forbidden)),
        (b, refusal(ErrorCode::Unauthenticated)),
    ]);
    assert!(
        format!("{auth:#}").contains("no broker accepted the token"),
        "{auth:#}"
    );
}

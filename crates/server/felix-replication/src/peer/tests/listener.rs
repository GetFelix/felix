//! What the internal listener refuses, and what it answers by default.

use super::*;

/// Connecting to a broker that is not the one the catalog named is a wrong
/// connection even if it would have answered.
#[tokio::test]
async fn a_peer_that_identifies_as_someone_else_is_refused() {
    let listener = Listener::start("broker-c", Arc::new(CountingHandler::default())).await;
    let pool = pool(config());

    let err = pool
        .request(PEER, listener.addr, forward())
        .await
        .expect_err("identity mismatch must be refused");
    assert!(matches!(err, PeerError::Handshake { .. }), "{err:?}");
    assert!(
        err.to_string().contains("broker-c"),
        "the error should name who actually answered: {err}",
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// A client-facing client must not be able to reach the internal listener.
///
/// The refusal happens in the TLS handshake: the internal listener requires
/// `felix-internal/1` and the client-facing role negotiates no ALPN at all, so
/// there is no protocol in common. That is what makes the two roles impossible
/// to confuse by pointing a client at the wrong port — it fails before a frame
/// is ever read, and before any broker state is touched.
#[tokio::test]
async fn the_internal_listener_refuses_a_client_facing_connection() {
    use felix_transport::{QuicClient, TransportConfig};

    let listener = Listener::start(PEER, Arc::new(CountingHandler::default())).await;

    // A client-shaped endpoint: no ALPN, exactly like the client-facing role.
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(rustls::ALL_VERSIONS)
    .expect("protocol versions")
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AcceptAnything))
    .with_no_client_auth();
    tls.alpn_protocols.clear();
    let quinn = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("crypto"),
    ));
    let client = QuicClient::bind(
        "127.0.0.1:0".parse().expect("addr"),
        quinn,
        TransportConfig::default(),
    )
    .expect("bind");

    let refused = client.connect(listener.addr, "felix-internal").await;
    assert!(
        refused.is_err(),
        "a client with no ALPN reached the internal listener",
    );

    // The listener is still serving: the refusal is per connection, not a
    // failure that takes this broker out of the cluster.
    let pool = pool(config());
    pool.request(PEER, listener.addr, forward())
        .await
        .expect("a real peer must still be served");

    pool.shutdown().await;
    listener.stop().await;
}

#[derive(Debug)]
struct AcceptAnything;

impl rustls::client::danger::ServerCertVerifier for AcceptAnything {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Until forwarding is wired (#106), the listener must say so rather than
/// leaving a peer to time out.
#[tokio::test]
async fn the_default_handler_answers_unavailable() {
    let listener = Listener::start(PEER, Arc::new(UnavailableHandler)).await;
    let pool = pool(config());

    let response = pool
        .request(PEER, listener.addr, forward())
        .await
        .expect("request");
    match response {
        InternalMessage::ForwardPublishError(err) => {
            assert_eq!(err.code, ErrorCode::Unavailable);
            assert!(err.code.is_retryable());
        }
        other => panic!("unexpected {other:?}"),
    }

    pool.shutdown().await;
    listener.stop().await;
}

/// **A kind this broker does not know is refused, and the stream lives.**
///
/// Adding a `Kind` is the protocol's sanctioned additive change, and it is only
/// additive if an older peer can refuse one frame instead of dropping the lane.
/// These streams are long-lived and multiplex every in-flight request, so
/// killing one on an unrecognised frame turns "the peer is newer than me" into
/// "every request in flight to that peer failed" — and made adding a kind a
/// cutover rather than an upgrade.
///
/// The frozen header is what makes stepping over it possible: it says how long
/// the body is, and every body begins with its correlation id, so the refusal
/// can name the request that caused it.
#[tokio::test]
async fn a_frame_kind_this_broker_does_not_know_is_refused_without_ending_the_stream() {
    use bytes::BufMut;
    use felix_transport::{QuicClient, TransportConfig};

    let listener = Listener::start(PEER, Arc::new(CountingHandler::default())).await;

    let client = QuicClient::bind(
        "127.0.0.1:0".parse().expect("addr"),
        crate::peer::tls::client_config(None).expect("client config"),
        TransportConfig::default(),
    )
    .expect("bind");
    let connection = client
        .connect(listener.addr, "felix-internal")
        .await
        .expect("connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("open");

    // A frame from some later build: our framing, our version, a kind that does
    // not exist yet, and a body whose first eight bytes are the correlation id
    // every body starts with.
    const FROM_THE_FUTURE: u16 = 60_000;
    const CORRELATION: u64 = 0x5eed_1234_5eed_1234;
    let mut frame = bytes::BytesMut::new();
    frame.put_u32(felix_wire::internal::INTERNAL_MAGIC);
    frame.put_u16(felix_wire::internal::INTERNAL_VERSION);
    frame.put_u16(FROM_THE_FUTURE);
    frame.put_u32(12);
    frame.put_u64(CORRELATION);
    frame.put_u32(0xabad_1dea);
    send.write_all(&frame).await.expect("write");

    let refusal = crate::peer::codec::read_frame(&mut recv)
        .await
        .expect("the stream must still be readable");
    let crate::peer::codec::Incoming::Message(InternalMessage::ForwardPublishError(err)) = refusal
    else {
        panic!("expected a typed refusal, got something else");
    };
    assert_eq!(err.code, ErrorCode::UnsupportedKind);
    assert_eq!(
        err.correlation_id, CORRELATION,
        "the refusal did not name the request, so a caller could not match it",
    );

    // The point of the whole thing: the same stream still serves.
    crate::peer::codec::write_frame(&mut send, &forward())
        .await
        .expect("write a request this broker does know");
    let answer = crate::peer::codec::read_frame(&mut recv)
        .await
        .expect("read");
    assert!(
        matches!(answer, crate::peer::codec::Incoming::Message(_)),
        "the stream was dropped after the unknown kind, so every request \
         sharing it would have failed",
    );

    connection.close(0u32.into(), b"done");
    listener.stop().await;
}

/// **The listener refuses past its inbound limit rather than accepting
/// everything.**
///
/// QUIC caps streams per connection and the decoder caps a frame, but nothing
/// capped *connections* — so one caller could make a broker hold 64 MiB × 1024
/// streams × however many it opened (#504). The cap survives peer
/// authentication too: an authenticated peer looping on a reconnect bug is
/// still unbounded, and is likelier than a hostile one.
#[tokio::test]
async fn the_listener_refuses_past_its_inbound_connection_limit() {
    use felix_transport::{QuicClient, TransportConfig};

    let mut config = config();
    config.max_inbound_connections = 2;
    config.max_inbound_per_source = 2;
    let listener = Listener::start_on(PEER, Arc::new(CountingHandler::default()), config).await;

    let client = QuicClient::bind(
        "127.0.0.1:0".parse().expect("addr"),
        crate::peer::tls::client_config(None).expect("client config"),
        TransportConfig::default(),
    )
    .expect("bind");

    // Two are served, and stay open.
    let mut held = Vec::new();
    for _ in 0..2 {
        let connection = client
            .connect(listener.addr, "felix-internal")
            .await
            .expect("within the limit");
        // Exercised, not merely established: a connection the listener has not
        // yet counted proves nothing about the limit.
        let (mut send, mut recv) = connection.open_bi().await.expect("open");
        crate::peer::codec::write_frame(&mut send, &forward())
            .await
            .expect("write");
        crate::peer::codec::read_frame(&mut recv)
            .await
            .expect("read");
        held.push(connection);
    }

    // The third is refused. The handshake succeeds — the listener closes it
    // after, which is what a QUIC peer at its limit can do — so the refusal
    // shows up on first use rather than on connect.
    let refused = client.connect(listener.addr, "felix-internal").await;
    let over_limit = match refused {
        Err(_) => true,
        Ok(connection) => {
            let opened = connection.open_bi().await;
            match opened {
                Err(_) => true,
                Ok((mut send, mut recv)) => {
                    let wrote = crate::peer::codec::write_frame(&mut send, &forward()).await;
                    wrote.is_err()
                        || !matches!(
                            crate::peer::codec::read_frame(&mut recv).await,
                            Ok(crate::peer::codec::Incoming::Message(_))
                        )
                }
            }
        }
    };
    assert!(
        over_limit,
        "a third connection was served against a limit of two, so the cap does \
         not bound anything",
    );

    // And dropping one gives its place back, or the broker stops accepting
    // peers after an uptime nobody can correlate with anything.
    held.pop();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after_release = client
        .connect(listener.addr, "felix-internal")
        .await
        .expect("connect after a release");
    let (mut send, mut recv) = after_release.open_bi().await.expect("open");
    crate::peer::codec::write_frame(&mut send, &forward())
        .await
        .expect("write");
    assert!(
        matches!(
            crate::peer::codec::read_frame(&mut recv).await,
            Ok(crate::peer::codec::Incoming::Message(_))
        ),
        "a released place was never given back",
    );

    listener.stop().await;
}

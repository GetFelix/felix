//! Record headers over QUIC: negotiated at auth, carried on publish and
//! event frames, stored with a durable record, and refused where they must be.

use bytes::{BufMut, Bytes, BytesMut};
use felix_storage::log::FsyncMode;
use felix_wire::{
    EXT_RECORD_HEADERS, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_BINARY_PUBLISH_ACKED, FLAG_EXTENDED,
    Frame, KNOWN_FLAGS, RecordHeaders,
};

use super::*;

const PERMS: &[&str] = &[
    "stream.publish:stream:t1/*/*",
    "stream.subscribe:stream:t1/*/*",
];

/// A running broker holding durable `orders` and `plain`.
struct Fixture {
    broker: Arc<Broker>,
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    server: tokio::task::JoinHandle<Result<()>>,
}

impl Fixture {
    /// `headers` turns the broker's record-header gates on, as a lone broker
    /// has them; `log_capacity` bounds the replay ring, so older records are
    /// read off disk.
    async fn start(dir: &std::path::Path, headers: bool, log_capacity: usize) -> Result<Self> {
        Self::start_with(dir, headers, headers, log_capacity).await
    }

    /// A lone broker whose `FELIX_RECORD_HEADERS` switch is off: it reads
    /// headers but refuses a publish that carries them.
    async fn start_switch_off(dir: &std::path::Path, log_capacity: usize) -> Result<Self> {
        Self::start_with(dir, false, true, log_capacity).await
    }

    async fn start_with(
        dir: &std::path::Path,
        accepts: bool,
        reads: bool,
        log_capacity: usize,
    ) -> Result<Self> {
        let storage = felix_broker::DurableStorage::open(
            dir,
            felix_storage::log::LogConfig {
                fsync_mode: FsyncMode::None,
                preallocate_segments: false,
                ..Default::default()
            },
        )?;
        let broker = Arc::new(
            Broker::new(EphemeralCache::new().into())
                .with_durable_storage(storage)
                .with_log_capacity(log_capacity)?,
        );
        if accepts {
            broker.record_headers_when(|| true);
        }
        if reads {
            broker.read_record_headers_when(|| true);
        }
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        for stream in ["orders", "plain"] {
            broker
                .register_stream(
                    "t1",
                    "default",
                    stream,
                    StreamMetadata {
                        durable: true,
                        shards: 1,
                        ..Default::default()
                    },
                )
                .await?;
        }
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;
        let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
        config.ack_on_commit = true;
        let auth = auth_fixture("t1", PERMS.iter().map(|perm| perm.to_string()).collect());
        let server = tokio::spawn(felix_broker_service::serving::quic::serve(
            server,
            Arc::clone(&broker),
            config,
            auth.auth,
        ));
        Ok(Self {
            broker,
            addr,
            cert,
            server,
        })
    }

    /// Stop serving and let go of the logs, as a restart does.
    async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
    }

    async fn session(&self, offer_headers: bool) -> Result<Session> {
        Session::open(self.addr, self.cert.clone(), offer_headers).await
    }
}

/// One authenticated control stream.
struct Session {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    scratch: felix_broker_service::serving::quic::FrameScratch,
    connection: felix_transport::QuicConnection,
    _client: QuicClient,
    /// The raw frame payload `Auth` was answered with.
    auth_answer: Bytes,
}

impl Session {
    async fn open(
        addr: std::net::SocketAddr,
        cert: CertificateDer<'static>,
        offer_headers: bool,
    ) -> Result<Self> {
        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            build_quinn_client_config(cert)?,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        let (client_flags, client_flags_ext) = if offer_headers {
            felix_wire::offer_flags(KNOWN_FLAGS, EXT_RECORD_HEADERS)
        } else {
            // No times, which differ between two publishes, so frames compare.
            (KNOWN_FLAGS & !felix_wire::FLAG_EVENT_BATCH_TIMESTAMPS, None)
        };
        let auth = auth_fixture("t1", PERMS.iter().map(|perm| perm.to_string()).collect());
        felix_broker_service::serving::quic::write_message(
            &mut send,
            Message::Auth {
                tenant_id: "t1".to_string(),
                token: auth.token,
                client_flags: Some(client_flags),
                client_features: None,
                client_features_hi: None,
                client_flags_ext,
            },
        )
        .await?;
        let mut scratch = felix_broker_service::serving::quic::FrameScratch::new();
        let answer = read_frame(&mut recv, &mut scratch).await?;
        Ok(Self {
            send,
            recv,
            scratch,
            connection,
            _client: client,
            auth_answer: answer.payload,
        })
    }

    /// What `AuthOk` offered back: the flags and the extension word.
    fn offered(&self) -> Result<(u16, Option<u32>)> {
        let frame = Frame {
            header: FrameHeader::new(0, self.auth_answer.len() as u32),
            payload: self.auth_answer.clone(),
        };
        match Message::decode(frame)? {
            Message::AuthOk {
                server_flags,
                server_flags_ext,
                ..
            } => Ok((server_flags, server_flags_ext)),
            other => anyhow::bail!("expected auth_ok, got {other:?}"),
        }
    }

    async fn publish(&mut self, bytes: &[u8]) -> Result<Frame> {
        self.send.write_all(bytes).await?;
        read_frame(&mut self.recv, &mut self.scratch).await
    }

    /// Subscribe to `stream` from `start` and return its event stream, past
    /// the hello.
    async fn subscribe(&mut self, stream: &str, start: Option<StartPosition>) -> Result<Events> {
        felix_broker_service::serving::quic::write_message(
            &mut self.send,
            Message::Subscribe {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: stream.to_string(),
                subscription_id: Some(7),
                start,
                shard: None,
                queue_capacity: None,
            },
        )
        .await?;
        let subscribed = read_frame(&mut self.recv, &mut self.scratch).await?;
        anyhow::ensure!(
            matches!(Message::decode(subscribed)?, Message::Subscribed { .. }),
            "not subscribed"
        );
        let mut recv = timeout(Duration::from_secs(5), self.connection.accept_uni())
            .await
            .context("event stream")??;
        let mut scratch = felix_broker_service::serving::quic::FrameScratch::new();
        let hello = read_frame(&mut recv, &mut scratch).await?;
        anyhow::ensure!(
            matches!(
                Message::decode(hello)?,
                Message::EventStreamHello { subscription_id: 7 }
            ),
            "no hello"
        );
        Ok(Events { recv, scratch })
    }
}

/// A subscription's event stream.
struct Events {
    recv: quinn::RecvStream,
    scratch: felix_broker_service::serving::quic::FrameScratch,
}

impl Events {
    async fn next(&mut self) -> Result<Frame> {
        read_frame(&mut self.recv, &mut self.scratch).await
    }

    /// The payloads and header blocks of the next `count` records, across
    /// however many frames they arrive in.
    async fn records(&mut self, count: usize) -> Result<Vec<(Bytes, Option<Bytes>)>> {
        let mut records = Vec::new();
        while records.len() < count {
            let frame = self.next().await?;
            let (payloads, headers) = decode_event(&frame)?;
            let headers = headers.unwrap_or_else(|| vec![None; payloads.len()]);
            records.extend(payloads.into_iter().zip(headers));
        }
        Ok(records)
    }
}

/// An event frame's payloads and, when it carries any, each record's block.
type DecodedEvent = (Vec<Bytes>, Option<Vec<Option<Bytes>>>);

fn decode_event(frame: &Frame) -> Result<DecodedEvent> {
    if frame.header.flags & FLAG_BINARY_EVENT_BATCH_SHARED != 0 {
        let batch = felix_wire::binary::decode_shared_event_batch(frame)?;
        Ok((batch.payloads, batch.record_headers))
    } else {
        let batch = felix_wire::binary::decode_event_batch(frame)?;
        Ok((batch.payloads, batch.record_headers))
    }
}

async fn read_frame(
    recv: &mut quinn::RecvStream,
    scratch: &mut felix_broker_service::serving::quic::FrameScratch,
) -> Result<Frame> {
    timeout(
        Duration::from_secs(10),
        felix_broker_service::serving::quic::read_frame_limited_into(recv, 1 << 20, scratch),
    )
    .await
    .context("timed out waiting for a frame")??
    .context("the broker closed the stream")
}

fn block(name: &str, key: &'static [u8]) -> Bytes {
    RecordHeaders {
        key: Some(Bytes::from_static(key)),
        headers: vec![(name.to_owned(), Bytes::from_static(b"v"))],
    }
    .encode()
    .expect("block")
}

/// An acked, per-batch publish to `t1/default/<stream>`. With `extended`,
/// every record is preceded by its block (or a zero length) behind an
/// extension word; without it the frame is the plain one, and `records`'
/// blocks are not written.
fn publish_frame(
    request_id: u64,
    stream: &str,
    records: &[(Option<Bytes>, &[u8])],
    extended: bool,
) -> Bytes {
    publish_frame_as(Some(request_id), stream, records, extended)
}

/// [`publish_frame`], fire-and-forget when `request_id` is `None`.
fn publish_frame_as(
    request_id: Option<u64>,
    stream: &str,
    records: &[(Option<Bytes>, &[u8])],
    extended: bool,
) -> Bytes {
    let mut body = BytesMut::new();
    if extended {
        body.put_u32(EXT_RECORD_HEADERS);
    }
    if let Some(request_id) = request_id {
        body.put_u64(request_id);
        body.put_u8(2);
    }
    for name in ["t1", "default", stream] {
        body.put_u16(name.len() as u16);
        body.extend_from_slice(name.as_bytes());
    }
    body.put_u32(records.len() as u32);
    for (block, payload) in records {
        if extended {
            match block {
                Some(block) => {
                    body.put_u32(block.len() as u32);
                    body.extend_from_slice(block);
                }
                None => body.put_u32(0),
            }
        }
        body.put_u32(payload.len() as u32);
        body.extend_from_slice(payload);
    }
    let mut flags = FLAG_BINARY_PUBLISH_BATCH;
    if request_id.is_some() {
        flags |= FLAG_BINARY_PUBLISH_ACKED;
    }
    if extended {
        flags |= FLAG_EXTENDED;
    }
    let mut out = BytesMut::new();
    FrameHeader::new(flags, body.len() as u32).encode(&mut out);
    out.extend_from_slice(&body);
    out.freeze()
}

fn ack(frame: &Frame) -> felix_wire::binary::PublishAck {
    felix_wire::binary::decode_publish_ack(frame).expect("publish ack")
}

async fn stored(broker: &Broker, stream: &str) -> Vec<(Bytes, Option<Bytes>)> {
    broker
        .read_durable("t1", "default", stream, 0, 0, 1 << 20)
        .await
        .expect("read")
        .into_iter()
        .map(|record| (record.payload, record.headers.map(|block| *block)))
        .collect()
}

/// **A client that offered record headers publishes them and a subscriber that
/// offered them reads them back, aligned; one that did not gets the frame a
/// publish without headers makes, byte for byte.**
#[tokio::test]
#[serial]
async fn headers_travel_from_publisher_to_subscribers_that_negotiated_them() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = Fixture::start(dir.path(), true, 1024).await?;

    let mut publisher = fixture.session(true).await?;
    assert_eq!(
        publisher.offered()?,
        (KNOWN_FLAGS | FLAG_EXTENDED, Some(EXT_RECORD_HEADERS))
    );
    let mut with_headers = fixture.session(true).await?;
    let mut without_headers = fixture.session(false).await?;
    let mut control = fixture.session(false).await?;
    let mut sees = with_headers.subscribe("orders", None).await?;
    let mut ignores = without_headers.subscribe("orders", None).await?;
    let mut baseline = control.subscribe("plain", None).await?;

    let first = block("trace", b"k1");
    let reply = publisher
        .publish(&publish_frame(
            1,
            "orders",
            &[(Some(first.clone()), b"alpha"), (None, b"beta")],
            true,
        ))
        .await?;
    assert_eq!(ack(&reply).error, None);
    // The same records to a stream whose publish carries no headers at all.
    let reply = publisher
        .publish(&publish_frame(
            2,
            "plain",
            &[(None, b"alpha"), (None, b"beta")],
            false,
        ))
        .await?;
    assert_eq!(ack(&reply).error, None);

    let got = sees.records(2).await?;
    assert_eq!(
        got,
        vec![
            (Bytes::from_static(b"alpha"), Some(first.clone())),
            (Bytes::from_static(b"beta"), None)
        ]
    );

    let plain = ignores.next().await?;
    let expected = baseline.next().await?;
    assert_eq!(plain.header.flags, expected.header.flags);
    assert_eq!(plain.payload, expected.payload);
    assert_eq!(decode_event(&plain)?.1, None);

    assert_eq!(
        stored(&fixture.broker, "orders").await,
        vec![
            (Bytes::from_static(b"alpha"), Some(first)),
            (Bytes::from_static(b"beta"), None)
        ]
    );
    assert_eq!(
        stored(&fixture.broker, "plain").await,
        vec![
            (Bytes::from_static(b"alpha"), None),
            (Bytes::from_static(b"beta"), None)
        ]
    );
    fixture.stop().await;
    Ok(())
}

/// **A frame with `FLAG_EXTENDED` from a session that did not negotiate it is
/// refused as before**, and nothing is published. Either the client did not
/// offer it, or the broker does not accept headers.
#[tokio::test]
#[serial]
async fn an_extended_frame_is_refused_without_negotiation() -> Result<()> {
    for (broker_accepts, client_offers) in [(true, false), (false, true), (false, false)] {
        let dir = tempfile::tempdir()?;
        let fixture = Fixture::start(dir.path(), broker_accepts, 1024).await?;
        let mut session = fixture.session(client_offers).await?;
        // Whatever it was offered, the answer carries no extension.
        let (flags, ext) = session.offered()?;
        assert_eq!(
            (flags & FLAG_EXTENDED, ext),
            (0, None),
            "broker {broker_accepts}, client {client_offers}"
        );

        let reply = session
            .publish(&publish_frame(
                1,
                "orders",
                &[(Some(block("trace", b"k")), b"alpha")],
                true,
            ))
            .await?;
        match Message::decode(reply)? {
            Message::Error { message, .. } => {
                assert!(message.contains("unsupported frame flags"), "{message}");
            }
            other => anyhow::bail!("expected an error, got {other:?}"),
        }
        assert!(stored(&fixture.broker, "orders").await.is_empty());
        fixture.stop().await;
    }
    Ok(())
}

/// **Names under `felix.` are reserved**: a publish carrying one is refused
/// with an invalid-request answer and nothing is written.
#[tokio::test]
#[serial]
async fn a_reserved_header_name_is_refused() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = Fixture::start(dir.path(), true, 1024).await?;
    let mut publisher = fixture.session(true).await?;

    let reply = publisher
        .publish(&publish_frame(
            1,
            "orders",
            &[
                (Some(block("trace", b"k")), b"alpha"),
                (Some(block("felix.mqtt.topic", b"k")), b"beta"),
            ],
            true,
        ))
        .await?;
    let refused = ack(&reply);
    assert!(
        refused
            .error
            .as_deref()
            .is_some_and(|m| m.contains("reserved")),
        "{refused:?}"
    );
    assert_eq!(
        refused.code.map(|(code, _)| code),
        Some(felix_wire::ErrorCode::InvalidRequest)
    );
    assert!(stored(&fixture.broker, "orders").await.is_empty());

    // The stream is still usable, and a look-alike name is not reserved.
    let reply = publisher
        .publish(&publish_frame(
            2,
            "orders",
            &[(Some(block("felixa", b"k")), b"gamma")],
            true,
        ))
        .await?;
    assert_eq!(ack(&reply).error, None);
    assert_eq!(stored(&fixture.broker, "orders").await.len(), 1);
    fixture.stop().await;
    Ok(())
}

/// **A client that offered nothing gets the `AuthOk` it always got**, whether
/// or not the broker accepts headers.
#[tokio::test]
#[serial]
async fn auth_ok_for_a_client_that_offered_nothing_is_unchanged() -> Result<()> {
    let mut answers = Vec::new();
    for accepts in [true, false] {
        let dir = tempfile::tempdir()?;
        let fixture = Fixture::start(dir.path(), accepts, 1024).await?;
        let session = fixture.session(false).await?;
        answers.push(session.auth_answer.clone());
        fixture.stop().await;
    }
    assert_eq!(answers[0], answers[1]);
    let text = std::str::from_utf8(&answers[0])?;
    assert!(!text.contains("server_flags_ext"), "{text}");
    assert!(
        text.contains(&format!("\"server_flags\":{KNOWN_FLAGS}")),
        "{text}"
    );
    Ok(())
}

/// **A durable stream replays the headers it stored**, from the ring and from
/// disk, after the broker was restarted.
#[tokio::test]
#[serial]
async fn replay_after_a_restart_returns_the_headers() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = Fixture::start(dir.path(), true, 1024).await?;
    let mut publisher = fixture.session(true).await?;
    let first = block("trace", b"k1");
    let second = block("type", b"k2");
    for (request_id, record) in [
        (1, (Some(first.clone()), &b"alpha"[..])),
        (2, (None, &b"beta"[..])),
        (3, (Some(second.clone()), &b"gamma"[..])),
    ] {
        let reply = publisher
            .publish(&publish_frame(request_id, "orders", &[record], true))
            .await?;
        assert_eq!(ack(&reply).error, None);
    }
    fixture.stop().await;
    drop(publisher);

    // A ring of one, so everything but the newest record is read off disk.
    let fixture = Fixture::start(dir.path(), true, 1).await?;
    let mut subscriber = fixture.session(true).await?;
    let mut events = subscriber
        .subscribe("orders", Some(StartPosition::Offset(0)))
        .await?;
    assert_eq!(
        events.records(3).await?,
        vec![
            (Bytes::from_static(b"alpha"), Some(first)),
            (Bytes::from_static(b"beta"), None),
            (Bytes::from_static(b"gamma"), Some(second)),
        ]
    );
    fixture.stop().await;
    Ok(())
}

/// **Turning `FELIX_RECORD_HEADERS` off does not hide what a log holds.** A
/// subscriber still negotiates headers and reads the stored blocks; a publish that carries headers is refused with
/// `feature_not_enabled` / `record_headers` and nothing is stored, and the
/// same publish sent fire-and-forget is dropped.
#[tokio::test]
#[serial]
async fn switch_off_still_delivers_stored_headers_and_refuses_new_ones() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = Fixture::start(dir.path(), true, 1024).await?;
    let mut publisher = fixture.session(true).await?;
    let first = block("trace", b"k1");
    let reply = publisher
        .publish(&publish_frame(
            1,
            "orders",
            &[(Some(first.clone()), b"alpha")],
            true,
        ))
        .await?;
    assert_eq!(ack(&reply).error, None);
    fixture.stop().await;
    drop(publisher);

    let fixture = Fixture::start_switch_off(dir.path(), 1024).await?;
    let mut session = fixture.session(true).await?;
    assert_eq!(
        session.offered()?,
        (KNOWN_FLAGS | FLAG_EXTENDED, Some(EXT_RECORD_HEADERS))
    );
    let mut events = session
        .subscribe("orders", Some(StartPosition::Offset(0)))
        .await?;
    assert_eq!(
        events.records(1).await?,
        vec![(Bytes::from_static(b"alpha"), Some(first.clone()))]
    );

    let mut publisher = fixture.session(true).await?;
    let refused = ack(&publisher
        .publish(&publish_frame(
            2,
            "orders",
            &[(Some(block("trace", b"k2")), b"beta")],
            true,
        ))
        .await?);
    assert_eq!(
        refused.code.clone().map(|(code, _)| code),
        Some(felix_wire::ErrorCode::FeatureNotEnabled),
        "{refused:?}"
    );
    assert_eq!(
        refused
            .detail
            .clone()
            .and_then(|detail| detail.reason)
            .as_deref(),
        Some(felix_wire::feature_reason::RECORD_HEADERS)
    );

    // Fire-and-forget has no reply, so the proof is that it is not stored
    // while a publish without headers behind it is.
    publisher
        .send
        .write_all(&publish_frame_as(
            None,
            "orders",
            &[(Some(block("trace", b"k3")), b"gamma")],
            true,
        ))
        .await?;
    let reply = publisher
        .publish(&publish_frame(3, "orders", &[(None, b"delta")], true))
        .await?;
    assert_eq!(ack(&reply).error, None);
    assert_eq!(
        stored(&fixture.broker, "orders").await,
        vec![
            (Bytes::from_static(b"alpha"), Some(first)),
            (Bytes::from_static(b"delta"), None)
        ]
    );
    fixture.stop().await;
    Ok(())
}

//! Pipelined publishes: who is granted a window, the order answers come back
//! in, and an idempotent producer keeping a window of batches in flight.

use super::*;
use felix_wire::Frame;

/// A broker holding `t1/default/orders` durably, with `configure` applied to
/// its config before it starts serving.
async fn serve_orders(
    dir: &std::path::Path,
    fsync_mode: felix_storage::log::FsyncMode,
    configure: impl FnOnce(&mut felix_broker_service::config::BrokerConfig),
) -> Result<(
    Arc<Broker>,
    std::net::SocketAddr,
    CertificateDer<'static>,
    AuthFixture,
    felix_broker_service::config::BrokerConfig,
)> {
    let storage = felix_broker::DurableStorage::open(
        dir,
        felix_storage::log::LogConfig {
            fsync_mode,
            preallocate_segments: false,
            ..Default::default()
        },
    )?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
    configure(&mut config);
    let auth = auth_fixture("t1", vec!["stream.publish:stream:t1/*/*".to_string()]);
    tokio::spawn(felix_broker_service::serving::quic::serve(
        server,
        Arc::clone(&broker),
        config.clone(),
        Arc::clone(&auth.auth),
    ));
    Ok((broker, addr, cert, auth, config))
}

/// One raw stream to the broker, and the frames it answers `auth` with.
async fn raw_stream(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
) -> Result<(
    felix_transport::QuicConnection,
    quinn::SendStream,
    quinn::RecvStream,
)> {
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, recv) = connection.open_bi().await?;
    Ok((connection, send, recv))
}

/// Authenticate one stream, returning the answer's frame payload as sent.
async fn auth_answer(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
    client_flags: Option<u16>,
    client_features: Option<u32>,
) -> Result<bytes::Bytes> {
    let (_connection, mut send, mut recv) = raw_stream(addr, cert).await?;
    felix_broker_service::serving::quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            client_flags,
            client_features,
        },
    )
    .await?;
    let mut scratch = bytes::BytesMut::new();
    let frame = felix_broker_service::serving::quic::read_frame_limited_into(
        &mut recv,
        1 << 20,
        &mut scratch,
    )
    .await?
    .context("no answer to auth")?;
    Ok(frame.payload)
}

/// **A window goes only to a client that asks.** One that offers the bit gets
/// the bit and the window; one that offers features without it, and one that
/// predates negotiation, get exactly the frame they always got; a broker with
/// pipelining off grants nothing to anyone.
#[tokio::test]
#[serial]
async fn pipelining_is_granted_only_to_a_client_that_asks() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (_broker, addr, cert, auth, config) =
        serve_orders(dir.path(), felix_storage::log::FsyncMode::None, |_| {}).await?;

    let asked = auth_answer(
        addr,
        cert.clone(),
        &auth,
        Some(felix_wire::KNOWN_FLAGS),
        Some(felix_wire::FEATURE_PUBLISH_PIPELINE),
    )
    .await?;
    match Message::decode(Frame::new(0, asked)?)? {
        Message::AuthOk {
            server_features,
            publish_window,
            ..
        } => {
            assert_eq!(publish_window, Some(config.publish_window));
            assert!(felix_wire::supports_feature(
                server_features.unwrap_or(0),
                felix_wire::FEATURE_PUBLISH_PIPELINE
            ));
        }
        other => anyhow::bail!("expected AuthOk, got {other:?}"),
    }

    let did_not_ask = auth_answer(
        addr,
        cert.clone(),
        &auth,
        Some(felix_wire::KNOWN_FLAGS),
        Some(felix_wire::FEATURE_ERROR_CODES),
    )
    .await?;
    let text = std::str::from_utf8(&did_not_ask)?;
    assert!(text.contains("auth_ok"), "{text}");
    assert!(!text.contains("publish_window"), "{text}");

    let legacy = auth_answer(addr, cert.clone(), &auth, None, None).await?;
    assert_eq!(Message::decode(Frame::new(0, legacy)?)?, Message::Ok);

    let off_dir = tempfile::tempdir()?;
    let (_off, off_addr, off_cert, off_auth, _) = serve_orders(
        off_dir.path(),
        felix_storage::log::FsyncMode::None,
        |config| config.publish_window = 0,
    )
    .await?;
    let refused = auth_answer(
        off_addr,
        off_cert,
        &off_auth,
        Some(felix_wire::KNOWN_FLAGS),
        Some(felix_wire::FEATURE_PUBLISH_PIPELINE),
    )
    .await?;
    match Message::decode(Frame::new(0, refused)?)? {
        Message::AuthOk {
            server_features,
            publish_window,
            ..
        } => {
            assert_eq!(publish_window, None);
            assert!(!felix_wire::supports_feature(
                server_features.unwrap_or(0),
                felix_wire::FEATURE_PUBLISH_PIPELINE
            ));
        }
        other => anyhow::bail!("expected AuthOk, got {other:?}"),
    }
    Ok(())
}

/// **Answers come back in the order the stream carried the publishes.** Each
/// round sends a publish that waits for an fsync'd commit, then one to a
/// stream that does not exist, which the broker can refuse at once. The
/// refusal finishes first, and is still answered second.
#[tokio::test]
#[serial]
async fn a_pipelining_stream_is_answered_in_request_order() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (_broker, addr, cert, auth, config) = serve_orders(
        dir.path(),
        felix_storage::log::FsyncMode::OnCommit,
        |config| config.ack_on_commit = true,
    )
    .await?;
    let (_connection, mut send, mut recv) = raw_stream(addr, cert).await?;
    let mut scratch = bytes::BytesMut::new();
    felix_broker_service::serving::quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            client_flags: Some(felix_wire::KNOWN_FLAGS),
            client_features: Some(felix_wire::FEATURE_PUBLISH_PIPELINE),
        },
    )
    .await?;
    let answer = felix_broker_service::serving::quic::read_message_limited(
        &mut recv,
        config.max_frame_bytes,
        &mut scratch,
    )
    .await?;
    anyhow::ensure!(
        matches!(
            answer,
            Some(Message::AuthOk {
                publish_window: Some(_),
                ..
            })
        ),
        "not granted a window: {answer:?}"
    );
    let publish = |stream: &str, request_id: u64| Message::PublishBatch {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: stream.to_string(),
        payloads: vec![format!("record-{request_id}").into_bytes()],
        key: None,
        request_id: Some(request_id),
        ack: Some(AckMode::PerBatch),
    };
    let mut answered = Vec::new();
    for round in 0..5u64 {
        let (slow, fast) = (round * 2 + 1, round * 2 + 2);
        felix_broker_service::serving::quic::write_message(&mut send, publish("orders", slow))
            .await?;
        felix_broker_service::serving::quic::write_message(&mut send, publish("missing", fast))
            .await?;
        for _ in 0..2 {
            let answer = timeout(
                Duration::from_secs(10),
                felix_broker_service::serving::quic::read_message_limited(
                    &mut recv,
                    config.max_frame_bytes,
                    &mut scratch,
                ),
            )
            .await
            .context("no answer")??;
            match answer {
                Some(Message::PublishOk { request_id }) => answered.push((request_id, true)),
                Some(Message::PublishError { request_id, .. }) => {
                    answered.push((request_id, false))
                }
                other => anyhow::bail!("expected a publish answer, got {other:?}"),
            }
        }
    }
    let expected: Vec<(u64, bool)> = (1..=10).map(|id| (id, id % 2 == 1)).collect();
    assert_eq!(answered, expected, "answered out of request order");
    Ok(())
}

/// Publish `count` one-record batches with `publish_batches` and read back
/// what the stream holds.
async fn publish_batches_and_read(
    broker: &Broker,
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
    count: usize,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>)> {
    let client = Client::connect(addr, "localhost", build_client_config(cert, auth)?).await?;
    let producer = client.idempotent_producer().await?;
    let records: Vec<Vec<u8>> = (0..count)
        .map(|i| format!("record-{i}").into_bytes())
        .collect();
    producer
        .publish_batches(
            "t1",
            "default",
            "orders",
            records.iter().map(|record| vec![record.clone()]).collect(),
        )
        .await?;
    // A second call carries on from the sequence the first left off at.
    producer
        .publish_batch("t1", "default", "orders", vec![b"last".to_vec()])
        .await?;
    let mut expected = records;
    expected.push(b"last".to_vec());
    Ok((stored_orders(broker).await?, expected))
}

/// **An idempotent producer pipelines against a broker that grants a
/// window**, and every batch lands once, in order.
#[tokio::test]
#[serial]
async fn an_idempotent_producer_pipelines_its_batches() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (broker, addr, cert, auth, _) =
        serve_orders(dir.path(), felix_storage::log::FsyncMode::None, |_| {}).await?;
    let (stored, expected) = publish_batches_and_read(&broker, addr, cert, &auth, 300).await?;
    assert_eq!(stored, expected);
    Ok(())
}

/// **Against a broker that does not pipeline, the same call goes one batch at
/// a time** and lands the same records.
#[tokio::test]
#[serial]
async fn an_idempotent_producer_falls_back_to_one_at_a_time() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (broker, addr, cert, auth, _) =
        serve_orders(dir.path(), felix_storage::log::FsyncMode::None, |config| {
            config.publish_window = 0
        })
        .await?;
    let (stored, expected) = publish_batches_and_read(&broker, addr, cert, &auth, 50).await?;
    assert_eq!(stored, expected);
    Ok(())
}

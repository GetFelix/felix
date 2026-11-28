//! Pipelined publishes: who is granted a window, the order answers come back
//! in, and an idempotent producer keeping a window of batches in flight.

use super::*;
use felix_wire::Frame;

/// A broker holding `t1/default/orders` durably, with `configure` applied to
/// its config before it starts serving.
pub(super) async fn serve_orders(
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
pub(super) async fn raw_stream(
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
) -> Result<bytes::Bytes> {
    let (_connection, mut send, mut recv) = raw_stream(addr, cert).await?;
    felix_broker_service::serving::quic::write_message(
        &mut send,
        felix_wire::baseline_auth(auth.tenant_id.clone(), auth.token.clone()),
    )
    .await?;
    let mut scratch = felix_broker_service::serving::quic::FrameScratch::new();
    let frame = felix_broker_service::serving::quic::read_frame_limited_into(
        &mut recv,
        1 << 20,
        &mut scratch,
    )
    .await?
    .context("no answer to auth")?;
    Ok(frame.payload)
}

/// **A baseline client is granted the window**, and a broker with
/// pipelining off grants nothing to anyone.
#[tokio::test]
#[serial]
async fn pipelining_is_granted_with_the_window() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (_broker, addr, cert, auth, config) =
        serve_orders(dir.path(), felix_storage::log::FsyncMode::None, |_| {}).await?;

    let asked = auth_answer(addr, cert.clone(), &auth).await?;
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

    let off_dir = tempfile::tempdir()?;
    let (_off, off_addr, off_cert, off_auth, _) = serve_orders(
        off_dir.path(),
        felix_storage::log::FsyncMode::None,
        |config| config.publish_window = 0,
    )
    .await?;
    let refused = auth_answer(off_addr, off_cert, &off_auth).await?;
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

/// Publish `count` one-record batches with `publish_batches` and read back
/// what the stream holds.
async fn publish_batches_and_read(
    broker: &Broker,
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
    count: usize,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>)> {
    let client =
        Arc::new(Connection::connect(addr, "localhost", build_client_config(cert, auth)?).await?);
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

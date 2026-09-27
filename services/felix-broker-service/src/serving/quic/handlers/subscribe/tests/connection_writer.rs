//! The shared per-connection writer.

use super::*;
use crate::serving::quic::handlers::subscribe::writer::ConnectionWriterConfig;

fn writer_config() -> ConnectionWriterConfig {
    ConnectionWriterConfig {
        max_bytes_per_write: 64 * 1024,
        max_queued_per_subscriber: 1024,
        block: false,
    }
}

#[tokio::test]
async fn run_connection_writer_coalesces_multiple_deliveries() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let subscription = broker.subscribe("t1", "default", "orders", 0).await?;
    let (_rx, guard) = subscription.into_parts();

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move { server.accept().await });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let client_conn = client.connect(addr, "localhost").await?;

    let connection = server_task.await.context("server join")??;
    let connection_id = connection.info().id.0;
    let event_send = connection.open_uni().await?;

    let (tx, rx) = mpsc::channel(8);
    let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, writer_config()));

    tx.send(ConnectionCommand::Register {
        subscriber_id: 1,
        connection: connection.clone(),
        connection_id: Some(connection_id),
        event_send,
        guard,
    })
    .await
    .context("register")?;

    let frame_a = felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"a")])?;
    let frame_b = felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"bb")])?;
    let now = Instant::now();
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame: frame_a,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery a")?;
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame: frame_b,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery b")?;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame1 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
    assert_eq!(batch1.payloads[0].as_ref(), b"a");
    let frame2 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame2");
    let batch2 = felix_wire::binary::decode_event_batch(&frame2).context("decode batch2")?;
    assert_eq!(batch2.payloads[0].as_ref(), b"bb");

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

#[tokio::test]
async fn run_connection_writer_handles_write_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let subscription = broker.subscribe("t1", "default", "orders", 0).await?;
    let (_rx, guard) = subscription.into_parts();

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move { server.accept().await });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let client_conn = client.connect(addr, "localhost").await?;

    let connection = server_task.await.context("server join")??;
    let connection_id = connection.info().id.0;
    let event_send = connection.open_uni().await?;
    drop(client_conn);

    let (tx, rx) = mpsc::channel(8);
    let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, writer_config()));

    tx.send(ConnectionCommand::Register {
        subscriber_id: 1,
        connection: connection.clone(),
        connection_id: Some(connection_id),
        event_send,
        guard,
    })
    .await
    .context("register")?;

    let frame = felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"a")])?;
    let now = Instant::now();
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery")?;

    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

#[tokio::test]
async fn run_connection_writer_unregister_drops_late_deliveries() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let subscription = broker.subscribe("t1", "default", "orders", 0).await?;
    let (_rx, guard) = subscription.into_parts();

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move { server.accept().await });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let client_conn = client.connect(addr, "localhost").await?;

    let connection = server_task.await.context("server join")??;
    let connection_id = connection.info().id.0;
    let event_send = connection.open_uni().await?;

    let (tx, rx) = mpsc::channel(8);
    let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, writer_config()));

    tx.send(ConnectionCommand::Register {
        subscriber_id: 1,
        connection: connection.clone(),
        connection_id: Some(connection_id),
        event_send,
        guard,
    })
    .await
    .context("register")?;

    let now = Instant::now();
    let frame = felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"a")])?;
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery")?;

    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame1 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
    assert_eq!(batch1.payloads[0].as_ref(), b"a");

    tx.send(ConnectionCommand::Unregister {
        subscriber_id: 1,
        last: None,
    })
    .await
    .context("unregister")?;

    let late = felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"late")])?;
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame: late,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("late delivery")?;

    let no_frame = tokio::time::timeout(
        Duration::from_millis(150),
        crate::serving::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        ),
    )
    .await;
    match no_frame {
        Err(_) => {}
        Ok(Ok(None)) => {}
        Ok(Ok(Some(_))) => panic!("unexpected late frame"),
        Ok(Err(err)) => return Err(err),
    }

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

/// A subscription that ends right after its last frames still gets them.
/// The feeder queues the last deliveries and then the unregister, and when
/// the writer picks all of them up in one batch it must write the frames
/// before letting the stream go -- the last one is what tells a client whose
/// shard moved where to resume.
#[tokio::test]
async fn run_connection_writer_writes_queued_frames_before_an_unregister() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let subscription = broker.subscribe("t1", "default", "orders", 0).await?;
    let (_rx, guard) = subscription.into_parts();

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move { server.accept().await });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let client_conn = client.connect(addr, "localhost").await?;
    let connection = server_task.await.context("server join")??;
    let connection_id = connection.info().id.0;
    let event_send = connection.open_uni().await?;

    // Queued before the writer runs, so it drains all three as one batch.
    let (tx, rx) = mpsc::channel(8);
    tx.send(ConnectionCommand::Register {
        subscriber_id: 1,
        connection: connection.clone(),
        connection_id: Some(connection_id),
        event_send,
        guard,
    })
    .await
    .context("register")?;
    let now = Instant::now();
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame: felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"last")])?,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery")?;
    tx.send(ConnectionCommand::Unregister {
        subscriber_id: 1,
        last: None,
    })
    .await
    .context("unregister")?;
    let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, writer_config()));

    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame = tokio::time::timeout(
        Duration::from_secs(2),
        crate::serving::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        ),
    )
    .await
    .context("the last frame never arrived")??
    .context("the stream ended without the last frame")?;
    let batch = felix_wire::binary::decode_event_batch(&frame).context("decode")?;
    assert_eq!(batch.payloads[0].as_ref(), b"last");
    let end = tokio::time::timeout(
        Duration::from_secs(2),
        crate::serving::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        ),
    )
    .await
    .context("the stream was not finished")??;
    assert!(end.is_none(), "nothing follows the unregister");

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

/// A subscription whose client stops reading must not stop the others on
/// the same connection. The writer used to take a batch of commands and then
/// wait for every write in it, so one flow-controlled stream parked the whole
/// writer and nothing queued behind it was ever written.
#[tokio::test]
async fn a_stalled_subscription_does_not_stop_the_others_on_its_connection() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let (_rx_a, guard_a) = broker
        .subscribe("t1", "default", "orders", 0)
        .await?
        .into_parts();
    let (_rx_b, guard_b) = broker
        .subscribe("t1", "default", "orders", 0)
        .await?
        .into_parts();

    let (server_config, cert) = make_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move { server.accept().await });
    // A small per-stream window, so a stream the client does not read stalls
    // the broker's writes to it quickly.
    let client_transport = TransportConfig {
        stream_receive_window: 64 * 1024,
        ..TransportConfig::default()
    };
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        make_client_config(cert)?,
        client_transport,
    )?;
    let client_conn = client.connect(addr, "localhost").await?;
    let connection = server_task.await.context("server join")??;
    let connection_id = connection.info().id.0;

    let (tx, rx) = mpsc::channel(64);
    for (subscriber_id, guard) in [(1, guard_a), (2, guard_b)] {
        tx.send(ConnectionCommand::Register {
            subscriber_id,
            connection: connection.clone(),
            connection_id: Some(connection_id),
            event_send: connection.open_uni().await?,
            guard,
        })
        .await
        .context("register")?;
    }
    // Far more for subscription 1 than its window takes.
    let big = Bytes::from(vec![b'x'; 32 * 1024]);
    for _ in 0..16 {
        let now = Instant::now();
        tx.send(ConnectionCommand::Delivery {
            subscriber_id: 1,
            frame: felix_wire::binary::encode_event_batch_bytes(1, std::slice::from_ref(&big))?,
            item_count: 1,
            first_enqueued_at: now,
            enqueue_at: now,
        })
        .await
        .context("delivery for the stalled subscription")?;
    }
    let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, writer_config()));
    // Let the writer take that batch and block on subscription 1.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let now = Instant::now();
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 2,
        frame: felix_wire::binary::encode_event_batch_bytes(2, &[Bytes::from_static(b"hello")])?,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery for the healthy subscription")?;

    // Streams show up on the client as their data arrives. Read one frame
    // from each until subscription 2's turns up, and nothing more from 1.
    let mut scratch = BytesMut::new();
    let mut unread = Vec::new();
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut event_recv = client_conn.accept_uni().await?;
            let frame = crate::serving::quic::codec::read_frame_limited_into(
                &mut event_recv,
                64 * 1024,
                &mut scratch,
            )
            .await?
            .context("stream ended")?;
            let batch = felix_wire::binary::decode_event_batch(&frame).context("decode")?;
            if batch.subscription_id == 2 {
                return anyhow::Ok(batch.payloads[0].clone());
            }
            // Subscription 1's stream stays open and unread.
            unread.push(event_recv);
        }
    })
    .await
    .context("the healthy subscription was never written")??;
    assert_eq!(found.as_ref(), b"hello");

    writer_task.abort();
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

/// Under `Block` the writer never drops a frame for being over a
/// subscriber's bound; it stops taking commands instead. A batch taken in one
/// go can overshoot the bound, and those frames must still be written.
#[tokio::test]
async fn block_policy_writes_a_batch_that_overshoots_the_bound() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let (_rx, guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await?
        .into_parts();

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move { server.accept().await });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let client_conn = client.connect(addr, "localhost").await?;
    let connection = server_task.await.context("server join")??;
    let connection_id = connection.info().id.0;

    // Everything is queued before the writer runs, so it takes it as one batch.
    const FRAMES: usize = 10;
    let (tx, rx) = mpsc::channel(64);
    tx.send(ConnectionCommand::Register {
        subscriber_id: 1,
        connection: connection.clone(),
        connection_id: Some(connection_id),
        event_send: connection.open_uni().await?,
        guard,
    })
    .await
    .context("register")?;
    for index in 0..FRAMES {
        let now = Instant::now();
        tx.send(ConnectionCommand::Delivery {
            subscriber_id: 1,
            frame: felix_wire::binary::encode_event_batch_bytes(
                1,
                &[Bytes::from(index.to_string())],
            )?,
            item_count: 1,
            first_enqueued_at: now,
            enqueue_at: now,
        })
        .await
        .context("delivery")?;
    }
    let config = ConnectionWriterConfig {
        max_queued_per_subscriber: 2,
        block: true,
        ..writer_config()
    };
    let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, config));

    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    for index in 0..FRAMES {
        let frame = tokio::time::timeout(
            Duration::from_secs(2),
            crate::serving::quic::codec::read_frame_limited_into(
                &mut event_recv,
                16 * 1024,
                &mut scratch,
            ),
        )
        .await
        .with_context(|| format!("frame {index} never arrived"))??
        .context("the stream ended early")?;
        let batch = felix_wire::binary::decode_event_batch(&frame).context("decode")?;
        assert_eq!(batch.payloads[0], Bytes::from(index.to_string()));
    }

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

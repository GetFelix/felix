//! The shared per-connection writer.

use super::*;
use crate::serving::quic::handlers::subscribe::egress::EgressBytes;
use crate::serving::quic::handlers::subscribe::writer::ConnectionWriterConfig;
use serial_test::serial;

fn writer_config() -> ConnectionWriterConfig {
    ConnectionWriterConfig {
        max_bytes_per_write: 64 * 1024,
        max_queued_per_subscriber: 1024,
        block: false,
    }
}

#[tokio::test]
#[serial]
async fn run_connection_writer_coalesces_multiple_deliveries() -> Result<()> {
    // Bounded broker-wide, which covers a frame until it is written, so the
    // test can see the writer give the bytes back.
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into()).with_subscriber_queue_max_bytes_total(100),
    );
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
    let (mut rx, guard) = subscription.into_parts();
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"a"))
        .await?;
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"bb"))
        .await?;
    let mut held_a = EgressBytes::default();
    held_a.carry(rx.recv_held().await.expect("a").1);
    let mut held_b = EgressBytes::default();
    held_b.carry(rx.recv_held().await.expect("bb").1);

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
        base_offset: None,
        first_enqueued_at: now,
        enqueue_at: now,
        held: held_a,
    })
    .await
    .context("delivery a")?;
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame: frame_b,
        item_count: 1,
        base_offset: None,
        first_enqueued_at: now,
        enqueue_at: now,
        held: held_b,
    })
    .await
    .context("delivery b")?;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = crate::serving::quic::FrameScratch::new();
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

    // A batch as large as the whole bound fits only once both written frames
    // have given their bytes back.
    let delivered = broker
        .publish("t1", "default", "orders", Bytes::from(vec![0u8; 100]))
        .await?;
    assert_eq!(delivered, 1, "written frames still hold their bytes");

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

#[tokio::test]
#[serial]
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
        base_offset: None,
        first_enqueued_at: now,
        enqueue_at: now,
        held: EgressBytes::default(),
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
#[serial]
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
        base_offset: None,
        first_enqueued_at: now,
        enqueue_at: now,
        held: EgressBytes::default(),
    })
    .await
    .context("delivery")?;

    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = crate::serving::quic::FrameScratch::new();
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
        base_offset: None,
        first_enqueued_at: now,
        enqueue_at: now,
        held: EgressBytes::default(),
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
#[serial]
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
        base_offset: None,
        first_enqueued_at: now,
        enqueue_at: now,
        held: EgressBytes::default(),
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
    let mut scratch = crate::serving::quic::FrameScratch::new();
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
#[serial]
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
            base_offset: None,
            first_enqueued_at: now,
            enqueue_at: now,
            held: EgressBytes::default(),
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
        base_offset: None,
        first_enqueued_at: now,
        enqueue_at: now,
        held: EgressBytes::default(),
    })
    .await
    .context("delivery for the healthy subscription")?;

    // Streams show up on the client as their data arrives. Read one frame
    // from each until subscription 2's turns up, and nothing more from 1.
    let mut unread = Vec::new();
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut event_recv = client_conn.accept_uni().await?;
            let mut scratch = crate::serving::quic::FrameScratch::new();
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
#[serial]
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
            base_offset: None,
            first_enqueued_at: now,
            enqueue_at: now,
            held: EgressBytes::default(),
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
    let mut scratch = crate::serving::quic::FrameScratch::new();
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

/// Frames the writer sheds over a subscriber's bound are the subscriber's
/// drops like any other: its stats, its lag note and
/// `felix_subscribe_dropped_total` count them, by record.
#[tokio::test]
#[serial]
async fn frames_dropped_over_the_bound_are_counted_as_the_subscribers_drops() -> Result<()> {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let rendered = recorder.handle();
    let _recorder = metrics::set_default_local_recorder(&recorder);

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
    let (rx, guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await?
        .into_parts();

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move { server.accept().await });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let _client_conn = client.connect(addr, "localhost").await?;
    let connection = server_task.await.context("server join")??;
    let connection_id = connection.info().id.0;

    // Queued before the writer runs, so it takes all of it in one pass and
    // keeps the first two frames.
    const FRAMES: usize = 10;
    const KEPT: usize = 2;
    const RECORDS_PER_FRAME: usize = 3;
    let (tx, command_rx) = mpsc::channel(64);
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
        let records = vec![Bytes::from_static(b"r"); RECORDS_PER_FRAME];
        tx.send(ConnectionCommand::Delivery {
            subscriber_id: 1,
            frame: felix_wire::binary::encode_event_batch_bytes(1, &records)?,
            item_count: RECORDS_PER_FRAME,
            base_offset: Some((index * RECORDS_PER_FRAME) as u64),
            first_enqueued_at: now,
            enqueue_at: now,
            held: EgressBytes::default(),
        })
        .await
        .context("delivery")?;
    }
    let config = ConnectionWriterConfig {
        max_queued_per_subscriber: KEPT,
        block: false,
        ..writer_config()
    };
    let writer_task = tokio::spawn(run_connection_writer(connection_id, command_rx, config));

    let dropped = ((FRAMES - KEPT) * RECORDS_PER_FRAME) as u64;
    wait_for_dropped(&broker, dropped).await;
    assert_eq!(rx.lagged(), Some((KEPT * RECORDS_PER_FRAME) as u64));
    let text = rendered.render();
    assert!(
        text.contains(&format!("felix_subscribe_dropped_total {dropped}")),
        "{text}"
    );
    assert!(
        text.contains(&format!("felix_sub_queue_dropped_total {dropped}")),
        "{text}"
    );

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

/// Wait until the one subscription on `broker` has `dropped` drops.
pub(super) async fn wait_for_dropped(broker: &Broker, dropped: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let page = broker
            .list_subscriptions(&felix_wire::SubscriptionFilter::default(), None, 10)
            .await;
        assert_eq!(page.subscriptions.len(), 1);
        let seen = page.subscriptions[0].dropped;
        if seen == dropped {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "dropped stayed at {seen}, expected {dropped}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A server and client connection over loopback.
pub(super) async fn quic_pair() -> Result<(
    felix_transport::QuicConnection,
    felix_transport::QuicConnection,
)> {
    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move { server.accept().await });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let client_conn = client.connect(addr, "localhost").await?;
    let connection = server_task.await.context("server join")??;
    Ok((connection, client_conn))
}

/// Once the writer drops a frame for a subscription that ends at a drop, it
/// sheds the rest for that subscriber too, though there is room again:
/// delivering them would leave a hole the client sees only as the later
/// records arriving. One that does not end at a drop carries on past it.
#[tokio::test]
#[serial]
async fn after_a_writer_drop_later_frames_are_shed_until_the_unregister() -> Result<()> {
    shed_after_a_writer_drop(true).await?;
    shed_after_a_writer_drop(false).await
}

async fn shed_after_a_writer_drop(ends_on_lag: bool) -> Result<()> {
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
    let (mut rx, guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await?
        .into_parts();
    if ends_on_lag {
        rx.end_on_lag();
    }
    let (connection, client_conn) = quic_pair().await?;
    let connection_id = connection.info().id.0;

    let delivery = |offset: u64| -> Result<ConnectionCommand> {
        let now = Instant::now();
        Ok(ConnectionCommand::Delivery {
            subscriber_id: 1,
            frame: felix_wire::binary::encode_event_batch_bytes(
                1,
                &[Bytes::from(offset.to_string())],
            )?,
            item_count: 1,
            base_offset: Some(offset),
            first_enqueued_at: now,
            enqueue_at: now,
            held: EgressBytes::default(),
        })
    };
    // Queued before the writer runs: it keeps 0 and 1 and drops 2.
    let (tx, command_rx) = mpsc::channel(64);
    tx.send(ConnectionCommand::Register {
        subscriber_id: 1,
        connection: connection.clone(),
        connection_id: Some(connection_id),
        event_send: connection.open_uni().await?,
        guard,
    })
    .await
    .context("register")?;
    for offset in 0..3 {
        tx.send(delivery(offset)?).await.context("delivery")?;
    }
    let config = ConnectionWriterConfig {
        max_queued_per_subscriber: 2,
        block: false,
        ..writer_config()
    };
    let writer_task = tokio::spawn(run_connection_writer(connection_id, command_rx, config));

    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let mut next = async || -> Result<Option<Bytes>> {
        let frame = tokio::time::timeout(
            Duration::from_secs(2),
            crate::serving::quic::codec::read_frame_limited_into(
                &mut event_recv,
                16 * 1024,
                &mut scratch,
            ),
        )
        .await
        .context("no frame")??;
        Ok(match frame {
            Some(frame) => Some(
                felix_wire::binary::decode_event_batch(&frame)
                    .context("decode")?
                    .payloads[0]
                    .clone(),
            ),
            None => None,
        })
    };
    assert_eq!(next().await?.as_deref(), Some(&b"0"[..]));
    assert_eq!(next().await?.as_deref(), Some(&b"1"[..]));

    // The queue has room again; 3 is shed anyway when the subscription ends
    // at the drop, and the last frame never is.
    tx.send(delivery(3)?).await.context("delivery 3")?;
    wait_for_dropped(&broker, if ends_on_lag { 2 } else { 1 }).await;
    assert_eq!(rx.lagged(), Some(2));
    tx.send(ConnectionCommand::Unregister {
        subscriber_id: 1,
        // What the feeder would choose: lagged once the subscription ends at
        // a drop, which the writer lowers to its own.
        last: Some(if ends_on_lag {
            felix_wire::Message::SubscriptionLagged {
                subscription_id: 1,
                resume_from: 3,
            }
            .encode()?
            .encode()
        } else {
            felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"last")])?
        }),
    })
    .await
    .context("unregister")?;
    if ends_on_lag {
        // The shed replaces the feeder's last frame: it is where to resume.
        let last = read_last_frame(&mut event_recv, &mut scratch).await?;
        assert_eq!(
            last,
            Some(felix_wire::Message::SubscriptionLagged {
                subscription_id: 1,
                resume_from: 2,
            })
        );
    } else {
        assert_eq!(next().await?.as_deref(), Some(&b"3"[..]));
        assert_eq!(next().await?.as_deref(), Some(&b"last"[..]));
        assert_eq!(next().await?, None, "nothing follows the last frame");
    }

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

/// Read the control frame that ends the event stream, and check that
/// nothing follows it.
async fn read_last_frame(
    event_recv: &mut quinn::RecvStream,
    scratch: &mut crate::serving::quic::FrameScratch,
) -> Result<Option<felix_wire::Message>> {
    let mut read = async || {
        tokio::time::timeout(
            Duration::from_secs(2),
            crate::serving::quic::codec::read_frame_limited_into(event_recv, 16 * 1024, scratch),
        )
        .await
        .context("no frame")?
    };
    let Some(frame) = read().await? else {
        return Ok(None);
    };
    let message = felix_wire::Message::decode(frame)?;
    assert!(read().await?.is_none(), "nothing follows the last frame");
    Ok(Some(message))
}

/// The feeder picks the last frame when it hands over its final delivery,
/// before the writer has taken those deliveries. A writer drop after that
/// lowers where the subscriber can resume, so the writer swaps the last
/// frame for a `subscription_lagged` from the lower offset: an older
/// `lagged` would skip the shed records, and `ended` would claim a range
/// the client did not get.
#[tokio::test]
#[serial]
async fn a_writer_drop_after_the_feeder_chose_lagged_lowers_resume_from() -> Result<()> {
    // The queue dropped from 5, so the feeder chose lagged(5).
    let lagged = felix_wire::Message::SubscriptionLagged {
        subscription_id: 1,
        resume_from: 5,
    };
    writer_drop_replaces_last(Some(5), lagged, lagged_from_2()).await
}

/// The bounded half of the above: the feeder saw the subscription reach its
/// end and chose `ended`.
#[tokio::test]
#[serial]
async fn a_writer_drop_after_the_feeder_chose_ended_sends_lagged() -> Result<()> {
    let ended = felix_wire::Message::SubscriptionEnded {
        subscription_id: 1,
        end: 3,
    };
    writer_drop_replaces_last(None, ended, lagged_from_2()).await
}

/// A revocation says why the subscription ended; a writer drop does not
/// turn it into a resume point.
#[tokio::test]
#[serial]
async fn a_writer_drop_keeps_a_revoked_last_frame() -> Result<()> {
    let revoked = felix_wire::Message::decode(felix_wire::Frame::decode(
        crate::serving::quic::handlers::subscribe::revoked_frame(0)?,
    )?)?;
    writer_drop_replaces_last(None, revoked.clone(), revoked).await
}

fn lagged_from_2() -> felix_wire::Message {
    felix_wire::Message::SubscriptionLagged {
        subscription_id: 1,
        resume_from: 2,
    }
}

async fn writer_drop_replaces_last(
    queue_drop: Option<u64>,
    feeder_last: felix_wire::Message,
    expected_last: felix_wire::Message,
) -> Result<()> {
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
    let (mut rx, guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await?
        .into_parts();
    rx.end_on_lag();
    if let Some(offset) = queue_drop {
        guard
            .drop_account()
            .context("drop account")?
            .dropped(Some(offset), 1);
    }
    let (connection, client_conn) = quic_pair().await?;
    let connection_id = connection.info().id.0;

    // All of it queued before the writer runs, as when the feeder has
    // finished: the writer keeps 0 and 1, drops 2, then sees the last frame.
    let (tx, command_rx) = mpsc::channel(64);
    tx.send(ConnectionCommand::Register {
        subscriber_id: 1,
        connection: connection.clone(),
        connection_id: Some(connection_id),
        event_send: connection.open_uni().await?,
        guard,
    })
    .await
    .context("register")?;
    for offset in 0..3u64 {
        let now = Instant::now();
        tx.send(ConnectionCommand::Delivery {
            subscriber_id: 1,
            frame: felix_wire::binary::encode_event_batch_bytes(
                1,
                &[Bytes::from(offset.to_string())],
            )?,
            item_count: 1,
            base_offset: Some(offset),
            first_enqueued_at: now,
            enqueue_at: now,
            held: EgressBytes::default(),
        })
        .await
        .context("delivery")?;
    }
    tx.send(ConnectionCommand::Unregister {
        subscriber_id: 1,
        last: Some(feeder_last.encode()?.encode()),
    })
    .await
    .context("unregister")?;
    let config = ConnectionWriterConfig {
        max_queued_per_subscriber: 2,
        block: false,
        ..writer_config()
    };
    let writer_task = tokio::spawn(run_connection_writer(connection_id, command_rx, config));

    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = crate::serving::quic::FrameScratch::new();
    for offset in 0..2u64 {
        let frame = tokio::time::timeout(
            Duration::from_secs(2),
            crate::serving::quic::codec::read_frame_limited_into(
                &mut event_recv,
                16 * 1024,
                &mut scratch,
            ),
        )
        .await
        .context("no frame")??
        .context("stream ended early")?;
        let batch = felix_wire::binary::decode_event_batch(&frame).context("decode")?;
        assert_eq!(batch.payloads[0], Bytes::from(offset.to_string()));
    }
    assert_eq!(
        read_last_frame(&mut event_recv, &mut scratch).await?,
        Some(expected_last)
    );

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

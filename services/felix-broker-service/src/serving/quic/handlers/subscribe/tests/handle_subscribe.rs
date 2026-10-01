//! Subscribe requests on the control stream, end to end over QUIC.

use super::*;

#[tokio::test]
async fn handle_subscribe_message_sends_event_stream_binary_batch() -> Result<()> {
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

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let broker_for_server = broker.clone();
    let lane_manager = WriterLaneManager::new(&test_config());
    let server_lane_manager = Arc::clone(&lane_manager);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let handled = handle_subscribe_message(
            broker_for_server,
            connection,
            test_config(),
            &Arc::new(SubscriptionLimiter::new()),
            &server_lane_manager,
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "orders".to_string(),
            Some(7),
            None,
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
            0,
        )
        .await?;
        Result::<bool>::Ok(handled)
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;

    let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
        .await
        .context("ack timeout")?
        .context("ack missing")?;
    match ack {
        Outgoing::Message(Message::Subscribed {
            subscription_id, ..
        }) => {
            assert_eq!(subscription_id, 7);
        }
        _ => panic!("unexpected ack"),
    }

    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let hello =
        crate::serving::quic::codec::read_message_limited(&mut event_recv, 16 * 1024, &mut scratch)
            .await?
            .expect("hello");
    match hello {
        Message::EventStreamHello { subscription_id } => {
            assert_eq!(subscription_id, 7);
        }
        other => panic!("unexpected hello: {other:?}"),
    }

    broker
        .publish(
            "t1",
            "default",
            "orders",
            bytes::Bytes::from_static(b"hello"),
        )
        .await?;

    let frame = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("event frame");
    let payloads = decode_delivery_payloads(&frame).context("decode batch")?;
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].as_ref(), b"hello");

    let _ = felix_broker::timings::take_samples();
    let handled = server_task.await.context("server join")??;
    assert!(handled);
    Ok(())
}

#[tokio::test]
async fn handle_subscribe_message_errors_when_stream_missing() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let broker_for_server = broker.clone();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        handle_subscribe_message(
            broker_for_server,
            connection,
            test_config(),
            &Arc::new(SubscriptionLimiter::new()),
            &WriterLaneManager::new(&test_config()),
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "missing".to_string(),
            Some(11),
            None,
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
            0,
        )
        .await
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let _connection = client.connect(addr, "localhost").await?;

    let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
        .await
        .context("ack timeout")?
        .context("ack missing")?;
    match ack {
        Outgoing::Message(Message::Error { message, .. }) => {
            assert!(message.contains("stream not found"));
        }
        _ => panic!("unexpected ack"),
    }

    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn handle_subscribe_message_batches_by_bytes() -> Result<()> {
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

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let mut config = test_config();
    config.fanout_batch_size = 10;
    config.event_batch_max_events = 10;
    config.event_batch_max_bytes = 6;

    let broker_for_server = broker.clone();
    let lane_manager = WriterLaneManager::new(&test_config());
    let server_lane_manager = Arc::clone(&lane_manager);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        handle_subscribe_message(
            broker_for_server,
            connection,
            config,
            &Arc::new(SubscriptionLimiter::new()),
            &server_lane_manager,
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "orders".to_string(),
            Some(21),
            None,
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
            0,
        )
        .await
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;

    let _ = out_ack_rx.recv().await;
    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let _ =
        crate::serving::quic::codec::read_message_limited(&mut event_recv, 16 * 1024, &mut scratch)
            .await?
            .expect("hello");

    broker
        .publish("t1", "default", "orders", bytes::Bytes::from_static(b"aa"))
        .await?;
    broker
        .publish(
            "t1",
            "default",
            "orders",
            bytes::Bytes::from_static(b"bbbb"),
        )
        .await?;
    broker
        .publish(
            "t1",
            "default",
            "orders",
            bytes::Bytes::from_static(b"ccccc"),
        )
        .await?;

    let frame1 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let payloads1 = decode_delivery_payloads(&frame1).expect("batch1");
    assert_eq!(payloads1.len(), 2);
    assert_eq!(payloads1[0].as_ref(), b"aa");
    assert_eq!(payloads1[1].as_ref(), b"bbbb");

    let frame2 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame2");
    let payloads2 = decode_delivery_payloads(&frame2).expect("batch2");
    assert_eq!(payloads2.len(), 1);
    assert_eq!(payloads2[0].as_ref(), b"ccccc");

    let _ = felix_broker::timings::take_samples();
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn handle_subscribe_message_hashed_pool_with_generated_id() -> Result<()> {
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

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let mut config = test_config();
    config.sub_stream_mode = crate::config::SubStreamMode::HashedPool;
    let broker_for_server = broker.clone();
    let lane_manager = WriterLaneManager::new(&test_config());
    let server_lane_manager = Arc::clone(&lane_manager);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        handle_subscribe_message(
            broker_for_server,
            connection,
            config,
            &Arc::new(SubscriptionLimiter::new()),
            &server_lane_manager,
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "orders".to_string(),
            None,
            None,
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
            0,
        )
        .await
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;

    let subscription_id = match out_ack_rx.recv().await.context("missing ack")? {
        Outgoing::Message(Message::Subscribed {
            subscription_id, ..
        }) => subscription_id,
        _ => panic!("unexpected ack"),
    };
    assert!(subscription_id > 0);

    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let hello =
        crate::serving::quic::codec::read_message_limited(&mut event_recv, 16 * 1024, &mut scratch)
            .await?
            .expect("hello");
    match hello {
        Message::EventStreamHello {
            subscription_id: hello_id,
        } => assert_eq!(hello_id, subscription_id),
        other => panic!("unexpected hello: {other:?}"),
    }

    broker
        .publish("t1", "default", "orders", bytes::Bytes::from_static(b"ok"))
        .await?;
    let frame = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("event frame");
    let payloads = decode_delivery_payloads(&frame).context("decode batch")?;
    assert_eq!(payloads[0].as_ref(), b"ok");

    let _ = felix_broker::timings::take_samples();
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn handle_subscribe_message_open_uni_failure_sends_error_ack() -> Result<()> {
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

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let broker_for_server = broker.clone();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle_subscribe_message(
            broker_for_server,
            connection,
            test_config(),
            &Arc::new(SubscriptionLimiter::new()),
            &WriterLaneManager::new(&test_config()),
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "orders".to_string(),
            Some(900),
            None,
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
            0,
        )
        .await
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    drop(connection);

    let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
        .await
        .context("ack timeout")?
        .context("missing ack")?;
    match ack {
        Outgoing::Message(Message::Error { message, .. }) => {
            assert!(!message.is_empty());
        }
        _ => panic!("expected error ack"),
    }
    server_task.await.context("server join")??;
    Ok(())
}

/// Subscribe with `peer_features`, publish one record, end the shard's
/// subscriptions as a move does, and return every raw frame after the hello
/// up to the end of the stream.
async fn frames_of_a_moved_subscription(peer_features: u32) -> Result<Vec<bytes::Bytes>> {
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

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);
    let broker_for_server = broker.clone();
    let lane_manager = WriterLaneManager::new(&test_config());
    let server_lane_manager = Arc::clone(&lane_manager);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        // Held by the test, as the connection handler holds it in the broker:
        // the last reference dropping would close the connection under the
        // reader.
        let held = connection.clone();
        handle_subscribe_message(
            broker_for_server,
            connection,
            test_config(),
            &Arc::new(SubscriptionLimiter::new()),
            &server_lane_manager,
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "orders".to_string(),
            Some(7),
            None,
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
            peer_features,
        )
        .await?;
        Result::<_>::Ok(held)
    });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let _held = server_task.await.context("server join")??;
    tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
        .await
        .context("ack timeout")?
        .context("ack missing")?;
    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = crate::serving::quic::FrameScratch::new();
    crate::serving::quic::codec::read_message_limited(&mut event_recv, 16 * 1024, &mut scratch)
        .await?
        .expect("hello");

    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"last"))
        .await?;
    broker
        .end_subscriptions(
            "t1",
            "default",
            "orders",
            0,
            Some(felix_broker::ShardHandoff {
                node_id: Some("broker-b".to_string()),
                addr: Some("127.0.0.1:5001".to_string()),
                generation: 4,
            }),
        )
        .await;

    let mut frames = Vec::new();
    while let Some(frame) = tokio::time::timeout(
        Duration::from_secs(2),
        crate::serving::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        ),
    )
    .await
    .context("the stream was not finished")??
    {
        frames.push(frame.encode());
    }
    let _ = felix_broker::timings::take_samples();
    Ok(frames)
}

/// A client that offered `FEATURE_SHARD_MOVED` gets one last frame saying
/// where the shard went; one that did not gets exactly the bytes it always
/// got, the last event and the end of the stream.
#[tokio::test]
async fn a_moved_subscription_ends_with_shard_moved_only_when_offered() -> Result<()> {
    let without = frames_of_a_moved_subscription(0).await?;
    let with = frames_of_a_moved_subscription(felix_wire::FEATURE_SHARD_MOVED).await?;

    assert_eq!(without.len(), 1, "the event, then the end");
    assert_eq!(with.len(), 2, "the event, shard_moved, then the end");
    assert_eq!(
        with[0], without[0],
        "the event is byte-identical either way"
    );
    let moved = Message::decode(felix_wire::Frame::decode(with[1].clone())?)?;
    assert_eq!(
        moved,
        Message::ShardMoved {
            subscription_id: 7,
            // An in-memory stream: its sequence means nothing elsewhere.
            resume_from: None,
            node_id: Some("broker-b".to_string()),
            addr: Some("127.0.0.1:5001".to_string()),
            generation: 4,
        }
    );
    Ok(())
}

/// Resume a durable stream from offset 0 over QUIC with `peer_flags`, across a
/// generation start in its history and another one while live, and return
/// every event frame up to the end of the stream.
async fn frames_across_generation_starts(peer_flags: u16) -> Result<Vec<bytes::Bytes>> {
    let dir = tempfile::tempdir()?;
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
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
            felix_broker::StreamMetadata {
                durable: true,
                ..Default::default()
            },
        )
        .await?;
    for value in ["a", "b"] {
        broker
            .publish("t1", "default", "orders", Bytes::from(value))
            .await?;
    }
    broker
        .append_generation_start("t1", "default", "orders", 0, 2)
        .await?;
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"c"))
        .await?;

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);
    let broker_for_server = broker.clone();
    let lane_manager = WriterLaneManager::new(&test_config());
    let server_lane_manager = Arc::clone(&lane_manager);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let held = connection.clone();
        handle_subscribe_message(
            broker_for_server,
            connection,
            test_config(),
            &Arc::new(SubscriptionLimiter::new()),
            &server_lane_manager,
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "orders".to_string(),
            Some(7),
            Some(felix_wire::StartPosition::Offset(0)),
            None,
            peer_flags,
            0,
        )
        .await?;
        Result::<_>::Ok(held)
    });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let _held = server_task.await.context("server join")??;
    tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
        .await
        .context("ack timeout")?
        .context("ack missing")?;
    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = crate::serving::quic::FrameScratch::new();
    crate::serving::quic::codec::read_message_limited(&mut event_recv, 16 * 1024, &mut scratch)
        .await?
        .expect("hello");

    // Live: the leader changes again with the subscription open.
    broker
        .append_generation_start("t1", "default", "orders", 0, 3)
        .await?;
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"d"))
        .await?;
    // Let the feeder deliver before the stream is ended under it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    broker
        .end_subscriptions("t1", "default", "orders", 0, None)
        .await;

    let mut frames = Vec::new();
    while let Some(frame) = tokio::time::timeout(
        Duration::from_secs(2),
        crate::serving::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        ),
    )
    .await
    .context("the stream was not finished")??
    {
        frames.push(frame.encode());
    }
    let _ = felix_broker::timings::take_samples();
    Ok(frames)
}

/// `(offset, skipped_before, payload)` for every event in `frames`.
fn events_of(frames: &[bytes::Bytes]) -> Result<Vec<(u64, u64, Bytes)>> {
    let mut events = Vec::new();
    for frame in frames {
        let frame = felix_wire::Frame::decode(frame.clone())?;
        let (payloads, base, skipped) =
            if frame.header.flags & felix_wire::FLAG_BINARY_EVENT_BATCH_SHARED != 0 {
                let batch = felix_wire::binary::decode_shared_event_batch(&frame)?;
                (batch.payloads, batch.base_offset, batch.skipped_before)
            } else {
                let batch = felix_wire::binary::decode_event_batch(&frame)?;
                (batch.payloads, batch.base_offset, batch.skipped_before)
            };
        let base = base.context("offsets were negotiated")?;
        for (index, payload) in payloads.into_iter().enumerate() {
            let skipped = if index == 0 { skipped } else { 0 };
            events.push((base + index as u64, skipped, payload));
        }
    }
    Ok(events)
}

/// Generation-start records are never delivered, history or live, and a
/// client that offered `EVENT_BATCH_SKIPPED` is told about each one on the
/// event after it. A client that did not gets offsets alone, and no frame
/// carrying a bit it would reject.
#[tokio::test]
async fn generation_starts_are_reported_only_to_a_client_that_offered_the_bit() -> Result<()> {
    let offsets = felix_wire::ORIGINAL_V1_FLAGS | felix_wire::FLAG_EVENT_BATCH_OFFSETS;

    let with = events_of(
        &frames_across_generation_starts(offsets | felix_wire::FLAG_EVENT_BATCH_SKIPPED).await?,
    )?;
    assert_eq!(
        with,
        vec![
            (0, 0, Bytes::from_static(b"a")),
            (1, 0, Bytes::from_static(b"b")),
            (3, 1, Bytes::from_static(b"c")),
            (5, 1, Bytes::from_static(b"d")),
        ]
    );

    let frames = frames_across_generation_starts(offsets).await?;
    assert!(frames.iter().all(|frame| {
        felix_wire::Frame::decode(frame.clone())
            .is_ok_and(|frame| frame.header.flags & felix_wire::FLAG_EVENT_BATCH_SKIPPED == 0)
    }));
    let without = events_of(&frames)?;
    assert_eq!(
        without
            .iter()
            .map(|(offset, skipped, _)| (*offset, *skipped))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 0), (3, 0), (5, 0)],
    );
    Ok(())
}

//! How the control loop continues, cancels and ends.

use super::*;

#[tokio::test]
async fn control_loop_handles_publish_and_cache_requests() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    broker
        .register_cache(
            "t1",
            "default",
            "primary",
            felix_broker::CacheMetadata::default(),
        )
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::None),
            key: None,
        }))),
        Ok(Some(frame_from_message(Message::PublishBatch {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payloads: vec![b"a".to_vec(), b"b".to_vec()],
            request_id: Some(2),
            ack: Some(felix_wire::AckMode::None),
            key: None,
        }))),
        Ok(Some(frame_from_message(Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: Some(10),
            ttl_ms: None,
        }))),
        Ok(Some(frame_from_message(Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "key".to_string(),
            request_id: Some(11),
        }))),
        Ok(Some(frame_from_message(Message::Ok))),
    ];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let result = run_control_loop(
        &mut source,
        Arc::clone(&broker),
        connection,
        BrokerConfig::default(),
        Arc::clone(&auth.auth),
        publish_ctx,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await?;
    assert!(!result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_subscribe_returns_true() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::Subscribe {
            start: None,
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
            subscription_id: None,
            shard: None,
            queue_capacity: None,
        }))),
    ];
    let (result, _messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(result);
    Ok(())
}

#[tokio::test]
async fn control_loop_handles_binary_and_decode_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let binary =
        binary_publish_batch_frame("t1", "default", "updates", &[Bytes::from_static(b"one")]);
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(binary)),
        Ok(Some(invalid_json_frame())),
    ];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = crate::serving::quic::FrameScratch::new();
    assert!(
        run_control_loop(
            &mut source,
            Arc::clone(&broker),
            connection,
            BrokerConfig::default(),
            Arc::clone(&auth.auth),
            publish_ctx,
            HashMap::new(),
            String::new(),
            out_ack_tx,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            ack_throttle_rx,
            ack_throttle_tx,
            ack_timeout_state,
            cancel_tx,
            cancel_rx,
            Arc::new(Semaphore::new(8)),
            ack_waiter_tx,
            Duration::from_millis(10),
            &mut scratch,
            Default::default(),
            Default::default(),
        )
        .await
        .is_err()
    );
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_handles_cancel_and_graceful_close() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let ready = Arc::new(AtomicBool::new(false));
    let mut source = PendingFrameSource {
        ready: Arc::clone(&ready),
    };
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let cancel_tx_clone = cancel_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let _ = cancel_tx_clone.send(true);
    });
    let result = run_control_loop(
        &mut source,
        Arc::clone(&broker),
        connection,
        BrokerConfig::default(),
        Arc::clone(&auth.auth),
        publish_ctx,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await?;
    assert!(!result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_handles_cancel_toggle_and_continues() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let mut source = DelayFrameSource {
        delay: Duration::from_millis(20),
    };
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let _ = cancel_tx.send(true);
    let _ = cancel_tx.send(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let result = run_control_loop(
        &mut source,
        Arc::clone(&broker),
        connection,
        BrokerConfig::default(),
        Arc::clone(&auth.auth),
        publish_ctx,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await?;
    assert!(result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_pre_canceled_exits() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let mut source = TestFrameSource::new(vec![Ok(None)]);
    let (out_ack_tx, _out_ack_rx) = mpsc::channel(1);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let _ = cancel_tx.send(true);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::default(),
        Arc::clone(&auth.auth),
        build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await?;
    assert!(!result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_cancel_changed_breaks() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let ready = Arc::new(AtomicBool::new(false));
    let mut source = PendingFrameSource {
        ready: Arc::clone(&ready),
    };
    let (out_ack_tx, _out_ack_rx) = mpsc::channel(1);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let cancel_tx_clone = cancel_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let _ = cancel_tx_clone.send(true);
    });
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::default(),
        Arc::clone(&auth.auth),
        build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await?;
    assert!(!result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_cancel_changed_continues() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let ready = Arc::new(AtomicBool::new(false));
    let mut source = PendingFrameSource {
        ready: Arc::clone(&ready),
    };
    let (out_ack_tx, _out_ack_rx) = mpsc::channel(1);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let cancel_tx_clone = cancel_tx.clone();
    let ready_clone = Arc::clone(&ready);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        let _ = cancel_tx_clone.send(false);
        ready_clone.store(true, Ordering::Relaxed);
    });
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::default(),
        Arc::clone(&auth.auth),
        build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await?;
    assert!(result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_subscribe_done_true() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::Subscribe {
            start: None,
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
            subscription_id: None,
            shard: None,
            queue_capacity: None,
        }))),
    ];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::default(),
        Arc::clone(&auth.auth),
        publish_ctx,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await?;
    assert!(result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_error_message_returns_false() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::error("bad")))),
    ];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, _out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::default(),
        Arc::clone(&auth.auth),
        build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await?;
    assert!(!result);
    server_task.await.context("server task")??;
    Ok(())
}

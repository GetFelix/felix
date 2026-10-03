//! Cache puts, gets and watches on the control stream.

use super::*;

#[tokio::test]
async fn control_loop_cache_put_best_effort_closed_reports_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache(
            "t1",
            "default",
            "primary",
            felix_broker::CacheMetadata::default(),
        )
        .await?;
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
        Ok(Some(frame_from_message(Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: None,
            ttl_ms: None,
        }))),
    ];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    drop(out_ack_rx);
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
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await;
    assert!(result.is_err());
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_cache_put_missing_scope_with_request_id_continues() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
            key: "key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: Some(34),
            ttl_ms: None,
        }))),
        Ok(None),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::CacheMessage(Message::Error { message, .. }) if message.contains("cache scope not found")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_cache_get_missing_scope_with_request_id_continues() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
            key: "key".to_string(),
            request_id: Some(42),
        }))),
        Ok(None),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::CacheMessage(Message::Error { message, .. }) if message.contains("cache scope not found")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_cache_put_records_timing_and_ack() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache(
            "t1",
            "default",
            "primary",
            felix_broker::CacheMetadata::default(),
        )
        .await?;
    let auth = auth_fixture("t1", default_perms());
    timings::enable_collection(1);
    timings::set_enabled(true);

    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: Some(7),
            ttl_ms: None,
        }))),
        Ok(None),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::CacheMessage(Message::CacheOk { request_id: 7 })
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_cache_put_best_effort_full_and_closed() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache(
            "t1",
            "default",
            "primary",
            felix_broker::CacheMetadata::default(),
        )
        .await?;
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
        tokio::time::sleep(Duration::from_millis(200)).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let cache_put = frame_from_message(Message::CachePut {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "primary".to_string(),
        key: "key".to_string(),
        value: Bytes::from_static(b"value"),
        request_id: None,
        ttl_ms: None,
    });
    let mut source = TestFrameSource::new(vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(cache_put.clone())),
    ]);
    let connection_clone = connection.clone();

    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    out_ack_tx
        .try_send(Outgoing::CacheMessage(Message::Ok))
        .expect("fill channel");
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
        ack_throttle_rx.clone(),
        ack_throttle_tx.clone(),
        Arc::clone(&ack_timeout_state),
        cancel_tx.clone(),
        cancel_rx.clone(),
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx.clone(),
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await?;
    assert!(result);
    drop(out_ack_rx);

    let mut source = TestFrameSource::new(vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(cache_put)),
    ]);
    let (closed_tx, _closed_rx) = mpsc::channel(1);
    drop(_closed_rx);
    let err = run_control_loop(
        &mut source,
        Arc::clone(&broker),
        connection_clone,
        BrokerConfig::default(),
        Arc::clone(&auth.auth),
        build_publish_context(Arc::clone(&broker)).await,
        HashMap::new(),
        String::new(),
        closed_tx,
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
    .await;
    assert!(err.is_err());
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
#[serial]
#[cfg(feature = "telemetry")]
async fn control_loop_cache_get_records_lookup_timing() -> Result<()> {
    timings::enable_collection(1);
    timings::set_enabled(true);
    let _ = timings::take_cache_samples();
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache(
            "t1",
            "default",
            "primary",
            felix_broker::CacheMetadata::default(),
        )
        .await?;
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

    let mut frames = Vec::new();
    frames.push(Ok(Some(frame_from_message(auth_message(&auth)))));
    for idx in 0..128u64 {
        frames.push(Ok(Some(frame_from_message(Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: format!("key-{idx}"),
            request_id: Some(idx),
        }))));
    }
    frames.push(Ok(None));
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
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        Default::default(),
        Default::default(),
    )
    .await?;
    assert!(result);
    let samples = timings::take_cache_samples().expect("cache samples");
    assert!(!samples.2.is_empty(), "lookup samples should be recorded");
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
#[serial]
async fn control_loop_cache_timings_recorded() -> Result<()> {
    timings::enable_collection(1);
    timings::set_enabled(true);
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache(
            "t1",
            "default",
            "primary",
            felix_broker::CacheMetadata::default(),
        )
        .await?;
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

    let mut frames = Vec::new();
    frames.push(Ok(Some(frame_from_message(auth_message(&auth)))));
    for idx in 0..20u64 {
        frames.push(Ok(Some(frame_from_message(Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: format!("key-{idx}"),
            value: Bytes::from_static(b"value"),
            request_id: Some(idx),
            ttl_ms: None,
        }))));
        frames.push(Ok(Some(frame_from_message(Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: format!("key-{idx}"),
            request_id: Some(idx + 100),
        }))));
    }
    frames.push(Ok(None));
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
async fn control_loop_cache_get_missing_no_request_id_returns_true() -> Result<()> {
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
        Ok(Some(frame_from_message(Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
            key: "key".to_string(),
            request_id: None,
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
async fn control_loop_cache_put_missing_no_request_id_returns_true() -> Result<()> {
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
        Ok(Some(frame_from_message(Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
            key: "key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: None,
            ttl_ms: None,
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

/// Retained and resume are refused together: the replay already reconstructs
/// the state a retained start would shortcut, and serving both would hand
/// over every value twice. The typed client cannot express the combination,
/// so the refusal is proven here at the control loop, where a raw frame can.
#[tokio::test]
async fn control_loop_refuses_a_retained_watch_with_an_offset() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cache = felix_storage::LogCache::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..felix_storage::log::LogConfig::default()
        },
    )?;
    let broker = Arc::new(Broker::new(Box::new(cache)));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache(
            "t1",
            "default",
            "primary",
            felix_broker::CacheMetadata::default(),
        )
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::CacheWatch {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: Some("k".to_string()),
            prefix: None,
            shard: None,
            from_offset: Some(3),
            retained: true,
            subscription_id: None,
        }))),
    ];
    let (_, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(
        messages.iter().any(|message| matches!(
            message,
            Outgoing::Message(Message::Error { message, .. })
                if message.contains("retained or from_offset")
        )),
        "the combination must be refused, not guessed at",
    );
    Ok(())
}

/// A watch with both key and prefix is refused, and so is one with neither.
#[tokio::test]
async fn control_loop_refuses_a_watch_with_an_ambiguous_filter() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cache = felix_storage::LogCache::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..felix_storage::log::LogConfig::default()
        },
    )?;
    let broker = Arc::new(Broker::new(Box::new(cache)));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache(
            "t1",
            "default",
            "primary",
            felix_broker::CacheMetadata::default(),
        )
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let watch = |key: Option<&str>, prefix: Option<&str>| Message::CacheWatch {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "primary".to_string(),
        key: key.map(str::to_string),
        prefix: prefix.map(str::to_string),
        shard: None,
        from_offset: None,
        retained: false,
        subscription_id: None,
    };
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(watch(Some("k"), Some("k"))))),
        Ok(Some(frame_from_message(watch(None, None)))),
    ];
    let (_, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    let refusals = messages
        .iter()
        .filter(|message| {
            matches!(
                message,
                Outgoing::Message(Message::Error { message, .. })
                    if message.contains("exactly one of key or prefix")
            )
        })
        .count();
    assert_eq!(refusals, 2, "both shapes must be refused");
    Ok(())
}

/// **A `group_poll` naming a consumer longer than the cap is refused** with an
/// error that says so, before it reaches the group. Each claim records its
/// holder, so an uncapped name would cost its length per record handed out.
#[tokio::test]
async fn control_loop_group_poll_with_an_oversized_consumer_is_refused() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", vec!["group.consume:stream:t1/*/*".to_string()]);
    let poll = |consumer: String, request_id| {
        Ok(Some(frame_from_message(Message::GroupPoll {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            group: "workers".to_string(),
            max_records: 10,
            wait_ms: 0,
            request_id,
            consumer: Some(consumer),
            reclaim: true,
        })))
    };
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        poll("w".repeat(129), 1),
        poll(String::new(), 2),
        Ok(None),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(result, "a refused poll leaves the stream open");
    let refusals: Vec<&str> = messages
        .iter()
        .filter_map(|message| match message {
            Outgoing::Message(Message::Error { message, .. }) => Some(message.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(refusals.len(), 2, "both polls are refused");
    for (index, message) in refusals.iter().enumerate() {
        assert!(
            message.contains("group consumer name must be 1 to 128 bytes"),
            "refusal {index} does not name the limit"
        );
    }
    Ok(())
}

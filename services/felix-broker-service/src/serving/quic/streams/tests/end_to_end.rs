//! Real QUIC connections driven through `handle_stream` and `handle_uni_stream`.

use super::*;

#[tokio::test]
async fn cache_put_get_round_trip() -> Result<()> {
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
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::default();
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    crate::serving::quic::write_message(&mut send, auth_message(&auth)).await?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert_eq!(response, Some(Message::Ok));
    crate::serving::quic::write_message(
        &mut send,
        Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "demo-key".to_string(),
            value: Bytes::from_static(b"cached"),
            request_id: None,
            ttl_ms: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert_eq!(response, Some(Message::Ok));

    let (mut send, mut recv) = connection.open_bi().await?;
    crate::serving::quic::write_message(&mut send, auth_message(&auth)).await?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert_eq!(response, Some(Message::Ok));
    crate::serving::quic::write_message(
        &mut send,
        Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "demo-key".to_string(),
            request_id: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert_eq!(
        response,
        Some(Message::CacheValue {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "demo-key".to_string(),
            value: Some(Bytes::from_static(b"cached")),
            request_id: None,
        })
    );

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn publish_rejects_unknown_stream() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::default();
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    crate::serving::quic::write_message(&mut send, auth_message(&auth)).await?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert_eq!(response, Some(Message::Ok));
    crate::serving::quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::PerMessage),
            key: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert!(matches!(
        response,
        Some(Message::PublishError { request_id: 1, .. })
    ));

    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "missing", Default::default())
        .await
        .expect("register");
    let (mut send, mut recv) =
        open_authenticated_bi(&connection, &auth, max_frame_bytes, &mut frame_scratch).await?;
    crate::serving::quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(2),
            ack: Some(felix_wire::AckMode::PerMessage),
            key: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert_eq!(response, Some(Message::PublishOk { request_id: 2 }));

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn publish_ack_on_commit_smoke() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig {
        ack_on_commit: true,
        ..BrokerConfig::default()
    };
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) =
        open_authenticated_bi(&connection, &auth, max_frame_bytes, &mut frame_scratch).await?;
    crate::serving::quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::PerMessage),
            key: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert_eq!(response, Some(Message::PublishOk { request_id: 1 }));

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn publish_sharding_preserves_stream_order() -> Result<()> {
    // Ordering assertion, not a latency assertion. The QUIC I/O runtime pool is
    // process-global, so under `cargo test` parallelism dozens of concurrent
    // brokers and clients share it; a tight per-event deadline then fails on
    // scheduling pressure rather than on a real ordering defect. Generous
    // enough that only a genuine stall trips it.
    const EVENT_TIMEOUT: Duration = Duration::from_secs(30);

    // Asserts lossless in-order delivery, so every queue on the path must
    // Block: the DropNew defaults may legally shed under a loaded test host,
    // which reads as a gap here.
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_subscriber_queue_policy(felix_broker::SubQueuePolicy::Block),
    );
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "alpha", Default::default())
        .await?;
    broker
        .register_stream("t1", "default", "beta", Default::default())
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig {
        subscriber_queue_policy: felix_broker::SubQueuePolicy::Block,
        subscriber_lane_queue_policy: felix_broker::SubQueuePolicy::Block,
        // Losslessness needs the *publish* side to block too. Checkpoint 4 (the
        // per-worker ingress queue, depth 64) defaults to `Drop`, so an unacked
        // burst that outruns the broker core is shed by design — measured here
        // as 77 of 400 publishes dropped, which reads as a delivery stall
        // because the events were never published at all. Blocking pins the
        // whole path, which is what this test's ordering assertion assumes.
        pub_ingress_wait: true,
        ..BrokerConfig::default()
    };
    let server_task = tokio::spawn(crate::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let mut client_config = build_client_config(cert, &auth)?;
    client_config.client_sub_queue_policy = felix_client::ClientSubQueuePolicy::Block;
    let client = felix_client::Client::connect_with_transport(
        addr,
        "localhost",
        client_config,
        TransportConfig::default(),
    )
    .await?;

    let mut sub_alpha = client.subscribe("t1", "default", "alpha").await?;
    let mut sub_beta = client.subscribe("t1", "default", "beta").await?;
    let publisher = client.publisher().await?;

    let total = 200u64;
    for i in 0..total {
        let payload = i.to_be_bytes().to_vec();
        publisher
            .publish(
                "t1",
                "default",
                "alpha",
                payload.clone(),
                felix_wire::AckMode::None,
            )
            .await?;
        publisher
            .publish("t1", "default", "beta", payload, felix_wire::AckMode::None)
            .await?;
    }

    let mut alpha_seen = Vec::with_capacity(total as usize);
    for _ in 0..total {
        let next = timeout(EVENT_TIMEOUT, sub_alpha.next_event()).await??;
        let event = next.expect("alpha event");
        let mut raw = [0u8; 8];
        raw.copy_from_slice(&event.payload[..8]);
        alpha_seen.push(u64::from_be_bytes(raw));
    }
    let expected: Vec<u64> = (0..total).collect();
    assert_eq!(alpha_seen, expected, "alpha sequence diverged");

    let mut beta_seen = Vec::with_capacity(total as usize);
    for _ in 0..total {
        let next = timeout(EVENT_TIMEOUT, sub_beta.next_event()).await??;
        let event = next.expect("beta event");
        let mut raw = [0u8; 8];
        raw.copy_from_slice(&event.payload[..8]);
        beta_seen.push(u64::from_be_bytes(raw));
    }
    assert_eq!(beta_seen, expected, "beta sequence diverged");

    publisher.finish().await?;
    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial]
async fn handle_stream_drain_timeout_branch() -> Result<()> {
    test_hooks::reset();
    test_hooks::set_force_drain_timeout(true);
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let auth_for_server = Arc::clone(&auth.auth);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (send, recv) = connection.accept_bi().await?;
        let config = BrokerConfig::default();
        let publish_ctx =
            build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await;
        handle_stream(
            Arc::new(Broker::new(EphemeralCache::new().into())),
            connection,
            config,
            auth_for_server,
            publish_ctx,
            send,
            recv,
        )
        .await?;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, _recv) = connection.open_bi().await?;
    crate::serving::quic::write_message(&mut send, auth_message(&auth)).await?;
    crate::serving::quic::write_message(&mut send, Message::Ok).await?;
    send.finish()?;

    server_task.await.context("server task")??;
    test_hooks::reset();
    Ok(())
}

#[tokio::test]
async fn control_stream_rejects_unexpected_message() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::default();
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) =
        open_authenticated_bi(&connection, &auth, max_frame_bytes, &mut frame_scratch).await?;
    crate::serving::quic::write_message(&mut send, Message::Ok).await?;
    send.finish()?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert!(matches!(response, Some(Message::Error { .. }) | None));

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn cache_put_unknown_cache_closes_stream() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::default();
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) =
        open_authenticated_bi(&connection, &auth, max_frame_bytes, &mut frame_scratch).await?;
    crate::serving::quic::write_message(
        &mut send,
        Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
            key: "demo-key".to_string(),
            value: Bytes::from_static(b"cached"),
            request_id: None,
            ttl_ms: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert!(matches!(response, Some(Message::Error { .. })));
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert!(response.is_none());

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn cache_get_unknown_cache_closes_stream() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::default();
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) =
        open_authenticated_bi(&connection, &auth, max_frame_bytes, &mut frame_scratch).await?;
    crate::serving::quic::write_message(
        &mut send,
        Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
            key: "demo-key".to_string(),
            request_id: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert!(matches!(response, Some(Message::Error { .. })));
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, &mut frame_scratch)
            .await?;
    assert!(response.is_none());

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn uni_stream_rejects_non_publish() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::default();
    let server_task = tokio::spawn(crate::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let mut send = connection.open_uni().await?;
    crate::serving::quic::write_message(&mut send, auth_message(&auth)).await?;
    let frame = Message::CacheGet {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "primary".to_string(),
        key: "demo-key".to_string(),
        request_id: None,
    }
    .encode()?;
    let bytes = frame.encode();
    send.write_all(&bytes).await?;
    send.finish()?;

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn handle_uni_stream_smoke() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let auth_for_server = Arc::clone(&auth.auth);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let recv = connection.accept_uni().await?;
        handle_uni_stream(
            broker,
            BrokerConfig::default(),
            auth_for_server,
            publish_ctx,
            recv,
            None,
        )
        .await?;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let mut send = connection.open_uni().await?;
    crate::serving::quic::write_message(&mut send, auth_message(&auth)).await?;
    let frame = Message::Publish {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "updates".to_string(),
        payload: b"payload".to_vec(),
        request_id: None,
        ack: Some(felix_wire::AckMode::None),
        key: None,
    }
    .encode()?;
    send.write_all(&frame.encode()).await?;
    send.finish()?;

    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
#[serial]
async fn handle_stream_drain_timeout_sleep_branch() -> Result<()> {
    test_hooks::reset();
    let auth = auth_fixture("t1", default_perms());
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let auth_for_server = Arc::clone(&auth.auth);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (send, recv) = connection.accept_bi().await?;
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream("t1", "default", "updates", Default::default())
            .await?;
        let (scheduler, _tx, mut rx) = test_channel(8);
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                if let Some(response) = job.response {
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        drop(response);
                    });
                }
            }
        });
        let publish_ctx = PublishContext {
            ingress: None,
            client_endpoints: None,
            peers: None,
            lease: None,
            lease_headroom: std::time::Duration::ZERO,
            marks: None,
            quorum_timeout: Duration::from_secs(1),
            scheduler,
            wait_timeout: Duration::from_millis(50),
            admission: Arc::new(PublishAdmission::unlimited()),
            conn_admission: Arc::new(PublishAdmission::unlimited()),
            subscriptions: Arc::new(SubscriptionLimiter::new()),
            lane_manager: WriterLaneManager::new(&BrokerConfig::default()),
            ingress_wait: false,
            preauth: std::sync::Arc::new(crate::serving::quic::preauth::PreAuthGate::new(
                &crate::config::BrokerConfig::default(),
            )),
            tenant_rates: std::sync::Arc::new(crate::serving::limits::TenantRates::unlimited()),
            publish_window: None,
        };
        let config = BrokerConfig {
            ack_on_commit: true,
            ack_wait_timeout_ms: 1000,
            control_stream_drain_timeout_ms: 1,
            ..BrokerConfig::default()
        };
        handle_stream(
            broker,
            connection,
            config,
            auth_for_server,
            publish_ctx,
            send,
            recv,
        )
        .await?;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, _recv) = connection.open_bi().await?;
    crate::serving::quic::write_message(&mut send, auth_message(&auth)).await?;
    crate::serving::quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::PerMessage),
            key: None,
        },
    )
    .await?;
    send.finish()?;

    server_task.await.context("server task")??;
    Ok(())
}

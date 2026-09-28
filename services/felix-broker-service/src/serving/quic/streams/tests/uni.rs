//! The uni-stream publish loop.

use super::*;

#[tokio::test]
async fn uni_loop_publish_and_errors() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let binary =
        binary_publish_batch_frame("t1", "default", "updates", &[Bytes::from_static(b"one")]);
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(binary)),
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
        Ok(None),
    ];
    let mut source = TestFrameSource::new(frames);
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        UniLoopArgs {
            config: BrokerConfig::default(),
            auth: Arc::clone(&auth.auth),
            publish_ctx,
            stream_cache: HashMap::new(),
            stream_cache_key: String::new(),
            peer_certs: None,
        },
        &mut scratch,
    )
    .await?;

    let mut source = TestFrameSource::new(vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::Ok))),
    ]);
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        UniLoopArgs {
            config: BrokerConfig::default(),
            auth: Arc::clone(&auth.auth),
            publish_ctx: build_publish_context(Arc::clone(&broker)).await,
            stream_cache: HashMap::new(),
            stream_cache_key: String::new(),
            peer_certs: None,
        },
        &mut scratch,
    )
    .await?;

    let mut source = TestFrameSource::new(vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(invalid_json_frame())),
    ]);
    assert!(
        run_uni_loop(
            &mut source,
            Arc::clone(&broker),
            UniLoopArgs {
                config: BrokerConfig::default(),
                auth: Arc::clone(&auth.auth),
                publish_ctx: build_publish_context(Arc::clone(&broker)).await,
                stream_cache: HashMap::new(),
                stream_cache_key: String::new(),
                peer_certs: None,
            },
            &mut scratch,
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn uni_loop_breaks_on_enqueue_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let (scheduler, _tx, rx) = test_channel(1);
    drop(rx);
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
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let binary =
        binary_publish_batch_frame("t1", "default", "updates", &[Bytes::from_static(b"one")]);
    let mut source = TestFrameSource::new(vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(binary)),
    ]);
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        UniLoopArgs {
            config: BrokerConfig::default(),
            auth: Arc::clone(&auth.auth),
            publish_ctx: publish_ctx.clone(),
            stream_cache: HashMap::new(),
            stream_cache_key: String::new(),
            peer_certs: None,
        },
        &mut scratch,
    )
    .await?;

    let mut source = TestFrameSource::new(vec![
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
    ]);
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        UniLoopArgs {
            config: BrokerConfig::default(),
            auth: Arc::clone(&auth.auth),
            publish_ctx: publish_ctx.clone(),
            stream_cache: HashMap::new(),
            stream_cache_key: String::new(),
            peer_certs: None,
        },
        &mut scratch,
    )
    .await?;

    let mut source = TestFrameSource::new(vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::PublishBatch {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payloads: vec![b"a".to_vec()],
            request_id: Some(2),
            ack: Some(felix_wire::AckMode::None),
            key: None,
        }))),
    ]);
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        UniLoopArgs {
            config: BrokerConfig::default(),
            auth: Arc::clone(&auth.auth),
            publish_ctx,
            stream_cache: HashMap::new(),
            stream_cache_key: String::new(),
            peer_certs: None,
        },
        &mut scratch,
    )
    .await?;
    Ok(())
}

/// Flags pick the body's layout. A uni stream cannot answer, so a frame whose
/// layout it cannot read ends the stream instead of being misparsed: an
/// unknown bit, and an acked batch, whose request id the plain decoder would
/// take for the tenant length.
#[tokio::test]
async fn uni_loop_refuses_layouts_it_cannot_read() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let mut sub = broker.subscribe("t1", "default", "updates", 0).await?;
    let auth = auth_fixture("t1", default_perms());
    let mut scratch = BytesMut::with_capacity(64 * 1024);

    let plain =
        binary_publish_batch_frame("t1", "default", "updates", &[Bytes::from_static(b"one")]);
    let undefined = !felix_wire::KNOWN_FLAGS & (!felix_wire::KNOWN_FLAGS).wrapping_neg();
    let unknown = Frame {
        header: felix_wire::FrameHeader::new(plain.header.flags | undefined, plain.header.length),
        payload: plain.payload.clone(),
    };
    let acked = Frame::decode(felix_wire::binary::encode_acked_publish_batch_bytes(
        7,
        felix_wire::AckMode::PerBatch,
        "t1",
        "default",
        "updates",
        &[b"two".to_vec()],
    )?)?;

    for frame in [unknown, acked] {
        let flags = frame.header.flags;
        let mut source = TestFrameSource::new(vec![
            Ok(Some(frame_from_message(auth_message(&auth)))),
            Ok(Some(frame)),
        ]);
        let result = run_uni_loop(
            &mut source,
            Arc::clone(&broker),
            UniLoopArgs {
                config: BrokerConfig::default(),
                auth: Arc::clone(&auth.auth),
                publish_ctx: build_publish_context(Arc::clone(&broker)).await,
                stream_cache: HashMap::new(),
                stream_cache_key: String::new(),
                peer_certs: None,
            },
            &mut scratch,
        )
        .await;
        assert!(
            result.is_err(),
            "flags {flags:#06x} were read on a uni stream"
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), sub.recv())
            .await
            .is_err(),
        "a frame the uni stream cannot read was published"
    );
    Ok(())
}

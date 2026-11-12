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
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let binary =
        binary_publish_batch_frame("t1", "default", "updates", &[Bytes::from_static(b"one")]);
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(binary)),
        Ok(Some(frame_from_message(Message::Publish {
            shard: None,
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::None),
            key: None,
        }))),
        Ok(Some(frame_from_message(Message::PublishBatch {
            shard: None,
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
            stream_cache: HashMap::default(),
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
            stream_cache: HashMap::default(),
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
                stream_cache: HashMap::default(),
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
        shard_status: None,
        ingress: None,
        client_endpoints: None,
        peers: None,
        lease: None,
        lease_headroom: std::time::Duration::ZERO,
        engine: Default::default(),
        quorum_timeout: Duration::from_secs(1),
        scheduler,
        wait_timeout: Duration::from_millis(50),
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        identity: IdentityLimits::unlimited(),
        lane_manager: WriterLaneManager::new(&BrokerConfig::default()),
        ingress_wait: false,
        preauth: std::sync::Arc::new(crate::serving::quic::preauth::PreAuthGate::new(
            &crate::config::BrokerConfig::default(),
        )),
        tenant_rates: std::sync::Arc::new(crate::serving::limits::TenantRates::unlimited()),
        publish_window: 0,
        ack_durable_on_commit: false,
        parked: None,
    };
    let mut scratch = crate::serving::quic::FrameScratch::new();
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
            stream_cache: HashMap::default(),
            stream_cache_key: String::new(),
            peer_certs: None,
        },
        &mut scratch,
    )
    .await?;

    let mut source = TestFrameSource::new(vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::Publish {
            shard: None,
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
            stream_cache: HashMap::default(),
            stream_cache_key: String::new(),
            peer_certs: None,
        },
        &mut scratch,
    )
    .await?;

    let mut source = TestFrameSource::new(vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::PublishBatch {
            shard: None,
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
            stream_cache: HashMap::default(),
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
    let mut scratch = crate::serving::quic::FrameScratch::new();

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
                stream_cache: HashMap::default(),
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

/// An unacked binary publish with a headers block on its one record, behind
/// an extension word.
fn extended_publish_frame(stream: &str, block: &Bytes, payload: &[u8]) -> Frame {
    use bytes::BufMut;
    let mut body = bytes::BytesMut::new();
    body.put_u32(felix_wire::EXT_RECORD_HEADERS);
    for name in ["t1", "default", stream] {
        body.put_u16(name.len() as u16);
        body.extend_from_slice(name.as_bytes());
    }
    body.put_u32(1);
    body.put_u32(block.len() as u32);
    body.extend_from_slice(block);
    body.put_u32(payload.len() as u32);
    body.extend_from_slice(payload);
    Frame::new(
        felix_wire::FLAG_BINARY_PUBLISH_BATCH | felix_wire::FLAG_EXTENDED,
        body.freeze(),
    )
    .expect("frame")
}

fn headers_block() -> Bytes {
    felix_wire::RecordHeaders {
        key: None,
        headers: vec![("trace".to_string(), Bytes::from_static(b"abc"))],
    }
    .encode()
    .expect("block")
}

fn uni_auth(auth: &AuthFixture, offer_headers: bool) -> Frame {
    let (client_flags, client_flags_ext) = if offer_headers {
        felix_wire::offer_flags(felix_wire::KNOWN_FLAGS, felix_wire::EXT_RECORD_HEADERS)
    } else {
        (felix_wire::KNOWN_FLAGS, None)
    };
    frame_from_message(Message::Auth {
        tenant_id: auth.tenant_id.clone(),
        token: auth.token.clone(),
        client_flags: Some(client_flags),
        client_features: None,
        client_features_hi: None,
        client_flags_ext,
    })
}

async fn in_memory_broker() -> Result<Arc<Broker>> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    Ok(broker)
}

/// The production worker, which carries headers; the harness executor in
/// `tests.rs` publishes payloads only.
fn real_publish_context(broker: &Arc<Broker>) -> PublishContext {
    crate::serving::quic::handlers::publish::build_publish_context(
        Arc::clone(broker),
        &BrokerConfig::default(),
        crate::serving::quic::ClusterContext::default(),
    )
}

fn auth_for(_broker: &Broker) -> AuthFixture {
    auth_fixture("t1", default_perms())
}

/// A uni stream that did not negotiate the extension has it refused, as the
/// stream always did, and nothing is published.
#[tokio::test]
async fn uni_loop_refuses_an_extended_frame_it_did_not_negotiate() -> Result<()> {
    for (accepts, offers) in [(true, false), (false, true)] {
        let broker = in_memory_broker().await?;
        if accepts {
            broker.record_headers_when(|| true);
        }
        let resumed = broker
            .subscribe_from(
                "t1",
                "default",
                "updates",
                0,
                felix_wire::StartPosition::Latest,
            )
            .await?;
        let (mut receiver, _guard) = resumed.subscription.into_parts();
        let auth = auth_for(&broker);
        let mut source = TestFrameSource::new(vec![
            Ok(Some(uni_auth(&auth, offers))),
            Ok(Some(extended_publish_frame(
                "updates",
                &headers_block(),
                b"x",
            ))),
        ]);
        let mut scratch = crate::serving::quic::FrameScratch::new();
        let result = run_uni_loop(
            &mut source,
            Arc::clone(&broker),
            UniLoopArgs {
                config: BrokerConfig::default(),
                auth: Arc::clone(&auth.auth),
                publish_ctx: real_publish_context(&broker),
                stream_cache: HashMap::default(),
                stream_cache_key: String::new(),
                peer_certs: None,
            },
            &mut scratch,
        )
        .await;
        let err = result.expect_err("the extended frame ends the stream");
        assert!(err.to_string().contains("unsupported frame flags"), "{err}");
        assert!(
            timeout(Duration::from_millis(200), receiver.recv())
                .await
                .is_err(),
            "nothing was published (broker accepts {accepts}, client offers {offers})"
        );
    }
    Ok(())
}

/// A uni stream whose `Auth` offered record headers to a broker that accepts
/// them publishes them, and a reserved name drops the batch.
#[tokio::test]
async fn uni_loop_publishes_negotiated_headers_and_drops_reserved_names() -> Result<()> {
    let broker = in_memory_broker().await?;
    broker.record_headers_when(|| true);
    let resumed = broker
        .subscribe_from(
            "t1",
            "default",
            "updates",
            0,
            felix_wire::StartPosition::Latest,
        )
        .await?;
    let (mut receiver, _guard) = resumed.subscription.into_parts();
    let auth = auth_for(&broker);
    let reserved = felix_wire::RecordHeaders {
        key: None,
        headers: vec![("felix.mqtt.topic".to_string(), Bytes::new())],
    }
    .encode()?;
    let mut source = TestFrameSource::new(vec![
        Ok(Some(uni_auth(&auth, true))),
        Ok(Some(extended_publish_frame(
            "updates", &reserved, b"dropped",
        ))),
        Ok(Some(extended_publish_frame(
            "updates",
            &headers_block(),
            b"kept",
        ))),
        Ok(None),
    ]);
    let mut scratch = crate::serving::quic::FrameScratch::new();
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        UniLoopArgs {
            config: BrokerConfig::default(),
            auth: Arc::clone(&auth.auth),
            publish_ctx: real_publish_context(&broker),
            stream_cache: HashMap::default(),
            stream_cache_key: String::new(),
            peer_certs: None,
        },
        &mut scratch,
    )
    .await?;
    let delivered = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .context("not delivered")?
        .context("closed")?;
    assert_eq!(delivered.payloads(), &[Bytes::from_static(b"kept")]);
    assert_eq!(
        delivered.record_headers(),
        Some(&[Some(headers_block())][..])
    );
    Ok(())
}

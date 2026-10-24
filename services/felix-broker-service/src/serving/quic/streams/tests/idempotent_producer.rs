//! The control stream hands out producer ids and takes idempotent publishes.

use super::*;

/// A producer id comes from the broker, to an authenticated stream only.
#[tokio::test]
async fn producer_init_answers_with_an_id_once_authenticated() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(Message::ProducerInit {
            request_id: 4,
        }))),
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::ProducerInit {
            request_id: 5,
        }))),
    ];
    let (_, messages) = run_control_loop_with_frames(
        Arc::clone(&broker),
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(
        messages.iter().any(|message| matches!(
            message,
            Outgoing::Message(Message::Error { message, .. }) if message.contains("not authenticated")
        )),
        "an unauthenticated producer_init was answered: {messages:?}"
    );
    assert!(
        !messages.iter().any(|message| matches!(
            message,
            Outgoing::Message(Message::ProducerInitOk { request_id: 4, .. })
        )),
        "an unauthenticated stream was given a producer id"
    );
    Ok(())
}

/// The same batch twice lands once, and a gap lands nothing.
#[tokio::test]
async fn an_idempotent_publish_is_appended_once() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", Default::default())
        .await?;
    let mut sub = broker.subscribe("t1", "default", "orders", 0).await?;
    let auth = auth_fixture("t1", default_perms());
    let batch = |request_id: u64, sequence: u64, payload: &[u8]| Message::PublishIdempotent {
        shard: None,
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
        payloads: vec![payload.to_vec()],
        key: None,
        request_id,
        producer_id: 42,
        sequence,
    };
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(batch(1, 0, b"a")))),
        Ok(Some(frame_from_message(batch(2, 0, b"a")))),
        Ok(Some(frame_from_message(batch(3, 5, b"gap")))),
        Ok(Some(frame_from_message(batch(4, 1, b"b")))),
    ];
    run_control_loop_with_frames(
        Arc::clone(&broker),
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;

    // The publishes ran on the harness's worker; what landed is what the
    // subscriber saw.
    assert_eq!(sub.recv().await.expect("a"), Bytes::from_static(b"a"));
    assert_eq!(
        sub.recv().await.expect("b"),
        Bytes::from_static(b"b"),
        "the re-send or the gap was appended"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), sub.recv())
            .await
            .is_err(),
        "more than two records landed"
    );
    Ok(())
}

/// The same as `an_idempotent_publish_is_appended_once`, as binary frames.
#[tokio::test]
async fn a_binary_idempotent_publish_is_appended_once() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", Default::default())
        .await?;
    let mut sub = broker.subscribe("t1", "default", "orders", 0).await?;
    let auth = auth_fixture("t1", default_perms());
    let batch = |request_id: u64, sequence: u64, payload: &[u8]| {
        let bytes = felix_wire::binary::encode_idempotent_publish_batch_bytes(
            request_id,
            felix_wire::binary::ProducerSequence {
                producer_id: 42,
                sequence,
            },
            None,
            "t1",
            "default",
            "orders",
            &[payload.to_vec()],
        )
        .expect("encode");
        Frame::decode(bytes).expect("frame")
    };
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(batch(1, 0, b"a"))),
        Ok(Some(batch(2, 0, b"a"))),
        Ok(Some(batch(3, 5, b"gap"))),
        Ok(Some(batch(4, 1, b"b"))),
    ];
    run_control_loop_with_frames(
        Arc::clone(&broker),
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;

    assert_eq!(sub.recv().await.expect("a"), Bytes::from_static(b"a"));
    assert_eq!(
        sub.recv().await.expect("b"),
        Bytes::from_static(b"b"),
        "the re-send or the gap was appended"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), sub.recv())
            .await
            .is_err(),
        "more than two records landed"
    );
    Ok(())
}

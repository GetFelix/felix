//! The control loop refuses what the stream's auth state does not allow.

use super::*;

#[tokio::test]
async fn control_loop_rejects_second_auth() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(auth_message(&auth)))),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(matches!(
        messages.first(),
        Some(Outgoing::Message(Message::AuthOk { .. }))
    ));
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("auth already established")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_rejects_auth_failed() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![Ok(Some(frame_from_message(felix_wire::baseline_auth(
        "t1", "invalid",
    ))))];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("auth failed")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_rejects_binary_publish_batch_without_auth() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let binary =
        binary_publish_batch_frame("t1", "default", "updates", &[Bytes::from_static(b"one")]);
    let frames = vec![Ok(Some(binary))];
    let (result, messages) = run_control_loop_with_frames(
        Arc::clone(&broker),
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("auth required")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_rejects_publish_batch_forbidden() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let auth = auth_fixture("t1", vec!["stream.subscribe:stream:t1/*/*".to_string()]);
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::PublishBatch {
            shard: None,
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payloads: vec![b"a".to_vec()],
            request_id: Some(9),
            ack: Some(felix_wire::AckMode::None),
            key: None,
        }))),
    ];
    let (result, messages) = run_control_loop_with_frames(
        Arc::clone(&broker),
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::PublishError { request_id: 9, message, .. }) if message.contains("forbidden")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_publish_without_auth_sends_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![Ok(Some(frame_from_message(Message::Publish {
        shard: None,
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "updates".to_string(),
        payload: b"payload".to_vec(),
        request_id: Some(10),
        ack: Some(felix_wire::AckMode::PerMessage),
        key: None,
    })))];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("auth required")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_publish_forbidden_without_request_id_sends_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", vec![]);
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::Publish {
            shard: None,
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payload: b"payload".to_vec(),
            request_id: None,
            ack: Some(felix_wire::AckMode::PerMessage),
            key: None,
        }))),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("forbidden")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_publish_tenant_mismatch_sends_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::Publish {
            shard: None,
            tenant_id: "t2".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(12),
            ack: Some(felix_wire::AckMode::PerMessage),
            key: None,
        }))),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("tenant mismatch")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_subscribe_forbidden_sends_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", vec![]);
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::Subscribe {
            start: None,
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            subscription_id: None,
            shard: None,
            queue_capacity: None,
            end: None,
        }))),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("forbidden")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_subscribe_tenant_mismatch_sends_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::Subscribe {
            start: None,
            tenant_id: "t2".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            subscription_id: None,
            shard: None,
            queue_capacity: None,
            end: None,
        }))),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("tenant mismatch")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_cache_put_forbidden_sends_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", vec![]);
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: Some(33),
            ttl_ms: None,
        }))),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("forbidden")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_rejects_subscribe_without_auth() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![Ok(Some(frame_from_message(Message::Subscribe {
        start: None,
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "updates".to_string(),
        subscription_id: Some(1),
        shard: None,
        queue_capacity: None,
        end: None,
    })))];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("auth required")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_cache_get_rejects_missing_auth() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![Ok(Some(frame_from_message(Message::CacheGet {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "primary".to_string(),
        key: "key".to_string(),
        request_id: Some(1),
    })))];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("auth required")
    )));
    Ok(())
}

#[tokio::test]
async fn control_loop_cache_put_rejects_tenant_mismatch() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::CachePut {
            tenant_id: "t2".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: Some(2),
            ttl_ms: None,
        }))),
    ];
    let (result, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    assert!(!result);
    assert!(messages.iter().any(|message| matches!(
        message,
        Outgoing::Message(Message::Error { message, .. }) if message.contains("tenant mismatch")
    )));
    Ok(())
}

/// The `AuthOk` the broker answers `auth_message` with.
async fn auth_ok_for(auth_message: Message) -> Result<Message> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![Ok(Some(frame_from_message(auth_message)))];
    let (_, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    messages
        .into_iter()
        .find_map(|message| match message {
            Outgoing::Message(message @ Message::AuthOk { .. }) => Some(message),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("no auth_ok"))
}

/// A client learns the extended features, inspection among them.
#[tokio::test]
async fn a_client_hears_the_extended_word() -> Result<()> {
    let auth = auth_fixture("t1", default_perms());
    let Message::AuthOk {
        server_features,
        server_features_hi,
        ..
    } = auth_ok_for(felix_wire::baseline_auth(&auth.tenant_id, &auth.token)).await?
    else {
        unreachable!()
    };
    let hi = felix_wire::peer_features_hi(server_features.unwrap_or(0), server_features_hi);
    assert!(felix_wire::supports_feature(
        hi,
        felix_wire::FEATURE_INSPECT
    ));
    assert!(felix_wire::supports_feature(
        hi,
        felix_wire::FEATURE_STREAM_DELETED
    ));
    Ok(())
}

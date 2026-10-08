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
        Some(Outgoing::Message(Message::Ok))
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
    let frames = vec![Ok(Some(frame_from_message(Message::Auth {
        tenant_id: "t1".to_string(),
        token: "invalid".to_string(),
        // Legacy handshake: no capabilities offered, so the broker
        // answers with a plain `Ok`.
        client_flags: None,
        client_features: None,
        client_features_hi: None,
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

/// The `AuthOk` a client gets for `client_features` and `client_features_hi`.
async fn auth_ok_for(client_features: u32, client_features_hi: Option<u32>) -> Result<Message> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![Ok(Some(frame_from_message(Message::Auth {
        tenant_id: auth.tenant_id.clone(),
        token: auth.token.clone(),
        client_flags: Some(felix_wire::KNOWN_FLAGS),
        client_features: Some(client_features),
        client_features_hi,
    })))];
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

/// A client offering an extended word gets the same `auth_ok` as one that
/// does not, while this broker serves no extended feature: no marker, no
/// `server_features_hi`.
#[tokio::test]
async fn an_extended_offer_to_a_broker_serving_none_changes_nothing() -> Result<()> {
    let plain = auth_ok_for(felix_wire::KNOWN_FEATURES, None).await?;
    let extended = auth_ok_for(
        felix_wire::KNOWN_FEATURES | felix_wire::FEATURE_EXTENDED,
        Some(1),
    )
    .await?;
    assert_eq!(plain.clone().encode()?, extended.encode()?);
    let Message::AuthOk {
        server_features,
        server_features_hi,
        ..
    } = plain
    else {
        unreachable!()
    };
    assert_eq!(server_features_hi, None);
    assert!(!felix_wire::supports_feature(
        server_features.unwrap_or(0),
        felix_wire::FEATURE_EXTENDED
    ));
    Ok(())
}

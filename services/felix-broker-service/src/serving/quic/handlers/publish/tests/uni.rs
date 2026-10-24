//! Publishes on uni streams, which have no response path.

use super::*;

#[tokio::test]
async fn handle_binary_publish_batch_uni_requires_auth() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let result =
        handle_binary_publish_batch_uni(&broker, &mut cache, &mut key, &publish_ctx, &frame, None)
            .await
            .expect("auth required");
    assert!(!result);
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_tenant_mismatch() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("other", &["stream.publish:stream:tenant/ns/stream"]);
    let result = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect("tenant mismatch");
    assert!(!result);
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_forbidden() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.subscribe:stream:tenant/ns/stream"]);
    let result = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect("forbidden");
    assert!(!result);
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_missing_stream_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect("missing stream");
    assert!(result);
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_enqueue_error_returns_false() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, rx, _tx) = make_publish_context(1);
    drop(rx);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect("enqueue error");
    assert!(!result);
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_decode_error_returns_err() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = Frame::new(0, Bytes::from_static(b"bad")).expect("frame");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let err = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect_err("decode error");
    assert!(err.to_string().contains("decode binary publish batch"));
}

#[tokio::test]
async fn handle_binary_publish_batch_uni_drop_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let frame = make_binary_publish_frame("tenant", "ns", "stream");
    let auth_ctx = make_auth_ctx("tenant", &["stream.publish:stream:tenant/ns/*"]);
    let result = handle_binary_publish_batch_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        &frame,
        Some(&auth_ctx),
    )
    .await
    .expect("drop");
    assert!(result);
}

#[tokio::test]
async fn handle_publish_message_uni_missing_stream_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        None,
        String::new(),
    )
    .await
    .expect("missing stream");
    assert!(result);
}

#[tokio::test]
async fn handle_publish_message_uni_enqueue_error_returns_false() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, rx, _tx) = make_publish_context(1);
    drop(rx);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        None,
        String::new(),
    )
    .await
    .expect("enqueue error");
    assert!(!result);
}

#[tokio::test]
async fn handle_publish_message_uni_drop_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        None,
        String::new(),
    )
    .await
    .expect("drop");
    assert!(result);
}

#[tokio::test]
async fn handle_publish_batch_message_uni_missing_stream_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_batch_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"a".to_vec(), b"b".to_vec()],
        None,
        String::new(),
    )
    .await
    .expect("missing stream");
    assert!(result);
}

#[tokio::test]
async fn handle_publish_batch_message_uni_enqueue_error_returns_false() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, rx, _tx) = make_publish_context(1);
    drop(rx);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_batch_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"a".to_vec(), b"b".to_vec()],
        None,
        String::new(),
    )
    .await
    .expect("enqueue error");
    assert!(!result);
}

#[tokio::test]
async fn handle_publish_batch_message_uni_drop_returns_true() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("tenant").await.expect("tenant");
    broker
        .register_namespace("tenant", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "tenant",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    let (publish_ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).expect("fill queue");
    let mut cache = HashMap::new();
    let mut key = String::new();
    let result = handle_publish_batch_message_uni(
        &broker,
        &mut cache,
        &mut key,
        &publish_ctx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"a".to_vec(), b"b".to_vec()],
        None,
        String::new(),
    )
    .await
    .expect("drop");
    assert!(result);
}

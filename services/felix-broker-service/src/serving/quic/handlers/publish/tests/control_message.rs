//! Single JSON publishes on the control stream.

use super::*;

#[tokio::test]
async fn handle_publish_message_throttled_sends_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        true,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1, 2, 3],
        None,
        Some(7),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("throttled path");
    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
            ..
        }) => {
            assert_eq!(request_id, 7);
            assert!(message.contains("overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_throttled_without_request_id_sends_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        true,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1, 2, 3],
        None,
        None,
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("throttled path");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::Error { message, .. }) => {
            assert!(message.contains("overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_missing_request_id_returns_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1, 2, 3],
        None,
        None,
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("missing request id");
    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::Error { message, .. }) => {
            assert!(message.contains("missing request_id"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_drop_when_queue_full_and_ack_none() {
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
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        None,
        None,
        Some(felix_wire::AckMode::None),
        false,
        String::new(),
    )
    .await
    .expect("publish");

    let recv = tokio::time::timeout(Duration::from_millis(20), out_rx.recv()).await;
    assert!(recv.is_err(), "no ack expected");
}

#[tokio::test]
async fn handle_publish_message_enqueue_error_reports_publish_error() {
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
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        None,
        Some(44),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
            ..
        }) => {
            assert_eq!(request_id, 44);
            assert!(message.contains("publish queue full"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_stream_not_found_sends_error() {
    let broker = Broker::new(EphemeralCache::new().into());
    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "missing".to_string(),
        vec![1, 2, 3],
        None,
        Some(42),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("stream not found");
    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
            ..
        }) => {
            assert_eq!(request_id, 42);
            assert!(message.contains("stream not found"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_ack_sends_ok() {
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
    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1, 2],
        None,
        Some(5),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishOk { request_id, .. }) => {
            assert_eq!(request_id, 5);
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_ack_waiters_exhausted() {
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
    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(0));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        None,
        Some(7),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
            ..
        }) => {
            assert_eq!(request_id, 7);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_ack_waiter_queue_full() {
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
    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);

    let permit = ack_waiters.clone().acquire_owned().await.expect("permit");
    ack_waiter_tx
        .try_send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 99,
            payload_len: 1,
            start: crate::serving::quic::telemetry::t_instant_now(),
            response_rx: oneshot::channel().1,
            permit,
        })
        .expect("fill queue");
    drop(ack_waiter_rx);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        None,
        Some(8),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
            ..
        }) => {
            assert_eq!(request_id, 8);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_ack_waiter_queue_full_with_permit() {
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
    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(2));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);

    let permit = ack_waiters.clone().acquire_owned().await.expect("permit");
    ack_waiter_tx
        .try_send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 101,
            payload_len: 1,
            start: crate::serving::quic::telemetry::t_instant_now(),
            response_rx: oneshot::channel().1,
            permit,
        })
        .expect("fill queue");

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        None,
        Some(9),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
            ..
        }) => {
            assert_eq!(request_id, 9);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_message_ack_waiter_queue_closed() {
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
    let (publish_ctx, _rx, _tx) = make_publish_context(8);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);
    drop(ack_waiter_rx);

    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1],
        None,
        Some(10),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("publish");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
            ..
        }) => {
            assert_eq!(request_id, 10);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

/// The single-publish path answers an unservable shard the same way the batch
/// path does: retryable, with its reason, and not as a missing stream.
#[tokio::test]
async fn handle_publish_message_unservable_shard_is_shard_unavailable() {
    let broker = broker_with_stream().await;
    let (mut publish_ctx, _rx, _tx) = make_publish_context(1);
    publish_ctx.ingress = Some(Arc::new(ingress_for("broker-a", false)));
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    handle_publish_message(
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        Duration::from_millis(10),
        "t1".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![1, 2, 3],
        None,
        Some(42),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("refused publish");
    let Outgoing::Message(Message::PublishError {
        message,
        code,
        retry,
        detail,
        ..
    }) = out_rx.recv().await.expect("outgoing")
    else {
        panic!("expected publish_error");
    };
    assert_eq!(code, Some(felix_wire::ErrorCode::ShardUnavailable));
    assert_eq!(retry, Some(felix_wire::RetryClass::Retry));
    assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("not_ready"));
    assert!(!message.contains("not found"), "{message}");
}

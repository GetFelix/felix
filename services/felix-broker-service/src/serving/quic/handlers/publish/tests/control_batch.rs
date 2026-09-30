//! JSON publish batches on the control stream.

use super::*;

#[tokio::test]
async fn handle_publish_batch_missing_request_id_returns_error() {
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

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        None,
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
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
async fn handle_publish_batch_stream_not_found_sends_error() {
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

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(9),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
    )
    .await
    .expect("stream missing path");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
            ..
        }) => {
            assert_eq!(request_id, 9);
            assert!(message.contains("stream not found"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_enqueue_full_reports_error() {
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

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(11),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
    )
    .await
    .expect("enqueue full path");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
            ..
        }) => {
            assert_eq!(request_id, 11);
            assert!(message.contains("publish queue full"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_enqueue_ok_sends_ack() {
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

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(13),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
    )
    .await
    .expect("publish batch");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishOk { request_id, .. }) => {
            assert_eq!(request_id, 13);
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_drop_when_queue_full_and_ack_none() {
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

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        None,
        Some(felix_wire::AckMode::None),
        false,
        String::new(),
        None,
    )
    .await
    .expect("publish");

    let recv = tokio::time::timeout(Duration::from_millis(20), out_rx.recv()).await;
    assert!(recv.is_err(), "no ack expected");
}

#[tokio::test]
async fn handle_publish_batch_message_enqueue_error_reports_publish_error() {
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

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(45),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
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
            assert_eq!(request_id, 45);
            assert!(message.contains("publish queue full"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_ack_on_commit_sends_waiter_message() {
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

    let (publish_ctx, _rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_tx, _out_rx) = mpsc::channel(1);
    let out_depth = Arc::new(AtomicUsize::new(0));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_waiters = Arc::new(Semaphore::new(1));
    let (ack_waiter_tx, mut ack_waiter_rx) = mpsc::channel(1);

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(46),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
    )
    .await
    .expect("publish");

    let msg = ack_waiter_rx.recv().await.expect("waiter msg");
    match msg {
        AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            request_id,
            ..
        } => {
            assert_eq!(request_id, 46);
        }
        _ => panic!("unexpected waiter message"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_throttled_with_request_id_sends_error() {
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

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        true,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(21),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
    )
    .await
    .expect("throttled path");

    let msg = out_rx.recv().await.expect("outgoing");
    match msg {
        Outgoing::Message(Message::PublishError {
            request_id,
            message,
            code,
            retry,
            ..
        }) => {
            assert_eq!(request_id, 21);
            assert!(message.contains("overloaded"));
            // Shed before any work, so nothing was applied.
            assert_eq!(code, Some(felix_wire::ErrorCode::Overloaded));
            assert_eq!(retry, Some(felix_wire::RetryClass::RetryAfter));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_throttled_without_request_id_sends_error() {
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

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        true,
        false,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        None,
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
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
async fn handle_publish_batch_message_ack_waiters_exhausted() {
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

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(22),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
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
            assert_eq!(request_id, 22);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_ack_waiter_queue_full() {
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
        .try_send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 99,
            payload_bytes: vec![1],
            response_rx: oneshot::channel().1,
            permit,
        })
        .expect("fill queue");

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(23),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
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
            assert_eq!(request_id, 23);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

#[tokio::test]
async fn handle_publish_batch_message_ack_waiter_queue_closed() {
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

    handle_publish_batch_message(
        // No client flags in a unit test: the ack owner hint is covered end to
        // end against a real cluster, where there is a forward to hint about.
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        true,
        AckEncoding::Json,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "tenant".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(24),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
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
            assert_eq!(request_id, 24);
            assert!(message.contains("server overloaded"));
        }
        _ => panic!("unexpected outgoing"),
    }
}

/// Run one acked batch for `t1/ns/stream` through a broker whose shard is
/// owned here but not yet opened, and return what it answered.
async fn refused_batch(encoding: AckEncoding) -> Outgoing {
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

    handle_publish_batch_message(
        0,
        &broker,
        &publish_ctx,
        &mut cache,
        &mut key,
        false,
        false,
        encoding,
        &out_tx,
        &out_depth,
        &throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &ack_waiters,
        &ack_waiter_tx,
        "t1".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![b"payload".to_vec()],
        None,
        Some(11),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
    )
    .await
    .expect("refused batch");
    out_rx.recv().await.expect("outgoing")
}

/// The stream exists; the shard just is not servable yet. Saying "stream not
/// found" makes a retryable condition look permanent.
#[tokio::test]
async fn an_unservable_shard_is_shard_unavailable_not_not_found() {
    let Outgoing::Message(Message::PublishError {
        request_id,
        message,
        code,
        retry,
        detail,
    }) = refused_batch(AckEncoding::Json).await
    else {
        panic!("expected publish_error");
    };
    assert_eq!(request_id, 11);
    assert_eq!(code, Some(felix_wire::ErrorCode::ShardUnavailable));
    assert_eq!(retry, Some(felix_wire::RetryClass::Retry));
    assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("not_ready"));
    assert!(!message.contains("not found"), "{message}");
    assert!(message.contains("t1/ns/stream"), "{message}");
}

/// A binary ack carries the same code and the reason behind it.
#[tokio::test]
async fn a_binary_ack_for_an_unservable_shard_carries_its_reason() {
    let Outgoing::PublishAck {
        request_id,
        error,
        code,
        detail,
        ..
    } = refused_batch(AckEncoding::Binary).await
    else {
        panic!("expected a binary ack");
    };
    assert_eq!(request_id, 11);
    let error = error.expect("a failed ack");
    assert!(!error.contains("not found"), "{error}");
    assert_eq!(
        code,
        Some((
            felix_wire::ErrorCode::ShardUnavailable,
            felix_wire::RetryClass::Retry
        ))
    );
    assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("not_ready"));
}

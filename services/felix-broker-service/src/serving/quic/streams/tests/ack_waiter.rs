//! The ack waiter: turning publish outcomes into responses, and what it does when the writer is gone.

use super::*;

#[tokio::test]
#[serial]
async fn ack_waiter_loop_branches() -> Result<()> {
    test_hooks::reset();
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let ack_waiter = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx.clone(),
        cancel_rx.clone(),
        Duration::from_millis(5),
    ));

    let waiters = Arc::new(Semaphore::new(10));
    let (tx_ok, rx_ok) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 1,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_ok,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_ok.send(Ok(()));

    let (tx_err, rx_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 2,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_err.send(Err(anyhow::anyhow!("nope")));

    let (_tx_drop, rx_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 3,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;

    let (tx_timeout, rx_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 4,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_timeout = tx_timeout;

    let (tx_batch_ok, rx_batch_ok) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 5,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_ok,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_batch_ok.send(Ok(()));

    let (tx_batch_err, rx_batch_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 6,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_batch_err.send(Err(anyhow::anyhow!("nope")));

    let (_tx_batch_drop, rx_batch_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 7,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;

    let (tx_batch_timeout, rx_batch_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 8,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_batch_timeout = tx_batch_timeout;

    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    ack_waiter.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_loop_cancel_branch() -> Result<()> {
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(1);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let waiter = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx.clone(),
        cancel_rx,
        Duration::from_millis(50),
    ));
    let waiters = Arc::new(Semaphore::new(1));
    let (_tx, rx) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 9,
            payload_len: 1,
            start: telemetry::t_instant_now(),
            response_rx: rx,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = cancel_tx.send(true);
    waiter.await.expect("waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_loop_cancel_changed_breaks() -> Result<()> {
    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    drop(out_ack_rx);
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let waiter = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx.clone(),
        cancel_rx,
        Duration::from_millis(50),
    ));
    let _ = cancel_tx.send(true);
    drop(ack_waiter_tx);
    waiter.await.expect("waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_loop_logs_on_enqueue_failure() -> Result<()> {
    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    drop(out_ack_rx);
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(16);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let ack_waiter = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Duration::from_millis(5),
    ));

    let waiters = Arc::new(Semaphore::new(10));
    let (tx_ok, rx_ok) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 10,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_ok,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_ok.send(Ok(()));

    let (tx_err, rx_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 11,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_err.send(Err(anyhow::anyhow!("nope")));

    let (_tx_drop, rx_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 12,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;

    let (tx_timeout, rx_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 13,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_timeout = tx_timeout;

    let (tx_batch_ok, rx_batch_ok) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 14,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_ok,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_batch_ok.send(Ok(()));

    let (tx_batch_err, rx_batch_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 15,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_batch_err.send(Err(anyhow::anyhow!("nope")));

    let (_tx_batch_drop, rx_batch_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 16,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;

    let (tx_batch_timeout, rx_batch_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 17,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_batch_timeout = tx_batch_timeout;

    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    ack_waiter.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_error() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (tx_err, rx_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 21,
            payload_len: 1,
            start: telemetry::t_instant_now(),
            response_rx: rx_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_err.send(Err(anyhow::anyhow!("nope")));
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_dropped() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (_tx_drop, rx_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 22,
            payload_len: 1,
            start: telemetry::t_instant_now(),
            response_rx: rx_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_timeout() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (tx_timeout, rx_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            encoding: AckEncoding::Json,
            request_id: 23,
            payload_len: 1,
            start: telemetry::t_instant_now(),
            response_rx: rx_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_timeout = tx_timeout;
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_batch_ok() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (tx_ok, rx_ok) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 24,
            payload_bytes: vec![1],
            response_rx: rx_ok,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_ok.send(Ok(()));
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_batch_error() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (tx_err, rx_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 25,
            payload_bytes: vec![1],
            response_rx: rx_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_err.send(Err(anyhow::anyhow!("nope")));
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_batch_dropped() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (_tx_drop, rx_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 26,
            payload_bytes: vec![1],
            response_rx: rx_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_batch_timeout() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (tx_timeout, rx_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            forwarded_to: None,
            encoding: AckEncoding::Json,
            request_id: 27,
            payload_bytes: vec![1],
            response_rx: rx_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_timeout = tx_timeout;
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

/// A quorum timeout reaches the client as `quorum_timeout`, outcome unknown,
/// in both encodings: the leader wrote the batch, so it is not a refusal.
#[tokio::test]
async fn a_quorum_timeout_is_answered_as_outcome_unknown() -> Result<()> {
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_waiter = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now()))),
        cancel_tx,
        cancel_rx,
        Duration::from_secs(5),
    ));
    let waiters = Arc::new(Semaphore::new(10));
    let timed_out = || {
        anyhow::Error::from(felix_replication::quorum::QuorumError::TimedOut {
            what: "batch",
            timeout: Duration::from_millis(50),
        })
    };

    for (request_id, encoding) in [(31, AckEncoding::Json), (32, AckEncoding::Binary)] {
        let (tx, rx) = oneshot::channel();
        ack_waiter_tx
            .send(AckWaiterMessage::PublishBatch {
                encoding,
                request_id,
                payload_bytes: vec![1],
                response_rx: rx,
                permit: waiters.clone().acquire_owned().await?,
                forwarded_to: None,
            })
            .await?;
        let _ = tx.send(Err(timed_out()));
    }

    match timeout(Duration::from_secs(2), out_ack_rx.recv()).await? {
        Some(Outgoing::Message(Message::PublishError {
            request_id: 31,
            code,
            retry,
            ..
        })) => {
            assert_eq!(code, Some(felix_wire::ErrorCode::QuorumTimeout));
            assert_eq!(retry, Some(felix_wire::RetryClass::OutcomeUnknown));
        }
        other => panic!("expected a json publish_error, got {other:?}"),
    }
    match timeout(Duration::from_secs(2), out_ack_rx.recv()).await? {
        Some(Outgoing::PublishAck {
            request_id: 32,
            code,
            error: Some(_),
            ..
        }) => assert_eq!(
            code,
            Some((
                felix_wire::ErrorCode::QuorumTimeout,
                felix_wire::RetryClass::OutcomeUnknown
            ))
        ),
        other => panic!("expected a binary ack, got {other:?}"),
    }
    drop(ack_waiter_tx);
    ack_waiter.await?;
    Ok(())
}

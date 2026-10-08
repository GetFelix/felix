//! Ack timeouts, the outgoing queue's throttle and ack enqueueing.

use super::*;

#[test]
fn ack_timeout_state_tracks_and_resets() {
    let start = Instant::now();
    let mut state = AckTimeoutState::new(start);
    assert_eq!(state.register_timeout(start), 1);
    assert_eq!(state.register_timeout(start + Duration::from_millis(10)), 2);
    let later = start + ACK_TIMEOUT_WINDOW + Duration::from_millis(1);
    assert_eq!(state.register_timeout(later), 1);
    let reset_at = later + Duration::from_secs(1);
    state.reset(reset_at);
    assert_eq!(state.register_timeout(reset_at), 1);
}

#[tokio::test]
#[serial_test::serial]
async fn send_outgoing_critical_increments_depth() {
    reset_global_ack_depth();
    let depth = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(1);
    let (throttle_tx, throttle_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = rx.recv().await;
    });
    let result = send_outgoing_critical(
        &tx,
        &depth,
        "test",
        &throttle_tx,
        Outgoing::Message(Message::error("e")),
    )
    .await;
    handle.await.unwrap();
    assert!(result.is_ok());
    assert_eq!(depth.load(Ordering::Relaxed), 1);
    assert!(GLOBAL_ACK_DEPTH.load(Ordering::Relaxed) >= 1);
    assert!(!*throttle_rx.borrow());
}

#[tokio::test]
#[serial_test::serial]
async fn send_outgoing_critical_triggers_throttle_at_hi_water() {
    reset_global_ack_depth();
    let depth = Arc::new(AtomicUsize::new(ACK_HI_WATER.saturating_sub(1)));
    let (tx, mut rx) = mpsc::channel(1);
    let (throttle_tx, throttle_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = rx.recv().await;
    });
    let result = send_outgoing_critical(
        &tx,
        &depth,
        "test",
        &throttle_tx,
        Outgoing::Message(Message::Ok),
    )
    .await;
    handle.await.unwrap();
    assert!(result.is_ok());
    assert!(*throttle_rx.borrow());
}

#[tokio::test]
#[serial_test::serial]
async fn send_outgoing_best_effort_reports_full() {
    reset_global_ack_depth();
    let depth = Arc::new(AtomicUsize::new(0));
    let (tx, _rx) = mpsc::channel(1);
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let _ = tx.send(Outgoing::Message(Message::error("f"))).await;
    let err = send_outgoing_best_effort(
        &tx,
        &depth,
        "test",
        &throttle_tx,
        Outgoing::Message(Message::error("overflow")),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, AckEnqueueError::Full));
}

#[tokio::test]
#[serial_test::serial]
async fn send_outgoing_best_effort_triggers_throttle_at_hi_water() {
    reset_global_ack_depth();
    let depth = Arc::new(AtomicUsize::new(ACK_HI_WATER.saturating_sub(1)));
    let (tx, mut rx) = mpsc::channel(1);
    let (throttle_tx, throttle_rx) = watch::channel(false);
    let result = send_outgoing_best_effort(
        &tx,
        &depth,
        "test",
        &throttle_tx,
        Outgoing::Message(Message::Ok),
    )
    .await;
    assert!(result.is_ok());
    assert!(*throttle_rx.borrow());
    let _ = rx.recv().await;
}

#[tokio::test]
#[serial_test::serial]
async fn send_outgoing_best_effort_reports_closed() {
    reset_global_ack_depth();
    let depth = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let err = send_outgoing_best_effort(
        &tx,
        &depth,
        "test",
        &throttle_tx,
        Outgoing::Message(Message::error("closed")),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, AckEnqueueError::Closed));
}

#[tokio::test]
async fn handle_ack_enqueue_timeout_threshold_triggers_cancel() {
    let state = Arc::new(parking_lot::Mutex::new(
        AckTimeoutState::new(Instant::now()),
    ));
    let (throttle_tx, throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    for _ in 0..(ACK_TIMEOUT_THRESHOLD - 1) {
        assert!(
            handle_ack_enqueue_result(
                Err(AckEnqueueError::Timeout),
                &state,
                &throttle_tx,
                &cancel_tx
            )
            .await
            .is_ok()
        );
    }
    let result = handle_ack_enqueue_result(
        Err(AckEnqueueError::Timeout),
        &state,
        &throttle_tx,
        &cancel_tx,
    )
    .await;
    assert!(result.is_err());
    assert!(*throttle_rx.borrow());
    assert!(*cancel_rx.borrow());
}

#[tokio::test]
async fn handle_ack_enqueue_full_returns_error() {
    let state = Arc::new(parking_lot::Mutex::new(
        AckTimeoutState::new(Instant::now()),
    ));
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let err =
        handle_ack_enqueue_result(Err(AckEnqueueError::Full), &state, &throttle_tx, &cancel_tx)
            .await
            .expect_err("full");
    assert!(err.to_string().contains("ack queue full"));
}

#[tokio::test]
async fn handle_ack_enqueue_closed_shutdowns_stream() {
    let state = Arc::new(parking_lot::Mutex::new(
        AckTimeoutState::new(Instant::now()),
    ));
    let (throttle_tx, throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let result = handle_ack_enqueue_result(
        Err(AckEnqueueError::Closed),
        &state,
        &throttle_tx,
        &cancel_tx,
    )
    .await;
    assert!(result.is_err());
    assert!(!*throttle_rx.borrow());
    assert!(*cancel_rx.borrow());
}

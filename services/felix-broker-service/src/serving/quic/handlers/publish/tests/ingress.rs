//! Depth accounting and the enqueue policies in front of the publish scheduler.

use super::*;

#[test]
fn decrement_depth_returns_none_when_empty() {
    let depth = Arc::new(AtomicUsize::new(0));
    let global = AtomicUsize::new(0);
    assert!(decrement_depth(&depth, &global, "test").is_none());
}

#[test]
fn decrement_depth_decreases_both_counters() {
    let depth = Arc::new(AtomicUsize::new(2));
    let global = AtomicUsize::new(3);
    let result = decrement_depth(&depth, &global, "test");
    assert!(result.is_some());
    let (prev, cur) = result.unwrap();
    assert_eq!(prev, 2);
    assert_eq!(cur, 1);
    assert_eq!(depth.load(Ordering::Relaxed), 1);
    assert_eq!(global.load(Ordering::Relaxed), 2);
}

#[test]
fn decrement_depth_handles_global_underflow() {
    let depth = Arc::new(AtomicUsize::new(1));
    let global = AtomicUsize::new(0);
    let result = decrement_depth(&depth, &global, "test");
    assert!(result.is_some());
    let (prev, cur) = result.unwrap();
    assert_eq!(prev, 1);
    assert_eq!(cur, 0);
    assert_eq!(depth.load(Ordering::Relaxed), 0);
    assert_eq!(global.load(Ordering::Relaxed), 0);
}

#[test]
fn reset_local_depth_only_conciles_global_counter() {
    let depth = Arc::new(AtomicUsize::new(4));
    let global = AtomicUsize::new(10);
    reset_local_depth_only(&depth, &global, "test");
    assert_eq!(depth.load(Ordering::Relaxed), 0);
    assert_eq!(global.load(Ordering::Relaxed), 6);
}

#[tokio::test]
async fn enqueue_publish_drop_returns_false_when_full() {
    let (ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).unwrap();
    let job = make_job();
    let result = enqueue_publish(&ctx, "tenant", job, EnqueuePolicy::Drop, None)
        .await
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn enqueue_publish_fail_returns_error_when_full() {
    let (ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).unwrap();
    let job = make_job();
    let err = enqueue_publish(&ctx, "tenant", job, EnqueuePolicy::Fail, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("publish queue full"));
}

#[tokio::test]
async fn enqueue_publish_wait_enqueues_when_receiver_ready() {
    let (ctx, mut rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).unwrap();
    let handle = tokio::spawn(async move {
        let _ = rx.recv().await;
        let _ = rx.recv().await;
    });
    let job = make_job();
    let result = enqueue_publish(&ctx, "tenant", job, EnqueuePolicy::Wait, None)
        .await
        .unwrap();
    assert!(result);
    handle.await.unwrap();
}

#[tokio::test]
async fn enqueue_publish_wait_answers_busy_when_the_queue_stays_full() {
    let (mut ctx, _rx, tx) = make_publish_context(1);
    ctx.wait_timeout = Duration::from_millis(5);
    tx.try_send(make_job()).unwrap();
    let err = enqueue_publish(&ctx, "tenant", make_job(), EnqueuePolicy::Wait, None)
        .await
        .unwrap_err();
    let busy = crate::serving::quic::client_error::ClientError::from_anyhow(&err);
    assert_eq!(busy.code(), &felix_wire::ErrorCode::Overloaded);
    assert_eq!(busy.retry(), felix_wire::RetryClass::RetryAfter);
}

#[tokio::test]
async fn enqueue_publish_returns_error_when_queue_closed() {
    let (ctx, rx, _tx) = make_publish_context(1);
    drop(rx);
    let err = enqueue_publish(&ctx, "tenant", make_job(), EnqueuePolicy::Fail, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("publish queue closed"));
}

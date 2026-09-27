//! Worker selection, depth accounting and the ingress queue's enqueue policies.

use super::*;

#[test]
fn publish_worker_index_is_deterministic() {
    let first = publish_worker_index("tenant", "ns", "stream", 3);
    let second = publish_worker_index("tenant", "ns", "stream", 3);
    assert_eq!(first, second);
    assert!(first < 3);
}

#[test]
fn publish_worker_index_returns_zero_with_no_workers() {
    assert_eq!(publish_worker_index("tenant", "ns", "stream", 0), 0);
}

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
    let result = enqueue_publish(&ctx, job, EnqueuePolicy::Drop, None)
        .await
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn enqueue_publish_fail_returns_error_when_full() {
    let (ctx, _rx, tx) = make_publish_context(1);
    tx.try_send(make_job()).unwrap();
    let job = make_job();
    let err = enqueue_publish(&ctx, job, EnqueuePolicy::Fail, None)
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
    let result = enqueue_publish(&ctx, job, EnqueuePolicy::Wait, None)
        .await
        .unwrap();
    assert!(result);
    handle.await.unwrap();
}

#[tokio::test]
async fn enqueue_publish_wait_times_out_when_queue_full() {
    let (tx, _rx) = mpsc::channel(1);
    tx.try_send(make_job()).unwrap();
    let ctx = PublishContext {
        ingress: None,
        client_endpoints: None,
        peers: None,
        lease: None,
        lease_headroom: std::time::Duration::ZERO,
        marks: None,
        quorum_timeout: Duration::from_secs(1),
        workers: Arc::new(vec![tx]),
        worker_count: 1,
        depth: Arc::new(AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(5),
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
        preauth: std::sync::Arc::new(crate::serving::quic::preauth::PreAuthGate::new(
            &crate::config::BrokerConfig::default(),
        )),
        tenant_rates: std::sync::Arc::new(crate::serving::limits::TenantRates::unlimited()),
    };
    let err = enqueue_publish(&ctx, make_job(), EnqueuePolicy::Wait, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("publish enqueue timed out"));
}

#[tokio::test]
async fn enqueue_publish_returns_error_when_queue_closed() {
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let ctx = PublishContext {
        ingress: None,
        client_endpoints: None,
        peers: None,
        lease: None,
        lease_headroom: std::time::Duration::ZERO,
        marks: None,
        quorum_timeout: Duration::from_secs(1),
        workers: Arc::new(vec![tx]),
        worker_count: 1,
        depth: Arc::new(AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(10),
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
        preauth: std::sync::Arc::new(crate::serving::quic::preauth::PreAuthGate::new(
            &crate::config::BrokerConfig::default(),
        )),
        tenant_rates: std::sync::Arc::new(crate::serving::limits::TenantRates::unlimited()),
    };
    let err = enqueue_publish(&ctx, make_job(), EnqueuePolicy::Fail, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("publish queue closed"));
}

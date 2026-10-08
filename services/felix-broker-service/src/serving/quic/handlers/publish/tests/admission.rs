//! Admission budgets and how the ingress queue waits or sheds when they run out.
//!
//! The waiting policies hold three properties:
//!   1. `Wait` spends one `wait_timeout` across both stages, not one each.
//!   2. `Backpressure` never sheds; it waits for capacity however long that takes.
//!   3. `Backpressure` still ends promptly when the connection is torn down.

use super::*;

#[tokio::test]
async fn publish_admission_bounds_shared_inflight_bytes() {
    let admission = PublishAdmission::new(4);
    let permit = admission.acquire(4).await.expect("initial permit");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), admission.acquire(1))
            .await
            .is_err()
    );
    drop(permit);
    let _permit = admission.acquire(1).await.expect("released permit");
}

#[tokio::test]
async fn publish_admission_try_acquire_fails_when_exhausted() {
    let admission = PublishAdmission::new(4);
    let _permit = admission.try_acquire(4).expect("initial permit");
    assert!(admission.try_acquire(1).is_err());
}

#[tokio::test]
async fn enqueue_publish_drop_sheds_load_when_byte_budget_exhausted() {
    let (scheduler, _tx, _rx) = test_channel(8);
    let ctx = PublishContext {
        shard_status: None,
        ingress: None,
        client_endpoints: None,
        peers: None,
        lease: None,
        lease_headroom: std::time::Duration::ZERO,
        marks: None,
        quorum_timeout: Duration::from_secs(1),
        scheduler,
        wait_timeout: Duration::from_millis(50),
        admission: Arc::new(PublishAdmission::new(4)),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
        preauth: std::sync::Arc::new(crate::serving::quic::preauth::PreAuthGate::new(
            &crate::config::BrokerConfig::default(),
        )),
        tenant_rates: std::sync::Arc::new(crate::serving::limits::TenantRates::unlimited()),
        publish_window: 0,
    };
    // Queue depth (8) has room, but the shared byte budget (4 bytes) does not fit this
    // 7-byte payload, so the job must be shed even though the item-count queue is empty.
    let mut job = make_job();
    job.payloads = vec![Bytes::from_static(b"payload")];
    let result = enqueue_publish(&ctx, "tenant", job, EnqueuePolicy::Drop, None)
        .await
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn enqueue_publish_drop_sheds_load_when_conn_byte_budget_exhausted() {
    let (scheduler, _tx, _rx) = test_channel(8);
    let ctx = PublishContext {
        shard_status: None,
        ingress: None,
        client_endpoints: None,
        peers: None,
        lease: None,
        lease_headroom: std::time::Duration::ZERO,
        marks: None,
        quorum_timeout: Duration::from_secs(1),
        scheduler,
        wait_timeout: Duration::from_millis(50),
        // Shared budget is generous; this connection's own share is not.
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::new(4)),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
        preauth: std::sync::Arc::new(crate::serving::quic::preauth::PreAuthGate::new(
            &crate::config::BrokerConfig::default(),
        )),
        tenant_rates: std::sync::Arc::new(crate::serving::limits::TenantRates::unlimited()),
        publish_window: 0,
    };
    let mut job = make_job();
    job.payloads = vec![Bytes::from_static(b"payload")];
    let result = enqueue_publish(&ctx, "tenant", job, EnqueuePolicy::Drop, None)
        .await
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn enqueue_publish_conn_budget_does_not_starve_other_connections() {
    let (scheduler, _tx, mut rx) = test_channel(8);
    // Two connections sharing one global budget, each with its own conn_admission.
    let admission = Arc::new(PublishAdmission::new(8));
    let ctx_a = PublishContext {
        shard_status: None,
        ingress: None,
        client_endpoints: None,
        peers: None,
        lease: None,
        lease_headroom: std::time::Duration::ZERO,
        marks: None,
        quorum_timeout: Duration::from_secs(1),
        scheduler,
        wait_timeout: Duration::from_millis(50),
        admission: Arc::clone(&admission),
        conn_admission: Arc::new(PublishAdmission::new(4)),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
        preauth: std::sync::Arc::new(crate::serving::quic::preauth::PreAuthGate::new(
            &crate::config::BrokerConfig::default(),
        )),
        tenant_rates: std::sync::Arc::new(crate::serving::limits::TenantRates::unlimited()),
        publish_window: 0,
    };
    let ctx_b = PublishContext {
        ingress: None,
        client_endpoints: None,
        peers: None,
        lease: None,
        lease_headroom: std::time::Duration::ZERO,
        conn_admission: Arc::new(PublishAdmission::new(4)),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ..ctx_a.clone()
    };

    // Connection A tries to claim more than its own share (would fit in the global budget
    // alone) and must be shed by its own per-connection gate, not the global one.
    let mut big_job = make_job();
    big_job.payloads = vec![Bytes::from_static(b"01234567")]; // 8 bytes > A's 4-byte share
    assert!(
        !enqueue_publish(&ctx_a, "tenant", big_job, EnqueuePolicy::Drop, None)
            .await
            .unwrap()
    );

    // Connection B is unaffected: its own share is untouched by A's rejected attempt.
    let mut small_job = make_job();
    small_job.payloads = vec![Bytes::from_static(b"ok")]; // 2 bytes, fits B's 4-byte share
    assert!(
        enqueue_publish(&ctx_b, "tenant", small_job, EnqueuePolicy::Drop, None)
            .await
            .unwrap()
    );
    assert!(rx.recv().await.is_some());
}

/// Admission and the queue send each used to get a full `wait_timeout`, so the
/// worst case was twice the configured budget. Here admission is held for most of
/// the budget and the queue is left full, so the send has to wait too; the whole
/// call must still finish within one budget.
#[tokio::test(start_paused = true)]
async fn wait_policy_spends_one_budget_across_both_stages() {
    let payload = b"payload".len();
    let (mut ctx, _rx, _tx) = make_publish_context(1);
    ctx.wait_timeout = Duration::from_millis(100);
    // Room for the primed job plus one held permit, so a third job must wait for
    // admission *and then* find the queue still full.
    ctx.admission = Arc::new(PublishAdmission::new(payload * 2));
    ctx.conn_admission = Arc::new(PublishAdmission::new(payload * 2));

    // Fill the only queue slot. This job keeps its admission permit while queued.
    enqueue_publish(&ctx, "tenant", make_job(), EnqueuePolicy::Drop, None)
        .await
        .expect("prime the queue");

    // Hold the remaining admission and release it partway through the budget.
    let held = ctx
        .admission
        .clone()
        .acquire(payload)
        .await
        .expect("hold admission");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        drop(held);
    });

    let start = tokio::time::Instant::now();
    let result = enqueue_publish(&ctx, "tenant", make_job(), EnqueuePolicy::Wait, None).await;
    let elapsed = start.elapsed();

    assert!(
        result.is_err(),
        "the queue never drains, so this must time out rather than enqueue"
    );
    assert!(
        elapsed >= Duration::from_millis(60),
        "admission should have blocked until the held permit was released, took {elapsed:?}"
    );
    // Before the fix each stage got its own budget, so this was ~160ms.
    assert!(
        elapsed <= Duration::from_millis(100),
        "Wait must not exceed one wait_timeout across both stages, took {elapsed:?}"
    );
}

/// The whole point of `Backpressure`: overload becomes slowness, never loss. A
/// timer here would have turned this into a silent drop, with no ack channel to
/// report it on.
#[tokio::test(start_paused = true)]
async fn backpressure_waits_for_capacity_instead_of_shedding() {
    let (ctx, mut rx, _tx) = make_publish_context(1);
    // Fill the single queue slot so the next enqueue has to wait.
    enqueue_publish(&ctx, "tenant", make_job(), EnqueuePolicy::Drop, None)
        .await
        .expect("prime the queue");

    // Drain long after any plausible timeout would have fired.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        let _ = rx.recv().await;
        // Hold the receiver open so the channel does not close under the waiter.
        tokio::time::sleep(Duration::from_secs(3600)).await;
        drop(rx);
    });

    let accepted = enqueue_publish(
        &ctx,
        "tenant",
        make_job(),
        EnqueuePolicy::Backpressure,
        None,
    )
    .await
    .expect("backpressure must not fail on a full queue");
    assert!(
        accepted,
        "backpressure must enqueue once capacity frees, never report a drop"
    );
}

/// Unbounded does not mean unstoppable: teardown is what ends the wait, which is
/// why the policy needs the connection's cancel signal rather than a clock.
#[tokio::test(start_paused = true)]
async fn backpressure_gives_up_when_the_connection_is_cancelled() {
    let (ctx, _rx, _tx) = make_publish_context(1);
    enqueue_publish(&ctx, "tenant", make_job(), EnqueuePolicy::Drop, None)
        .await
        .expect("prime the queue");

    let (cancel_tx, cancel_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = cancel_tx.send(true);
    });

    let err = enqueue_publish(
        &ctx,
        "tenant",
        make_job(),
        EnqueuePolicy::Backpressure,
        Some(cancel_rx),
    )
    .await
    .expect_err("cancellation must surface as an error, not a silent drop");
    assert!(
        err.to_string().contains("cancelled"),
        "unexpected error: {err}"
    );
}

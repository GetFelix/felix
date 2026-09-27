//! A tenant's publish quota at admission: what each enqueue policy does with a
//! publish over it, and that nothing over it reaches the worker queue.

use super::*;
use crate::config::{LimitsConfig, TenantQuota};
use crate::serving::limits::TenantRates;
use crate::serving::quic::client_error::{ClientError, TENANT_QUOTA_REASON};
use crate::serving::quic::handlers::publish::ingress::enqueue_tenant_publish;

/// One message per second for every tenant, burst of one.
fn one_per_second(ctx: &mut PublishContext) {
    ctx.tenant_rates = Arc::new(TenantRates::new(&LimitsConfig {
        tenant_publish_default: TenantQuota {
            bytes_per_sec: 0,
            msgs_per_sec: 1,
        },
        tenant_publish_burst_ms: 1_000,
        ..LimitsConfig::default()
    }));
}

#[tokio::test]
async fn an_acked_publish_over_quota_is_refused_with_a_retry_hint_and_not_queued() {
    let (mut ctx, mut rx, _tx) = make_publish_context(8);
    one_per_second(&mut ctx);

    // The first takes the bucket to zero, the second into debt.
    for _ in 0..2 {
        assert!(
            enqueue_tenant_publish(&ctx, "t1", make_job(), EnqueuePolicy::Fail, None)
                .await
                .expect("within quota")
        );
    }
    let err = enqueue_tenant_publish(&ctx, "t1", make_job(), EnqueuePolicy::Fail, None)
        .await
        .expect_err("over quota");
    let refusal = ClientError::not_enqueued(&err);
    assert_eq!(refusal.code(), &felix_wire::ErrorCode::Overloaded);
    assert_eq!(refusal.retry(), felix_wire::RetryClass::RetryAfter);
    let detail = refusal.detail().expect("detail");
    assert_eq!(detail.reason.as_deref(), Some(TENANT_QUOTA_REASON));
    let wait = detail.retry_after_ms.expect("a wait");
    assert!((1..=1_000).contains(&wait), "retry after {wait} ms");

    // The same for a commit-acked publish.
    let err = enqueue_tenant_publish(&ctx, "t1", make_job(), EnqueuePolicy::Wait, None)
        .await
        .expect_err("over quota");
    assert_eq!(
        ClientError::not_enqueued(&err)
            .detail()
            .and_then(|d| d.reason.clone())
            .as_deref(),
        Some(TENANT_QUOTA_REASON)
    );

    let mut queued = 0;
    while rx.try_recv().is_ok() {
        queued += 1;
    }
    assert_eq!(queued, 2, "only what the quota admitted reached a worker");

    // Another tenant has its own bucket.
    assert!(
        enqueue_tenant_publish(&ctx, "t2", make_job(), EnqueuePolicy::Fail, None)
            .await
            .expect("t2 is not throttled by t1")
    );
}

#[tokio::test]
async fn a_fire_and_forget_publish_over_quota_is_shed() {
    let (mut ctx, _rx, _tx) = make_publish_context(8);
    one_per_second(&mut ctx);
    for _ in 0..2 {
        enqueue_tenant_publish(&ctx, "t1", make_job(), EnqueuePolicy::Drop, None)
            .await
            .expect("within quota");
    }
    let accepted = enqueue_tenant_publish(&ctx, "t1", make_job(), EnqueuePolicy::Drop, None)
        .await
        .expect("dropping is not an error");
    assert!(!accepted, "shed like any other overload");
}

#[tokio::test]
async fn a_backpressured_publish_over_quota_waits_for_it() {
    let (mut ctx, mut rx, _tx) = make_publish_context(8);
    one_per_second(&mut ctx);
    for _ in 0..2 {
        enqueue_tenant_publish(&ctx, "t1", make_job(), EnqueuePolicy::Backpressure, None)
            .await
            .expect("within quota");
    }
    let started = Instant::now();
    let accepted = tokio::time::timeout(
        Duration::from_secs(5),
        enqueue_tenant_publish(&ctx, "t1", make_job(), EnqueuePolicy::Backpressure, None),
    )
    .await
    .expect("admitted once the quota refills")
    .expect("enqueued");
    assert!(accepted);
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "waited for the bucket, after {:?}",
        started.elapsed()
    );
    let mut queued = 0;
    while rx.try_recv().is_ok() {
        queued += 1;
    }
    assert_eq!(queued, 3, "nothing was dropped");
}

#[tokio::test]
async fn a_backpressured_wait_ends_when_the_connection_does() {
    let (mut ctx, _rx, _tx) = make_publish_context(8);
    one_per_second(&mut ctx);
    for _ in 0..2 {
        enqueue_tenant_publish(&ctx, "t1", make_job(), EnqueuePolicy::Backpressure, None)
            .await
            .expect("within quota");
    }
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let waiting = tokio::spawn({
        let ctx = ctx.clone();
        async move {
            enqueue_tenant_publish(
                &ctx,
                "t1",
                make_job(),
                EnqueuePolicy::Backpressure,
                Some(cancel_rx),
            )
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel_tx.send(true).expect("cancel");
    let result = tokio::time::timeout(Duration::from_millis(500), waiting)
        .await
        .expect("cancelled promptly")
        .expect("join");
    assert!(result.is_err(), "a cancelled wait enqueues nothing");
}

#[tokio::test]
async fn with_no_quota_every_publish_is_admitted() {
    let (ctx, _rx, _tx) = make_publish_context(64);
    for _ in 0..50 {
        assert!(
            enqueue_tenant_publish(&ctx, "t1", make_job(), EnqueuePolicy::Fail, None)
                .await
                .expect("unlimited")
        );
    }
}

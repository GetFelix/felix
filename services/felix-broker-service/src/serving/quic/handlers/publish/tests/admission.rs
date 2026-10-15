//! Admission budgets and how the ingress queue waits or sheds when they run out.
//!
//! The waiting policies hold three properties:
//!   1. `Wait` spends one `wait_timeout` across both stages, not one each.
//!   2. `Backpressure` never sheds; it waits for capacity however long that takes.
//!   3. `Backpressure` still ends promptly when the connection is torn down.

use super::*;
use crate::serving::quic::handlers::publish::admission::SubscriptionCap;

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
        identity: IdentityLimits::unlimited(),
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
        identity: IdentityLimits::unlimited(),
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
        identity: IdentityLimits::unlimited(),
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
        identity: IdentityLimits::unlimited(),
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

/// Limits as a connection's: 7 bytes (one `make_job`) and one subscription
/// per identity, room for three identities' worth in all.
fn shared_conn() -> (crate::config::BrokerConfig, Arc<ConnLimits>) {
    let payload = b"payload".len();
    let config = crate::config::BrokerConfig {
        pub_conn_inflight_bytes: payload,
        pub_conn_total_inflight_bytes: Some(payload * 3),
        max_subscriptions_per_conn: 1,
        max_subscriptions_per_conn_total: Some(3),
        ..crate::config::BrokerConfig::default()
    };
    let limits = ConnLimits::new(&config);
    (config, limits)
}

fn alice() -> IdentityKey {
    IdentityKey::new("t1", "alice")
}

fn bob() -> IdentityKey {
    IdentityKey::new("t1", "bob")
}

/// A gateway's users share its connection. One user's unanswered publishes
/// filling her budget must not shed another user's.
#[tokio::test]
async fn one_identity_at_its_byte_budget_leaves_another_room() {
    let (config, limits) = shared_conn();
    let (mut ctx, _rx, _tx) = make_publish_context(8);
    ctx.conn_admission = Arc::new(PublishAdmission::new(config.conn_publish_ceiling()));
    let mut alice_ctx = ctx.clone();
    alice_ctx.identity = limits.share(alice());
    let mut bob_ctx = ctx;
    bob_ctx.identity = limits.share(bob());

    let queued = enqueue_publish(&alice_ctx, "t1", make_job(), EnqueuePolicy::Drop, None)
        .await
        .expect("enqueue");
    assert!(queued, "alice's first publish fits her budget");
    let over = enqueue_publish(&alice_ctx, "t1", make_job(), EnqueuePolicy::Fail, None)
        .await
        .expect_err("alice is over her budget");
    assert!(
        over.to_string()
            .contains("per-connection byte budget exhausted"),
        "{over}"
    );

    let queued = enqueue_publish(&bob_ctx, "t1", make_job(), EnqueuePolicy::Drop, None)
        .await
        .expect("enqueue");
    assert!(queued, "alice's full budget shed bob's publish");
}

/// The connection's ceiling still bounds the identities together.
#[tokio::test]
async fn the_connection_ceiling_bounds_every_identity_together() {
    let (config, limits) = shared_conn();
    let (mut ctx, _rx, _tx) = make_publish_context(8);
    ctx.conn_admission = Arc::new(PublishAdmission::new(config.conn_publish_ceiling()));
    let mut held = Vec::new();
    for user in ["a", "b", "c"] {
        let mut user_ctx = ctx.clone();
        user_ctx.identity = limits.share(IdentityKey::new("t1", user));
        assert!(
            enqueue_publish(&user_ctx, "t1", make_job(), EnqueuePolicy::Drop, None)
                .await
                .expect("enqueue")
        );
        held.push(user_ctx);
    }
    ctx.identity = limits.share(IdentityKey::new("t1", "d"));
    let refused = enqueue_publish(&ctx, "t1", make_job(), EnqueuePolicy::Fail, None)
        .await
        .expect_err("past the connection's ceiling");
    assert!(
        refused.to_string().contains("across identities"),
        "{refused}"
    );
}

#[test]
fn subscriptions_are_capped_per_identity_under_the_connection_ceiling() {
    let (_, limits) = shared_conn();
    let alice = limits.share(alice());
    let bob = limits.share(bob());
    alice.try_reserve().expect("alice's first");
    assert_eq!(alice.try_reserve(), Err(SubscriptionCap::Identity));
    assert_eq!(
        SubscriptionCap::Identity.message(),
        "max subscriptions per connection exceeded",
        "a plain client's refusal reads as it always did"
    );
    bob.try_reserve().expect("alice at her cap leaves bob room");
    limits
        .share(IdentityKey::new("t1", "carol"))
        .try_reserve()
        .expect("carol");
    assert_eq!(
        limits.share(IdentityKey::new("t1", "dave")).try_reserve(),
        Err(SubscriptionCap::Connection)
    );
    alice.release();
    alice.try_reserve().expect("a released slot is reusable");
}

/// A client cycling through identities must not grow the connection's table:
/// an identity's entry goes with the last stream, subscription or in-flight
/// publish that holds it.
#[tokio::test]
async fn an_identity_with_nothing_held_is_forgotten() {
    let (config, limits) = shared_conn();
    for user in 0..1000 {
        let share = limits.share(IdentityKey::new("t1", &user.to_string()));
        share.try_reserve().expect("reserve");
        share.release();
    }
    assert_eq!(limits.identities(), 0);

    // Streams of one identity share one entry.
    let first = limits.share(alice());
    let second = limits.share(alice());
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(limits.identities(), 1);
    drop((first, second));
    assert_eq!(limits.identities(), 0);

    // An in-flight publish keeps its identity's entry, and its budget, after
    // the stream that sent it is gone.
    let (mut ctx, mut rx, _tx) = make_publish_context(8);
    ctx.conn_admission = Arc::new(PublishAdmission::new(config.conn_publish_ceiling()));
    ctx.identity = limits.share(alice());
    assert!(
        enqueue_publish(&ctx, "t1", make_job(), EnqueuePolicy::Drop, None)
            .await
            .expect("enqueue")
    );
    drop(ctx);
    assert_eq!(limits.identities(), 1);
    let (mut reopened, _rx2, _tx2) = make_publish_context(8);
    reopened.identity = limits.share(alice());
    assert!(
        enqueue_publish(&reopened, "t1", make_job(), EnqueuePolicy::Fail, None)
            .await
            .is_err(),
        "a reopened stream found a fresh budget while alice's bytes were in flight"
    );
    drop(reopened);
    drop(rx.try_recv().expect("the queued job"));
    assert_eq!(limits.identities(), 0);
}

/// Rebinding a stream to the identity it authenticated as.
#[test]
fn a_stream_rebinds_to_its_identitys_share() {
    let (_, limits) = shared_conn();
    let unauthenticated = limits.share(IdentityKey::default());
    let alice_stream = unauthenticated.rebind(alice());
    assert!(Arc::ptr_eq(&alice_stream, &limits.share(alice())));
    assert!(Arc::ptr_eq(&alice_stream.rebind(alice()), &alice_stream));
    drop(unauthenticated);
    assert_eq!(limits.identities(), 1);
    // A standalone share has no connection to move within.
    let lone = IdentityLimits::unlimited();
    assert!(Arc::ptr_eq(&lone.rebind(alice()), &lone));
}

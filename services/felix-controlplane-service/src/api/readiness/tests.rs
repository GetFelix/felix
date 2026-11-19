//! Readiness: what it answers, how often it asks, and what it costs.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::*;
use crate::store::StoreError;

/// A backend that answers on command and counts how often it is asked.
struct Fake {
    healthy: AtomicBool,
    hang: AtomicBool,
    asked: AtomicUsize,
}

impl Fake {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            healthy: AtomicBool::new(true),
            hang: AtomicBool::new(false),
            asked: AtomicUsize::new(0),
        })
    }

    fn asked(&self) -> usize {
        self.asked.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl HealthProbe for Fake {
    async fn health_check(&self) -> StoreResult<()> {
        self.asked.fetch_add(1, Ordering::Relaxed);
        if self.hang.load(Ordering::Relaxed) {
            // Longer than any timeout a test sets.
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        if self.healthy.load(Ordering::Relaxed) {
            Ok(())
        } else {
            Err(StoreError::Unexpected(anyhow::anyhow!(
                "connection refused"
            )))
        }
    }
}

fn readiness(store: Arc<Fake>, ttl: Duration) -> Readiness {
    Readiness::with_limits(store, Duration::from_millis(50), ttl)
}

fn draining_readiness(
    store: Arc<Fake>,
    ttl: Duration,
) -> (Readiness, felix_common::lifecycle::Readiness) {
    let lifecycle = felix_common::lifecycle::Readiness::ready();
    let readiness =
        Readiness::with_lifecycle(lifecycle.clone(), store, Duration::from_millis(50), ttl);
    (readiness, lifecycle)
}

#[tokio::test]
async fn a_healthy_store_is_ready() {
    let store = Fake::new();
    let readiness = readiness(Arc::clone(&store), Duration::ZERO);

    assert_eq!(readiness.check().await, Ok(()));
}

/// A load balancer has to be able to take this instance out.
#[tokio::test]
async fn a_failing_store_is_not_ready_and_says_why() {
    let store = Fake::new();
    store.healthy.store(false, Ordering::Relaxed);
    let readiness = readiness(Arc::clone(&store), Duration::ZERO);

    match readiness.check().await {
        Err(NotReady::Store { detail }) => assert!(detail.contains("connection refused")),
        other => panic!("expected a store failure, got {other:?}"),
    }
}

/// **A probe that hangs is worse than one that fails.** The prober keeps
/// sending traffic to an instance nobody has heard from.
#[tokio::test]
async fn a_store_that_never_answers_is_not_ready() {
    let store = Fake::new();
    store.hang.store(true, Ordering::Relaxed);
    let readiness = readiness(Arc::clone(&store), Duration::ZERO);

    let started = Instant::now();
    let outcome = readiness.check().await;

    assert_eq!(outcome, Err(NotReady::Timeout { after_ms: 50 }));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the check waited on the store instead of bounding it",
    );
}

/// **Readiness comes back on its own.** A transient outage must not need an
/// operator, or a restart, to clear.
#[tokio::test]
async fn readiness_recovers_when_the_store_does() {
    let store = Fake::new();
    let readiness = readiness(Arc::clone(&store), Duration::ZERO);

    store.healthy.store(false, Ordering::Relaxed);
    assert!(readiness.check().await.is_err());

    store.healthy.store(true, Ordering::Relaxed);
    assert_eq!(readiness.check().await, Ok(()));
}

/// Probers multiply. Ten of them polling must not be ten queries on the pool
/// that real work needs.
#[tokio::test]
async fn probes_inside_the_window_share_one_answer() {
    let store = Fake::new();
    let readiness = readiness(Arc::clone(&store), Duration::from_secs(30));
    let now = Instant::now();

    for _ in 0..10 {
        assert_eq!(readiness.check_at(now).await, Ok(()));
    }

    assert_eq!(store.asked(), 1, "the cache did not hold");
}

/// And the window ends, or a failure would never be noticed.
#[tokio::test]
async fn the_store_is_asked_again_once_the_window_passes() {
    let store = Fake::new();
    let ttl = Duration::from_secs(1);
    let readiness = readiness(Arc::clone(&store), ttl);
    let now = Instant::now();

    readiness.check_at(now).await.expect("ready");
    readiness.check_at(now + ttl / 2).await.expect("ready");
    assert_eq!(store.asked(), 1);

    readiness.check_at(now + ttl).await.expect("ready");
    assert_eq!(store.asked(), 2);
}

/// A failure is cached like a success, so recovery is visible within the window
/// rather than immediately — the cost of not hammering a struggling database.
#[tokio::test]
async fn a_failure_is_cached_for_the_same_window() {
    let store = Fake::new();
    let ttl = Duration::from_secs(1);
    let readiness = readiness(Arc::clone(&store), ttl);
    let now = Instant::now();

    store.healthy.store(false, Ordering::Relaxed);
    assert!(readiness.check_at(now).await.is_err());

    store.healthy.store(true, Ordering::Relaxed);
    assert!(
        readiness.check_at(now + ttl / 2).await.is_err(),
        "the cached failure was not held",
    );
    assert_eq!(
        readiness.check_at(now + ttl).await,
        Ok(()),
        "recovery took longer than the window",
    );
}

// --- Draining ----------------------------------------------------------------

/// **The point of the drain.** A shutting-down instance must leave rotation
/// while it can still serve, not when its listener stops.
#[tokio::test]
async fn a_draining_instance_is_not_ready_even_with_a_healthy_store() {
    let store = Fake::new();
    let (readiness, lifecycle) = draining_readiness(Arc::clone(&store), Duration::ZERO);

    assert_eq!(readiness.check().await, Ok(()));

    lifecycle.begin_draining();
    assert_eq!(readiness.check().await, Err(NotReady::Draining));
}

/// Startup is not draining: until the API listener is bound the instance is
/// not ready, and it says so without claiming a shutdown.
#[tokio::test]
async fn a_starting_instance_is_not_ready_until_marked() {
    let store = Fake::new();
    let lifecycle = felix_common::lifecycle::Readiness::starting();
    let readiness = Readiness::with_lifecycle(
        lifecycle.clone(),
        Arc::clone(&store) as Arc<dyn HealthProbe>,
        Duration::from_millis(50),
        Duration::ZERO,
    );

    let err = readiness.check().await.expect_err("not bound yet");
    assert_eq!(err, NotReady::Starting);
    assert!(!err.to_string().contains("shutting down"));
    assert_eq!(store.asked(), 0);

    lifecycle.mark_ready();
    assert_eq!(readiness.check().await, Ok(()));

    lifecycle.begin_draining();
    assert_eq!(readiness.check().await, Err(NotReady::Draining));
}

/// Draining is answered without asking the store. Nothing a database says
/// changes whether this process is shutting down, and a struggling database
/// must not delay an instance leaving rotation.
#[tokio::test]
async fn draining_does_not_query_the_store() {
    let store = Fake::new();
    let (readiness, lifecycle) = draining_readiness(Arc::clone(&store), Duration::ZERO);
    lifecycle.begin_draining();

    readiness.check().await.expect_err("draining");

    assert_eq!(store.asked(), 0, "a drain check reached the store");
}

/// It is answered before the cache, too: an instance that had just cached a
/// healthy answer would otherwise keep taking traffic for a whole window after
/// it began shutting down.
#[tokio::test]
async fn draining_is_not_delayed_by_a_cached_healthy_answer() {
    let store = Fake::new();
    let ttl = Duration::from_secs(30);
    let (readiness, lifecycle) = draining_readiness(Arc::clone(&store), ttl);
    let now = Instant::now();

    assert_eq!(readiness.check_at(now).await, Ok(()));
    lifecycle.begin_draining();

    assert_eq!(
        readiness.check_at(now).await,
        Err(NotReady::Draining),
        "a cached ready answer outlived the drain",
    );
}

/// The two endpoints read one flag, so they cannot disagree about whether this
/// instance is in rotation.
#[tokio::test]
async fn the_metrics_flag_and_the_api_check_agree() {
    let store = Fake::new();
    let (readiness, lifecycle) = draining_readiness(Arc::clone(&store), Duration::ZERO);

    assert!(lifecycle.is_ready());
    assert_eq!(readiness.check().await, Ok(()));

    lifecycle.begin_draining();
    assert!(!lifecycle.is_ready());
    assert!(readiness.check().await.is_err());
}

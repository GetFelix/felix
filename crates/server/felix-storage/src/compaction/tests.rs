//! The compaction budget: pacing, the hold, and shutdown.
use super::*;

#[tokio::test(start_paused = true)]
async fn spending_is_paced_to_the_budget() {
    let compactor = Compactor::with_budget(1000);
    let start = Instant::now();
    for _ in 0..4 {
        assert!(compactor.spend(500).await);
    }
    // The first spend is free; the next three each wait out the one before.
    assert_eq!(start.elapsed(), Duration::from_millis(1500));
}

#[tokio::test(start_paused = true)]
async fn an_unlimited_budget_never_waits() {
    let compactor = Compactor::with_budget(0);
    let start = Instant::now();
    for _ in 0..4 {
        assert!(compactor.spend(u64::MAX / 8).await);
    }
    assert_eq!(start.elapsed(), Duration::ZERO);
}

#[tokio::test]
async fn shutdown_releases_a_spend_waiting_on_the_budget() {
    let compactor = std::sync::Arc::new(Compactor::with_budget(1));
    assert!(compactor.spend(1).await);
    let waiting = {
        let compactor = std::sync::Arc::clone(&compactor);
        tokio::spawn(async move { compactor.spend(1_000_000).await })
    };
    tokio::task::yield_now().await;
    compactor.shutdown().await;
    assert!(
        !waiting.await.expect("spend"),
        "a spend during shutdown should report the pass abandoned"
    );
    assert!(!compactor.spawn(async {}), "nothing starts after shutdown");
}

#[tokio::test]
async fn a_held_compactor_parks_spends_until_released() {
    let compactor = std::sync::Arc::new(Compactor::with_budget(0));
    compactor.hold();
    let waiting = {
        let compactor = std::sync::Arc::clone(&compactor);
        tokio::spawn(async move { compactor.spend(1).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished());
    compactor.release();
    assert!(waiting.await.expect("spend"));
}

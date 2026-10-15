use std::sync::atomic::AtomicUsize;

use super::*;

fn counting_flush(
    counter: Arc<AtomicUsize>,
    reach: Arc<AtomicU64>,
) -> impl Fn() -> std::pin::Pin<Box<dyn Future<Output = Result<Offset>> + Send>> {
    move || {
        let counter = Arc::clone(&counter);
        let reach = Arc::clone(&reach);
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(reach.load(Ordering::SeqCst))
        })
    }
}

#[tokio::test]
async fn an_already_durable_target_does_not_flush() {
    let durability = Durability::new(FsyncMode::OnCommit, 10);
    let flushes = Arc::new(AtomicUsize::new(0));
    durability
        .ensure_durable(
            10,
            counting_flush(Arc::clone(&flushes), Arc::new(AtomicU64::new(10))),
        )
        .await
        .expect("durable");
    assert_eq!(flushes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_flush_advances_the_durable_bound() {
    let durability = Durability::new(FsyncMode::OnCommit, 0);
    let flushes = Arc::new(AtomicUsize::new(0));
    durability
        .ensure_durable(
            5,
            counting_flush(Arc::clone(&flushes), Arc::new(AtomicU64::new(5))),
        )
        .await
        .expect("durable");
    assert_eq!(flushes.load(Ordering::SeqCst), 1);
    assert_eq!(durability.durable_upto(), 5);
}

#[tokio::test]
async fn concurrent_appends_share_one_flush() {
    let durability = Arc::new(Durability::new(FsyncMode::OnCommit, 0));
    let flushes = Arc::new(AtomicUsize::new(0));
    // Every flush makes all 32 offsets durable, as a real fsync of the whole
    // file would.
    let reach = Arc::new(AtomicU64::new(32));

    let mut tasks = Vec::new();
    for target in 1..=32u64 {
        let durability = Arc::clone(&durability);
        let flush = counting_flush(Arc::clone(&flushes), Arc::clone(&reach));
        tasks.push(tokio::spawn(async move {
            durability.ensure_durable(target, flush).await
        }));
    }
    for task in tasks {
        task.await.expect("join").expect("durable");
    }

    assert_eq!(durability.durable_upto(), 32);
    // The whole point: far fewer flushes than appends.
    let count = flushes.load(Ordering::SeqCst);
    assert!(count < 32, "expected group commit, got {count} flushes");
}

/// A waiter whose target another flush has covered returns without taking
/// the flush lock, so one flush releases every waiter together rather than
/// each in turn as the lock is handed down.
#[tokio::test]
async fn a_covered_waiter_returns_without_the_flush_lock() {
    let durability = Arc::new(Durability::new(FsyncMode::OnCommit, 0));
    let flushes = Arc::new(AtomicUsize::new(0));
    // Someone else is flushing, and holds the lock throughout.
    let flushing = durability.lock_flushes().await;
    let waiter = {
        let durability = Arc::clone(&durability);
        let flush = counting_flush(Arc::clone(&flushes), Arc::new(AtomicU64::new(0)));
        tokio::spawn(async move { durability.ensure_durable(7, flush).await })
    };
    tokio::task::yield_now().await;
    durability.note_durable(7);
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("a covered waiter waited for the flush lock")
        .expect("join")
        .expect("durable");
    drop(flushing);
    assert_eq!(flushes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_flush_failure_propagates_to_the_caller() {
    let durability = Durability::new(FsyncMode::OnCommit, 0);
    let result = durability
        .ensure_durable(1, || async {
            Err(StorageError::SyncFailed("device gone".into()))
        })
        .await;
    assert!(matches!(result, Err(StorageError::SyncFailed(_))));
    // A failed flush must not advance the durable bound.
    assert_eq!(durability.durable_upto(), 0);
}

#[tokio::test]
async fn a_flush_that_never_advances_gives_up_instead_of_spinning() {
    let durability = Durability::new(FsyncMode::OnCommit, 0);
    let flushes = Arc::new(AtomicUsize::new(0));
    let result = durability
        .ensure_durable(
            99,
            counting_flush(Arc::clone(&flushes), Arc::new(AtomicU64::new(1))),
        )
        .await;
    assert!(matches!(result, Err(StorageError::SyncFailed(_))));
    assert_eq!(flushes.load(Ordering::SeqCst), MAX_FLUSH_ATTEMPTS);
}

#[test]
fn spawning_a_periodic_syncer_without_a_runtime_is_an_error() {
    let err = PeriodicSyncer::spawn(Duration::from_millis(1), || async { Ok(0) })
        .expect_err("no runtime");
    assert!(matches!(err, StorageError::InvalidConfig(_)));
    assert!(err.to_string().contains("Tokio runtime"), "{err}");
}

#[test]
fn only_on_commit_defers_acknowledgement() {
    assert!(!Durability::new(FsyncMode::OnCommit, 0).acknowledges_before_sync());
    assert!(Durability::new(FsyncMode::None, 0).acknowledges_before_sync());
    assert!(
        Durability::new(
            FsyncMode::Periodic {
                interval: Duration::from_millis(1)
            },
            0
        )
        .acknowledges_before_sync()
    );
}

#[test]
fn the_durable_bound_never_moves_backwards() {
    let durability = Durability::new(FsyncMode::None, 5);
    durability.note_durable(9);
    assert_eq!(durability.durable_upto(), 9);
    durability.note_durable(2);
    assert_eq!(durability.durable_upto(), 9);
}

#[tokio::test]
async fn truncation_can_lower_the_durable_bound_exclusively() {
    let durability = Durability::new(FsyncMode::OnCommit, 9);
    let _guard = durability.lock_flushes().await;
    durability.reset_after_truncate(3);
    assert_eq!(durability.durable_upto(), 3);
}

#[tokio::test(start_paused = true)]
async fn the_periodic_syncer_flushes_on_its_interval() {
    let flushes = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&flushes);
    let syncer = PeriodicSyncer::spawn(Duration::from_millis(50), move || {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(0)
        }
    })
    .expect("spawn");

    tokio::time::sleep(Duration::from_millis(175)).await;
    let ticks = flushes.load(Ordering::SeqCst);
    assert!((3..=4).contains(&ticks), "got {ticks} flushes");

    syncer.shutdown().await;
    // Shutdown flushes once more so nothing pending is lost.
    assert!(flushes.load(Ordering::SeqCst) > ticks);
}

#[tokio::test]
async fn the_periodic_syncer_survives_a_failing_flush() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&attempts);
    let syncer = PeriodicSyncer::spawn(Duration::from_millis(5), move || {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Err(StorageError::SyncFailed("transient".into()))
        }
    })
    .expect("spawn");

    tokio::time::sleep(Duration::from_millis(40)).await;
    syncer.shutdown().await;
    // It kept ticking rather than dying on the first error.
    assert!(attempts.load(Ordering::SeqCst) > 2);
}

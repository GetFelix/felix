use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

/// A cache whose shards are held while the returned flag is set.
async fn holdable(dir: &std::path::Path) -> (LogCache, Arc<AtomicBool>) {
    let cache = cache(dir).await;
    let held = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&held);
    cache.hold_local_writes_when(move |_, _, _, _| flag.load(Ordering::SeqCst));
    (cache, held)
}

/// **A held shard takes no local write.** A put or a delete would move the
/// tail a seal has named. Lifting the hold lets them through again.
#[tokio::test]
async fn a_held_shard_refuses_puts_and_deletes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (cache, held) = holdable(dir.path()).await;
    cache
        .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"v"), None)
        .await
        .expect("put");
    let log = cache.shard_log(T, NS, C, 0).await.expect("log");
    let tail = log.tail_offset().await.expect("tail");

    held.store(true, Ordering::SeqCst);
    let put = cache
        .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"w"), None)
        .await;
    assert!(matches!(put, Err(StorageError::Sealed(_))), "{put:?}");
    let deleted = cache.delete_checked(T, NS, C, 0, "k").await;
    assert!(
        matches!(deleted, Err(StorageError::Sealed(_))),
        "{deleted:?}"
    );
    assert_eq!(log.tail_offset().await.expect("tail"), tail);
    assert!(cache.local_writes_quiet(T, NS, C, 0));

    held.store(false, Ordering::SeqCst);
    cache
        .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"w"), None)
        .await
        .expect("put after the hold lifts");
}

/// **A write under way is seen until it ends.** Whoever sets the hold waits
/// for this before trusting the log to stay as it is.
#[tokio::test]
async fn a_local_write_under_way_is_not_quiet() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (cache, held) = holdable(dir.path()).await;
    let shard = cache.shard(T, NS, C, 0).expect("shard");
    assert!(cache.local_writes_quiet(T, NS, C, 0));

    let entered = shard.enter_local().expect("enter");
    held.store(true, Ordering::SeqCst);
    assert!(!cache.local_writes_quiet(T, NS, C, 0));
    drop(entered);
    assert!(cache.local_writes_quiet(T, NS, C, 0));
    assert!(shard.enter_local().is_err());
    assert!(cache.local_writes_quiet(T, NS, C, 0));
}

/// **A held shard is not compacted.** A pass rolls the log, copies records
/// to the tail and trims the head: the start and the tail a seal named
/// would both move.
#[tokio::test]
async fn a_held_shard_is_not_compacted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (cache, held) = holdable(dir.path()).await;
    let expected = overwritten(&cache, 64).await;
    cache.compactor.idle().await;
    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let log = cache.shard_log(T, NS, C, 0).await.expect("log");
    let (start, tail) = (log.base_offset(), log.tail_offset().await.expect("tail"));

    held.store(true, Ordering::SeqCst);
    let pass = shard.compact().await;
    assert!(matches!(pass, Err(StorageError::Sealed(_))), "{pass:?}");
    assert_eq!(
        (log.base_offset(), log.tail_offset().await.expect("tail")),
        (start, tail),
        "a held shard's log moved",
    );

    held.store(false, Ordering::SeqCst);
    shard.compact().await.expect("compact");
    assert!(log.base_offset() > start, "the pass trimmed nothing");
    assert_reads(&cache, &expected, "after compaction").await;
}

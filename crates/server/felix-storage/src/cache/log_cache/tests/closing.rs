//! Closing a shard that moved away, and getting it back.

use super::*;

#[tokio::test]
async fn a_closed_shard_reopens_from_disk_with_its_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    cache
        .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"v1"), None)
        .await
        .expect("put");
    let old = cache.shard_log(T, NS, C, 0).await.expect("log");

    cache.close_shard(T, NS, C, 0).await.expect("close");
    assert!(old.is_closed());
    assert!(matches!(
        old.tail_offset().await.and(old.sync().await),
        Err(StorageError::Closed(_))
    ));

    assert_eq!(
        cache.get_checked(T, NS, C, 0, "k").await.expect("get"),
        Some(Bytes::from_static(b"v1")),
    );
    cache
        .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"v2"), None)
        .await
        .expect("put after reopen");
    let fresh = cache.shard_log(T, NS, C, 0).await.expect("log");
    assert!(!fresh.is_closed());
    assert_eq!(fresh.tail_offset().await.expect("tail"), 2);
}

/// A caller that found the shard before the close and reaches its lock after
/// must be refused, not left writing into files a newer open may own.
#[tokio::test]
async fn work_that_found_the_shard_before_the_close_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    cache
        .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"v"), None)
        .await
        .expect("put");
    let found = cache.shard(T, NS, C, 0).expect("shard");

    cache.close_shard(T, NS, C, 0).await.expect("close");

    let mut state = found.state.lock().await;
    assert!(matches!(
        found.ensure_index(&mut state).await,
        Err(StorageError::Closed(_))
    ));
}

#[tokio::test]
async fn closing_one_shard_leaves_the_others_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    for shard in 0..2 {
        cache
            .put_checked(T, NS, C, shard, "k", Bytes::from_static(b"v"), None)
            .await
            .expect("put");
    }
    let other = cache.shard_log(T, NS, C, 1).await.expect("log");

    cache.close_shard(T, NS, C, 0).await.expect("close");

    assert!(!other.is_closed());
    cache
        .put_checked(T, NS, C, 1, "k2", Bytes::from_static(b"v"), None)
        .await
        .expect("put");
}

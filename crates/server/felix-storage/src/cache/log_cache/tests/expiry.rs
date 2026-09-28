use super::*;

#[tokio::test]
async fn an_expired_entry_reads_as_absent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(
            T,
            NS,
            C,
            0,
            "k",
            Bytes::from_static(b"v"),
            Some(Duration::from_millis(1)),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(cache.get(T, NS, C, 0, "k").await.unwrap().is_none());
}

#[tokio::test]
async fn an_unexpired_entry_still_reads() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(
            T,
            NS,
            C,
            0,
            "k",
            Bytes::from_static(b"v"),
            Some(Duration::from_secs(300)),
        )
        .await
        .unwrap();

    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.unwrap().as_deref(),
        Some(&b"v"[..])
    );
}

/// An expiry that passes while the process is down is still an expiry. This is
/// why the record stores an absolute time rather than a duration: a duration
/// would start its life again on every recovery.
#[tokio::test]
async fn an_expiry_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let cache = cache(dir.path()).await;
        cache
            .put(
                T,
                NS,
                C,
                0,
                "k",
                Bytes::from_static(b"v"),
                Some(Duration::from_millis(1)),
            )
            .await
            .unwrap();
        cache.shutdown().await.expect("shutdown");
    }
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        cache(dir.path())
            .await
            .get(T, NS, C, 0, "k")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn len_counts_live_entries_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, C, 0, "a", Bytes::from_static(b"1"), None)
        .await
        .unwrap();
    cache
        .put(T, NS, C, 0, "b", Bytes::from_static(b"2"), None)
        .await
        .unwrap();
    cache.delete(T, NS, C, 0, "a").await.unwrap();
    cache
        .put(
            T,
            NS,
            C,
            0,
            "c",
            Bytes::from_static(b"3"),
            Some(Duration::from_millis(1)),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(cache.len().await, 1, "only b is live");
    assert!(!cache.is_empty().await);
}

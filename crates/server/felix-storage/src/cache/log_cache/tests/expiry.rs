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

/// **An entry whose TTL passes is deleted in the log**, so a watch hears it
/// go rather than holding it forever, and one still live is left alone.
#[tokio::test]
async fn an_expired_entry_is_deleted_and_the_observer_is_told() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));
    let short = Some(Duration::from_millis(1));
    cache
        .put(T, NS, C, 0, "gone", Bytes::from_static(b"v"), short)
        .await
        .unwrap();
    cache
        .put(T, NS, C, 0, "kept", Bytes::from_static(b"v"), None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(
        cache.expire_due(T, NS, C, 0, 100, &|| true).await.unwrap(),
        1
    );
    let changes = observer.changes.lock().clone();
    let last = changes.last().expect("a change");
    assert_eq!((last.key.as_str(), last.value.as_ref()), ("gone", None));
    assert_eq!(
        cache.keys(T, NS, C, 0).await.unwrap(),
        vec!["kept".to_string()]
    );
    // Nothing more is due.
    assert_eq!(
        cache.expire_due(T, NS, C, 0, 100, &|| true).await.unwrap(),
        0
    );
}

/// A key refreshed after its old TTL passed keeps its new value: the delete
/// is decided when it is staged, not when the key was found due.
#[tokio::test]
async fn a_refreshed_entry_is_not_expired() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    cache
        .put(
            T,
            NS,
            C,
            0,
            "k",
            Bytes::from_static(b"old"),
            Some(Duration::from_millis(1)),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Found due here, then refreshed before its delete is staged.
    let found_due_at = shard::now_millis();
    cache
        .put(
            T,
            NS,
            C,
            0,
            "k",
            Bytes::from_static(b"new"),
            Some(Duration::from_secs(300)),
        )
        .await
        .unwrap();

    let deleted = cache
        .delete_entry(T, NS, C, 0, "k", DeleteWhen::Expired(found_due_at))
        .await
        .unwrap();
    assert!(!deleted.written);
    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.unwrap().as_deref(),
        Some(&b"new"[..])
    );
}

/// The pass stops as soon as its caller says the shard is no longer its to
/// write, even with more keys due.
#[tokio::test]
async fn an_expiry_pass_stops_when_told_to() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let short = Some(Duration::from_millis(1));
    for key in ["a", "b", "c"] {
        cache
            .put(T, NS, C, 0, key, Bytes::from_static(b"v"), short)
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(20)).await;

    let asked = std::sync::atomic::AtomicUsize::new(0);
    let once = || asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
    assert_eq!(cache.expire_due(T, NS, C, 0, 100, &once).await.unwrap(), 1);
}

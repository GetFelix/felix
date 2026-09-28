use super::*;

/// Every applied write reaches the observer, with the offset the log gave it
/// and the shard's order — a put carries its value, a delete carries none.
#[tokio::test]
async fn the_observer_sees_writes_in_log_order_with_offsets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    cache
        .put(T, NS, C, 0, "a", Bytes::from_static(b"1"), None)
        .await;
    cache
        .put(T, NS, C, 0, "b", Bytes::from_static(b"2"), None)
        .await;
    cache.delete(T, NS, C, 0, "a").await;

    let changes = observer.changes.lock();
    assert_eq!(changes.len(), 3);
    assert_eq!(
        (changes[0].key.as_str(), changes[0].value.as_deref()),
        ("a", Some(&b"1"[..]))
    );
    assert_eq!(
        (changes[1].key.as_str(), changes[1].value.as_deref()),
        ("b", Some(&b"2"[..]))
    );
    assert_eq!(
        (changes[2].key.as_str(), changes[2].value.as_deref()),
        ("a", None),
        "a delete is a change with no value",
    );
    assert_eq!(
        changes.iter().map(|c| c.offset).collect::<Vec<_>>(),
        vec![0, 1, 2],
        "offsets are the log's, in the log's order",
    );
    assert_eq!((changes[0].tenant_id.as_str(), changes[0].shard), (T, 0));
}

/// A delete of a key that was never there appends nothing, so nothing is
/// observed: the observer reports what the log applied, and the log did not
/// change.
#[tokio::test]
async fn a_no_op_delete_is_not_observed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    assert!(cache.delete(T, NS, C, 0, "ghost").await.is_none());

    assert!(
        observer.changes.lock().is_empty(),
        "a delete that appended nothing must observe nothing",
    );
}

/// Compaction rewrites where records live, not what the cache holds, so it
/// must be silent: a watcher told about re-appended live records would see
/// phantom writes nothing performed.
#[tokio::test]
async fn compaction_is_not_observed_as_changes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    let value = Bytes::from(vec![b'x'; 64 * 1024]);
    for _ in 0..40 {
        cache.put(T, NS, C, 0, "hot", value.clone(), None).await;
    }
    cache.compactor.idle().await;

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let state = shard.state.lock().await;
    assert!(
        state.index.log_bytes < 40 * value.len() as u64,
        "the log was never compacted, so this test proved nothing",
    );
    drop(state);

    let changes = observer.changes.lock();
    assert_eq!(
        changes.len(),
        40,
        "compaction re-appended records as observable changes",
    );
}

/// The snapshot reports each live key's current value at the offset that
/// defines it, and leaves out deleted and expired keys.
#[tokio::test]
async fn live_entries_reports_current_state_with_offsets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, C, 0, "kept", Bytes::from_static(b"old"), None)
        .await;
    cache
        .put(T, NS, C, 0, "gone", Bytes::from_static(b"x"), None)
        .await;
    cache
        .put(T, NS, C, 0, "kept", Bytes::from_static(b"new"), None)
        .await;
    cache.delete(T, NS, C, 0, "gone").await;
    cache
        .put(
            T,
            NS,
            C,
            0,
            "brief",
            Bytes::from_static(b"y"),
            Some(Duration::from_millis(1)),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut entries = cache.live_entries(T, NS, C, 0).await.expect("snapshot");
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(entries.len(), 1, "deleted and expired keys are not live");
    assert_eq!(entries[0].key, "kept");
    assert_eq!(&entries[0].value[..], b"new");
    assert_eq!(
        entries[0].offset, 2,
        "the offset is the record that currently defines the key",
    );
}

/// The default store cannot observe writes, and says so — callers must not
/// offer watches over it.
#[tokio::test]
async fn an_ephemeral_cache_refuses_observation() {
    let cache = crate::EphemeralCache::new();
    assert!(!StorageApi::set_change_observer(
        &cache,
        Arc::new(RecordingObserver::default())
    ));
    assert!(cache.live_entries(T, NS, C, 0).await.is_err());
}

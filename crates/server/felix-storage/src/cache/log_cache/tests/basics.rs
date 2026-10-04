use super::*;

#[tokio::test]
async fn a_value_reads_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, C, 0, "k", Bytes::from_static(b"v"), None)
        .await
        .unwrap();

    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.unwrap().as_deref(),
        Some(&b"v"[..])
    );
}

#[tokio::test]
async fn a_missing_key_reads_as_absent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    assert!(cache.get(T, NS, C, 0, "nothing").await.unwrap().is_none());
}

#[tokio::test]
async fn a_later_write_wins() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, C, 0, "k", Bytes::from_static(b"first"), None)
        .await
        .unwrap();
    cache
        .put(T, NS, C, 0, "k", Bytes::from_static(b"second"), None)
        .await
        .unwrap();

    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.unwrap().as_deref(),
        Some(&b"second"[..]),
        "the log is append-only, so the *newest* record has to win",
    );
}

#[tokio::test]
async fn a_delete_hides_the_value_and_returns_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, C, 0, "k", Bytes::from_static(b"v"), None)
        .await
        .unwrap();
    assert_eq!(
        cache.delete(T, NS, C, 0, "k").await.unwrap().as_deref(),
        Some(&b"v"[..])
    );
    assert!(cache.get(T, NS, C, 0, "k").await.unwrap().is_none());
}

/// Caches are scoped, so the same key in two of them is two entries.
#[tokio::test]
async fn caches_do_not_share_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(T, NS, "a", 0, "k", Bytes::from_static(b"a"), None)
        .await
        .unwrap();
    cache
        .put(T, NS, "b", 0, "k", Bytes::from_static(b"b"), None)
        .await
        .unwrap();

    assert_eq!(
        cache.get(T, NS, "a", 0, "k").await.unwrap().as_deref(),
        Some(&b"a"[..])
    );
    assert_eq!(
        cache.get(T, NS, "b", 0, "k").await.unwrap().as_deref(),
        Some(&b"b"[..])
    );
}

/// Tenants are the outermost boundary, and the disk layout has to honour it.
#[tokio::test]
async fn tenants_do_not_share_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put("t1", NS, C, 0, "k", Bytes::from_static(b"one"), None)
        .await
        .unwrap();
    cache
        .put("t2", NS, C, 0, "k", Bytes::from_static(b"two"), None)
        .await
        .unwrap();

    assert_eq!(
        cache.get("t1", NS, C, 0, "k").await.unwrap().as_deref(),
        Some(&b"one"[..])
    );
    assert_eq!(
        cache.get("t2", NS, C, 0, "k").await.unwrap().as_deref(),
        Some(&b"two"[..])
    );
}

/// **A cache survives a restart.** The point of the whole design: the entries
/// are on disk, and the index is rebuilt from them rather than being the only
/// place they ever lived.
#[tokio::test]
async fn a_cache_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let cache = cache(dir.path()).await;
        cache
            .put(T, NS, C, 0, "a", Bytes::from_static(b"1"), None)
            .await
            .unwrap();
        cache
            .put(T, NS, C, 0, "b", Bytes::from_static(b"2"), None)
            .await
            .unwrap();
        cache
            .put(T, NS, C, 0, "a", Bytes::from_static(b"3"), None)
            .await
            .unwrap();
        cache.delete(T, NS, C, 0, "b").await.unwrap();
        cache.shutdown().await.expect("shutdown");
    }

    let reopened = cache(dir.path()).await;
    assert_eq!(
        reopened.get(T, NS, C, 0, "a").await.unwrap().as_deref(),
        Some(&b"3"[..]),
        "the newest value for a key has to survive, not the first",
    );
    assert!(
        reopened.get(T, NS, C, 0, "b").await.unwrap().is_none(),
        "a delete has to survive too, or a restart resurrects deleted keys",
    );
}

/// Records can reach a cache's log without going through `put`.
///
/// That is exactly what replication does to a follower: it appends to the log
/// directly, and the follower may later be promoted and asked to serve what it
/// was shipped. An index built once and trusted forever would answer those
/// reads as misses — a value that is on disk, reported absent.
#[tokio::test]
async fn the_index_catches_up_with_records_appended_behind_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    // Read once so the index exists and believes it is complete.
    cache
        .put_checked(T, NS, C, 0, "first", Bytes::from_static(b"1"), None)
        .await
        .expect("put");
    assert_eq!(
        cache.get_checked(T, NS, C, 0, "first").await.expect("get"),
        Some(Bytes::from_static(b"1")),
    );

    // Now append straight to the log, the way a replica is shipped records.
    let log = cache.shard_log(T, NS, C, 0).await.expect("shard log");
    let payload = CacheOp::Put {
        key: "shipped".to_string(),
        value: Bytes::from_static(b"2"),
        expires_at_millis: 0,
    }
    .encode();
    log.append(&[AppendRecord {
        payload,
        timestamp_micros: 0,
        mark: Default::default(),
        publisher: None,
    }])
    .await
    .expect("append");

    assert_eq!(
        cache
            .get_checked(T, NS, C, 0, "shipped")
            .await
            .expect("get"),
        Some(Bytes::from_static(b"2")),
        "a record on disk was reported absent",
    );
    // And the record that was already indexed is still there.
    assert_eq!(
        cache.get_checked(T, NS, C, 0, "first").await.expect("get"),
        Some(Bytes::from_static(b"1")),
    );
}

/// A generation-start record in a cache log is stepped over, not decoded as a
/// cache op and reported as corruption.
#[tokio::test]
async fn the_index_steps_over_a_generation_start_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    cache
        .put_checked(T, NS, C, 0, "first", Bytes::from_static(b"1"), None)
        .await
        .expect("put");

    let log = cache.shard_log(T, NS, C, 0).await.expect("shard log");
    let shipped = CacheOp::Put {
        key: "shipped".to_string(),
        value: Bytes::from_static(b"2"),
        expires_at_millis: 0,
    }
    .encode();
    log.append(&[
        AppendRecord {
            payload: Bytes::copy_from_slice(&4u64.to_be_bytes()),
            timestamp_micros: 0,
            mark: crate::log::RecordMark::GenerationStart,
            publisher: None,
        },
        AppendRecord {
            payload: shipped,
            timestamp_micros: 0,
            mark: Default::default(),
            publisher: None,
        },
    ])
    .await
    .expect("append");

    assert_eq!(
        cache
            .get_checked(T, NS, C, 0, "shipped")
            .await
            .expect("get"),
        Some(Bytes::from_static(b"2")),
    );
    assert_eq!(
        cache.get_checked(T, NS, C, 0, "first").await.expect("get"),
        Some(Bytes::from_static(b"1")),
    );
}

/// **Records cut from the log and written again leave no trace in the
/// index once it is forgotten.** Replication cuts a superseded suffix and
/// appends the winning log's records at the same offsets; an index caught up
/// from the tail would still point `k` at an offset that now holds another
/// key's put.
#[tokio::test]
async fn a_forgotten_index_reads_the_log_as_it_now_is() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    for value in [&b"v1"[..], &b"v2"[..]] {
        cache
            .put(T, NS, C, 0, "k", Bytes::copy_from_slice(value), None)
            .await
            .expect("put");
    }
    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.expect("get").as_deref(),
        Some(&b"v2"[..])
    );

    let log = cache.shard_log(T, NS, C, 0).await.expect("log");
    log.truncate(1).await.expect("truncate");
    let other = CacheOp::Put {
        key: "other".to_string(),
        value: Bytes::from_static(b"x"),
        expires_at_millis: 0,
    };
    log.append(&[AppendRecord {
        payload: other.encode(),
        timestamp_micros: 0,
        mark: Default::default(),
    }])
    .await
    .expect("append");
    cache.forget_index(T, NS, C, 0).await.expect("forget");

    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.expect("get").as_deref(),
        Some(&b"v1"[..])
    );
    assert_eq!(
        cache
            .get(T, NS, C, 0, "other")
            .await
            .expect("get")
            .as_deref(),
        Some(&b"x"[..])
    );
    // The commit order restarted at the tail, so a write still lands.
    cache
        .put(T, NS, C, 0, "k", Bytes::from_static(b"v3"), None)
        .await
        .expect("put after the reset");
    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.expect("get").as_deref(),
        Some(&b"v3"[..])
    );
}

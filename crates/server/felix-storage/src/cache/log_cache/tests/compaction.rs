use super::*;

/// **Compaction reclaims overwrites and keeps the data.** Without it the log
/// grows forever and "the cache is a log" is a slow leak rather than a design.
#[tokio::test]
async fn compaction_reclaims_overwritten_records() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    // One key, rewritten until the log is mostly garbage. The value is large
    // enough that the floor is crossed without writing for a minute.
    let value = Bytes::from(vec![b'x'; 64 * 1024]);
    for _ in 0..40 {
        cache.put(T, NS, C, 0, "hot", value.clone(), None).await;
    }
    cache.compactor.idle().await;

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let state = shard.state.lock().await;
    assert!(
        state.index.log_bytes < 40 * value.len() as u64,
        "the log was never compacted: {} bytes for 40 writes of {}",
        state.index.log_bytes,
        value.len(),
    );
    drop(state);

    assert_eq!(
        cache.get(T, NS, C, 0, "hot").await.map(|v| v.len()),
        Some(value.len()),
        "compaction must not lose the value it is compacting around",
    );
}

/// Compaction drops expired entries rather than copying them forward.
#[tokio::test]
async fn compaction_drops_expired_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    cache
        .put(
            T,
            NS,
            C,
            0,
            "doomed",
            Bytes::from(vec![b'y'; 32 * 1024]),
            Some(Duration::from_millis(1)),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let value = Bytes::from(vec![b'x'; 64 * 1024]);
    for _ in 0..40 {
        cache.put(T, NS, C, 0, "hot", value.clone(), None).await;
    }
    cache.compactor.idle().await;

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let state = shard.state.lock().await;
    assert!(
        !state.index.entries.contains_key("doomed"),
        "an expired entry was carried through compaction",
    );
}

/// A compacted cache still reopens. Compaction deletes segments, so a bug
/// there would be invisible until the next restart.
#[tokio::test]
async fn a_compacted_cache_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let value = Bytes::from(vec![b'x'; 64 * 1024]);
    {
        let cache = cache(dir.path()).await;
        for _ in 0..40 {
            cache.put(T, NS, C, 0, "hot", value.clone(), None).await;
        }
        cache
            .put(T, NS, C, 0, "cold", Bytes::from_static(b"kept"), None)
            .await;
        cache.compactor.idle().await;
        cache.shutdown().await.expect("shutdown");
    }

    let reopened = cache(dir.path()).await;
    assert_eq!(
        reopened.get(T, NS, C, 0, "hot").await.map(|v| v.len()),
        Some(value.len())
    );
    assert_eq!(
        reopened.get(T, NS, C, 0, "cold").await.as_deref(),
        Some(&b"kept"[..])
    );
}

/// Compaction must not renumber the log.
///
/// A cache shard is replicated by shipping its records at their offsets, so an
/// offset has to mean the same record on the leader and on every follower, for
/// the life of the shard. A compaction that restarts numbering makes the
/// leader's offset 0 a different record from the follower's, and the two logs
/// have silently diverged with no way to tell.
#[tokio::test]
async fn compaction_does_not_rewind_the_offset_space() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    // Enough overwriting of one key to put the log well past the compaction
    // threshold while the live set stays tiny.
    let value = Bytes::from(vec![b'x'; 4096]);
    for _ in 0..64 {
        cache
            .put_checked(T, NS, C, 0, "k", value.clone(), None)
            .await
            .expect("put");
    }

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let before = {
        let state = shard.state.lock().await;
        state.log.tail_offset().await.expect("tail")
    };

    shard.compact().await.expect("compact");

    let after = {
        let state = shard.state.lock().await;
        state.log.tail_offset().await.expect("tail")
    };

    assert!(
        after >= before,
        "compaction rewound the log from {before} to {after}; \
         every offset a follower already holds now names a different record",
    );
    assert_eq!(
        cache.get_checked(T, NS, C, 0, "k").await.expect("get"),
        Some(value),
        "compaction must keep the live set readable",
    );
}

/// The offset space keeps growing across repeated compactions and a restart.
///
/// One compaction preserving the tail is not enough: the base offset has to
/// survive being written to disk and read back, or the shard rewinds the next
/// time the process starts and a follower's history stops matching.
#[tokio::test]
async fn the_offset_space_survives_compaction_and_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let value = Bytes::from(vec![b'x'; 4096]);
    let mut high_water = 0;

    for round in 0..3 {
        let cache = cache(dir.path()).await;
        for _ in 0..48 {
            cache
                .put_checked(T, NS, C, 0, "k", value.clone(), None)
                .await
                .expect("put");
        }

        let shard = cache.shard(T, NS, C, 0).expect("shard");
        shard.compact().await.expect("compact");
        let tail = {
            let state = shard.state.lock().await;
            state.log.tail_offset().await.expect("tail")
        };

        assert!(
            tail > high_water,
            "round {round}: tail went from {high_water} to {tail}",
        );
        high_water = tail;

        assert_eq!(
            cache.get_checked(T, NS, C, 0, "k").await.expect("get"),
            Some(value.clone()),
        );
        cache.shutdown().await.expect("shutdown");
    }
}

/// **A write never waits on compaction.** Compaction is held for as long as
/// the test likes, standing in for one rewriting a large live set on a slow
/// device, and every put, including the one that crosses the threshold, still
/// completes promptly.
#[tokio::test]
async fn writes_do_not_wait_on_a_slow_compaction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    cache.compactor.hold();

    let value = Bytes::from(vec![b'x'; 64 * 1024]);
    for i in 0..40 {
        tokio::time::timeout(
            Duration::from_secs(5),
            cache.put_checked(T, NS, C, 0, "hot", value.clone(), None),
        )
        .await
        .unwrap_or_else(|_| panic!("put {i} waited on compaction"))
        .expect("put");
    }
    tokio::time::timeout(
        Duration::from_secs(5),
        cache.put_checked(T, NS, C, 0, "cold", Bytes::from_static(b"kept"), None),
    )
    .await
    .expect("a put to another key waited on compaction")
    .expect("put");

    cache.compactor.release();
    cache.compactor.idle().await;

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let log_bytes = shard.state.lock().await.index.log_bytes;
    assert!(
        log_bytes < 40 * value.len() as u64,
        "the held compaction never ran once released: {log_bytes} bytes",
    );
    assert_eq!(
        cache.get(T, NS, C, 0, "hot").await.map(|v| v.len()),
        Some(value.len())
    );
    assert_eq!(
        cache.get(T, NS, C, 0, "cold").await.as_deref(),
        Some(&b"kept"[..])
    );
}

/// A pass trims the log's head: the base moves up past everything below the
/// cut, and the live set is still there, in memory and after a restart.
#[tokio::test]
async fn compaction_trims_the_head_and_keeps_the_live_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let expected = overwritten(&cache, 200).await;
    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let tail_before = shard.current_log().await.tail_offset().await.expect("tail");

    shard.compact().await.expect("compact");

    let log = shard.current_log().await;
    assert!(
        log.base_offset() >= tail_before,
        "base {} is below the pre-compaction tail {tail_before}: garbage was kept",
        log.base_offset(),
    );
    assert_reads(&cache, &expected, "after compaction").await;
    cache.shutdown().await.expect("shutdown");
    let reopened = super::cache(dir.path()).await;
    assert_reads(&reopened, &expected, "after compaction and a restart").await;
}

/// **A crash mid-compaction recovers.** Half the live set copied forward and
/// nothing trimmed is exactly what a crash between two batches leaves, and it
/// must replay to the same cache, which a later pass then finishes.
#[tokio::test]
async fn a_crash_mid_compaction_replays_to_the_same_cache() {
    let dir = tempfile::tempdir().expect("tempdir");
    let expected = {
        let cache = cache(dir.path()).await;
        let expected = overwritten(&cache, 200).await;
        let shard = cache.shard(T, NS, C, 0).expect("shard");
        let log = shard.current_log().await;
        let cut = log.roll_now().await.expect("roll");
        let below = shard.live_below(cut).await.expect("live");
        assert!(below.len() > 2 * crate::compaction::COPY_BATCH);
        for batch in below[..below.len() / 2].chunks(crate::compaction::COPY_BATCH) {
            assert!(shard.copy_forward(&log, batch).await.expect("copy"));
        }
        // No shutdown: the process dies here.
        expected
    };

    let reopened = cache(dir.path()).await;
    assert_reads(&reopened, &expected, "after a crash mid-compaction").await;

    let shard = reopened.shard(T, NS, C, 0).expect("shard");
    shard
        .compact()
        .await
        .expect("a later pass finishes the job");
    assert_reads(&reopened, &expected, "after the later pass").await;
    reopened.shutdown().await.expect("shutdown");
    let again = cache(dir.path()).await;
    assert_reads(&again, &expected, "after the later pass and a restart").await;
}

/// A crash partway through deleting the trimmed segments leaves the newer of
/// them behind. That is a longer log, not a broken one.
#[tokio::test]
async fn a_crash_mid_trim_leaves_a_longer_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let expected = {
        let cache = cache(dir.path()).await;
        let expected = overwritten(&cache, 200).await;
        let shard = cache.shard(T, NS, C, 0).expect("shard");
        let log = shard.current_log().await;
        let cut = log.roll_now().await.expect("roll");
        let below = shard.live_below(cut).await.expect("live");
        for batch in below.chunks(crate::compaction::COPY_BATCH) {
            assert!(shard.copy_forward(&log, batch).await.expect("copy"));
        }
        log.sync().await.expect("sync");
        // Only the oldest segment is gone when the process dies.
        let oldest = log.segments()[0].id;
        assert!(
            log.segments()[1].last_offset < cut,
            "want two segments below the cut"
        );
        drop(log);
        drop(shard);
        drop(cache);
        let shard_dir = layout::shard_dir(
            dir.path(),
            &crate::log::ShardKey {
                tenant: T.to_string(),
                namespace: NS.to_string(),
                stream: C.to_string(),
                shard: 0,
            },
        );
        std::fs::remove_file(shard_dir.join(crate::segment::segment_file_name(oldest)))
            .expect("unlink the oldest segment");
        expected
    };

    let reopened = cache(dir.path()).await;
    assert_reads(&reopened, &expected, "after a crash mid-trim").await;
}

/// Shutdown abandons a pass that cannot make progress rather than waiting on
/// it, and what the abandoned pass left replays to the same cache.
#[tokio::test]
async fn shutdown_abandons_a_held_compaction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let value = Bytes::from(vec![b'x'; 64 * 1024]);
    {
        let cache = cache(dir.path()).await;
        cache.compactor.hold();
        for _ in 0..40 {
            cache
                .put_checked(T, NS, C, 0, "hot", value.clone(), None)
                .await
                .expect("put");
        }
        // A pass of our own, so there is certainly one parked on the budget
        // whatever the puts' own passes managed before the hold bit.
        let shard = cache.shard(T, NS, C, 0).expect("shard");
        let (done, finished) = tokio::sync::oneshot::channel();
        assert!(cache.compactor.spawn(async move {
            let _ = done.send(shard.compact().await);
        }));
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::time::timeout(Duration::from_secs(5), cache.shutdown())
            .await
            .expect("shutdown waited on a held compaction")
            .expect("shutdown");
        finished
            .await
            .expect("the pass ran")
            .expect("an abandoned pass is not an error");
    }
    let reopened = cache(dir.path()).await;
    assert_eq!(
        reopened.get(T, NS, C, 0, "hot").await.map(|v| v.len()),
        Some(value.len())
    );
}

/// A key with a write staged but not yet applied is not copied: the copy
/// would land after the write and bring the old value back on replay.
#[tokio::test]
async fn a_key_with_a_write_in_flight_is_not_copied() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let expected = overwritten(&cache, 4).await;
    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let log = shard.current_log().await;
    let cut = log.roll_now().await.expect("roll");
    let below = shard.live_below(cut).await.expect("live");
    let tail = log.tail_offset().await.expect("tail");

    let in_flight: Vec<_> = expected
        .iter()
        .map(|(key, _)| shard.key_in_flight(key))
        .collect();
    assert!(shard.copy_forward(&log, &below).await.expect("copy"));
    assert_eq!(
        log.tail_offset().await.expect("tail"),
        tail,
        "copied a key whose write was still in flight",
    );

    drop(in_flight);
    assert!(shard.copy_forward(&log, &below).await.expect("copy"));
    assert_eq!(log.tail_offset().await.expect("tail"), tail + 4);
}

/// The replica state in the shard directory (accepted generation, commit
/// offset) survives compaction, which no longer replaces the directory.
#[tokio::test]
async fn replica_state_survives_compaction() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let cache = cache(dir.path()).await;
        overwritten(&cache, 200).await;
        let log = cache.shard_log(T, NS, C, 0).await.expect("log");
        log.accept_generation(7).await.expect("accept");
        cache
            .shard(T, NS, C, 0)
            .expect("shard")
            .compact()
            .await
            .expect("compact");
        cache.shutdown().await.expect("shutdown");
    }
    let reopened = cache(dir.path()).await;
    let log = reopened.shard_log(T, NS, C, 0).await.expect("log");
    assert_eq!(log.accepted_generation(), 7);
}

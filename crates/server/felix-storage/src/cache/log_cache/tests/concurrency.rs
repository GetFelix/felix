use super::*;

/// The group-commit regression (`docs/cache-put-group-commit-plan.md`): the
/// write path must not hold the shard lock across the fsync, or every writer
/// pays a whole device flush and concurrency buys nothing.
///
/// Asserted by counting device flushes: 32 acked `OnCommit` puts from 8
/// concurrent writers must share flushes. The serialised path performed
/// exactly one flush per put — reverting the stage/commit/apply split makes
/// this count 32 again and the assertion fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_share_fsyncs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(
        LogCache::open(
            dir.path(),
            LogConfig {
                segment_size_bytes: 1024 * 1024,
                fsync_mode: crate::log::FsyncMode::OnCommit,
                preallocate_segments: false,
                ..LogConfig::default()
            },
        )
        .expect("open"),
    );

    let writers = 8;
    let puts_per_writer = 4;
    let mut tasks = Vec::new();
    for writer in 0..writers {
        let cache = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            for round in 0..puts_per_writer {
                cache
                    .put_checked(
                        T,
                        NS,
                        C,
                        0,
                        &format!("w{writer}-r{round}"),
                        Bytes::from(format!("v{writer}-{round}")),
                        None,
                    )
                    .await
                    .expect("put");
            }
        }));
    }
    for task in tasks {
        task.await.expect("writer");
    }

    let total = (writers * puts_per_writer) as u64;
    let log = cache.shard_log(T, NS, C, 0).await.expect("log");
    let flushes = log.flushes_performed();
    assert!(
        flushes < total,
        "{total} acked OnCommit puts took {flushes} device flushes: \
         every writer paid its own fsync, so the flushes were not shared",
    );

    // Every acked write is readable: sharing flushes must not lose acks.
    for writer in 0..writers {
        for round in 0..puts_per_writer {
            assert_eq!(
                cache
                    .get(T, NS, C, 0, &format!("w{writer}-r{round}"))
                    .await
                    .unwrap()
                    .as_deref(),
                Some(format!("v{writer}-{round}").as_bytes()),
            );
        }
    }
}

/// Stage a write by hand — offset claimed, turn reserved, fsync *not* run —
/// so the window between staging and commit can be held open and examined.
async fn stage_without_committing(
    cache: &LogCache,
    key: &str,
    value: &[u8],
) -> (
    Arc<CacheShard>,
    crate::disk_log::PendingAppend,
    DiskLog,
    CacheOp,
) {
    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let mut state = shard.state.lock().await;
    shard.ensure_index(&mut state).await.expect("index");
    let op = CacheOp::Put {
        key: key.to_string(),
        value: Bytes::copy_from_slice(value),
        expires_at_millis: 0,
    };
    let pending = state
        .log
        .append_pending(&[AppendRecord {
            payload: op.encode(),
            timestamp_micros: now_millis() * 1000,
            mark: Default::default(),
        }])
        .await
        .expect("stage");
    // Reserve and immediately leak the turn's effect the way a writer's guard
    // would on drop: tests below want the *unresolved* window, so they hold
    // the turn by re-reserving through the shard when they need it. Here the
    // turn is deliberately dropped only by the caller's choice.
    let log = state.log.clone();
    state.sequenced_through = Some(pending.last_offset() + 1);
    drop(state);
    (shard, pending, log, op)
}

/// **Durability before visibility.** A staged write whose fsync has not
/// happened must not be readable and must not reach a watcher — a reader that
/// saw it would be seeing a put a crash could still lose.
#[tokio::test]
async fn a_staged_write_is_invisible_until_committed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    let (shard, pending, log, _op) = stage_without_committing(&cache, "k", b"staged").await;
    let turn = shard
        .sequencer
        .reserve(pending.first_offset(), pending.last_offset() + 1);

    // The record is on disk holding its offset, but not committed or applied.
    assert!(
        cache.get(T, NS, C, 0, "k").await.unwrap().is_none(),
        "a staged, uncommitted write became readable",
    );
    assert!(
        observer.changes.lock().is_empty(),
        "a staged, uncommitted write reached the observer",
    );

    // Once the fsync has run and the turn resolves, the index may fold it.
    log.commit(&pending).await.expect("commit");
    drop(turn);
    assert_eq!(
        cache.get(T, NS, C, 0, "k").await.unwrap().as_deref(),
        Some(&b"staged"[..]),
        "a committed record must become readable once the sequence passes it",
    );
}

/// A writer stages while an earlier writer's commit is still unfinished —
/// the overlap that lets one fsync serve both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_stages_behind_an_unfinished_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(cache(dir.path()).await);

    let (shard, pending, log, _op) = stage_without_committing(&cache, "first", b"one").await;
    let turn = shard
        .sequencer
        .reserve(pending.first_offset(), pending.last_offset() + 1);

    // The second writer must be able to claim its offset while the first is
    // uncommitted; it then queues behind the first's turn.
    let second = {
        let cache = Arc::clone(&cache);
        tokio::spawn(async move {
            cache
                .put_checked(T, NS, C, 0, "second", Bytes::from_static(b"two"), None)
                .await
        })
    };
    let staged = async {
        loop {
            if log.tail_offset().await.expect("tail") > pending.last_offset() + 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), staged)
        .await
        .expect("the second writer never staged: the lock was held across the commit");
    assert!(
        !second.is_finished(),
        "the second writer overtook the first"
    );

    log.commit(&pending).await.expect("commit");
    drop(turn);
    tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("second writer")
        .expect("join")
        .expect("put");
    assert_eq!(
        cache.get(T, NS, C, 0, "second").await.unwrap().as_deref(),
        Some(&b"two"[..]),
    );
}

/// A put whose caller is dropped after its record is on disk is still
/// finished: applied, and reported to watchers, like any other write. The log
/// and its followers hold the record either way, so a watcher that never
/// hears of it has diverged from them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_put_is_still_applied() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(cache(dir.path()).await);
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    // Hold an earlier turn so the put stages and then waits.
    let (shard, pending, log, _op) = stage_without_committing(&cache, "first", b"one").await;
    let turn = shard
        .sequencer
        .reserve(pending.first_offset(), pending.last_offset() + 1);
    let put = {
        let cache = Arc::clone(&cache);
        tokio::spawn(async move {
            cache
                .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"v"), None)
                .await
        })
    };
    let staged = async {
        while log.tail_offset().await.expect("tail") <= pending.last_offset() + 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), staged)
        .await
        .expect("the put never staged");
    put.abort();
    assert!(put.await.expect_err("aborted").is_cancelled());

    log.commit(&pending).await.expect("commit");
    drop(turn);
    let applied = async {
        while cache.get(T, NS, C, 0, "k").await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), applied)
        .await
        .expect("the cancelled put never reached the index");
    assert!(
        observer
            .changes
            .lock()
            .iter()
            .any(|change| change.key == "k"),
        "the observer never saw the cancelled put",
    );
}

/// A writer abandoned mid-flight — commit never run, turn dropped, the shape
/// of a cancelled future — must not strand the writers behind it.
#[tokio::test]
async fn an_abandoned_write_does_not_strand_the_shard() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    let (shard, pending, _log, _op) = stage_without_committing(&cache, "gone", b"never").await;
    drop(
        shard
            .sequencer
            .reserve(pending.first_offset(), pending.last_offset() + 1),
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        cache.put_checked(T, NS, C, 0, "after", Bytes::from_static(b"lives"), None),
    )
    .await
    .expect("the abandoned write stranded the shard")
    .expect("put");
    assert_eq!(
        cache.get(T, NS, C, 0, "after").await.unwrap().as_deref(),
        Some(&b"lives"[..]),
    );
}

/// Concurrent writers to one key: the index and every watcher settle on the
/// record with the highest offset — disk order, not completion order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_to_one_key_settle_on_the_highest_offset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(cache(dir.path()).await);
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    let mut tasks = Vec::new();
    for writer in 0..8 {
        let cache = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            for round in 0..5 {
                cache
                    .put_checked(
                        T,
                        NS,
                        C,
                        0,
                        "contended",
                        Bytes::from(format!("w{writer}-r{round}")),
                        None,
                    )
                    .await
                    .expect("put");
            }
        }));
    }
    for task in tasks {
        task.await.expect("writer");
    }

    let value = {
        let changes = observer.changes.lock();
        assert_eq!(changes.len(), 40);
        for pair in changes.windows(2) {
            assert!(
                pair[0].offset < pair[1].offset,
                "watch order disagreed with disk order: {} then {}",
                pair[0].offset,
                pair[1].offset,
            );
        }
        let winner = changes.last().expect("changes");
        winner.value.clone().expect("a put carries its value")
    };
    assert_eq!(
        cache
            .get(T, NS, C, 0, "contended")
            .await
            .unwrap()
            .as_deref(),
        Some(&value[..]),
        "the index winner must be the highest offset, as the log reads",
    );
}

/// Compaction and concurrent writers coexist: nothing acked is lost, offsets
/// stay monotone, and the log still gets compacted once the storm passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_under_concurrent_writers_loses_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(cache(dir.path()).await);
    let observer = Arc::new(RecordingObserver::default());
    assert!(cache.set_change_observer(observer.clone()));

    // Each writer overwrites its own key with values large enough to push the
    // log well past the compaction threshold.
    let payload = vec![7u8; 64 * 1024];
    let mut tasks = Vec::new();
    for writer in 0..4 {
        let cache = Arc::clone(&cache);
        let payload = payload.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..40 {
                cache
                    .put_checked(
                        T,
                        NS,
                        C,
                        0,
                        &format!("key-{writer}"),
                        Bytes::from(payload.clone()),
                        None,
                    )
                    .await
                    .expect("put");
            }
        }));
    }
    for task in tasks {
        task.await.expect("writer");
    }
    cache.compactor.idle().await;

    {
        let changes = observer.changes.lock();
        for pair in changes.windows(2) {
            assert!(pair[0].offset < pair[1].offset, "offsets went backwards");
        }
    }

    for writer in 0..4 {
        let value = cache
            .get(T, NS, C, 0, &format!("key-{writer}"))
            .await
            .unwrap()
            .expect("key survived");
        assert_eq!(value.len(), payload.len());
    }

    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let state = shard.state.lock().await;
    let written = 4u64 * 40 * payload.len() as u64;
    assert!(
        state.index.log_bytes < written,
        "{} bytes on the log for {written} written: compaction never ran",
        state.index.log_bytes,
    );
    drop(state);

    // Replay of the compacted log is the same cache.
    cache.shutdown().await.expect("shutdown");
    let reopened = super::cache(dir.path()).await;
    for writer in 0..4 {
        let value = reopened
            .get(T, NS, C, 0, &format!("key-{writer}"))
            .await
            .unwrap()
            .expect("key survived a restart");
        assert_eq!(value.len(), payload.len());
    }
}

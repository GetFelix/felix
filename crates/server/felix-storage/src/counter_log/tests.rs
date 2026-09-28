//! The counter behaving as a fold, and as a log.
use super::*;
use crate::log::FsyncMode;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 64 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn store(dir: &std::path::Path) -> CounterStore {
    CounterStore::open(dir, config()).expect("open")
}

const T: &str = "t1";
const NS: &str = "ns";
const C: &str = "metrics";

/// The whole semantic: the sum is the fold over the deltas, negatives
/// included, and each add answers with the sum including itself.
#[tokio::test]
async fn the_sum_is_the_fold_over_the_deltas() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store(dir.path());

    let (sum, first) = store.add(T, NS, C, 0, "k", 5).await.expect("add");
    assert_eq!(sum, 5);
    let (sum, second) = store.add(T, NS, C, 0, "k", -2).await.expect("add");
    assert_eq!(sum, 3);
    assert!(second > first, "each delta consumes the next offset");
    assert_eq!(store.get(T, NS, C, 0, "k").await.expect("get"), Some(3));
}

/// Never-written and summed-to-zero are different answers.
#[tokio::test]
async fn an_untouched_counter_is_absent_not_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store(dir.path());

    assert_eq!(store.get(T, NS, C, 0, "nothing").await.expect("get"), None);
    store.add(T, NS, C, 0, "k", 7).await.expect("add");
    store.add(T, NS, C, 0, "k", -7).await.expect("add");
    assert_eq!(
        store.get(T, NS, C, 0, "k").await.expect("get"),
        Some(0),
        "a counter whose deltas cancel exists, at zero",
    );
}

/// Two keys in one shard fold independently, and two scopes do too.
#[tokio::test]
async fn keys_and_scopes_fold_independently() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store(dir.path());

    store.add(T, NS, C, 0, "a", 1).await.expect("add");
    store.add(T, NS, C, 0, "b", 10).await.expect("add");
    store.add(T, NS, "other", 0, "a", 100).await.expect("add");

    assert_eq!(store.get(T, NS, C, 0, "a").await.expect("get"), Some(1));
    assert_eq!(store.get(T, NS, C, 0, "b").await.expect("get"), Some(10));
    assert_eq!(
        store.get(T, NS, "other", 0, "a").await.expect("get"),
        Some(100)
    );
}

/// **The sum survives a restart.** Nothing in memory is trusted: the fold is
/// rebuilt from the log, exactly as the cache index is.
#[tokio::test]
async fn the_sum_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let store = store(dir.path());
        store.add(T, NS, C, 0, "k", 41).await.expect("add");
        store.add(T, NS, C, 0, "k", 1).await.expect("add");
        store.shutdown().await.expect("shutdown");
    }
    let reopened = store(dir.path());
    assert_eq!(reopened.get(T, NS, C, 0, "k").await.expect("get"), Some(42));
}

/// **Compaction changes neither the observable sum nor the log's numbering.**
/// The acceptance regression of #350: enough overwriting to force compaction,
/// then the sum is what the deltas say and the next offset continues past the
/// reclaimed history rather than restarting at zero.
#[tokio::test]
async fn compaction_moves_neither_the_sum_nor_the_offsets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store(dir.path());

    // Small records, so the floor is crossed by count; every add lands on one
    // key, so the live set is one checkpoint and the ratio trips.
    let mut expected: i64 = 0;
    let mut last_offset = 0;
    for i in 0..4000i64 {
        let delta = if i % 3 == 0 { -1 } else { 2 };
        expected += delta;
        let (sum, offset) = store.add(T, NS, C, 0, "hot", delta).await.expect("add");
        assert_eq!(sum, expected, "the running answer drifted at add {i}");
        assert!(
            offset >= last_offset,
            "offset went backwards across compaction: {offset} after {last_offset}",
        );
        last_offset = offset;
    }
    store.compactor.idle().await;

    let shard = store.shard(T, NS, C, 0).expect("shard");
    let state = shard.state.lock().await;
    assert!(
        state.index.log_bytes < 4000 * 24,
        "the log was never compacted: {} bytes",
        state.index.log_bytes,
    );
    drop(state);

    assert_eq!(
        store.get(T, NS, C, 0, "hot").await.expect("get"),
        Some(expected),
    );

    // And the whole thing still reopens to the same answer: the checkpoint on
    // disk is the fold, restated.
    store.shutdown().await.expect("shutdown");
    let reopened = CounterStore::open(dir.path(), config()).expect("reopen");
    assert_eq!(
        reopened.get(T, NS, C, 0, "hot").await.expect("get"),
        Some(expected),
    );
}

/// **An add never waits on compaction.** Compaction is held indefinitely, and
/// every add, including the one that crosses the threshold, still completes.
#[tokio::test]
async fn adds_do_not_wait_on_a_slow_compaction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store(dir.path());
    store.compactor.hold();

    for i in 0..4000i64 {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.add(T, NS, C, 0, "hot", 1),
        )
        .await
        .unwrap_or_else(|_| panic!("add {i} waited on compaction"))
        .expect("add");
    }

    store.compactor.release();
    store.compactor.idle().await;
    let shard = store.shard(T, NS, C, 0).expect("shard");
    let log_bytes = shard.state.lock().await.index.log_bytes;
    assert!(
        log_bytes < 4000 * 24,
        "the held compaction never ran once released: {log_bytes} bytes",
    );
    assert_eq!(
        store.get(T, NS, C, 0, "hot").await.expect("get"),
        Some(4000)
    );
}

/// A pass trims the head and the sums survive it, a crash partway through it,
/// and a restart. The crash is a pass stopped after its first batch of
/// checkpoints, which is what a process death between batches leaves.
#[tokio::test]
async fn a_crash_mid_compaction_keeps_every_sum() {
    let dir = tempfile::tempdir().expect("tempdir");
    let keys = 3 * crate::compaction::COPY_BATCH;
    {
        let store = store(dir.path());
        for round in 0..3i64 {
            for key in 0..keys {
                store
                    .add(T, NS, C, 0, &format!("c{key}"), round + key as i64)
                    .await
                    .expect("add");
            }
        }
        store.compactor.hold();
        let shard = store.shard(T, NS, C, 0).expect("shard");
        let pass = tokio::spawn(async move { shard.compact().await });
        // Let the pass seal its cut and park on the budget, then stop it the
        // way a dying process would: nothing after this point happens.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        pass.abort();
        let _ = pass.await;
    }

    let expected = |key: usize| 3 + 3 * key as i64;
    let reopened = store(dir.path());
    for key in 0..keys {
        assert_eq!(
            reopened
                .get(T, NS, C, 0, &format!("c{key}"))
                .await
                .expect("get"),
            Some(expected(key)),
        );
    }

    let shard = reopened.shard(T, NS, C, 0).expect("shard");
    let tail_before = shard.current_log().await.tail_offset().await.expect("tail");
    shard
        .compact()
        .await
        .expect("a later pass finishes the job");
    assert!(shard.current_log().await.base_offset() >= tail_before);
    reopened.shutdown().await.expect("shutdown");

    let again = store(dir.path());
    for key in 0..keys {
        assert_eq!(
            again
                .get(T, NS, C, 0, &format!("c{key}"))
                .await
                .expect("get"),
            Some(expected(key)),
            "a sum moved across compaction and a restart",
        );
    }
}

/// The fold catches up with records that reached the log without going
/// through `add` — a shipped follower's log, later asked for the sum.
#[tokio::test]
async fn the_fold_catches_up_with_records_appended_behind_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store(dir.path());
    store.add(T, NS, C, 0, "k", 1).await.expect("add");

    // Write to the shard's log directly, as replication does.
    let log = store.shard_log(T, NS, C, 0).await.expect("log");
    log.append(&[crate::log::AppendRecord {
        payload: CounterOp::Delta {
            key: "k".to_string(),
            delta: 9,
        }
        .encode(),
        timestamp_micros: 1,
        mark: Default::default(),
    }])
    .await
    .expect("append behind the fold");

    assert_eq!(
        store.get(T, NS, C, 0, "k").await.expect("get"),
        Some(10),
        "the fold trusted itself instead of the log",
    );
}

/// A generation-start record in a counter log is stepped over, not folded as
/// a delta or reported as corruption.
#[tokio::test]
async fn the_fold_steps_over_a_generation_start_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store(dir.path());
    store.add(T, NS, C, 0, "k", 1).await.expect("add");

    let log = store.shard_log(T, NS, C, 0).await.expect("log");
    log.append(&[
        crate::log::AppendRecord {
            payload: bytes::Bytes::copy_from_slice(&4u64.to_be_bytes()),
            timestamp_micros: 1,
            mark: crate::log::RecordMark::GenerationStart,
        },
        crate::log::AppendRecord {
            payload: CounterOp::Delta {
                key: "k".to_string(),
                delta: 9,
            }
            .encode(),
            timestamp_micros: 1,
            mark: Default::default(),
        },
    ])
    .await
    .expect("append behind the fold");

    assert_eq!(store.get(T, NS, C, 0, "k").await.expect("get"), Some(10));
}

/// The forwarded-sum bytes round trip, and the wrong width is refused.
#[test]
fn a_forwarded_sum_round_trips() {
    assert_eq!(decode_sum(&encode_sum(-42)).expect("decode"), -42);
    assert_eq!(decode_sum(&encode_sum(i64::MAX)).expect("decode"), i64::MAX);
    assert!(decode_sum(b"seven").is_err());
}

/// A shard that moved away is closed, refuses work that found it before the
/// close, and comes back from disk with its sum.
#[tokio::test]
async fn a_closed_shard_is_refused_then_reopens_with_its_sum() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store(dir.path());
    store.add(T, NS, C, 0, "k", 5).await.expect("add");
    let old = store.shard_log(T, NS, C, 0).await.expect("log");
    let found = store.shard(T, NS, C, 0).expect("shard");

    store.close_shard(T, NS, C, 0).await.expect("close");
    assert!(old.is_closed());
    {
        let mut state = found.state.lock().await;
        assert!(matches!(
            found.ensure_index(&mut state).await,
            Err(StorageError::Closed(_))
        ));
    }

    assert_eq!(store.get(T, NS, C, 0, "k").await.expect("get"), Some(5));
    let (sum, _) = store.add(T, NS, C, 0, "k", 2).await.expect("add");
    assert_eq!(sum, 7);
    assert!(!store.shard_log(T, NS, C, 0).await.expect("log").is_closed());
}

/// A counter shard an older build left mid-swap opens with every sum.
#[tokio::test]
async fn a_shard_left_mid_swap_by_an_older_build_keeps_every_sum() {
    use crate::legacy_swap::fixture::{Stop, assert_no_siblings, old_swap};

    for stop in Stop::ALL {
        let root = tempfile::tempdir().expect("tempdir");
        let expected: Vec<(String, i64)> = (0..20).map(|i| (format!("k{i}"), 3 * i - 7)).collect();
        {
            let store = store(root.path());
            for (key, sum) in &expected {
                store.add(T, NS, C, 0, key, *sum + 1).await.expect("add");
                store.add(T, NS, C, 0, key, -1).await.expect("add");
            }
            store.shutdown().await.expect("shutdown");
        }
        let dir = layout::shard_dir(
            root.path(),
            &ShardKey {
                tenant: T.into(),
                namespace: NS.into(),
                stream: C.into(),
                shard: 0,
            },
        );
        let live = expected
            .iter()
            .map(|(key, sum)| {
                CounterOp::Checkpoint {
                    key: key.clone(),
                    sum: *sum,
                }
                .encode()
            })
            .collect();
        old_swap(&dir, config(), live, stop).await;

        let when = format!("after an old swap stopped at {stop:?}");
        let store = store(root.path());
        for (key, sum) in &expected {
            assert_eq!(
                store.get(T, NS, C, 0, key).await.expect("get"),
                Some(*sum),
                "{key} {when}",
            );
        }
        assert_no_siblings(&dir, &when);
        store.shutdown().await.expect("shutdown");
    }
}

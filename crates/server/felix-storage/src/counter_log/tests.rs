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

/// A crash between compaction's two renames leaves the shard directory
/// missing and every delta in `.retired` (plus a finished `.compacting`). An
/// open that ignored that would start every counter at zero.
#[tokio::test]
async fn a_shard_interrupted_mid_compaction_keeps_its_sums() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shard_dir = layout::shard_dir(
        dir.path(),
        &ShardKey {
            tenant: T.to_string(),
            namespace: NS.to_string(),
            stream: C.to_string(),
            shard: 0,
        },
    );
    {
        let store = store(dir.path());
        store.add(T, NS, C, 0, "a", 40).await.expect("add");
        store.add(T, NS, C, 0, "a", 2).await.expect("add");
        store.add(T, NS, C, 0, "b", -3).await.expect("add");
        store.shutdown().await.expect("shutdown");
    }

    // Exactly the state a crash between the renames leaves behind.
    std::fs::rename(&shard_dir, shard_dir.with_extension("retired")).expect("retire");
    std::fs::create_dir_all(shard_dir.with_extension("compacting")).expect("staging");

    let reopened = store(dir.path());
    assert_eq!(reopened.get(T, NS, C, 0, "a").await.expect("get"), Some(42));
    assert_eq!(reopened.get(T, NS, C, 0, "b").await.expect("get"), Some(-3));
    assert!(!shard_dir.with_extension("retired").exists());
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

/// The forwarded-sum bytes round trip, and the wrong width is refused.
#[test]
fn a_forwarded_sum_round_trips() {
    assert_eq!(decode_sum(&encode_sum(-42)).expect("decode"), -42);
    assert_eq!(decode_sum(&encode_sum(i64::MAX)).expect("decode"), i64::MAX);
    assert!(decode_sum(b"seven").is_err());
}

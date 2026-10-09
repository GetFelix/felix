use super::*;
use crate::cache::{CacheCondition, ConditionalWrite};

async fn put_if(
    cache: &LogCache,
    key: &str,
    value: &'static [u8],
    condition: CacheCondition,
) -> ConditionalWrite {
    cache
        .put_if_checked(T, NS, C, 0, key, Bytes::from_static(value), None, condition)
        .await
        .expect("put_if")
}

async fn version_of(cache: &LogCache, key: &str) -> Option<u64> {
    cache
        .get_versioned_checked(T, NS, C, 0, key)
        .await
        .expect("get")
        .map(|found| found.version)
}

#[tokio::test]
async fn put_if_absent_applies_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    let first = put_if(&cache, "k", b"a", CacheCondition::Absent).await;
    assert!(first.applied);
    let written = first.version.expect("an applied put has a version");
    assert_eq!(version_of(&cache, "k").await, Some(written));

    let second = put_if(&cache, "k", b"b", CacheCondition::Absent).await;
    assert_eq!(
        second,
        ConditionalWrite {
            applied: false,
            version: Some(written),
        }
    );
    assert_eq!(
        cache
            .get_checked(T, NS, C, 0, "k")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"a"[..])
    );
}

#[tokio::test]
async fn put_if_version_compares_against_the_current_version() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    let missing = put_if(&cache, "k", b"a", CacheCondition::Version(0)).await;
    assert_eq!(
        missing,
        ConditionalWrite {
            applied: false,
            version: None,
        }
    );

    cache
        .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"a"), None)
        .await
        .unwrap();
    let current = version_of(&cache, "k").await.expect("written");

    let stale = put_if(&cache, "k", b"b", CacheCondition::Version(current + 1)).await;
    assert!(!stale.applied);
    assert_eq!(stale.version, Some(current));

    let swapped = put_if(&cache, "k", b"b", CacheCondition::Version(current)).await;
    assert!(swapped.applied);
    assert!(swapped.version > Some(current), "a version only grows");
}

#[tokio::test]
async fn an_expired_entry_is_absent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    cache
        .put_checked(
            T,
            NS,
            C,
            0,
            "lease",
            Bytes::from_static(b"holder-1"),
            Some(Duration::from_millis(1)),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let taken = put_if(&cache, "lease", b"holder-2", CacheCondition::Absent).await;
    assert!(taken.applied);
}

#[tokio::test]
async fn delete_if_removes_only_the_named_version() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    let written = put_if(&cache, "k", b"a", CacheCondition::Absent)
        .await
        .version
        .unwrap();

    let stale = cache
        .delete_if_checked(T, NS, C, 0, "k", written + 1)
        .await
        .unwrap();
    assert_eq!(
        stale,
        ConditionalWrite {
            applied: false,
            version: Some(written),
        }
    );

    let removed = cache
        .delete_if_checked(T, NS, C, 0, "k", written)
        .await
        .unwrap();
    assert_eq!(
        removed,
        ConditionalWrite {
            applied: true,
            version: None,
        }
    );
    assert_eq!(version_of(&cache, "k").await, None);

    // Deleted and written again: a new version, never the old one.
    let again = put_if(&cache, "k", b"b", CacheCondition::Absent).await;
    assert!(again.version > Some(written));
}

/// A version is what the value was written at, not where its record is now:
/// compaction moves the record and a restart replays the moved copy.
#[tokio::test]
async fn a_version_survives_compaction_and_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let written = {
        let cache = cache(dir.path()).await;
        let written = put_if(&cache, "kept", b"v", CacheCondition::Absent)
            .await
            .version
            .unwrap();
        // Garbage on another key until a pass runs and copies `kept` forward.
        let filler = Bytes::from(vec![b'x'; 64 * 1024]);
        for _ in 0..40 {
            cache
                .put_checked(T, NS, C, 0, "hot", filler.clone(), None)
                .await
                .unwrap();
        }
        cache.compactor.idle().await;
        let shard = cache.shard(T, NS, C, 0).expect("shard");
        let base = shard.current_log().await.base_offset();
        assert!(base > written, "compaction never moved the record");
        assert_eq!(version_of(&cache, "kept").await, Some(written));
        cache.shutdown().await.unwrap();
        written
    };

    let reopened = cache(dir.path()).await;
    assert_eq!(version_of(&reopened, "kept").await, Some(written));
    let swapped = put_if(&reopened, "kept", b"w", CacheCondition::Version(written)).await;
    assert!(swapped.applied);
}

/// Holds the turn of a record staged ahead of everything else, so a write
/// staged after it commits but cannot apply until [`HeldTurn::release`].
struct HeldTurn {
    shard: Arc<CacheShard>,
    pending: crate::disk_log::PendingAppend,
    turn: crate::commit_order::CommitTurn<'static>,
    op: CacheOp,
    bytes: u64,
}

impl HeldTurn {
    async fn hold(cache: &LogCache) -> Self {
        let shard = cache.shard(T, NS, C, 0).expect("shard");
        let op = CacheOp::Put {
            key: "unrelated".to_string(),
            value: Bytes::from_static(b"x"),
            expires_at_millis: 0,
            version: None,
        };
        let payload = op.encode();
        let bytes = payload.len() as u64;
        let (pending, turn) = {
            let mut state = shard.state.lock().await;
            shard.ensure_index(&mut state).await.unwrap();
            let claimed = state
                .log
                .append_claimed(
                    &[AppendRecord {
                        payload,
                        timestamp_micros: 0,
                        mark: Default::default(),
                        publisher: None,
                    }],
                    &shard.sequencer,
                )
                .await
                .unwrap();
            state.sequenced_through = Some(claimed.0.last_offset() + 1);
            claimed
        };
        Self {
            shard,
            pending,
            turn,
            op,
            bytes,
        }
    }

    async fn wait_for_in_flight(&self, key: &str) {
        while !self.shard.keys_in_flight.lock().contains_key(key) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn release(self) {
        let log = self.shard.current_log().await;
        log.commit(&self.pending).await.unwrap();
        let mut state = self.shard.state.lock().await;
        CacheShard::apply_op(
            &mut state,
            &self.op,
            self.pending.first_offset(),
            self.bytes,
        );
        drop(self.turn);
    }
}

/// **The check and the write are one step.** A put that is staged but not yet
/// applied is invisible to the index; a conditional put checked against the
/// index alone would see the key absent, and both writers would win.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_if_absent_waits_for_a_staged_write_to_the_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(cache(dir.path()).await);
    let held = HeldTurn::hold(&cache).await;

    let first = tokio::spawn({
        let cache = Arc::clone(&cache);
        async move {
            cache
                .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"first"), None)
                .await
        }
    });
    held.wait_for_in_flight("k").await;

    let second = tokio::spawn({
        let cache = Arc::clone(&cache);
        async move { put_if(&cache, "k", b"second", CacheCondition::Absent).await }
    });
    // Long enough for the conditional put to stage, were it going to.
    tokio::time::sleep(Duration::from_millis(50)).await;
    held.release().await;

    first.await.unwrap().unwrap();
    let second = second.await.unwrap();
    assert!(
        !second.applied,
        "two writers both found the key absent: {second:?}"
    );
    assert_eq!(second.version, version_of(&cache, "k").await);
    assert_eq!(
        cache
            .get_checked(T, NS, C, 0, "k")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"first"[..])
    );
}

/// The same race for an expiry: a refresh staged but not yet applied must not
/// be erased by a delete of the expired value it replaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expiry_waits_for_a_staged_refresh() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(cache(dir.path()).await);
    cache
        .put_checked(
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
    tokio::time::sleep(Duration::from_millis(10)).await;
    let held = HeldTurn::hold(&cache).await;

    let refresh = tokio::spawn({
        let cache = Arc::clone(&cache);
        async move {
            cache
                .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"new"), None)
                .await
        }
    });
    held.wait_for_in_flight("k").await;
    let expiry = tokio::spawn({
        let cache = Arc::clone(&cache);
        async move { cache.expire_due(T, NS, C, 0, 10, &|| true).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    held.release().await;

    refresh.await.unwrap().unwrap();
    assert_eq!(expiry.await.unwrap().unwrap(), 0);
    assert_eq!(
        cache
            .get_checked(T, NS, C, 0, "k")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"new"[..])
    );
}

/// Many clients incrementing one value by compare-and-set: every applied swap
/// must be counted exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_compare_and_set_loses_no_increment() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(
        LogCache::open(
            dir.path(),
            LogConfig {
                fsync_mode: crate::log::FsyncMode::OnCommit,
                ..config()
            },
        )
        .expect("open"),
    );
    cache
        .put_checked(
            T,
            NS,
            C,
            0,
            "n",
            Bytes::from(0u64.to_le_bytes().to_vec()),
            None,
        )
        .await
        .unwrap();

    let writers = 8;
    let increments = 10;
    let mut tasks = Vec::new();
    for _ in 0..writers {
        let cache = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            let mut done = 0;
            while done < increments {
                let current = cache
                    .get_versioned_checked(T, NS, C, 0, "n")
                    .await
                    .unwrap()
                    .expect("present");
                let n = u64::from_le_bytes(current.value[..].try_into().unwrap());
                let swapped = cache
                    .put_if_checked(
                        T,
                        NS,
                        C,
                        0,
                        "n",
                        Bytes::from((n + 1).to_le_bytes().to_vec()),
                        None,
                        CacheCondition::Version(current.version),
                    )
                    .await
                    .unwrap();
                done += usize::from(swapped.applied);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let value = cache.get_checked(T, NS, C, 0, "n").await.unwrap().unwrap();
    assert_eq!(
        u64::from_le_bytes(value[..].try_into().unwrap()),
        (writers * increments) as u64
    );
}

/// A settled read waits out a put that is staged but not applied, as the put
/// of a cancelled caller is until it finishes on its own task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_settled_read_sees_a_staged_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Arc::new(cache(dir.path()).await);
    let held = HeldTurn::hold(&cache).await;

    let put = tokio::spawn({
        let cache = Arc::clone(&cache);
        async move {
            cache
                .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"staged"), None)
                .await
        }
    });
    held.wait_for_in_flight("k").await;
    put.abort();

    let read = tokio::spawn({
        let cache = Arc::clone(&cache);
        async move { cache.get_settled_checked(T, NS, C, 0, "k").await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!read.is_finished(), "read before the staged write applied");
    held.release().await;

    assert_eq!(
        read.await.unwrap().unwrap().as_deref(),
        Some(&b"staged"[..])
    );
}

/// A put whose caller is cancelled before its task first runs still holds off
/// a settled read. On a current-thread runtime the spawned task cannot run
/// until this one yields, so the cancel lands before the put has started.
#[tokio::test(flavor = "current_thread")]
async fn a_settled_read_sees_a_put_cancelled_before_it_started() {
    use std::future::Future;
    use std::task::Poll;

    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    cache
        .put_checked(T, NS, C, 0, "k", Bytes::from_static(b"old"), None)
        .await
        .unwrap();

    let mut put = Box::pin(cache.put_checked(T, NS, C, 0, "k", Bytes::from_static(b"new"), None));
    let polled = std::future::poll_fn(|cx| Poll::Ready(put.as_mut().poll(cx))).await;
    assert!(polled.is_pending(), "the put waits on its spawned task");
    drop(put);

    assert_eq!(
        cache
            .get_settled_checked(T, NS, C, 0, "k")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"new"[..])
    );
}

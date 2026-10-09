//! The key index snapshot a compaction pass leaves, and every way it can be
//! wrong when the shard next opens. A wrong one must cost a full replay, never
//! a wrong read.

use std::path::{Path, PathBuf};

use super::*;
use crate::index_snapshot::{self, FILE_NAME, Header, STORE_CACHE};

const KEYS: usize = 16;

/// Every live key's value and version, read through the cache.
async fn contents(cache: &LogCache) -> Vec<(String, Option<VersionedValue>)> {
    let mut keys = cache.keys(T, NS, C, 0).await.expect("keys");
    keys.extend(["gone".to_string(), "k0".to_string()]);
    keys.sort();
    keys.dedup();
    let mut out = Vec::new();
    for key in keys {
        let value = cache
            .get_versioned_checked(T, NS, C, 0, &key)
            .await
            .expect("get");
        out.push((key, value));
    }
    out
}

async fn shard_dir(cache: &LogCache) -> PathBuf {
    cache.shard(T, NS, C, 0).expect("shard").dir.clone()
}

async fn compact(cache: &LogCache) {
    let shard = cache.shard(T, NS, C, 0).expect("shard");
    shard.compact().await.expect("compact");
}

async fn covered(cache: &LogCache) -> u64 {
    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let state = shard.state.lock().await;
    state.index.covered_through.expect("covered")
}

/// The offset a freshly opened shard's replay started from, if a snapshot
/// was used.
async fn restored_through(cache: &LogCache) -> Option<u64> {
    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let mut state = shard.state.lock().await;
    shard.ensure_index(&mut state).await.expect("index");
    state.index.restored_through
}

/// Writes after the snapshot that a replay past it has to see: an overwrite,
/// a delete of a key the snapshot holds, and a new key.
async fn write_past_the_snapshot(cache: &LogCache) {
    cache
        .put_checked(T, NS, C, 0, "k1", Bytes::from_static(b"later"), None)
        .await
        .expect("put");
    cache
        .delete_checked(T, NS, C, 0, "k2")
        .await
        .expect("delete");
    cache
        .put_checked(T, NS, C, 0, "new", Bytes::from_static(b"fresh"), None)
        .await
        .expect("put");
}

fn flip(path: &Path, at: usize) {
    let mut bytes = std::fs::read(path).expect("read");
    bytes[at] ^= 0x01;
    std::fs::write(path, bytes).expect("write");
}

#[tokio::test]
async fn compaction_leaves_a_snapshot_a_restart_replays_past() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (expected, snapshot_at) = {
        let cache = cache(dir.path()).await;
        overwritten(&cache, KEYS).await;
        compact(&cache).await;
        let snapshot_at = covered(&cache).await;
        assert!(shard_dir(&cache).await.join(FILE_NAME).exists());
        write_past_the_snapshot(&cache).await;
        let expected = contents(&cache).await;
        cache.shutdown().await.expect("shutdown");
        (expected, snapshot_at)
    };

    let reopened = cache(dir.path()).await;
    assert_eq!(restored_through(&reopened).await, Some(snapshot_at));
    assert_eq!(contents(&reopened).await, expected);
    assert_eq!(
        reopened.get(T, NS, C, 0, "k2").await.expect("get"),
        None,
        "a delete past the snapshot was lost"
    );
}

#[tokio::test]
async fn a_restored_index_keeps_compacting_correctly() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let cache = cache(dir.path()).await;
        overwritten(&cache, KEYS).await;
        compact(&cache).await;
        cache.shutdown().await.expect("shutdown");
    }
    let reopened = cache(dir.path()).await;
    let expected = overwritten(&reopened, KEYS).await;
    compact(&reopened).await;
    assert_reads(&reopened, &expected, "after compacting a restored index").await;
    reopened.shutdown().await.expect("shutdown");
    let again = cache(dir.path()).await;
    assert!(restored_through(&again).await.is_some());
    assert_reads(&again, &expected, "after the second restart").await;
}

/// A crash after the temporary file was flushed but before the rename: the
/// temporary is never read, and the snapshot before it still is.
#[tokio::test]
async fn a_snapshot_written_but_not_renamed_is_not_used() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (expected, snapshot_at, shard) = {
        let cache = cache(dir.path()).await;
        overwritten(&cache, KEYS).await;
        compact(&cache).await;
        let snapshot_at = covered(&cache).await;
        write_past_the_snapshot(&cache).await;
        let shard = shard_dir(&cache).await;
        // Claims to cover the whole log and holds nothing. Read, it would
        // empty the cache.
        let tail = covered(&cache).await;
        index_snapshot::write_temporary(
            &shard,
            &Header {
                store: STORE_CACHE,
                covered_through: tail,
                log_bytes: 0,
                last_checksum: 0,
            },
            &mut [],
        )
        .expect("write the temporary");
        let expected = contents(&cache).await;
        cache.shutdown().await.expect("shutdown");
        (expected, snapshot_at, shard)
    };
    assert!(shard.join("keys.idx.tmp").exists());

    let reopened = cache(dir.path()).await;
    assert_eq!(restored_through(&reopened).await, Some(snapshot_at));
    assert_eq!(contents(&reopened).await, expected);
}

/// A snapshot installed beside a log that never got the records it covers,
/// as if the pass's copies had not reached the disk.
#[tokio::test]
async fn a_snapshot_ahead_of_its_log_is_ignored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let before = tempfile::tempdir().expect("copy");
    let (expected, shard) = {
        let cache = cache(dir.path()).await;
        let expected = overwritten(&cache, KEYS).await;
        let shard = shard_dir(&cache).await;
        let log = cache.shard_log(T, NS, C, 0).await.expect("log");
        log.sync().await.expect("sync");
        copy_dir(&shard, before.path());
        compact(&cache).await;
        cache.shutdown().await.expect("shutdown");
        (expected, shard)
    };
    let snapshot = std::fs::read(shard.join(FILE_NAME)).expect("snapshot");
    std::fs::remove_dir_all(&shard).expect("remove the compacted shard");
    copy_dir(before.path(), &shard);
    std::fs::write(shard.join(FILE_NAME), snapshot).expect("install it beside the old log");

    let reopened = cache(dir.path()).await;
    assert_eq!(restored_through(&reopened).await, None);
    assert_reads(&reopened, &expected, "beside a snapshot ahead of it").await;
}

/// A crash after a later pass trimmed the log but before its snapshot was
/// renamed leaves the earlier snapshot, which covers less than the log now
/// begins at.
#[tokio::test]
async fn a_snapshot_behind_a_trimmed_log_is_ignored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let kept = tempfile::tempdir().expect("kept");
    let expected = {
        let cache = cache(dir.path()).await;
        overwritten(&cache, KEYS).await;
        compact(&cache).await;
        let shard = shard_dir(&cache).await;
        std::fs::copy(shard.join(FILE_NAME), kept.path().join(FILE_NAME)).expect("keep");
        let expected = overwritten(&cache, KEYS).await;
        compact(&cache).await;
        cache.shutdown().await.expect("shutdown");
        std::fs::copy(kept.path().join(FILE_NAME), shard.join(FILE_NAME)).expect("restore");
        expected
    };

    let reopened = cache(dir.path()).await;
    assert_eq!(restored_through(&reopened).await, None);
    assert_reads(&reopened, &expected, "beside an older snapshot").await;
}

#[tokio::test]
async fn a_corrupt_snapshot_is_ignored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let expected = {
        let cache = cache(dir.path()).await;
        overwritten(&cache, KEYS).await;
        compact(&cache).await;
        let expected = contents(&cache).await;
        cache.shutdown().await.expect("shutdown");
        let path = shard_dir(&cache).await.join(FILE_NAME);
        // The low byte of the last entry's version: still a plausible file,
        // so only the checksum can tell.
        let len = std::fs::metadata(&path).expect("stat").len() as usize;
        flip(&path, len - 4 - 4 - 8 - 1);
        expected
    };

    let reopened = cache(dir.path()).await;
    assert_eq!(restored_through(&reopened).await, None);
    assert_eq!(contents(&reopened).await, expected);
}

/// Replication can cut a log back and append other records until it is as
/// long as before. The cut removes the snapshot, but one written from an
/// index captured before the cut can still be installed after it, and its
/// offsets all look valid.
#[tokio::test]
async fn a_snapshot_of_a_log_cut_and_rewritten_is_ignored() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let cache = cache(dir.path()).await;
        overwritten(&cache, KEYS).await;
        compact(&cache).await;
        let snapshot_at = covered(&cache).await;
        let log = cache.shard_log(T, NS, C, 0).await.expect("log");
        let base = log.base_offset();
        let path = shard_dir(&cache).await.join(FILE_NAME);
        let snapshot = std::fs::read(&path).expect("snapshot");
        log.truncate(base).await.expect("truncate");
        assert!(!path.exists(), "the cut left the snapshot in place");
        let mut records = Vec::new();
        for i in base..snapshot_at + 2 {
            let op = CacheOp::Put {
                key: format!("other{i}"),
                value: Bytes::from_static(b"x"),
                expires_at_millis: 0,
                version: None,
            };
            records.push(AppendRecord {
                payload: op.encode(),
                timestamp_micros: 0,
                mark: Default::default(),
                publisher: None,
            });
        }
        log.append(&records).await.expect("append");
        log.sync().await.expect("sync");
        cache.shutdown().await.expect("shutdown");
        std::fs::write(&path, snapshot).expect("install the late snapshot");
    }

    let reopened = cache(dir.path()).await;
    assert_eq!(restored_through(&reopened).await, None);
    assert_eq!(reopened.get(T, NS, C, 0, "k0").await.expect("get"), None);
    let mut keys = reopened.keys(T, NS, C, 0).await.expect("keys");
    keys.retain(|key| !key.starts_with("other"));
    assert!(keys.is_empty(), "keys from the cut records: {keys:?}");
}

#[tokio::test]
async fn forget_index_removes_the_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    overwritten(&cache, KEYS).await;
    compact(&cache).await;
    let path = shard_dir(&cache).await.join(FILE_NAME);
    assert!(path.exists());
    cache.forget_index(T, NS, C, 0).await.expect("forget");
    assert!(!path.exists());
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create");
    for entry in std::fs::read_dir(from).expect("list") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), to.join(entry.file_name())).expect("copy");
    }
}

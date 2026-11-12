//! The snapshot file.

use super::*;
use crate::raft_log::snapshot::decode;
use crate::raft_log::{SnapshotId, SnapshotStore, StoredSnapshot};

fn snapshot(term: u64, index: u64, data: &str) -> StoredSnapshot {
    StoredSnapshot {
        last: Some(SnapshotId { term, index }),
        meta: Bytes::from(format!("meta of {index}")),
        data: Bytes::from(data.to_string()),
    }
}

/// A snapshot reads back with its id, metadata and data, also after a
/// reopen, and a later one replaces it whole.
#[tokio::test]
async fn a_snapshot_reads_back_whole() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    assert_eq!(store.current().await.expect("read"), None);
    assert_eq!(store.covers(), None);

    let first = snapshot(1, 5, "five");
    assert!(store.install(first.clone()).await.expect("install"));
    assert_eq!(store.current().await.expect("read"), Some(first));
    let second = snapshot(2, 9, "nine, and longer than the first");
    assert!(store.install(second.clone()).await.expect("install"));
    assert_eq!(store.covers(), Some(SnapshotId { term: 2, index: 9 }));

    let reopened = SnapshotStore::open(dir.path()).expect("open");
    assert_eq!(reopened.covers(), Some(SnapshotId { term: 2, index: 9 }));
    assert_eq!(reopened.current().await.expect("read"), Some(second));
}

/// A snapshot never goes back: one covering less than the stored one is
/// not written. One of a state machine that applied nothing can be stored
/// first and is replaced by any other.
#[tokio::test]
async fn a_snapshot_never_goes_back() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    let nothing = StoredSnapshot {
        last: None,
        meta: Bytes::new(),
        data: Bytes::new(),
    };
    assert!(store.install(nothing.clone()).await.expect("install"));
    assert_eq!(store.current().await.expect("read"), Some(nothing.clone()));
    assert_eq!(store.covers(), None);

    let later = snapshot(3, 20, "twenty");
    assert!(store.install(later.clone()).await.expect("install"));
    assert!(
        !store
            .install(snapshot(3, 12, "twelve"))
            .await
            .expect("install")
    );
    assert!(!store.install(nothing).await.expect("install"));
    assert_eq!(store.current().await.expect("read"), Some(later.clone()));
    // The same entry again is allowed: openraft may install what it has.
    assert!(store.install(later).await.expect("install"));
}

/// The id, the metadata and the data are under one checksum, so no byte of
/// one snapshot is read under another's id, and a damaged file fails the
/// open instead of reading as no snapshot.
#[tokio::test]
async fn a_damaged_snapshot_is_not_read_as_none() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    store
        .install(snapshot(4, 7, "seven"))
        .await
        .expect("install");
    let path = dir.path().join("raft-snapshot");
    let good = std::fs::read(&path).expect("read");
    assert!(decode(&good).is_some());

    for at in [9, 17, 30, good.len() - 12, good.len() - 1] {
        let mut bytes = good.clone();
        bytes[at] ^= 0x40;
        assert_eq!(decode(&bytes), None, "byte {at}");
    }
    assert_eq!(decode(&good[..good.len() - 1]), None, "cut short");

    let mut bytes = good;
    bytes[15] ^= 1;
    std::fs::write(&path, bytes).expect("write");
    let err = SnapshotStore::open(dir.path()).expect_err("damaged");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    store.current().await.expect_err("damaged");
}

/// A temporary a crash left is not the snapshot.
#[tokio::test]
async fn a_leftover_temporary_is_not_the_snapshot() {
    let dir = tempdir().expect("dir");
    std::fs::write(dir.path().join("raft-snapshot.tmp"), b"half a snapshot").expect("write");
    let store = SnapshotStore::open(dir.path()).expect("open");
    assert_eq!(store.current().await.expect("read"), None);
    let first = snapshot(1, 2, "two");
    assert!(store.install(first.clone()).await.expect("install"));
    assert_eq!(store.current().await.expect("read"), Some(first));
}

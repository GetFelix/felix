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

/// A temporary a crash left is not the snapshot, and the open deletes it.
#[tokio::test]
async fn a_leftover_temporary_is_not_the_snapshot() {
    let dir = tempdir().expect("dir");
    let temporary = dir.path().join("raft-snapshot.tmp");
    std::fs::write(&temporary, b"half a snapshot").expect("write");
    let store = SnapshotStore::open(dir.path()).expect("open");
    assert!(!temporary.exists());
    assert_eq!(store.current().await.expect("read"), None);
    let first = snapshot(1, 2, "two");
    assert!(store.install(first.clone()).await.expect("install"));
    assert_eq!(store.current().await.expect("read"), Some(first));
}

/// The bytes on disk are the documented ones: term 3 is generation 4.
#[tokio::test]
async fn the_format_is_pinned_by_bytes() {
    const FIXTURE: [u8; 52] = [
        0x46, 0x4C, 0x53, 0x53, // "FLSS"
        0x00, 0x01, // version
        0x00, 0x00, // reserved
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, // index
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, // generation
        0x00, 0x00, 0x00, 0x07, // metadata length
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // data length
        0xC8, 0x6B, 0xE3, 0xBB, // crc
        b'm', b'e', b'm', b'b', b'e', b'r', b's', b's', b't', b'a', b't', b'e',
    ];
    let stored = StoredSnapshot {
        last: Some(SnapshotId {
            term: 3,
            index: 0x1234,
        }),
        meta: Bytes::from_static(b"members"),
        data: Bytes::from_static(b"state"),
    };
    assert_eq!(decode(&FIXTURE), Some(stored.clone()));

    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    assert!(store.install(stored.clone()).await.expect("install"));
    let path = dir.path().join("raft-snapshot");
    assert_eq!(std::fs::read(&path).expect("read"), FIXTURE);

    let other = tempdir().expect("dir");
    std::fs::write(other.path().join("raft-snapshot"), FIXTURE).expect("write");
    let store = SnapshotStore::open(other.path()).expect("open");
    assert_eq!(store.current().await.expect("read"), Some(stored));
}

/// A snapshot of another format version fails the open even when its
/// checksum is right: it is not read as this version, and not as none.
#[tokio::test]
async fn a_snapshot_of_another_version_is_refused() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    store
        .install(snapshot(4, 7, "seven"))
        .await
        .expect("install");
    let path = dir.path().join("raft-snapshot");
    let good = std::fs::read(&path).expect("read");

    for version in [0u16, 2] {
        let mut bytes = good.clone();
        bytes[4..6].copy_from_slice(&version.to_be_bytes());
        let crc = crate::segment::format::crc32(&[&bytes[..36], &bytes[40..]]);
        bytes[36..40].copy_from_slice(&crc.to_be_bytes());
        assert_eq!(decode(&bytes), None, "version {version}");
        std::fs::write(&path, &bytes).expect("write");
        let err = SnapshotStore::open(dir.path()).expect_err("another version");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        assert_eq!(std::fs::read(&path).expect("read"), bytes, "left as found");
    }
}

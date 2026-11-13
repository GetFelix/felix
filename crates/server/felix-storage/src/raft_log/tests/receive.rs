//! A snapshot received from a peer, and the stored one read for sending.

use super::*;
use crate::raft_log::{SnapshotId, SnapshotStore, StagedSnapshot, StoredSnapshot};

fn snapshot(term: u64, index: u64, data: &str) -> StoredSnapshot {
    StoredSnapshot {
        last: Some(SnapshotId { term, index }),
        meta: Bytes::from(format!("meta of {index}")),
        data: Bytes::from(data.to_string()),
    }
}

async fn stage(store: &SnapshotStore, snapshot: &StoredSnapshot) -> StagedSnapshot {
    let mut staged = store
        .begin_receive(
            snapshot.last,
            snapshot.meta.clone(),
            snapshot.data.len() as u64,
        )
        .await
        .expect("begin");
    for piece in snapshot.data.chunks(3) {
        staged
            .write(Bytes::copy_from_slice(piece))
            .await
            .expect("write");
    }
    staged.finish().await.expect("finish");
    staged
}

fn receive_file(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("raft-snapshot.recv")
}

/// A received snapshot reads back whole, and once committed it is the
/// stored one, byte for byte what a local install writes.
#[tokio::test]
async fn a_received_snapshot_becomes_the_stored_one() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    let sent = snapshot(2, 9, "the state, in several pieces");
    let mut staged = stage(&store, &sent).await;
    assert_eq!(staged.read().await.expect("read"), sent);
    assert!(staged.commit().await.expect("commit"));
    assert!(!receive_file(&dir).exists());
    assert_eq!(store.covers(), Some(SnapshotId { term: 2, index: 9 }));
    assert_eq!(store.current().await.expect("read"), Some(sent.clone()));

    let local = tempdir().expect("dir");
    let installed = SnapshotStore::open(local.path()).expect("open");
    assert!(installed.install(sent.clone()).await.expect("install"));
    assert_eq!(
        std::fs::read(dir.path().join("raft-snapshot")).expect("read"),
        std::fs::read(local.path().join("raft-snapshot")).expect("read"),
    );
    let reopened = SnapshotStore::open(dir.path()).expect("open");
    assert_eq!(reopened.current().await.expect("read"), Some(sent));
}

/// A receive that is dropped, or one behind the stored snapshot, leaves no
/// file behind and the stored snapshot as it was.
#[tokio::test]
async fn a_refused_snapshot_leaves_no_staged_file() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    let stored = snapshot(3, 20, "twenty");
    assert!(store.install(stored.clone()).await.expect("install"));

    let mut dropped = store
        .begin_receive(Some(SnapshotId { term: 4, index: 30 }), Bytes::new(), 10)
        .await
        .expect("begin");
    dropped
        .write(Bytes::from_static(b"half"))
        .await
        .expect("write");
    assert!(receive_file(&dir).exists());
    drop(dropped);
    assert!(
        !receive_file(&dir).exists(),
        "a dropped receive left its file"
    );

    let mut behind = stage(&store, &snapshot(3, 12, "twelve")).await;
    behind.read().await.expect("read");
    assert!(!behind.commit().await.expect("commit"));
    assert!(
        !receive_file(&dir).exists(),
        "a refused receive left its file"
    );
    assert_eq!(store.current().await.expect("read"), Some(stored));
}

/// A crash before the rename leaves the old snapshot, and the next open
/// deletes the half-received file.
#[tokio::test]
async fn a_crash_mid_receive_leaves_the_old_snapshot() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    let stored = snapshot(1, 5, "five");
    assert!(store.install(stored.clone()).await.expect("install"));
    // Forgotten, as a crash forgets it: nothing deletes its file.
    std::mem::forget(stage(&store, &snapshot(2, 9, "nine")).await);
    assert!(receive_file(&dir).exists());

    let reopened = SnapshotStore::open(dir.path()).expect("open");
    assert!(
        !receive_file(&dir).exists(),
        "the leftover survived the open"
    );
    assert_eq!(reopened.current().await.expect("read"), Some(stored));
}

/// One receive at a time: there is one receive file.
#[tokio::test]
async fn a_second_receive_is_refused_while_one_runs() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    let first = store
        .begin_receive(None, Bytes::new(), 4)
        .await
        .expect("begin");
    let err = store
        .begin_receive(None, Bytes::new(), 4)
        .await
        .expect_err("second");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "{err}");
    drop(first);
    store
        .begin_receive(None, Bytes::new(), 4)
        .await
        .expect("after the first");
}

/// The declared length is exact: a byte more fails the write, and a body
/// that ends early fails the finish.
#[tokio::test]
async fn a_body_that_does_not_match_its_length_is_refused() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    let mut long = store
        .begin_receive(None, Bytes::new(), 3)
        .await
        .expect("begin");
    let err = long
        .write(Bytes::from_static(b"four"))
        .await
        .expect_err("long");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    drop(long);

    let mut short = store
        .begin_receive(None, Bytes::new(), 3)
        .await
        .expect("begin");
    short.write(Bytes::from_static(b"tw")).await.expect("write");
    short.finish().await.expect_err("short");
}

/// A closed store refuses a receive, and a close waits for one in flight:
/// it tells the receive to stop and finishes only once its file is gone,
/// so the store's lock is never let go of under it.
#[tokio::test]
async fn a_close_waits_for_a_receive_in_flight() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    let staged = store
        .begin_receive(None, Bytes::new(), 4)
        .await
        .expect("begin");
    let closer = store.clone();
    let mut close = tokio::spawn(async move { closer.close().await });
    tokio::time::timeout(std::time::Duration::from_secs(5), staged.closing())
        .await
        .expect("the receive was not told to stop");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut close)
            .await
            .is_err(),
        "the close finished under a receive in flight"
    );
    drop(staged);
    close.await.expect("join").expect("close");
    assert!(!receive_file(&dir).exists());

    let err = store
        .begin_receive(None, Bytes::new(), 4)
        .await
        .expect_err("closed");
    assert!(err.to_string().contains("closed"), "{err}");
}

/// A sender reads the file it opened, whatever is installed after, and a
/// damaged file fails at its end instead of reaching a peer.
#[tokio::test]
async fn a_sender_reads_the_file_it_opened() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    assert!(store.open_current().await.expect("open").is_none());
    let first = snapshot(1, 5, "five, the first one");
    assert!(store.install(first.clone()).await.expect("install"));
    let mut reader = store.open_current().await.expect("open").expect("stored");
    assert!(
        store
            .install(snapshot(2, 9, "nine"))
            .await
            .expect("install")
    );
    assert_eq!(reader.last(), first.last);
    assert_eq!(reader.meta(), &first.meta);
    let mut data = Vec::new();
    while let Some(chunk) = reader.next_chunk(4).await.expect("read") {
        assert!(chunk.len() <= 4);
        data.extend_from_slice(&chunk);
    }
    assert_eq!(data, first.data);

    let path = dir.path().join("raft-snapshot");
    let mut bytes = std::fs::read(&path).expect("read");
    let last = bytes.len() - 1;
    bytes[last] ^= 0x40;
    std::fs::write(&path, bytes).expect("write");
    let mut damaged = store.open_current().await.expect("open").expect("stored");
    while let Ok(Some(_)) = damaged.next_chunk(64).await {}
    damaged.next_chunk(64).await.expect_err("damaged");
}

/// Only a file read back whole and checked is committed, whether it was
/// read at once or in pieces. One damaged on disk after it was finished
/// fails the read and is never committed.
#[tokio::test]
async fn a_staged_snapshot_is_committed_only_once_checked() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    let stored = snapshot(1, 5, "five");
    assert!(store.install(stored.clone()).await.expect("install"));
    let later = snapshot(2, 9, "nine, received from a peer");

    let unread = stage(&store, &later).await;
    unread.commit().await.expect_err("committed unread");

    let mut pieces = stage(&store, &later).await;
    let mut data = Vec::new();
    while let Some(chunk) = pieces.next_chunk(4).await.expect("read") {
        data.extend_from_slice(&chunk);
    }
    assert_eq!(data, later.data);
    drop(pieces);

    for whole in [true, false] {
        let mut damaged = stage(&store, &later).await;
        let path = receive_file(&dir);
        let mut bytes = std::fs::read(&path).expect("read");
        let last = bytes.len() - 1;
        bytes[last] ^= 0x40;
        std::fs::write(&path, bytes).expect("write");
        if whole {
            damaged.read().await.expect_err("damaged");
        } else {
            while let Ok(Some(_)) = damaged.next_chunk(8).await {}
            damaged.next_chunk(8).await.expect_err("damaged");
        }
        damaged
            .commit()
            .await
            .expect_err("committed a damaged file");
    }
    assert_eq!(store.current().await.expect("read"), Some(stored));
    assert!(!receive_file(&dir).exists());
}

/// Wait until a receive can start again.
async fn slot_frees(store: &SnapshotStore) -> StagedSnapshot {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match store.begin_receive(None, Bytes::new(), 4).await {
            Ok(staged) => return staged,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the slot never freed"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(err) => panic!("{err}"),
        }
    }
}

/// A second receive while another's file I/O is held at the gate. It must
/// be refused at once: with the slot wrongly free it would block behind
/// that I/O, so the wait is bounded to fail instead of hang.
async fn refused_beside(store: &SnapshotStore, what: &str) -> io::Error {
    let second = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.begin_receive(None, Bytes::new(), 4),
    )
    .await
    .unwrap_or_else(|_| panic!("a second receive waited behind a {what}"));
    match second {
        Ok(_) => panic!("a second receive ran beside a {what}"),
        Err(err) => err,
    }
}

/// A begin whose caller goes away while the file is being created keeps
/// the slot until the create is done, so the next receive cannot have its
/// file truncated under it.
#[tokio::test]
async fn a_dropped_begin_holds_the_slot_until_its_create_ends() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    let (entered, release) = store.gate_next_receive_io();
    let begin = {
        let store = store.clone();
        tokio::spawn(async move { store.begin_receive(None, Bytes::new(), 4).await.map(drop) })
    };
    tokio::task::spawn_blocking(move || entered.recv())
        .await
        .expect("join")
        .expect("the create was not reached");
    begin.abort();
    let _ = begin.await;
    let err = refused_beside(&store, "create").await;
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "{err}");
    release.send(()).expect("release");
    drop(slot_frees(&store).await);
}

/// A commit whose caller goes away during the rename keeps the slot, and
/// the file, until the rename is done: the next receive's partial file can
/// never be the one moved into place.
#[tokio::test]
async fn a_dropped_commit_holds_the_slot_until_its_rename_ends() {
    let dir = tempdir().expect("dir");
    let store = SnapshotStore::open(dir.path()).expect("open");
    assert!(
        store
            .install(snapshot(1, 5, "five"))
            .await
            .expect("install")
    );
    let later = snapshot(2, 9, "nine");
    let mut staged = stage(&store, &later).await;
    staged.read().await.expect("read");
    let (entered, release) = store.gate_next_receive_io();
    let commit = tokio::spawn(async move { staged.commit().await.map(drop) });
    tokio::task::spawn_blocking(move || entered.recv())
        .await
        .expect("join")
        .expect("the rename was not reached");
    commit.abort();
    let _ = commit.await;
    let err = refused_beside(&store, "rename").await;
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "{err}");
    release.send(()).expect("release");
    drop(slot_frees(&store).await);
    assert_eq!(store.current().await.expect("read"), Some(later));
}

//! [`MetadataStore::open`]: the order it does things in, what it refuses
//! and repairs, and the purge and append rules its log runs with.

use tempfile::tempdir;

use super::*;
use crate::metadata_store::MetadataStore;
use crate::raft_log::tests::writes::{append, append_flushed};
use crate::raft_log::tests::{Codec, LogWrites, TestEntry, Types, all, blank, log_id, normal};
use crate::raft_log::{SnapshotId, StoredSnapshot};

type Store = MetadataStore<Types, Codec>;

async fn open(data_dir: &Path) -> io::Result<Store> {
    Store::open(data_dir, CLUSTER, Codec, "meta/raft").await
}

/// Entries `from..to` of term 1, the first a blank one.
fn entries(from: u64, to: u64) -> Vec<TestEntry> {
    (from..to)
        .map(|index| match index {
            0 => blank(1, 0),
            _ => normal(1, index, &format!("command-{index}")),
        })
        .collect()
}

fn snapshot(index: u64) -> StoredSnapshot {
    StoredSnapshot {
        last: Some(SnapshotId { term: 1, index }),
        meta: bytes::Bytes::from_static(b"meta"),
        data: bytes::Bytes::from(format!("state through {index}")),
    }
}

async fn purged(store: &Store) -> Option<u64> {
    let state = store.log().log_state().await.expect("state");
    state.last_purged_log_id.map(|id| id.index)
}

/// The start is decided before the member file is opened: an unconverted
/// `raft.redb` is refused with nothing written, not even a half-built store.
#[tokio::test]
async fn an_unconverted_directory_is_refused_with_nothing_written() {
    let dir = tempdir().expect("dir");
    std::fs::write(dir.path().join(REDB_FILE), b"a group's state").expect("redb");
    let before = listing(dir.path());
    let err = open(dir.path()).await.err().expect("refused");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    assert_eq!(listing(dir.path()), before, "the refusal wrote something");
}

/// A second open of the same data directory is refused while the first
/// holds the store.
#[tokio::test]
async fn a_store_is_held_by_one_opener() {
    let dir = tempdir().expect("dir");
    let store = open(dir.path()).await.expect("open");
    let err = open(dir.path()).await.err().expect("second open");
    assert_eq!(err.kind(), io::ErrorKind::ResourceBusy, "{err}");
    store.shutdown().await.expect("shutdown");
    open(dir.path()).await.expect("released");
}

/// A new store, and a started one whose log is still blank, hold no Raft
/// state. The vote is not withheld here: the caller decides.
#[tokio::test]
async fn a_blank_log_without_a_snapshot_is_no_raft_state() {
    let dir = tempdir().expect("dir");
    let store = open(dir.path()).await.expect("open");
    assert!(!store.has_raft_state());
    store.mark_started().await.expect("start");
    store.shutdown().await.expect("shutdown");

    let store = open(dir.path()).await.expect("reopen");
    assert!(!store.has_raft_state(), "a started mark is not Raft state");
    assert!(!store.member_file().member().vote_withheld);
    let mut log = store.log();
    append_flushed(&mut log, &entries(0, 3)).await;
    store.shutdown().await.expect("shutdown");
    assert!(open(dir.path()).await.expect("reopen").has_raft_state());
}

/// A blank log beside a snapshot has lost its vote. The open withholds the
/// member's vote, on disk, and starts the log right after the snapshot.
#[tokio::test]
async fn a_blank_log_beside_a_snapshot_withholds_the_vote() {
    let dir = tempdir().expect("dir");
    let store = open(dir.path()).await.expect("open");
    let mut log = store.log();
    append_flushed(&mut log, &entries(0, 6)).await;
    assert!(
        store
            .snapshots()
            .install(snapshot(4))
            .await
            .expect("install")
    );
    store.shutdown().await.expect("shutdown");
    let log_dir = dir.path().join(STORE_DIR).join(LOG_DIR);
    std::fs::remove_dir_all(&log_dir).expect("wipe");
    std::fs::create_dir(&log_dir).expect("empty");

    let store = open(dir.path()).await.expect("reopen");
    assert!(store.member_file().member().vote_withheld);
    assert!(store.has_raft_state());
    assert_eq!(purged(&store).await, Some(4));
    store.shutdown().await.expect("shutdown");
    // The purged id the repair wrote does not give the vote back.
    let store = open(dir.path()).await.expect("again");
    assert!(store.member_file().member().vote_withheld);
}

/// A purge past the stored snapshot removes up to the snapshot and hides
/// the rest: this process reads the log as purged, and a restart finds
/// every entry above the snapshot.
#[tokio::test]
async fn a_purge_past_the_snapshot_is_hidden_and_undone_by_a_restart() {
    let dir = tempdir().expect("dir");
    let store = open(dir.path()).await.expect("open");
    let mut log = store.log();
    let written = entries(0, 12);
    append_flushed(&mut log, &written).await;
    store
        .snapshots()
        .install(snapshot(3))
        .await
        .expect("install");

    log.purge(log_id(1, 8)).await.expect("purge");
    assert_eq!(purged(&store).await, Some(8));
    assert_eq!(all(&log).await.expect("read"), written[9..]);
    assert_eq!(
        log.read_committed().await.expect("committed"),
        Some(log_id(1, 8))
    );
    log.truncate_after(Some(log_id(1, 8)))
        .await
        .expect("truncate at the hidden id");
    assert!(all(&log).await.expect("read").is_empty());
    log.truncate_after(Some(log_id(1, 5)))
        .await
        .expect_err("below the hidden id");
    // Right after the hidden id, which is where the disk's entries end.
    let next = [normal(2, 9, "after")];
    append_flushed(&mut log, &next).await;
    store.shutdown().await.expect("shutdown");

    let store = open(dir.path()).await.expect("reopen");
    assert_eq!(
        purged(&store).await,
        Some(3),
        "only the snapshot's part went"
    );
    let mut found = written[4..9].to_vec();
    found.extend(next);
    assert_eq!(all(&store.log()).await.expect("read"), found);
}

/// A hidden id is purged for real once a snapshot covers it, at the next
/// append or purge.
#[tokio::test]
async fn a_hidden_id_becomes_real_once_a_snapshot_covers_it() {
    for by_append in [true, false] {
        let dir = tempdir().expect("dir");
        let store = open(dir.path()).await.expect("open");
        let mut log = store.log();
        append_flushed(&mut log, &entries(0, 6)).await;
        // Past the log's end, with no snapshot.
        log.purge(log_id(1, 9)).await.expect("purge");
        store
            .snapshots()
            .install(snapshot(9))
            .await
            .expect("install");
        if by_append {
            append_flushed(&mut log, &[normal(2, 10, "after")]).await;
        } else {
            log.purge(log_id(1, 9)).await.expect("purge again");
        }
        store.shutdown().await.expect("shutdown");
        let store = open(dir.path()).await.expect("reopen");
        assert_eq!(purged(&store).await, Some(9), "by append: {by_append}");
        let found = all(&store.log()).await.expect("read");
        assert_eq!(found.len(), usize::from(by_append));
    }
}

/// A purge past the log's end with no snapshot, then an append: on disk it
/// would leave a log that starts after its snapshot, so it fails, and what
/// restarts is the log as it was.
#[tokio::test]
async fn an_append_after_a_purge_past_the_end_with_no_snapshot_fails() {
    let dir = tempdir().expect("dir");
    let store = open(dir.path()).await.expect("open");
    let mut log = store.log();
    let written = entries(0, 6);
    append_flushed(&mut log, &written).await;
    log.purge(log_id(1, 9)).await.expect("purge");
    assert_eq!(purged(&store).await, Some(9));

    let err = append(&mut log, &[normal(2, 10, "after")])
        .await
        .err()
        .expect("refused");
    assert!(
        err.to_string().contains("starts after its snapshot"),
        "{err}"
    );
    // Not at the disk's tail either: openraft knows those as purged.
    append(&mut log, &[normal(2, 6, "below")])
        .await
        .err()
        .expect("refused");
    // A snapshot that stops short of the hidden id does not help.
    store
        .snapshots()
        .install(snapshot(7))
        .await
        .expect("install");
    append(&mut log, &[normal(2, 10, "after")])
        .await
        .err()
        .expect("refused");
    store.shutdown().await.expect("shutdown");

    let store = open(dir.path()).await.expect("reopen");
    assert_eq!(purged(&store).await, Some(7), "emptied under the snapshot");
    assert!(all(&store.log()).await.expect("read").is_empty());
}

/// A log purged past its snapshot has lost entries: the open refuses it.
#[tokio::test]
async fn a_log_purged_past_its_snapshot_is_refused() {
    let dir = tempdir().expect("dir");
    let store = open(dir.path()).await.expect("open");
    let mut log = store.log();
    append_flushed(&mut log, &entries(0, 6)).await;
    store
        .snapshots()
        .install(snapshot(4))
        .await
        .expect("install");
    log.purge(log_id(1, 4)).await.expect("purge");
    store.shutdown().await.expect("shutdown");
    std::fs::remove_file(dir.path().join(STORE_DIR).join("raft-snapshot")).expect("lose it");

    let err = open(dir.path()).await.err().expect("refused");
    assert!(err.to_string().contains("past its snapshot"), "{err}");
}

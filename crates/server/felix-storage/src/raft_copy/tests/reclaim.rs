//! Kept copies: where a rotation puts them and when they are deleted.

use super::*;
use crate::raft_log::RaftLog;
use crate::raft_log::tests::writes::append;
use crate::raft_log::tests::{Codec, Types, log_id, normal};
use openraft::storage::RaftLogStorage;

type Log = RaftLog<Types, Codec>;

/// A directory holding entries 0 to `last`, then rotated by marks that name
/// another copy. Returns the root that opens it, the old copy id and the new
/// copy, opened.
async fn rotated(root: &Root, last: u64) -> (CopyRoot, u64, OpenedCopy) {
    rotated_purged(root, last, None).await
}

/// [`rotated`], with the old log purged through `purge` first.
async fn rotated_purged(root: &Root, last: u64, purge: Option<u64>) -> (CopyRoot, u64, OpenedCopy) {
    let copies = root.open();
    let opened = copies
        .open_copy(SHARD, &root.shard_dir(), "t/ns/orders/0", config())
        .await
        .expect("open");
    let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    let entries: Vec<_> = (0..=last).map(|index| normal(1, index, "x")).collect();
    let flushed = append(&mut log, &entries).await.expect("append");
    flushed.await.expect("flush");
    if let Some(purge) = purge {
        log.purge(log_id(1, purge)).await.expect("purge");
    }
    drop(log);
    opened.log.close().await.expect("close");
    let old = opened.copy;
    copies
        .mark_log()
        .fresh(Arc::from(SHARD), 0xdead)
        .await
        .expect("marks");
    drop(copies);
    let copies = root.open();
    let new = copies
        .open_copy(SHARD, &root.shard_dir(), "t/ns/orders/0", config())
        .await
        .expect("rotate");
    assert!(new.rotation.is_some());
    (copies, old, new)
}

fn current(first: u64, flushed_last: Option<u64>, committed: Option<u64>) -> CurrentCopy {
    CurrentCopy {
        first,
        flushed_last,
        committed,
    }
}

fn kept_dir(root: &Root) -> PathBuf {
    root.path().join("shards").join(".kept")
}

#[tokio::test]
async fn a_rotation_moves_the_old_directory_under_dot_kept() {
    let root = Root::new();
    let (_copies, old, new) = rotated(&root, 2).await;
    let kept = new.rotation.clone().expect("rotated").kept.expect("kept");
    assert_eq!(kept, kept_dir(&root).join(format!("0.{old:016x}")));
    assert!(kept.is_dir());
    let beside: Vec<_> = std::fs::read_dir(root.path().join("shards"))
        .expect("parent")
        .map(|entry| entry.expect("entry").file_name())
        .collect();
    assert_eq!(beside.len(), 2, "{beside:?}");
    assert_eq!(
        kept_copies(&root.shard_dir()).expect("kept"),
        vec![KeptCopy {
            path: kept,
            copy: Some(old)
        }]
    );
}

#[tokio::test]
async fn a_kept_copy_is_held_until_the_current_copy_commits_past_it() {
    let root = Root::new();
    let (_copies, old, _new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    let held = |last_index| HeldCopy {
        path: kept.clone(),
        copy: Some(old),
        last_index,
        reason: HeldReason::Behind,
    };

    let dir = root.shard_dir();
    let pass = |current| reclaim_kept(&dir, current, config());

    let report = pass(current(0, Some(1000), None)).await.expect("reclaim");
    assert_eq!(report.held, vec![held(Some(2))]);
    assert_eq!(report.wait_for(), Some(2));

    let report = pass(current(0, Some(1000), Some(1)))
        .await
        .expect("reclaim");
    assert_eq!(report.held, vec![held(Some(2))], "one short of it");
    assert!(kept.is_dir());

    let report = pass(current(0, Some(1000), Some(2)))
        .await
        .expect("reclaim");
    assert_eq!(report.reclaimed, vec![kept.clone()]);
    assert!(report.held.is_empty());
    assert!(!kept.exists());
    assert_eq!(kept_copies(&root.shard_dir()).expect("kept"), Vec::new());
}

/// A commit index can be saved before its entry is flushed, and a snapshot
/// install publishes it before the log is rebased: committed past the kept
/// copy is not enough without the records flushed.
#[tokio::test]
async fn a_kept_copy_is_held_until_the_current_log_has_flushed_it() {
    let root = Root::new();
    let (_copies, old, _new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    for flushed in [None, Some(1)] {
        let report = reclaim_kept(&root.shard_dir(), current(0, flushed, Some(10)), config())
            .await
            .expect("reclaim");
        assert_eq!(report.held.len(), 1, "flushed {flushed:?}");
        assert_eq!(report.held[0].reason, HeldReason::Behind);
        assert!(kept.is_dir());
    }
    let report = reclaim_kept(&root.shard_dir(), current(0, Some(2), Some(10)), config())
        .await
        .expect("reclaim");
    assert_eq!(report.reclaimed, vec![kept]);
}

#[tokio::test]
async fn a_kept_copy_that_cannot_be_read_is_held() {
    let root = Root::new();
    let (_copies, old, new) = rotated(&root, 5).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    let segment = std::fs::read_dir(&kept)
        .expect("kept")
        .map(|entry| entry.expect("entry").path())
        .find(|path| path.extension().is_some_and(|ext| ext == "log"))
        .expect("a segment");
    let mut bytes = std::fs::read(&segment).expect("read");
    let at = bytes.len() / 3;
    bytes[at] ^= 0xff;
    std::fs::write(&segment, bytes).expect("write");

    let _ = &new;
    let report = reclaim_kept(
        &root.shard_dir(),
        current(0, Some(1000), Some(1000)),
        config(),
    )
    .await
    .expect("reclaim");
    assert_eq!(
        report.held,
        vec![HeldCopy {
            path: kept.clone(),
            copy: Some(old),
            last_index: None,
            reason: HeldReason::Unreadable
        }]
    );
    assert!(report.reclaimed.is_empty());
    assert!(kept.is_dir());
}

/// A copy whose log starts above the kept copy's lowest record, as after a
/// snapshot install or a purge, has committed past it without holding it.
#[tokio::test]
async fn a_kept_copy_is_held_when_the_current_log_starts_above_it() {
    let root = Root::new();
    let (_copies, old, _new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    let report = reclaim_kept(&root.shard_dir(), current(11, Some(20), Some(20)), config())
        .await
        .expect("reclaim");
    assert_eq!(
        report.held,
        vec![HeldCopy {
            path: kept.clone(),
            copy: Some(old),
            last_index: Some(2),
            reason: HeldReason::NotHeld
        }]
    );
    assert_eq!(report.wait_for(), None);
    assert!(kept.is_dir());
}

/// A purge drops whole segments, so the kept copy's segment still holds
/// records below its purged id. The current log must reach down to them.
#[tokio::test]
async fn a_kept_copy_purged_mid_segment_is_compared_by_its_segment_base() {
    let root = Root::new();
    let (_copies, old, _new) = rotated_purged(&root, 5, Some(3)).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    // Past the purged id, but inside the segment that still holds 0 to 3.
    let report = reclaim_kept(&root.shard_dir(), current(2, Some(20), Some(20)), config())
        .await
        .expect("reclaim");
    assert_eq!(report.held.len(), 1, "{report:?}");
    assert_eq!(report.held[0].reason, HeldReason::NotHeld);
    assert!(kept.is_dir());
    let report = reclaim_kept(&root.shard_dir(), current(0, Some(20), Some(20)), config())
        .await
        .expect("reclaim");
    assert_eq!(report.reclaimed, vec![kept]);
}

/// Looking at a kept copy that is held changes none of its files.
#[tokio::test]
async fn a_held_pass_leaves_the_kept_directory_byte_for_byte() {
    fn snapshot(dir: &Path, into: &mut Vec<(PathBuf, Vec<u8>)>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .expect("dir")
            .map(|entry| entry.expect("entry").path())
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                snapshot(&path, into);
            } else {
                into.push((path.clone(), std::fs::read(&path).expect("read")));
            }
        }
    }
    let root = Root::new();
    let (_copies, old, new) = rotated(&root, 5).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    // A torn tail that opening the log would cut.
    let segment = std::fs::read_dir(&kept)
        .expect("kept")
        .map(|entry| entry.expect("entry").path())
        .find(|path| path.extension().is_some_and(|ext| ext == "log"))
        .expect("a segment");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&segment)
        .expect("open");
    std::io::Write::write_all(&mut file, &[0xab; 64]).expect("tail");
    drop(file);
    let (mut before, mut after) = (Vec::new(), Vec::new());
    snapshot(&kept, &mut before);

    let _ = &new;
    let report = reclaim_kept(&root.shard_dir(), current(0, None, None), config())
        .await
        .expect("reclaim");
    assert_eq!(report.held.len(), 1, "{report:?}");
    assert_eq!(report.held[0].reason, HeldReason::Behind, "{report:?}");
    snapshot(&kept, &mut after);
    assert_eq!(before, after);
}

//! Kept copies: where a rotation puts them and when they are deleted.

use super::*;
use crate::raft_log::RaftLog;
use crate::raft_log::tests::writes::append;
use crate::raft_log::tests::{Codec, Types, normal};

type Log = RaftLog<Types, Codec>;

/// A directory holding entries 0 to `last`, then rotated by marks that name
/// another copy. Returns the root that opens it, the old copy id and the new
/// copy, opened.
async fn rotated(root: &Root, last: u64) -> (CopyRoot, u64, OpenedCopy) {
    let copies = root.open();
    let opened = copies
        .open_copy(SHARD, &root.shard_dir(), "t/ns/orders/0", config())
        .await
        .expect("open");
    let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    let entries: Vec<_> = (0..=last).map(|index| normal(1, index, "x")).collect();
    let flushed = append(&mut log, &entries).await.expect("append");
    flushed.await.expect("flush");
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
    let (_copies, old, new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    let held = |last_index| HeldCopy {
        path: kept.clone(),
        copy: Some(old),
        last_index,
    };

    let report = reclaim_kept(&root.shard_dir(), &new.log, config())
        .await
        .expect("reclaim");
    assert_eq!(report.held, vec![held(Some(2))]);
    assert_eq!(report.wait_for(), Some(2));

    new.log.save_committed(1, 1).await.expect("commit");
    let report = reclaim_kept(&root.shard_dir(), &new.log, config())
        .await
        .expect("reclaim");
    assert_eq!(report.held, vec![held(Some(2))], "one short of it");
    assert!(kept.is_dir());

    new.log.save_committed(2, 1).await.expect("commit");
    let report = reclaim_kept(&root.shard_dir(), &new.log, config())
        .await
        .expect("reclaim");
    assert_eq!(report.reclaimed, vec![kept.clone()]);
    assert!(report.held.is_empty());
    assert!(!kept.exists());
    assert_eq!(kept_copies(&root.shard_dir()).expect("kept"), Vec::new());
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

    // Committed past anything it could hold.
    new.log.save_committed(1000, 1).await.expect("commit");
    let report = reclaim_kept(&root.shard_dir(), &new.log, config())
        .await
        .expect("reclaim");
    assert_eq!(
        report.held,
        vec![HeldCopy {
            path: kept.clone(),
            copy: Some(old),
            last_index: None
        }]
    );
    assert!(report.reclaimed.is_empty());
    assert!(kept.is_dir());
}

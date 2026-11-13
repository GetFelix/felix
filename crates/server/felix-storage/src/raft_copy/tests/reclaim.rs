//! Kept copies: where a rotation puts them and when they are deleted.

use openraft::storage::RaftLogStorage;

use super::*;
use crate::DiskLog;
use crate::raft_log::RaftLog;
use crate::raft_log::tests::writes::append;
use crate::raft_log::tests::{Codec, Types, log_id, normal};

type Log = RaftLog<Types, Codec>;

/// Append entries `from` to `to` of `term` to a log and wait for the flush.
async fn fill(log: &DiskLog, term: u64, from: u64, to: u64) {
    let mut raft = Log::new(log.clone(), Codec).expect("a Raft log");
    let entries: Vec<_> = (from..=to).map(|index| normal(term, index, "x")).collect();
    let flushed = append(&mut raft, &entries).await.expect("append");
    flushed.await.expect("flush");
}

/// A directory holding entries 0 to `last` of term 1, then rotated by marks
/// that name another copy. Returns the root that opens it, the old copy id
/// and the new copy, opened.
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
    fill(&opened.log, 1, 0, last).await;
    if let Some(purge) = purge {
        let mut raft = Log::new(opened.log.clone(), Codec).expect("a Raft log");
        raft.purge(log_id(1, purge)).await.expect("purge");
    }
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

/// `log` as the copy a group runs, whose first index reads `first`.
fn current(log: &DiskLog, first: u64) -> CurrentCopy {
    CurrentCopy::new(log.clone(), Arc::new(move || first))
}

async fn pass(root: &Root, current: &CurrentCopy) -> KeptReport {
    reclaim_kept(&root.shard_dir(), current, config())
        .await
        .expect("reclaim")
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
    fill(&new.log, 1, 0, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    let held = |last_index| HeldCopy {
        path: kept.clone(),
        copy: Some(old),
        last_index,
        reason: HeldReason::Behind,
    };
    let copy = current(&new.log, 0);

    let report = pass(&root, &copy).await;
    assert_eq!(report.held, vec![held(Some(2))]);
    assert_eq!(report.wait_for(), Some(2));

    new.log.save_committed(1, 2).await.expect("commit");
    let report = pass(&root, &copy).await;
    assert_eq!(report.held, vec![held(Some(2))], "one short of it");
    assert!(kept.is_dir());

    new.log.save_committed(2, 2).await.expect("commit");
    let report = pass(&root, &copy).await;
    assert_eq!(report.reclaimed, vec![kept.clone()]);
    assert!(report.held.is_empty());
    assert!(!kept.exists());
    assert_eq!(kept_copies(&root.shard_dir()).expect("kept"), Vec::new());
}

/// A commit index can be saved before its entry is flushed: committed past
/// the kept copy is not enough without its records on disk.
#[tokio::test]
async fn a_kept_copy_is_held_until_the_current_log_has_flushed_it() {
    let root = Root::new();
    let (_copies, old, new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    new.log.save_committed(10, 2).await.expect("commit");
    let copy = current(&new.log, 0);

    let report = pass(&root, &copy).await;
    assert_eq!(report.held.len(), 1, "{report:?}");
    assert_eq!(report.held[0].reason, HeldReason::Behind);
    fill(&new.log, 1, 0, 1).await;
    let report = pass(&root, &copy).await;
    assert_eq!(report.held[0].reason, HeldReason::Behind, "{report:?}");
    assert!(kept.is_dir());

    fill(&new.log, 1, 2, 2).await;
    let report = pass(&root, &copy).await;
    assert_eq!(report.reclaimed, vec![kept]);
}

/// A copy that holds up to the kept copy's last index under another term
/// is another branch: a snapshot install can commit past it without the log
/// being replaced yet.
#[tokio::test]
async fn a_kept_copy_is_held_when_the_current_log_holds_another_term() {
    let root = Root::new();
    let (_copies, old, new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    fill(&new.log, 2, 0, 2).await;
    new.log.save_committed(20, 3).await.expect("commit");

    let report = pass(&root, &current(&new.log, 0)).await;
    assert_eq!(report.held.len(), 1, "{report:?}");
    assert_eq!(report.held[0].reason, HeldReason::Diverged);
    assert_eq!(report.wait_for(), None);
    assert!(kept.is_dir());
}

/// A log restarted since its last append has reported nothing to Raft, but
/// its tail is on disk: an idle, caught-up copy reclaims at open.
#[tokio::test]
async fn a_reopened_copy_reclaims_without_a_new_append() {
    let root = Root::new();
    let (_copies, old, new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    fill(&new.log, 1, 0, 2).await;
    new.log.save_committed(2, 2).await.expect("commit");
    new.log.close().await.expect("close");
    let reopened = DiskLog::open(root.shard_dir(), "t/ns/orders/0", config()).expect("reopen");

    let report = pass(&root, &current(&reopened, 0)).await;
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
    fill(&new.log, 1, 0, 1000).await;
    new.log.save_committed(1000, 2).await.expect("commit");

    let report = pass(&root, &current(&new.log, 0)).await;
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
    let (_copies, old, new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    fill(&new.log, 1, 0, 2).await;
    new.log.save_committed(20, 2).await.expect("commit");

    let report = pass(&root, &current(&new.log, 11)).await;
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
    let (_copies, old, new) = rotated_purged(&root, 5, Some(3)).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    fill(&new.log, 1, 0, 5).await;
    new.log.save_committed(20, 2).await.expect("commit");

    // Past the purged id, but inside the segment that still holds 0 to 3.
    let report = pass(&root, &current(&new.log, 2)).await;
    assert_eq!(report.held.len(), 1, "{report:?}");
    assert_eq!(report.held[0].reason, HeldReason::NotHeld);
    assert!(kept.is_dir());
    let report = pass(&root, &current(&new.log, 0)).await;
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
    // An unsynced tail that opening the log would cut.
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

    let report = pass(&root, &current(&new.log, 0)).await;
    assert_eq!(report.held.len(), 1, "{report:?}");
    assert_eq!(report.held[0].reason, HeldReason::Behind, "{report:?}");
    snapshot(&kept, &mut after);
    assert_eq!(before, after);
}

//! When a directory keeps its copy id and when it rotates.

use super::*;
use crate::DiskLog;
use crate::disk_log::crash_point;
use crate::raft_log::RaftLog;
use crate::raft_log::tests::writes::append;
use crate::raft_log::tests::{Codec, Types, normal};

type Log = RaftLog<Types, Codec>;

async fn open_copy(copies: &CopyRoot, dir: &Path) -> OpenedCopy {
    copies
        .open_copy(SHARD, dir, "t/ns/orders/0", config())
        .await
        .expect("open copy")
}

/// Save a vote in the directory and its mark at the root, as the Raft
/// log's `save_vote` does.
async fn save_vote(opened: &OpenedCopy, generation: u64) {
    opened
        .log
        .save_vote(generation, "1", false)
        .await
        .expect("vote");
    opened
        .marks
        .vote(vote(generation, "1", false))
        .await
        .expect("mark");
}

async fn close(opened: OpenedCopy) -> u64 {
    opened.log.close().await.expect("close");
    opened.copy
}

#[tokio::test]
async fn a_new_directory_takes_a_copy_id_that_then_stays() {
    let root = Root::new();
    let copies = root.open();
    let first = open_copy(&copies, &root.shard_dir()).await;
    let rotation = first.rotation.clone().expect("a new copy");
    assert_eq!(rotation.reason, RotateReason::Empty);
    assert_eq!(rotation.kept, None);
    let copy = close(first).await;
    assert_ne!(copy, 0);
    assert_eq!(root.marks_on_disk(), Some(ShardMarks::fresh(copy)));

    let again = open_copy(&copies, &root.shard_dir()).await;
    assert_eq!((again.copy, again.rotation.clone()), (copy, None));
    close(again).await;
}

/// A wiped directory is new to Raft, whatever the marks say about the copy
/// that was there.
#[tokio::test]
async fn an_emptied_directory_takes_a_new_copy_id() {
    let root = Root::new();
    let copies = root.open();
    let opened = open_copy(&copies, &root.shard_dir()).await;
    save_vote(&opened, 5).await;
    let old = close(opened).await;

    std::fs::remove_dir_all(root.shard_dir()).expect("wipe");
    std::fs::create_dir_all(root.shard_dir()).expect("empty");
    let reopened = open_copy(&copies, &root.shard_dir()).await;
    assert_eq!(
        reopened.rotation.as_ref().map(|rotation| rotation.reason),
        Some(RotateReason::Empty)
    );
    assert_ne!(reopened.copy, old);
    assert_eq!(
        reopened.log.accepted_generation(),
        0,
        "the new copy never voted"
    );
    let new = close(reopened).await;
    assert_eq!(root.marks_on_disk(), Some(ShardMarks::fresh(new)));
}

#[tokio::test]
async fn a_copy_minted_under_another_storage_id_rotates() {
    let root = Root::new();
    let old = {
        let copies = root.open();
        close(open_copy(&copies, &root.shard_dir()).await).await
    };
    // The root lost its identity and got a new one.
    storage_identity::replace(root.path(), [3; 16]).expect("new id");
    root.witness_current_id();
    let copies = root.open();
    let reopened = open_copy(&copies, &root.shard_dir()).await;
    let rotation = reopened.rotation.clone().expect("rotated");
    assert_eq!(rotation.reason, RotateReason::StorageId);
    assert_eq!(rotation.replaced, Some(old));
    assert_ne!(reopened.copy, old);
    close(reopened).await;
}

/// A wipe rotates the copy, and the marks name the new one, so the old
/// directory put back later does not pass under its old id.
#[tokio::test]
async fn a_directory_put_back_after_a_wipe_rotates() {
    let root = Root::new();
    let copies = root.open();
    let backup = tempfile::tempdir().expect("backup");
    let old = close(open_copy(&copies, &root.shard_dir()).await).await;
    copy_tree(&root.shard_dir(), backup.path());

    std::fs::remove_dir_all(root.shard_dir()).expect("wipe");
    let wiped = close(open_copy(&copies, &root.shard_dir()).await).await;
    assert_ne!(wiped, old);

    restore(backup.path(), &root.shard_dir());
    let put_back = open_copy(&copies, &root.shard_dir()).await;
    let rotation = put_back.rotation.clone().expect("rotated");
    assert_eq!(rotation.reason, RotateReason::OtherCopy);
    assert_eq!(rotation.replaced, Some(old));
    assert!(![old, wiped].contains(&put_back.copy));
    close(put_back).await;
}

#[tokio::test]
async fn a_directory_the_marks_do_not_know_rotates() {
    let root = Root::new();
    {
        let copies = root.open();
        close(open_copy(&copies, &root.shard_dir()).await).await;
    }
    std::fs::remove_file(root.path().join(super::super::marks::FILE_NAME)).expect("lose marks");
    // Or the gate would fail the root for the sequence it lost.
    root.forget_witness();
    let copies = root.open();
    let reopened = open_copy(&copies, &root.shard_dir()).await;
    assert_eq!(
        reopened.rotation.as_ref().map(|rotation| rotation.reason),
        Some(RotateReason::NoMarks)
    );
    close(reopened).await;
}

/// The directory went back to before a vote it saved: it would vote again
/// in a term it already voted in.
#[tokio::test]
async fn a_directory_restored_to_an_older_vote_rotates() {
    let root = Root::new();
    let copies = root.open();
    let backup = tempfile::tempdir().expect("backup");
    let opened = open_copy(&copies, &root.shard_dir()).await;
    save_vote(&opened, 3).await;
    let old = close(opened).await;
    copy_tree(&root.shard_dir(), backup.path());

    let opened = open_copy(&copies, &root.shard_dir()).await;
    assert_eq!((opened.copy, opened.rotation.clone()), (old, None));
    save_vote(&opened, 4).await;
    close(opened).await;

    restore(backup.path(), &root.shard_dir());
    let restored = open_copy(&copies, &root.shard_dir()).await;
    let rotation = restored.rotation.clone().expect("rotated");
    assert_eq!(rotation.reason, RotateReason::VoteBelowMark);
    assert_ne!(restored.copy, old);
    assert_eq!(restored.log.accepted_generation(), 0);
    close(restored).await;
}

/// The directory went back to a shorter log than it had acknowledged, under
/// each way of keeping the log mark.
#[tokio::test]
async fn a_directory_restored_to_a_shorter_log_rotates() {
    for batched in [false, true] {
        let root = Root::new();
        let copies = root.open();
        let backup = tempfile::tempdir().expect("backup");
        let mode = if batched {
            LogMarkMode::Batched(std::time::Duration::from_secs(3600))
        } else {
            LogMarkMode::Exact
        };
        let opened = open_copy(&copies, &root.shard_dir()).await;
        let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
        log.mark_with(opened.marks.clone(), mode).expect("marks");
        let flushed = append(&mut log, &[normal(1, 0, "a"), normal(1, 1, "b")])
            .await
            .expect("append");
        flushed.await.expect("flush");
        copies.mark_log().mark_batched().await.expect("batch");
        drop(log);
        let old = close(opened).await;
        copy_tree(&root.shard_dir(), backup.path());

        let opened = open_copy(&copies, &root.shard_dir()).await;
        assert_eq!(
            (opened.copy, opened.rotation.clone()),
            (old, None),
            "batched {batched}"
        );
        let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
        log.mark_with(opened.marks.clone(), mode).expect("marks");
        let flushed = append(&mut log, &[normal(1, 2, "c")])
            .await
            .expect("append");
        flushed.await.expect("flush");
        copies.mark_log().mark_batched().await.expect("batch");
        drop(log);
        close(opened).await;
        assert_eq!(
            root.marks_on_disk().expect("marks").log,
            Some(point(1, 2)),
            "batched {batched}"
        );

        restore(backup.path(), &root.shard_dir());
        let restored = open_copy(&copies, &root.shard_dir()).await;
        let rotation = restored.rotation.clone().expect("rotated");
        assert_eq!(
            rotation.reason,
            RotateReason::LogBelowMark,
            "batched {batched}"
        );
        assert_ne!(restored.copy, old);
        let kept = rotation.kept.expect("kept");
        assert_eq!(
            kept_copies(&root.shard_dir()).expect("kept"),
            vec![KeptCopy {
                path: kept,
                copy: Some(old)
            }]
        );
        close(restored).await;
    }
}

/// A copy wrote an entry, which a leader could count toward a written
/// majority (#2233), and then its flush failed. A restart cuts a poisoned log back
/// to its last sync, so the copy must not come back under its id: it
/// rotates, and the old directory does not open as a log on its own.
#[tokio::test]
async fn a_copy_whose_flush_failed_rotates() {
    let root = Root::new();
    let copies = root.open();
    let opened = open_copy(&copies, &root.shard_dir()).await;
    save_vote(&opened, 2).await;
    let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    log.mark_with(opened.marks.clone(), LogMarkMode::Exact)
        .expect("marks");
    let flushed = append(&mut log, &[normal(1, 0, "synced")])
        .await
        .expect("append");
    flushed.await.expect("flush");

    let positions = log.positions();
    opened.log.fail_next_flush();
    let flushed = append(&mut log, &[normal(1, 1, "written")])
        .await
        .expect("append");
    flushed.await.expect_err("the flush failed");
    assert_eq!(positions.borrow().written, Some(point(1, 1)));
    drop(log);
    let old = opened.copy;
    // A poisoned log has nothing left to flush, so its close may fail.
    let _ = opened.log.close().await;

    // An open that goes round the copy root would cut the log and drop the
    // marker. It is refused, and the marker stays.
    let marker = root.shard_dir().join("poisoned");
    assert!(marker.exists(), "the failed flush left its marker");
    assert!(DiskLog::open(root.shard_dir(), "t/ns/orders/0", config()).is_err());
    assert!(marker.exists());

    let reopened = open_copy(&copies, &root.shard_dir()).await;
    let rotation = reopened.rotation.clone().expect("rotated");
    assert_eq!(rotation.reason, RotateReason::Poisoned);
    assert_eq!(rotation.replaced, Some(old));
    assert_ne!(reopened.copy, old);
    assert_eq!(
        reopened.log.accepted_generation(),
        0,
        "the new copy never voted"
    );
    assert_eq!(
        crate::raft_log::last_point(&reopened.log)
            .await
            .expect("last"),
        None,
        "the new copy holds no entry"
    );
    let kept = rotation.kept.expect("kept");
    assert!(kept.join("poisoned").exists(), "kept as it was left");
    assert_eq!(
        kept_copies(&root.shard_dir()).expect("kept"),
        vec![KeptCopy {
            path: kept,
            copy: Some(old)
        }]
    );
    let new = close(reopened).await;
    assert_eq!(root.marks_on_disk(), Some(ShardMarks::fresh(new)));

    let again = open_copy(&copies, &root.shard_dir()).await;
    assert_eq!((again.copy, again.rotation.clone()), (new, None));
    close(again).await;
}

#[tokio::test]
async fn a_damaged_copy_file_is_an_error() {
    let root = Root::new();
    let copies = root.open();
    close(open_copy(&copies, &root.shard_dir()).await).await;
    let path = root.shard_dir().join(super::super::copy_file::FILE_NAME);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[8] ^= 1;
    std::fs::write(&path, bytes).expect("write");
    let err = copies
        .open_copy(SHARD, &root.shard_dir(), "t/ns/orders/0", config())
        .await
        .err()
        .expect("damaged");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
}

/// A rotation stopped at each of its steps, as a crash stops it, leaves the
/// old directory or one the next open rotates again. Nothing passes under
/// the old copy id.
#[tokio::test]
async fn a_rotation_stopped_at_any_step_rotates_again() {
    let mut stop = 0;
    loop {
        let root = Root::new();
        let dir = root.shard_dir();
        let old = {
            let copies = root.open();
            let opened = open_copy(&copies, &dir).await;
            save_vote(&opened, 6).await;
            let old = close(opened).await;
            // Marks for another copy, so the next open rotates.
            copies
                .mark_log()
                .fresh(Arc::from(SHARD), 0xdead)
                .await
                .expect("marks");
            old
        };

        let copies = root.open();
        let parent = dir.parent().expect("parent").to_path_buf();
        crash_point::watch(&parent, Some(stop));
        let stopped = copies
            .open_copy(SHARD, &dir, "t/ns/orders/0", config())
            .await;
        let steps = crash_point::unwatch(&parent);
        drop(copies);
        let finished = match stopped {
            Ok(opened) => {
                close(opened).await;
                true
            }
            Err(_) => false,
        };

        let copies = root.open();
        let reopened = open_copy(&copies, &dir).await;
        assert!(![old, 0xdead].contains(&reopened.copy), "stop {stop}");
        assert_eq!(reopened.log.accepted_generation(), 0, "stop {stop}");
        assert!(
            finished || reopened.rotation.is_some(),
            "stop {stop}: an interrupted rotation passed"
        );
        assert_eq!(
            root.marks_on_disk().map(|marks| marks.copy),
            Some(reopened.copy),
            "stop {stop}"
        );
        // Only the old copy is kept: a directory a crash left holding just
        // a new copy file is not.
        assert_eq!(
            kept_copies(&dir)
                .expect("kept")
                .iter()
                .map(|kept| kept.copy)
                .collect::<Vec<_>>(),
            vec![Some(old)],
            "stop {stop}"
        );
        close(reopened).await;
        if finished {
            assert!(stop >= steps, "stop {stop} of {steps}");
            break;
        }
        stop += 1;
    }
    assert!(
        stop >= 4,
        "the rotation has fewer steps than expected: {stop}"
    );
}

/// A copy file's temporary is all a crash in its first write leaves, and
/// nothing to keep.
#[tokio::test]
async fn a_directory_with_only_a_copy_file_temporary_is_not_kept() {
    let root = Root::new();
    let dir = root.shard_dir();
    std::fs::create_dir_all(&dir).expect("dir");
    std::fs::write(dir.join("raft-copy.tmp"), b"torn").expect("temporary");
    let copies = root.open();
    let opened = open_copy(&copies, &dir).await;
    let rotation = opened.rotation.clone().expect("rotated");
    assert_eq!(rotation.reason, RotateReason::NoCopyFile);
    assert_eq!(rotation.kept, None);
    assert_eq!(kept_copies(&dir).expect("kept"), Vec::new());
    close(opened).await;
}

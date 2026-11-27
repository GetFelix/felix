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
    super::epoch_file::store(&root.shard_dir(), &epoch_one()).expect("epoch");
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
            rotated_by::load(&kept).expect("by"),
            RotatedBy::PowerCut { to: restored.copy },
            "batched {batched}"
        );
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
    let refused = DiskLog::open(root.shard_dir(), "t/ns/orders/0", config())
        .expect_err("the direct open is refused");
    assert!(
        refused
            .to_string()
            .contains("holds a Raft copy whose log was poisoned"),
        "{refused}"
    );
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

/// The marker write failed, so only the run record knows the log was
/// poisoned. The copy root watches its root, so the failed flush forgets the
/// boot, and the restart rotates the storage id and with it every copy.
#[tokio::test]
async fn a_poisoned_copy_with_no_marker_rotates_every_copy_on_restart() {
    let other_shard = "t/ns/orders/1/Stream";
    let root = Root::new();
    let other_dir = root.path().join("shards").join("1");
    let first = crate::run_state::begin(root.path(), false, None, None, || false, || [7; 16])
        .expect("begin");
    let copies = root.open();
    let opened = open_copy(&copies, &root.shard_dir()).await;
    let other = copies
        .open_copy(other_shard, &other_dir, "t/ns/orders/1", config())
        .await
        .expect("open copy");
    let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    log.mark_with(opened.marks.clone(), LogMarkMode::Exact)
        .expect("marks");
    opened.log.fail_next_flush();
    let flushed = append(&mut log, &[normal(1, 0, "written")])
        .await
        .expect("append");
    flushed.await.expect_err("the flush failed");
    drop(log);
    std::fs::remove_file(root.shard_dir().join("poisoned")).expect("drop the marker");
    let old = (opened.copy, other.copy);
    let _ = opened.log.close().await;
    other.log.close().await.expect("close");
    assert_eq!(
        crate::run_state::read(root.path(), first.id).expect("read"),
        crate::run_state::LastRun::Crashed {
            relaxed: true,
            boot: None
        },
        "the failed flush forgot the boot"
    );

    // The process crashes and starts again.
    let begun = crate::run_state::begin(root.path(), false, None, None, || false, || [8; 16])
        .expect("begin");
    assert!(begun.rotated);
    root.witness_current_id();
    let copies = root.open();
    let reopened = open_copy(&copies, &root.shard_dir()).await;
    let other = copies
        .open_copy(other_shard, &other_dir, "t/ns/orders/1", config())
        .await
        .expect("open copy");
    for (opened, old) in [(&reopened, old.0), (&other, old.1)] {
        let rotation = opened.rotation.clone().expect("rotated");
        assert_eq!(rotation.reason, RotateReason::StorageId);
        assert_eq!(rotation.replaced, Some(old));
        assert_ne!(opened.copy, old);
    }
    close(reopened).await;
    close(other).await;
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
/// the old copy id. The steps include those inside the reservation and
/// copy file writes, which are keyed on the shard directory.
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
            crate::raft_log::reserved::store(&dir, 1 << 20).expect("reserve");
            super::epoch_file::store(&dir, &epoch_one()).expect("epoch");
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
        crash_point::watch_under(&parent, Some(stop));
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

        // The walk also counts any step in the open that follows the
        // rotation. A stop there leaves a finished rotation, which the next
        // open rightly keeps.
        let rotated = copy_file::load(&dir)
            .ok()
            .flatten()
            .is_some_and(|found| root.marks_on_disk().map(|marks| marks.copy) == Some(found.copy));

        let copies = root.open();
        let reopened = open_copy(&copies, &dir).await;
        assert!(![old, 0xdead].contains(&reopened.copy), "stop {stop}");
        assert_eq!(reopened.log.accepted_generation(), 0, "stop {stop}");
        assert!(
            finished || rotated || reopened.rotation.is_some(),
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
        assert_eq!(
            crate::raft_log::reserved::load(&dir).expect("reservation"),
            Some(1 << 20),
            "stop {stop}: the new copy lost the old one's reservation"
        );
        assert_eq!(
            super::epoch_file::load(&dir).expect("epoch"),
            Some(epoch_one()),
            "stop {stop}: the new copy lost the old one's group epoch"
        );
        close(reopened).await;
        if finished {
            assert!(stop >= steps, "stop {stop} of {steps}");
            break;
        }
        stop += 1;
    }
    // Four steps of the rotation itself, then the reservation and the copy
    // file, each a state_file::store of four steps and one step after it.
    assert!(
        stop >= 14,
        "the rotation has fewer steps than expected: {stop}"
    );
}

/// A rotation moves nothing aside only from a directory an earlier
/// rotation left before its copy file, which already holds the carried
/// mark: the mark is written before the reservation. Stopped at each step,
/// the next open rotates again and the copy still reads as reached, with no
/// kept copy to say so.
#[tokio::test]
async fn an_in_place_rotation_stopped_at_any_step_still_reads_reached() {
    let spacing = config().index_spacing_bytes;
    let mut stop = 0;
    loop {
        let root = Root::new();
        let dir = root.shard_dir();
        std::fs::create_dir_all(&dir).expect("dir");
        crate::raft_log::reserved::mark_carried(&dir).expect("mark");
        crate::raft_log::reserved::store(&dir, 1 << 20).expect("reserve");

        let copies = root.open();
        let parent = dir.parent().expect("parent").to_path_buf();
        crash_point::watch_under(&parent, Some(stop));
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
        assert!(kept_copies(&dir).expect("kept").is_empty(), "stop {stop}");
        assert!(
            reached_before(&dir, spacing).expect("read"),
            "stop {stop}: a copy whose carried reservation lost its mark reads as never reached"
        );
        close(reopened).await;
        if finished {
            assert!(stop >= steps, "stop {stop} of {steps}");
            break;
        }
        stop += 1;
    }
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

/// A broker marks a shard directory as a Raft member's before the member
/// first opens. The marker is not a record: the directory is still new,
/// and nothing is moved aside.
#[tokio::test]
async fn a_directory_holding_only_the_engine_marker_is_new() {
    use crate::disk_log::engine_marker;
    let root = Root::new();
    let dir = root.shard_dir();
    engine_marker::claim_for_raft(&dir, SHARD).expect("claim");
    let copies = root.open();
    let opened = open_copy(&copies, &dir).await;
    let rotation = opened.rotation.clone().expect("a new copy");
    assert_eq!(rotation.reason, RotateReason::Empty);
    assert_eq!(rotation.kept, None);
    assert_eq!(kept_copies(&dir).expect("kept"), Vec::new());
    assert!(engine_marker::claimed_for_raft(&dir, SHARD).expect("marker"));
    close(opened).await;
}

/// The marker goes into `.kept` with a rotated directory. The new directory
/// is marked again before its copy file is written, so the hybrid engine
/// never opens the log a rotated member writes.
#[tokio::test]
async fn a_rotated_raft_directory_is_marked_again() {
    use crate::disk_log::engine_marker;
    let root = Root::new();
    let dir = root.shard_dir();
    engine_marker::claim_for_raft(&dir, SHARD).expect("claim");
    let old = {
        let copies = root.open();
        let opened = open_copy(&copies, &dir).await;
        save_vote(&opened, 5).await;
        close(opened).await
    };
    std::fs::remove_file(root.path().join(super::super::marks::FILE_NAME)).expect("lose marks");
    root.forget_witness();
    let copies = root.open();
    let reopened = open_copy(&copies, &dir).await;
    let rotation = reopened.rotation.clone().expect("rotated");
    assert_eq!(rotation.reason, RotateReason::NoMarks);
    assert_eq!(rotation.replaced, Some(old));
    assert!(rotation.kept.is_some(), "the old directory held a vote");
    assert!(engine_marker::claimed_for_raft(&dir, SHARD).expect("marker"));
    close(reopened).await;
}

/// A log with no copy file is what a member opened with no copy id leaves.
/// A copy's own directory never reads as one: its copy file is written
/// before its first segment.
#[tokio::test]
async fn a_log_with_no_copy_file_is_told_apart() {
    use crate::disk_log::engine_marker;
    let root = Root::new();
    let dir = root.shard_dir();
    assert!(!holds_log_without_copy(&dir).expect("missing"));
    engine_marker::claim_for_raft(&dir, SHARD).expect("claim");
    assert!(!holds_log_without_copy(&dir).expect("marker only"));

    let copies = root.open();
    let opened = open_copy(&copies, &dir).await;
    let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    let flushed = append(&mut log, &[normal(1, 0, "a")])
        .await
        .expect("append");
    flushed.await.expect("flush");
    drop(log);
    close(opened).await;
    assert!(!holds_log_without_copy(&dir).expect("a copy"));

    std::fs::remove_file(dir.join(super::super::copy_file::FILE_NAME)).expect("no copy file");
    assert!(holds_log_without_copy(&dir).expect("a log alone"));
}

/// A re-form counts a power-cut copy's vote as read, so the vote has to be
/// on disk once `save_vote` returns and move with the directory into
/// `.kept` when a power cut rotates it.
#[tokio::test]
async fn a_power_cut_rotation_keeps_the_vote() {
    let root = Root::new();
    let copies = root.open();
    let backup = tempfile::tempdir().expect("backup");
    let opened = open_copy(&copies, &root.shard_dir()).await;
    save_vote(&opened, 3).await;
    super::epoch_file::store(&root.shard_dir(), &epoch_one()).expect("epoch");
    let old = close(opened).await;
    copy_tree(&root.shard_dir(), backup.path());

    let opened = open_copy(&copies, &root.shard_dir()).await;
    let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    log.mark_with(opened.marks.clone(), LogMarkMode::Exact)
        .expect("marks");
    let flushed = append(&mut log, &[normal(3, 0, "a")])
        .await
        .expect("append");
    flushed.await.expect("flush");
    drop(log);
    close(opened).await;

    restore(backup.path(), &root.shard_dir());
    let restored = open_copy(&copies, &root.shard_dir()).await;
    let rotation = restored.rotation.clone().expect("rotated");
    assert_eq!(rotation.reason, RotateReason::LogBelowMark);
    let kept = rotation.kept.expect("kept");
    assert_eq!(
        rotated_by::load(&kept).expect("by"),
        RotatedBy::PowerCut { to: restored.copy }
    );
    close(restored).await;

    let read =
        read_copies(&root.shard_dir(), 4096, &std::collections::BTreeSet::new()).expect("read");
    let [kept_read] = &read.kept[..] else {
        panic!("one kept copy: {:?}", read.kept);
    };
    assert_eq!(kept_read.copy, Some(old));
    assert_eq!(kept_read.vote, Some(2), "generation 3 holds term 2");
    assert_eq!(read.current.vote, None, "the new copy never voted");
    // The new copy votes in the same group epoch, so it reads as that
    // epoch (#2671).
    assert_eq!(read.current.epoch, Some(epoch_one()));
}

/// A group epoch past 0, as a re-form start writes it.
fn epoch_one() -> super::EpochFile {
    super::EpochFile {
        incarnation: 3,
        bases: vec![super::EpochBase {
            base_last: None,
            first_index: 4,
        }],
    }
}

fn is_busy(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::ResourceBusy
}

/// The copy's directory is held from the judge through a rotation into the
/// open log: no step of the rotation leaves a directory at the path that
/// another open can take, and
/// a second open of the copy is refused before it judges anything.
#[tokio::test]
async fn a_rotation_holds_the_copy_directory_at_every_step() {
    use crate::disk_log::dir_lock::DirLock;

    let root = Root::new();
    let dir = root.shard_dir();
    let copies = root.open();
    let opened = open_copy(&copies, &dir).await;
    save_vote(&opened, 6).await;
    let old = close(opened).await;
    // Marks for another copy, so the next open moves this one aside.
    copies
        .mark_log()
        .fresh(Arc::from(SHARD), 0xdead)
        .await
        .expect("marks");

    let parent = dir.parent().expect("parent").to_path_buf();
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    {
        let (dir, seen) = (dir.clone(), Arc::clone(&seen));
        crash_point::probe_under(&parent, move || {
            let taken = DirLock::take(&dir).map(drop).map_err(|err| err.kind());
            seen.lock().push(taken);
        });
    }
    let opened = copies
        .open_copy(SHARD, &dir, "t/ns/orders/0", config())
        .await;
    crash_point::unprobe();
    let opened = opened.expect("rotated");
    let rotation = opened.rotation.clone().expect("rotated");
    assert_eq!(rotation.replaced, Some(old));
    assert!(
        rotation.kept.is_some(),
        "the old directory was not moved aside"
    );
    let seen = seen.lock().clone();
    assert!(!seen.is_empty(), "no step of the rotation ran");
    // Only the crash point right after the rename finds no directory at the
    // path. An open that makes one there fails the rotation; see the next
    // test.
    let absent = seen
        .iter()
        .filter(|taken| **taken == Err(io::ErrorKind::NotFound))
        .count();
    assert_eq!(absent, 1, "{seen:?}");
    assert!(
        seen.iter().all(|taken| matches!(
            taken,
            Err(io::ErrorKind::ResourceBusy | io::ErrorKind::NotFound)
        )),
        "a step of the rotation left the directory unlocked: {seen:?}"
    );

    let second = copies
        .open_copy(SHARD, &dir, "t/ns/orders/0", config())
        .await;
    assert!(
        second.as_ref().is_err_and(is_busy),
        "a second open of the copy was let in"
    );
    let copy = close(opened).await;
    let again = open_copy(&copies, &dir).await;
    assert_eq!((again.copy, again.rotation.clone()), (copy, None));
    close(again).await;
}

/// Another open that takes the new directory between the rename and its
/// lock fails the rotation before anything is written there. What is left
/// is what a crash right after the rename leaves, and the next open
/// rotates again with the old copy kept.
#[tokio::test]
async fn an_open_that_takes_the_new_directory_first_fails_the_rotation_and_the_next_open_recovers()
{
    use crate::disk_log::dir_lock::DirLock;

    let root = Root::new();
    let dir = root.shard_dir();
    let copies = root.open();
    let opened = open_copy(&copies, &dir).await;
    save_vote(&opened, 6).await;
    let old = close(opened).await;
    copies
        .mark_log()
        .fresh(Arc::from(SHARD), 0xdead)
        .await
        .expect("marks");

    // At the first step that finds the directory renamed away, another open
    // makes the new one, locks it and writes into it.
    let parent = dir.parent().expect("parent").to_path_buf();
    let outsider = Arc::new(parking_lot::Mutex::new(None));
    {
        let (dir, outsider) = (dir.clone(), Arc::clone(&outsider));
        crash_point::probe_under(&parent, move || {
            let mut outsider = outsider.lock();
            if outsider.is_none() && !dir.exists() {
                std::fs::create_dir_all(&dir).expect("mkdir");
                *outsider = Some(DirLock::take(&dir).expect("the outsider's lock"));
                std::fs::write(dir.join("outsider"), b"x").expect("write");
            }
        });
    }
    let refused = copies
        .open_copy(SHARD, &dir, "t/ns/orders/0", config())
        .await;
    crash_point::unprobe();
    assert!(
        refused.as_ref().is_err_and(is_busy),
        "the rotation went on in a directory another open holds"
    );
    let names: Vec<_> = std::fs::read_dir(&dir)
        .expect("list")
        .map(|entry| entry.expect("entry").file_name())
        .collect();
    assert_eq!(
        names,
        ["outsider"],
        "the rotation wrote into the outsider's directory"
    );
    outsider.lock().take();

    let reopened = open_copy(&copies, &dir).await;
    assert!(![old, 0xdead].contains(&reopened.copy));
    assert_eq!(
        reopened.log.accepted_generation(),
        0,
        "the new copy never voted"
    );
    let kept = kept_copies(&dir).expect("kept");
    assert!(
        kept.iter().any(|kept| kept.copy == Some(old)),
        "the old copy is not kept: {kept:?}"
    );
    close(reopened).await;
}

/// A close lets go of the directory before it returns, whatever background
/// work of the log is still finishing, so a reopen right after it never
/// finds the directory busy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_reopens_at_once_after_a_close_under_load() {
    let root = Root::new();
    let dir = root.shard_dir();
    let copies = root.open();
    for round in 0..20 {
        let opened = open_copy(&copies, &dir).await;
        let writers: Vec<_> = (0..4)
            .map(|_| {
                let log = opened.log.clone();
                // A new generation each time, so every save writes: the same
                // vote again is answered from memory and never fails.
                tokio::spawn(async move {
                    for generation in 1.. {
                        if log.save_vote(generation, "1", false).await.is_err() {
                            break;
                        }
                    }
                })
            })
            .collect();
        tokio::task::yield_now().await;
        // A clone that outlives the close, as a stopped member's task can.
        let lingering = opened.log.clone();
        close(opened).await;
        let again = copies
            .open_copy(SHARD, &dir, "t/ns/orders/0", config())
            .await;
        assert!(again.is_ok(), "round {round}: {:?}", again.err());
        close(again.expect("open")).await;
        drop(lingering);
        for writer in writers {
            writer.await.expect("writer");
        }
    }
}

/// What the copy of `SHARD` holds when a relaxed run's machine crashes.
enum Crashed {
    /// The vote it saved last.
    Whole,
    /// A backup from before its last vote, put back over it.
    OlderVote,
}

/// Run a relaxed broker's copy that votes, crash the machine, and start
/// again with `control_plane` as the control plane's copy of the root
/// sequence, given the old id and the root's sequence. How the rotated copy
/// is kept.
async fn after_a_relaxed_crash(
    crashed: Crashed,
    control_plane: impl FnOnce(StorageId, u64) -> Option<(StorageId, u64)>,
) -> RotatedBy {
    after_a_crash_with(crashed, false, None, control_plane).await
}

/// [`after_a_relaxed_crash`], with the run witness seeded to `witness`
/// (given the root's sequence) after the machine restart, and with the
/// directory older than its registration when `restored`.
async fn after_a_crash_with(
    crashed: Crashed,
    restored: bool,
    witness: Option<fn(u64) -> u64>,
    control_plane: impl FnOnce(StorageId, u64) -> Option<(StorageId, u64)>,
) -> RotatedBy {
    use crate::run_state::{BootId, RootCopies, begin_with, record_registration};
    let root = Root::new();
    let boot = |byte| Some(BootId::from_bytes([byte; 16]));
    let begun = begin_with(
        root.path(),
        true,
        None,
        RootCopies::default(),
        boot(1),
        || false,
        || [2; 16],
    )
    .expect("begin");
    assert!(!begun.rotated);
    let backup = tempfile::tempdir().expect("backup");
    let (old_id, sequence) = {
        let copies = root.open();
        let opened = open_copy(&copies, &root.shard_dir()).await;
        save_vote(&opened, 3).await;
        if let Crashed::OlderVote = crashed {
            close(opened).await;
            copy_tree(&root.shard_dir(), backup.path());
            let opened = open_copy(&copies, &root.shard_dir()).await;
            save_vote(&opened, 5).await;
            close(opened).await;
            restore(backup.path(), &root.shard_dir());
        } else {
            close(opened).await;
        }
        (copies.storage_id(), copies.mark_log().sequence())
    };
    // The machine restarts: the witness is gone, and the run left no clean
    // stop.
    root.forget_witness();
    if let Some(at) = witness {
        crate::run_state::witness::record_sequence(
            root.path(),
            Some(root.shm.path()),
            old_id,
            at(sequence),
        );
    }
    if restored {
        record_registration(root.path(), 1).expect("registration");
    }
    let begun = begin_with(
        root.path(),
        true,
        restored.then_some(2),
        RootCopies {
            witness_dir: Some(root.shm.path()),
            control_plane: control_plane(old_id, sequence),
        },
        boot(2),
        || false,
        || [3; 16],
    )
    .expect("begin");
    assert!(begun.rotated);
    root.witness_current_id();
    let copies = root.open();
    let reopened = open_copy(&copies, &root.shard_dir()).await;
    let rotation = reopened.rotation.clone().expect("rotated");
    assert_eq!(rotation.reason, RotateReason::StorageId);
    let by = rotated_by::load(&rotation.kept.expect("kept")).expect("by");
    if let RotatedBy::PowerCut { to } = by {
        assert_eq!(to, reopened.copy);
    }
    close(reopened).await;
    by
}

/// A relaxed crash of the machine rotates the storage id, and with it every
/// copy. The root's marks are not behind the control plane's copy for the
/// old id, so the copy that still holds its vote is kept as a power cut,
/// whose vote a re-form counts (#3309).
#[tokio::test]
async fn a_relaxed_crash_keeps_a_copy_that_meets_its_marks_as_a_power_cut() {
    let by = after_a_relaxed_crash(Crashed::Whole, |id, sequence| Some((id, sequence))).await;
    assert!(matches!(by, RotatedBy::PowerCut { .. }), "{by:?}");
}

/// The marks are kept only when something outside the root vouches for
/// them, and a copy is a power cut only while it meets them: a root behind
/// the control plane, one nothing has a copy of, a copy under another id,
/// and a copy holding an older vote all rotate as restores.
#[tokio::test]
async fn a_relaxed_crash_nothing_vouches_for_rotates_as_a_restore() {
    let behind =
        after_a_relaxed_crash(Crashed::Whole, |id, sequence| Some((id, sequence + 1))).await;
    assert_eq!(
        behind,
        RotatedBy::Restore,
        "a root behind the control plane"
    );
    let unchecked = after_a_relaxed_crash(Crashed::Whole, |_, _| None).await;
    assert_eq!(unchecked, RotatedBy::Restore, "no copy of the sequence");
    let other_id = after_a_relaxed_crash(Crashed::Whole, |_, sequence| {
        Some((StorageId::from_bytes([9; 16]), sequence))
    })
    .await;
    assert_eq!(other_id, RotatedBy::Restore, "a copy under another id");
    let older =
        after_a_relaxed_crash(Crashed::OlderVote, |id, sequence| Some((id, sequence))).await;
    assert_eq!(
        older,
        RotatedBy::Restore,
        "a copy put back with an older vote"
    );
}

/// The run witness is a copy of the root sequence too: one equal to the
/// root's vouches for its marks, and one ahead of it means the root was put
/// back from a backup, so the copy rotates as a restore.
#[tokio::test]
async fn a_witness_vouches_for_the_marks_only_while_the_root_is_not_behind_it() {
    let equal = after_a_crash_with(Crashed::Whole, false, Some(|s| s), |_, _| None).await;
    assert!(matches!(equal, RotatedBy::PowerCut { .. }), "{equal:?}");
    let ahead = after_a_crash_with(Crashed::Whole, false, Some(|s| s + 1), |_, _| None).await;
    assert_eq!(ahead, RotatedBy::Restore, "a witness ahead of the root");
}

/// A directory older than its registration is a restore even when the
/// control plane's copy would vouch for its marks.
#[tokio::test]
async fn a_restored_root_keeps_its_copies_judged_as_restores() {
    let by = after_a_crash_with(Crashed::Whole, true, None, |id, sequence| {
        Some((id, sequence))
    })
    .await;
    assert_eq!(by, RotatedBy::Restore);
}

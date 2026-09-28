//! `restore_to`: cutting a copied log back to a backup point.

use super::*;

#[tokio::test]
async fn a_restore_cuts_below_the_commit_offset_and_lowers_it_durably() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.append(&records(&["a", "b", "c", "d", "e", "f"]))
            .await
            .expect("append");
        log.advance_commit_offset(5).await.expect("commit");

        log.restore_to(3).await.expect("restore");
        assert_eq!(read_all(&log, 0).await, vec!["a", "b", "c"]);
        assert_eq!(log.commit_offset(), 3);
        assert_eq!(log.tail_offset().await.expect("tail"), 3);
    }
    // Reopened without a shutdown: both the cut and the lowered commit offset
    // were on disk when it returned.
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(read_all(&log, 0).await, vec!["a", "b", "c"]);
    assert_eq!(log.commit_offset(), 3);
    // And the log goes on from the point.
    let appended = log.append(&records(&["x"])).await.expect("append");
    assert_eq!(appended.first_offset, 3);
}

#[tokio::test]
async fn a_restore_past_the_end_of_the_copy_is_refused() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a", "b"])).await.expect("append");
    log.advance_commit_offset(2).await.expect("commit");

    let refused = log.restore_to(5).await.expect_err("the copy is short");
    assert!(matches!(
        refused,
        StorageError::OutsideLog {
            offset: 5,
            base: 0,
            tail: 2
        }
    ));
    assert_eq!(read_all(&log, 0).await, vec!["a", "b"]);
    assert_eq!(log.commit_offset(), 2);
}

#[tokio::test]
async fn a_restore_before_the_start_of_the_copy_is_refused() {
    let dir = tempdir().expect("tempdir");
    let log = DiskLog::open_at(dir.path(), "t/ns/s/0", config(FsyncMode::OnCommit), 10)
        .expect("open at 10");
    log.append(&records(&["k"])).await.expect("append");

    let refused = log.restore_to(4).await.expect_err("dropped already");
    assert!(matches!(
        refused,
        StorageError::OutsideLog {
            offset: 4,
            base: 10,
            tail: 11
        }
    ));
    assert_eq!(read_all(&log, 10).await, vec!["k"]);
}

#[tokio::test]
async fn restoring_to_the_same_point_twice_changes_nothing_the_second_time() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    // Enough to span several of the test config's small segments.
    let payloads: Vec<String> = (0..12).map(|i| format!("record-{i}")).collect();
    let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
    log.append(&records(&refs)).await.expect("append");
    log.advance_commit_offset(12).await.expect("commit");

    let shape = |log: &DiskLog| {
        log.segments()
            .iter()
            .map(|s| (s.id, s.base_offset, s.last_offset, s.size_bytes))
            .collect::<Vec<_>>()
    };
    log.restore_to(7).await.expect("restore");
    let segments = shape(&log);
    log.restore_to(7).await.expect("restore again");

    assert_eq!(shape(&log), segments);
    assert_eq!(read_all(&log, 0).await, refs[..7].to_vec());
    assert_eq!(log.commit_offset(), 7);
}

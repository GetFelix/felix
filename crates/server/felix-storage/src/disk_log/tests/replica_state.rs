//! The accepted generation and the commit offset, through the log: what
//! survives a restart, and which cuts are refused.
use super::*;

#[tokio::test]
async fn an_accepted_generation_survives_a_restart() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        assert_eq!(
            log.accept_generation(5).await.expect("accept"),
            GenerationCheck::Raised
        );
        assert_eq!(
            log.accept_generation(5).await.expect("accept"),
            GenerationCheck::Current
        );
    }
    // Reopened without a shutdown: the raise was on disk before it returned.
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.accepted_generation(), 5);
    assert_eq!(
        log.accept_generation(4).await.expect("accept"),
        GenerationCheck::Superseded { accepted: 5 }
    );
}

#[tokio::test]
async fn truncation_below_the_commit_offset_is_refused() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a", "b", "c", "d"]))
        .await
        .expect("append");
    log.advance_commit_offset(2).await.expect("commit");

    let refused = log.truncate(1).await.expect_err("below the commit offset");
    assert!(matches!(
        refused,
        StorageError::BelowCommit {
            offset: 1,
            commit: 2
        }
    ));
    assert_eq!(read_all(&log, 0).await, vec!["a", "b", "c", "d"]);

    // At the commit offset and above is an uncommitted suffix.
    log.truncate(2).await.expect("truncate");
    assert_eq!(read_all(&log, 0).await, vec!["a", "b"]);
}

#[tokio::test]
async fn a_reset_that_would_discard_committed_records_is_refused() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a", "b", "c", "d"]))
        .await
        .expect("append");
    log.advance_commit_offset(3).await.expect("commit");

    assert!(matches!(
        log.reset_to(0).await,
        Err(StorageError::BelowCommit {
            offset: 0,
            commit: 3
        })
    ));
    assert_eq!(read_all(&log, 0).await, vec!["a", "b", "c", "d"]);

    // A leader whose log starts past every committed record here has already
    // lost them to retention; this copy cannot bring them back.
    log.reset_to(3)
        .await
        .expect("reset past the committed records");
    assert_eq!(log.tail_offset().await.expect("tail"), 3);
}

#[tokio::test]
async fn the_commit_offset_is_written_through_and_read_back() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.append(&records(&["a", "b", "c"]))
            .await
            .expect("append");
        // The first advance is due at once; the next waits out the interval.
        log.advance_commit_offset(1).await.expect("commit");
        log.advance_commit_offset(3).await.expect("commit");
    }
    let log = open(&dir, FsyncMode::OnCommit);
    // Behind is allowed after a crash; ahead never is.
    assert_eq!(log.commit_offset(), 1);

    log.advance_commit_offset(3).await.expect("commit");
    log.shutdown().await.expect("shutdown");
    drop(log);
    assert_eq!(open(&dir, FsyncMode::OnCommit).commit_offset(), 3);
}

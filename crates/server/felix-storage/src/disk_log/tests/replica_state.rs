//! The accepted generation and the commit offset, through the log: what
//! survives a restart, and which cuts are refused.
use super::*;

#[tokio::test]
async fn an_accepted_generation_survives_a_restart() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        assert_eq!(
            log.accept_generation(5, None).await.expect("accept"),
            GenerationCheck::Raised
        );
        assert_eq!(
            log.accept_generation(5, None).await.expect("accept"),
            GenerationCheck::Current
        );
    }
    // Reopened without a shutdown: the raise was on disk before it returned.
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.accepted_generation(), 5);
    assert_eq!(
        log.accept_generation(4, None).await.expect("accept"),
        GenerationCheck::Superseded { accepted: 5 }
    );
}

#[tokio::test]
async fn a_second_leader_at_an_accepted_generation_is_refused() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(
        log.accept_generation(5, Some("broker-a"))
            .await
            .expect("accept"),
        GenerationCheck::Raised
    );
    assert_eq!(
        log.accept_generation(5, Some("broker-a"))
            .await
            .expect("accept"),
        GenerationCheck::Current
    );
    assert_eq!(
        log.accept_generation(5, Some("broker-b"))
            .await
            .expect("accept"),
        GenerationCheck::Promised {
            leader: "broker-a".to_string()
        }
    );
    // A newer generation is a new ballot, whoever asks.
    assert_eq!(
        log.accept_generation(6, Some("broker-b"))
            .await
            .expect("accept"),
        GenerationCheck::Raised
    );
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-b"));
}

#[tokio::test]
async fn a_ballot_survives_a_crash_before_anything_is_acknowledged() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.accept_generation(5, Some("broker-a"))
            .await
            .expect("accept");
        // Dropped without a shutdown: whatever the caller would acknowledge
        // next never happened, and the ballot must already be on disk.
    }
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.accepted_generation(), 5);
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));
    assert_eq!(
        log.accept_generation(5, Some("broker-b"))
            .await
            .expect("accept"),
        GenerationCheck::Promised {
            leader: "broker-a".to_string()
        }
    );
}

#[tokio::test]
async fn a_ballot_written_before_a_crash_is_taken_at_open() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.accept_generation(4, Some("broker-a"))
            .await
            .expect("accept");
    }
    // The raise to 5 wrote its ballot and crashed before `replica` caught up.
    super::super::ballot::store(
        dir.path(),
        &super::super::ballot::Ballot {
            generation: 5,
            leader: "broker-b".to_string(),
        },
    )
    .expect("store");
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.accepted_generation(), 5);
    assert_eq!(
        log.accept_generation(5, Some("broker-c"))
            .await
            .expect("accept"),
        GenerationCheck::Promised {
            leader: "broker-b".to_string()
        }
    );
    assert_eq!(
        log.accept_generation(4, Some("broker-a"))
            .await
            .expect("accept"),
        GenerationCheck::Superseded { accepted: 5 }
    );
}

#[tokio::test]
async fn a_generation_accepted_without_a_leader_takes_the_first_one_named() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        // As a build before ballots, or one with them turned off, accepts.
        log.accept_generation(5, None).await.expect("accept");
        assert_eq!(log.accepted_leader(), None);
        assert_eq!(
            log.accept_generation(5, Some("broker-a"))
                .await
                .expect("accept"),
            GenerationCheck::Current
        );
    }
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(
        log.accept_generation(5, Some("broker-b"))
            .await
            .expect("accept"),
        GenerationCheck::Promised {
            leader: "broker-a".to_string()
        }
    );
    // Without a leader named only the generation is checked.
    assert_eq!(
        log.accept_generation(5, None).await.expect("accept"),
        GenerationCheck::Current
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
async fn the_commit_offset_is_written_through_on_every_advance_under_on_commit() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.append(&records(&["a", "b", "c"]))
            .await
            .expect("append");
        log.advance_commit_offset(1).await.expect("commit");
        log.advance_commit_offset(3).await.expect("commit");
    }
    // Reopened without a shutdown: each advance was on disk before it returned.
    assert_eq!(open(&dir, FsyncMode::OnCommit).commit_offset(), 3);
}

#[tokio::test]
async fn without_on_commit_the_commit_offset_is_written_behind() {
    let dir = tempdir().expect("tempdir");
    let periodic = FsyncMode::Periodic {
        interval: std::time::Duration::from_secs(3600),
    };
    {
        let log = open(&dir, periodic);
        log.append(&records(&["a", "b", "c"]))
            .await
            .expect("append");
        // The first advance is due at once; the next waits out the interval.
        log.advance_commit_offset(1).await.expect("commit");
        log.advance_commit_offset(3).await.expect("commit");
    }
    let log = open(&dir, periodic);
    // Behind is allowed after a crash; ahead never is.
    assert_eq!(log.commit_offset(), 1);

    log.advance_commit_offset(3).await.expect("commit");
    log.shutdown().await.expect("shutdown");
    drop(log);
    assert_eq!(open(&dir, periodic).commit_offset(), 3);
}

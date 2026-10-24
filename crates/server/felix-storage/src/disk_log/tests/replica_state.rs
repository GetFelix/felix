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

/// A broker that gave up the generation it led takes no leader at it again,
/// itself included, across a restart; a newer generation replaces it (#1295).
#[tokio::test]
async fn a_given_up_generation_takes_no_leader_until_a_newer_one() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.accept_generation(5, Some("broker-a"))
            .await
            .expect("accept");
        assert!(!log.gave_up());
        assert_eq!(log.give_up_generation().await.expect("give up"), 5);
        assert!(log.gave_up());
    }
    let log = open(&dir, FsyncMode::OnCommit);
    assert!(log.gave_up(), "kept across a restart");
    assert_eq!(log.accepted_generation(), 5);
    assert_eq!(
        log.accept_generation(5, Some("broker-a"))
            .await
            .expect("accept"),
        GenerationCheck::Promised {
            leader: String::new()
        }
    );
    assert_eq!(
        log.accept_generation(6, Some("broker-b"))
            .await
            .expect("accept"),
        GenerationCheck::Raised
    );
    assert!(!log.gave_up());
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

/// Wait until the `replica` file holds `offset` as the commit offset.
async fn on_disk(dir: &TempDir, offset: u64) {
    let written = async {
        loop {
            let state = crate::disk_log::replica_state::load(dir.path()).expect("replica state");
            if state.commit_offset == offset {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(COMMIT_PERSIST_INTERVAL * 5, written)
        .await
        .expect("the commit offset never reached disk");
}

#[tokio::test]
async fn the_commit_offset_is_written_behind_every_advance_under_on_commit() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.append(&records(&["a", "b", "c"]))
            .await
            .expect("append");
        log.advance_commit_offset(1).await.expect("commit");
        log.advance_commit_offset(3).await.expect("commit");
        // Without waiting for an interval or another advance.
        on_disk(&dir, 3).await;
    }
    // Reopened without a shutdown.
    assert_eq!(open(&dir, FsyncMode::OnCommit).commit_offset(), 3);
}

/// **An advance does not wait on a replica-state write** (#1456). Under
/// `OnCommit` a follower advances on almost every batch, and a write costs a
/// create, two fsyncs and a rename on the ack path.
// Holding the writer's lock across the awaits is the point: it stands in for
// a write that has not finished.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn an_advance_does_not_wait_on_a_replica_state_write() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a", "b", "c", "d"]))
        .await
        .expect("append");

    // The writer's lock, as a slow write in flight holds it.
    let held = log.inner.replica_persisted.lock();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        log.advance_commit_offset(2),
    )
    .await
    .expect("the advance waited on the replica-state write")
    .expect("commit");

    // The guard holds before the write lands; disk is behind, never ahead.
    assert!(matches!(
        log.truncate(1).await,
        Err(StorageError::BelowCommit {
            offset: 1,
            commit: 2
        })
    ));
    assert_eq!(
        crate::disk_log::replica_state::load(dir.path())
            .expect("replica state")
            .commit_offset,
        0
    );

    drop(held);
    on_disk(&dir, 2).await;
    drop(log);
    let reopened = open(&dir, FsyncMode::OnCommit);
    assert_eq!(reopened.commit_offset(), 2);
    assert!(matches!(
        reopened.truncate(1).await,
        Err(StorageError::BelowCommit { .. })
    ));
}

/// **Advances made while a write runs share the next one**, so a busy
/// follower writes its replica state far less often than once per batch.
#[tokio::test]
async fn advances_coalesce_into_few_replica_state_writes() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    let batches = 200u64;
    let payloads: Vec<String> = (0..batches).map(|n| n.to_string()).collect();
    let payloads: Vec<&str> = payloads.iter().map(String::as_str).collect();
    log.append(&records(&payloads)).await.expect("append");
    for offset in 1..=batches {
        log.advance_commit_offset(offset).await.expect("commit");
    }
    on_disk(&dir, batches).await;
    let writes = log.inner.replica_writes.load(Ordering::Relaxed);
    assert!(
        writes <= 2,
        "{writes} replica-state writes for {batches} advances"
    );
}

/// **An advance made as the writer finishes is not left behind.** The writer
/// clears its flag and then checks memory; an advance that found the flag
/// still set relies on that check. A race, so this cannot fail on demand; it
/// advances right as each write lands to give it the most chances.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_advance_as_the_writer_finishes_still_reaches_disk() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    let rounds = 100u64;
    let payloads: Vec<String> = (0..rounds * 2).map(|n| n.to_string()).collect();
    let payloads: Vec<&str> = payloads.iter().map(String::as_str).collect();
    log.append(&records(&payloads)).await.expect("append");
    for round in 1..=rounds {
        log.advance_commit_offset(round * 2 - 1)
            .await
            .expect("commit");
        while log.inner.commit_persisted.load(Ordering::Acquire) < round * 2 - 1 {
            std::hint::spin_loop();
        }
        log.advance_commit_offset(round * 2).await.expect("commit");
        on_disk(&dir, round * 2).await;
    }
}

/// **`close` waits out a replica-state write that found the log open**, so
/// nothing is written into a directory another log may own once it returns.
// The held locks stand in for a writer partway through its write.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn close_waits_for_a_replica_state_write_in_flight() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a", "b"])).await.expect("append");

    // Holding the flush lock stops `close` after its last write and before
    // it marks the log closed, where a writer could still find it open.
    let flushes = log.inner.durability.lock_flushes().await;
    let closing = tokio::spawn({
        let log = log.clone();
        async move { log.close().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!closing.is_finished(), "close did not reach the flush lock");

    // A writer that has seen the log open and holds the writer's lock.
    let writer = log.inner.replica_persisted.lock();
    assert!(!log.inner.closed.load(Ordering::Acquire));
    drop(flushes);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !closing.is_finished(),
        "close returned while a replica-state write was in flight"
    );
    drop(writer);
    closing.await.expect("join").expect("close");
}

/// **A raised generation is still on disk before it is answered**, with a
/// commit-offset write behind in flight.
#[tokio::test]
async fn a_raised_generation_is_written_through_while_the_commit_offset_is_behind() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.append(&records(&["a", "b"])).await.expect("append");
        log.advance_commit_offset(2).await.expect("commit");
        assert_eq!(
            log.accept_generation(7, None).await.expect("accept"),
            GenerationCheck::Raised
        );
    }
    // Reopened at once: the raise was durable when it returned, and carried
    // the commit offset raised before it.
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.accepted_generation(), 7);
    assert_eq!(log.commit_offset(), 2);
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

#[tokio::test]
async fn a_failed_commit_offset_write_is_retried_with_the_same_offset() {
    let dir = tempdir().expect("tempdir");
    // Not `OnCommit`, which writes behind and does not report the failure.
    let periodic = FsyncMode::Periodic {
        interval: std::time::Duration::from_secs(3600),
    };
    let log = open(&dir, periodic);
    log.append(&records(&["a", "b", "c"]))
        .await
        .expect("append");

    // A directory where the temporary goes makes the write fail.
    let blocker = dir.path().join("replica.tmp");
    std::fs::create_dir(&blocker).expect("block the write");
    log.advance_commit_offset(2)
        .await
        .expect_err("injected write failure");
    assert_eq!(log.commit_offset(), 2, "the offset holds in memory");
    std::fs::remove_dir(&blocker).expect("unblock");

    log.advance_commit_offset(2).await.expect("retry");
    drop(log);
    let reopened = open(&dir, periodic);
    assert_eq!(reopened.commit_offset(), 2, "the retry reached disk");
}

/// **A failed commit-offset write is retried with nothing calling again.** On
/// an idle follower no batch comes to carry the offset a second time.
#[tokio::test]
async fn a_failed_commit_offset_write_is_retried_on_its_own() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a", "b", "c"]))
        .await
        .expect("append");

    let blocker = dir.path().join("replica.tmp");
    std::fs::create_dir(&blocker).expect("block the write");
    log.advance_commit_offset(2)
        .await
        .expect("written behind, so the failure is not the caller's");
    // Let the write behind fail before unblocking.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    std::fs::remove_dir(&blocker).expect("unblock");

    let written = async {
        loop {
            let state = crate::disk_log::replica_state::load(dir.path()).expect("replica state");
            if state.commit_offset == 2 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(COMMIT_PERSIST_INTERVAL * 5, written)
        .await
        .expect("the failed write was never retried");
}

/// **A closed log writes no replica state**, so a retry still pending cannot
/// land in a directory another log now owns.
#[tokio::test]
async fn a_closed_log_writes_no_replica_state() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a", "b", "c"]))
        .await
        .expect("append");
    log.close().await.expect("close");

    log.advance_commit_offset(2)
        .await
        .expect_err("a closed log wrote its replica state");
    assert_eq!(
        crate::disk_log::replica_state::load(dir.path())
            .expect("replica state")
            .commit_offset,
        0
    );
}

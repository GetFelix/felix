//! The mark log: what a write leaves on disk, group commit, a torn tail,
//! damage, and the rewrite.

use super::*;

fn open(root: &Root) -> MarkLog {
    let id = storage_identity::load(root.path())
        .expect("id")
        .expect("an id");
    MarkLog::open(root.path(), id, Some(root.shm.path().to_path_buf())).expect("open")
}

fn file(root: &Root) -> PathBuf {
    root.path().join(super::super::marks::FILE_NAME)
}

#[tokio::test]
async fn marks_read_back_after_a_reopen_and_each_write_raises_the_sequence() {
    let root = Root::new();
    let log = open(&root);
    assert_eq!(log.sequence(), 0);
    log.fresh(Arc::from(SHARD), 7).await.expect("fresh");
    let marks = log.for_copy(Arc::from(SHARD), 7);
    marks.vote(vote(3, "a#7", false)).await.expect("vote");
    marks.log(Some(point(2, 9))).await.expect("log");
    assert_eq!(log.sequence(), 3);
    // Saving what is already synced writes nothing.
    marks.log(Some(point(2, 9))).await.expect("again");
    assert_eq!(log.sequence(), 3);
    drop(log);

    let reopened = open(&root);
    assert_eq!(reopened.sequence(), 3);
    assert_eq!(
        reopened.marks(SHARD),
        Some(ShardMarks {
            copy: 7,
            vote: vote(3, "a#7", false),
            log: Some(point(2, 9)),
        })
    );
}

#[tokio::test]
async fn a_mark_for_another_copy_is_refused() {
    let root = Root::new();
    let log = open(&root);
    log.fresh(Arc::from(SHARD), 7).await.expect("fresh");
    let stale = log.for_copy(Arc::from(SHARD), 6);
    let err = stale.vote(vote(1, "a#6", false)).await.expect_err("stale");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{err}");
    assert_eq!(log.marks(SHARD).expect("marks").vote, VoteMark::default());
}

/// Writers that queue while a write is in flight share the next one: one
/// append and one fsync, and the sequence rises once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_share_one_sync() {
    let root = Root::new();
    let log = open(&root);
    let shards: Vec<Arc<str>> = (0..64).map(|n| Arc::from(format!("shard-{n}"))).collect();
    for shard in &shards {
        log.fresh(Arc::clone(shard), 1).await.expect("fresh");
    }
    let before = log.sequence();
    let held = log.hold_writer().await;
    let writes: Vec<_> = shards
        .iter()
        .map(|shard| {
            let marks = log.for_copy(Arc::clone(shard), 1);
            tokio::spawn(async move { marks.vote(vote(5, "b#1", false)).await })
        })
        .collect();
    // Every writer has queued before the write in flight ends.
    while log.queued() < shards.len() {
        tokio::task::yield_now().await;
    }
    drop(held);
    for write in writes {
        write.await.expect("task").expect("vote");
    }
    assert_eq!(log.sequence(), before + 1);
    drop(log);
    let reopened = open(&root);
    for shard in &shards {
        assert_eq!(
            reopened.marks(shard).expect("marks").vote,
            vote(5, "b#1", false)
        );
    }
}

/// A lowering waits for a write that has left the queue but not yet landed:
/// that write may raise the mark above the cut, and a crash after the cut
/// would then leave a mark above the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lowering_waits_for_a_write_in_flight() {
    let root = Root::new();
    let log = open(&root);
    log.fresh(Arc::from(SHARD), 1).await.expect("fresh");
    let marks = log.for_copy(Arc::from(SHARD), 1);
    marks.log(Some(point(1, 5))).await.expect("log");

    let stalled = log.stall_writes().await;
    let taken = log.writes_taken();
    let raise = {
        let marks = marks.clone();
        tokio::spawn(async move { marks.log(Some(point(1, 10))).await })
    };
    while log.writes_taken() == taken {
        tokio::task::yield_now().await;
    }
    let lower = {
        let marks = marks.clone();
        tokio::spawn(async move { marks.lower_log(Some(point(1, 7))).await })
    };
    while log.queued() == 0 && !lower.is_finished() {
        tokio::task::yield_now().await;
    }
    drop(stalled);
    raise.await.expect("task").expect("raise");
    lower.await.expect("task").expect("lower");
    assert_eq!(log.marks(SHARD).expect("marks").log, Some(point(1, 7)));
    drop(log);
    assert_eq!(
        open(&root).marks(SHARD).expect("marks").log,
        Some(point(1, 7))
    );
}

#[tokio::test]
async fn each_synced_write_reaches_the_witness() {
    let root = Root::new();
    let log = open(&root);
    log.fresh(Arc::from(SHARD), 7).await.expect("fresh");
    log.for_copy(Arc::from(SHARD), 7)
        .vote(vote(2, "a#7", false))
        .await
        .expect("vote");
    let recorded = crate::run_state::witness::recorded_sequence(root.path(), Some(root.shm.path()));
    assert_eq!(recorded, Some((log.storage_id(), 2)));
}

/// The last write was never synced, so nobody was answered for it: the
/// open cuts it and the marks before it stand.
#[tokio::test]
async fn a_torn_last_write_is_cut() {
    let root = Root::new();
    let log = open(&root);
    log.fresh(Arc::from(SHARD), 7).await.expect("fresh");
    log.for_copy(Arc::from(SHARD), 7)
        .vote(vote(2, "a#7", false))
        .await
        .expect("vote");
    drop(log);
    let good = std::fs::read(file(&root)).expect("read");
    // Half of a copy of the last write, as a power cut mid-append leaves.
    let mut torn = good.clone();
    torn.extend_from_slice(&good[good.len() - 20..good.len() - 5]);
    std::fs::write(file(&root), &torn).expect("write");

    let reopened = open(&root);
    assert_eq!(reopened.sequence(), 2);
    assert_eq!(
        reopened.marks(SHARD).expect("marks").vote,
        vote(2, "a#7", false)
    );
    assert_eq!(std::fs::read(file(&root)).expect("read"), good);
    reopened
        .for_copy(Arc::from(SHARD), 7)
        .vote(vote(3, "a#7", false))
        .await
        .expect("a write after the cut");
    drop(reopened);
    assert_eq!(open(&root).sequence(), 3);
}

#[tokio::test]
async fn damage_before_a_good_write_fails_the_open() {
    let root = Root::new();
    let log = open(&root);
    log.fresh(Arc::from(SHARD), 7).await.expect("fresh");
    log.for_copy(Arc::from(SHARD), 7)
        .vote(vote(2, "a#7", false))
        .await
        .expect("vote");
    drop(log);
    let mut bytes = std::fs::read(file(&root)).expect("read");
    // A byte inside the first write's body.
    bytes[18 + 16] ^= 0xff;
    std::fs::write(file(&root), &bytes).expect("write");
    let id = storage_identity::load(root.path())
        .expect("id")
        .expect("an id");
    let err = MarkLog::open(root.path(), id, None).err().expect("damaged");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
}

/// Past its floor the file is rewritten as the live marks alone, and the
/// sequence goes on from where it was.
#[tokio::test]
async fn a_rewrite_keeps_the_live_marks_and_the_sequence() {
    let root = Root::new();
    let log = open(&root);
    log.set_compaction_floor(2048).await;
    let shards: Vec<Arc<str>> = (0..3).map(|n| Arc::from(format!("shard-{n}"))).collect();
    for shard in &shards {
        log.fresh(Arc::clone(shard), 9).await.expect("fresh");
    }
    let mut written = 0;
    for term in 1..=200u64 {
        let shard = &shards[term as usize % shards.len()];
        log.for_copy(Arc::clone(shard), 9)
            .log(Some(point(term, term * 10)))
            .await
            .expect("log");
        written += 1;
    }
    let sequence = log.sequence();
    assert_eq!(sequence, 3 + written);
    assert!(log.file_len().await < 2048, "never rewritten");
    let live: Vec<_> = shards.iter().map(|shard| log.marks(shard)).collect();
    drop(log);

    let reopened = open(&root);
    assert_eq!(reopened.sequence(), sequence);
    let read: Vec<_> = shards.iter().map(|shard| reopened.marks(shard)).collect();
    assert_eq!(read, live);
    reopened
        .for_copy(Arc::clone(&shards[0]), 9)
        .log(Some(point(999, 1)))
        .await
        .expect("log");
    assert_eq!(reopened.sequence(), sequence + 1);
}

/// The caller that takes a batch writes it for everyone in it. Dropping
/// that caller's future part way answers nobody early, and the log can
/// still write afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_writer_answers_nobody_before_the_sync() {
    use futures_util::FutureExt;

    let root = Root::new();
    let log = open(&root);
    for shard in ["a", "b"] {
        log.fresh(Arc::from(shard), 1).await.expect("fresh");
    }
    let held = log.hold_writer().await;
    let first = log.for_copy(Arc::from("a"), 1);
    let mut cancelled = Box::pin(async move { first.vote(vote(2, "x#1", false)).await });
    assert!((&mut cancelled).now_or_never().is_none(), "queued");
    let second = log.for_copy(Arc::from("b"), 1);
    let answered = tokio::spawn(async move { second.vote(vote(2, "y#1", false)).await });
    while log.queued() < 2 {
        tokio::task::yield_now().await;
    }
    let taken = log.writes_taken();
    let stalled = log.stall_writes().await;
    drop(held);
    // The first caller takes both changes and stops before the file.
    while log.writes_taken() == taken {
        assert!((&mut cancelled).now_or_never().is_none(), "stalled");
        tokio::task::yield_now().await;
    }
    drop(cancelled);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !answered.is_finished(),
        "a vote was answered before its mark was synced"
    );
    drop(stalled);
    answered.await.expect("task").expect("vote");
    assert_eq!(log.marks("b").expect("marks").vote, vote(2, "y#1", false));
    log.for_copy(Arc::from("a"), 1)
        .vote(vote(3, "x#1", false))
        .await
        .expect("a later write");
    assert_eq!(
        open(&root).marks("a").expect("marks").vote,
        vote(3, "x#1", false)
    );
}

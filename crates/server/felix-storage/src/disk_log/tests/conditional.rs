//! A claimed append that writes only at an expected offset.

use std::sync::Arc;

use super::*;
use crate::CommitSequencer;

/// Refused, the batch writes nothing and claims nothing, and the refusal
/// names the tail it was checked against. The next append takes the offset
/// the refused one would have had, and its turn is not held up.
#[tokio::test]
async fn an_append_at_a_stale_offset_writes_and_claims_nothing() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    let order = Arc::new(CommitSequencer::new(0));

    let (pending, turn) = log
        .append_claimed_at(0, &records(&["first"]), &order)
        .await
        .expect("append")
        .expect("the tail is 0");
    assert_eq!(pending.first_offset(), 0);
    turn.wait().await.expect("turn");
    drop(turn);
    log.commit(&pending).await.expect("commit");

    let refused = log
        .append_claimed_at(0, &records(&["stale"]), &order)
        .await
        .expect("append");
    assert_eq!(refused.err(), Some(1), "refused against the tail");
    assert_eq!(log.tail_offset().await.expect("tail"), 1);

    let (pending, turn) = log
        .append_claimed(&records(&["next"]), &order)
        .await
        .expect("append");
    assert_eq!(pending.first_offset(), 1);
    tokio::time::timeout(Duration::from_secs(5), turn.wait())
        .await
        .expect("a refused append left a range unreleased")
        .expect("turn");
    drop(turn);
    log.commit(&pending).await.expect("commit");
    assert_eq!(read_all(&log, 0).await, ["first", "next"]);
}

/// Many writers expecting the same offset: exactly one is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_of_many_appends_at_the_same_offset_is_written() {
    let dir = tempdir().expect("dir");
    let log = Arc::new(open(&dir, FsyncMode::None));
    let order = Arc::new(CommitSequencer::new(0));
    log.append(&records(&["a", "b"])).await.expect("append");
    order.reset(2);

    let writers: Vec<_> = (0..16)
        .map(|i| {
            let log = Arc::clone(&log);
            let order = Arc::clone(&order);
            tokio::spawn(async move {
                let payload = format!("writer-{i}");
                log.append_claimed_at(2, &records(&[&payload]), &order)
                    .await
                    .expect("append")
                    .map(|(pending, _turn)| pending.first_offset())
            })
        })
        .collect();
    let mut written = Vec::new();
    for writer in writers {
        match writer.await.expect("task") {
            Ok(first) => written.push(first),
            Err(tail) => assert_eq!(tail, 3),
        }
    }
    assert_eq!(written, [2]);
    assert_eq!(log.tail_offset().await.expect("tail"), 3);
}

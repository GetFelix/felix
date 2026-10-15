//! A cursor stands only on the dead letters it passed records on.
//!
//! Each commit records how far the shard's dead-letter log reached and the
//! generation that wrote its end. A cursor whose dead letters this broker no
//! longer holds is put back, before the shard is served, to its latest record
//! that still stands or to where the group was last placed. Never removed:
//! a seek `if_new` would then create the group afresh past what it skipped.

use super::*;
use crate::queue::cursors::DeadLetterMark;

/// The shard's dead-letter log, as replication or recovery reaches it.
async fn dead_letter_log(dir: &std::path::Path) -> (DeadLetters, StreamLog) {
    let dead = DeadLetters::open(dir.join("dead"), config()).expect("dead letters");
    let log = StreamLog::from_log(dead.shard_log(T, NS, S, 0).await.expect("log"));
    (dead, log)
}

fn other_group() -> GroupKey {
    GroupKey {
        group: "others".to_string(),
        ..key()
    }
}

/// **A cursor that outlived its dead letter falls back.** The group gave up
/// on offset 0 and moved past it; a crash kept the cursor log's tail and not
/// the dead-letter log's. Served as it stood, offset 0 would be neither
/// finished nor dead.
#[tokio::test]
async fn a_cursor_left_without_its_dead_letter_falls_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key();
    {
        let fx = open_with_attempts(dir.path(), 1);
        dead_letter_the_only_record(&fx, &key, Instant::now()).await;
    }
    {
        let (_dead, log) = dead_letter_log(dir.path()).await;
        log.truncate(0).await.expect("lose the dead letter");
    }

    let fx = open(dir.path());

    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(0));
    assert_eq!(
        payloads(&poll_at(&fx, &key, Instant::now(), 0).await),
        vec!["poison"]
    );
}

/// **A group that fell back still exists**, so a seek `if_new` leaves it
/// where it is rather than creating it afresh at the tail.
#[tokio::test]
async fn an_if_new_seek_finds_a_group_that_fell_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key();
    {
        let fx = open_with_attempts(dir.path(), 1);
        dead_letter_the_only_record(&fx, &key, Instant::now()).await;
    }
    {
        let (_dead, log) = dead_letter_log(dir.path()).await;
        log.truncate(0).await.expect("lose the dead letter");
    }

    let fx = open(dir.path());
    let seek = fx.reader.seek(&key, 100, true).await.expect("seek");

    assert_eq!(
        seek,
        Seek {
            offset: 0,
            moved: false
        }
    );
}

/// **The latest record that still stands is kept**, not the group's origin:
/// only what the lost dead letters could have let it pass is given back.
#[tokio::test]
async fn the_latest_cursor_that_still_stands_is_kept() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key();
    {
        let fx = open(dir.path());
        let cursors = &fx.reader.cursors;
        let live = AtomicBool::new(false);
        let commit = |offset, end| {
            cursors.commit_unless(
                T,
                NS,
                S,
                0,
                G,
                offset,
                &live,
                DeadLetterMark { end, generation: 0 },
            )
        };
        commit(1, 0).await.expect("commit");
        fx.reader.dead_letters.record(&key, 1).await.expect("dead");
        commit(2, 1).await.expect("commit");
        fx.reader.dead_letters.record(&key, 2).await.expect("dead");
        commit(3, 2).await.expect("commit");
    }
    {
        let (_dead, log) = dead_letter_log(dir.path()).await;
        log.truncate(1).await.expect("lose the second dead letter");
    }

    let fx = open(dir.path());

    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(2));
}

/// **A dead letter lost and its offset refilled by another generation does
/// not bring the cursor back.** The log is as long again, but what is at its
/// end now is another group's dead letter, written by the next leader.
#[tokio::test]
async fn a_refilled_dead_letter_log_does_not_restore_a_cursor() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key();
    {
        let fx = open_with_attempts(dir.path(), 1);
        let log = StreamLog::from_log(
            fx.reader
                .dead_letters
                .shard_log(T, NS, S, 0)
                .await
                .expect("log"),
        );
        log.record_generation(4, 0).expect("label");
        dead_letter_the_only_record(&fx, &key, Instant::now()).await;
    }
    {
        let (dead, log) = dead_letter_log(dir.path()).await;
        log.truncate(0).await.expect("lose the dead letter");
        log.record_generation(5, 0).expect("label");
        dead.record(&other_group(), 0).await.expect("dead");
    }

    let fx = open(dir.path());

    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(0));
}

/// **A commit records the dead letters it passed records on.**
#[tokio::test]
async fn a_commit_records_the_dead_letters_beneath_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open_with_attempts(dir.path(), 1);
    let key = key();

    dead_letter_the_only_record(&fx, &key, Instant::now()).await;

    let stored = fx
        .reader
        .cursors
        .stored(T, NS, S, 0, G)
        .await
        .expect("read")
        .expect("a cursor");
    assert_eq!(stored.offset, 1);
    assert_eq!(stored.requires.end, 1);
    assert_eq!(stored.origin, 0);
}

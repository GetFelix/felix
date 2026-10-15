//! Moving, describing and deleting a group.

use super::*;

fn offsets(claimed: &[Claimed]) -> Vec<u64> {
    claimed.iter().map(|c| c.offset).collect()
}

async fn finish(fx: &Fixture, key: &GroupKey, claimed: &[Claimed]) {
    for claimed in claimed {
        fx.reader.ack(key, claimed.offset).await.expect("ack");
    }
}

#[tokio::test]
async fn a_seek_backwards_replays_what_the_group_finished() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "b", "c"]).await;
    let base = Instant::now();
    let first = poll_at(&fx, &key, base, 0).await;
    finish(&fx, &key, &first).await;
    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(3));

    let seek = fx.reader.seek(&key, 1, false).await.expect("seek");
    assert_eq!(
        seek,
        Seek {
            offset: 1,
            moved: true
        }
    );
    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(1));
    assert_eq!(payloads(&poll_at(&fx, &key, base, 1).await), ["b", "c"]);
}

#[tokio::test]
async fn a_seek_forwards_skips_what_is_below_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "b", "c"]).await;

    fx.reader.seek(&key, 2, false).await.expect("seek");
    assert_eq!(
        payloads(&poll_at(&fx, &key, Instant::now(), 0).await),
        ["c"]
    );
}

/// `if_new` creates a group where asked, and leaves one that exists alone.
#[tokio::test]
async fn creating_a_group_that_exists_leaves_it_where_it_is() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "b", "c"]).await;

    let created = fx.reader.seek(&key, 2, true).await.expect("create");
    assert_eq!(
        created,
        Seek {
            offset: 2,
            moved: true
        }
    );
    let again = fx.reader.seek(&key, 0, true).await.expect("create again");
    assert_eq!(
        again,
        Seek {
            offset: 2,
            moved: false
        }
    );
    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(2));
}

/// A group being consumed has no cursor until its first run closes, and it
/// still exists.
#[tokio::test]
async fn creating_a_group_with_claims_but_no_cursor_leaves_it_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "b"]).await;
    assert_eq!(poll_at(&fx, &key, Instant::now(), 0).await.len(), 2);

    let created = fx.reader.seek(&key, 2, true).await.expect("create");
    assert_eq!(
        created,
        Seek {
            offset: 0,
            moved: false
        }
    );
    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), None);
}

/// **A late ack from before a seek does not count.** The claim was void the
/// moment the group moved, and taking the ack would mark a record the group
/// now owes as finished.
#[tokio::test]
async fn an_ack_for_a_claim_from_before_a_seek_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "b", "c"]).await;
    let base = Instant::now();
    assert_eq!(poll_at(&fx, &key, base, 0).await.len(), 3);

    fx.reader.seek(&key, 0, false).await.expect("seek");
    // As the broker does before every settle, so a claim from before a
    // failover is taken.
    let tail = fx.log.tail_offset().await.expect("tail");
    fx.reader.inherit_below(&key, tail).await.expect("inherit");
    let err = fx.reader.ack(&key, 1).await.expect_err("stale ack taken");
    assert!(matches!(err, BrokerError::GroupOffsetNotHandedOut { .. }));
    assert_eq!(offsets(&poll_at(&fx, &key, base, 1).await), [0, 1, 2]);
}

/// **An ack already past its checks when a seek lands does not move the new
/// cursor.** The old tracker settles it, but its commit is dropped.
#[tokio::test]
async fn an_ack_racing_a_seek_does_not_move_the_new_cursor() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "b", "c"]).await;
    let base = Instant::now();
    assert_eq!(poll_at(&fx, &key, base, 0).await.len(), 3);
    // What an ack holds between finding the tracker and committing.
    let held = fx.reader.tracker_for(&key).await.expect("tracker");

    fx.reader.seek(&key, 0, false).await.expect("seek");
    for offset in 0..3 {
        fx.reader.settle(&key, &held, offset).await.expect("settle");
    }

    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(0));
    assert_eq!(
        payloads(&poll_at(&fx, &key, base, 1).await),
        ["a", "b", "c"]
    );
}

/// A seek moves the cursor and leaves the dead letters; a delete takes both.
#[tokio::test]
async fn a_seek_keeps_dead_letters_and_a_delete_removes_them() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open_with_attempts(dir.path(), 1);
    let key = key();
    let base = Instant::now();
    dead_letter_the_only_record(&fx, &key, base).await;

    fx.reader.seek(&key, 1, false).await.expect("seek");
    assert_eq!(fx.reader.dead_lettered(&key).await.expect("list"), vec![0]);

    assert!(fx.reader.delete(&key).await.expect("delete"));
    assert!(
        fx.reader
            .dead_lettered(&key)
            .await
            .expect("list")
            .is_empty()
    );
    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), None);
    assert!(!fx.reader.delete(&key).await.expect("delete again"));
    let created = fx.reader.seek(&key, 1, true).await.expect("create");
    assert_eq!(
        created,
        Seek {
            offset: 1,
            moved: true
        }
    );
}

/// A deleted group polled again starts afresh, and a pending redrive of the
/// old one is not owed to it.
#[tokio::test]
async fn a_deleted_group_starts_again_from_the_beginning() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open_with_attempts(dir.path(), 1);
    let key = key();
    let base = Instant::now();
    dead_letter_the_only_record(&fx, &key, base).await;
    assert!(fx.reader.redrive(&key, 0).await.expect("redrive"));
    publish(&fx.log, &["next"]).await;

    fx.reader.delete(&key).await.expect("delete");
    let again = poll_at(&fx, &key, base, 62).await;
    assert_eq!(offsets(&again), [0, 1]);
    assert_eq!(again[0].attempts, 1);
    assert!(
        fx.reader
            .dead_letters
            .redriven(&key)
            .await
            .expect("redriven")
            .is_empty()
    );
}

/// Deleting one group leaves another on the same shard alone.
#[tokio::test]
async fn deleting_one_group_leaves_another_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    let other = GroupKey {
        group: "other".into(),
        ..key.clone()
    };
    publish(&fx.log, &["a"]).await;
    for key in [&key, &other] {
        let claimed = poll_at(&fx, key, Instant::now(), 0).await;
        finish(&fx, key, &claimed).await;
    }

    fx.reader.delete(&key).await.expect("delete");
    assert_eq!(fx.reader.committed(&other).await.expect("cursor"), Some(1));
}

#[tokio::test]
async fn describe_reports_the_cursor_and_what_is_outstanding() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    assert_eq!(
        fx.reader.describe(&key).await.expect("describe"),
        GroupSnapshot {
            committed: None,
            in_flight: 0,
            owed: 0,
            dead_letters: 0,
        }
    );
    publish(&fx.log, &["a", "b", "c", "d"]).await;
    let base = Instant::now();
    let claimed = poll_at(&fx, &key, base, 0).await;
    fx.reader.ack(&key, claimed[0].offset).await.expect("ack");
    fx.reader.nack(&key, claimed[1].offset).await.expect("nack");

    assert_eq!(
        fx.reader.describe(&key).await.expect("describe"),
        GroupSnapshot {
            committed: Some(1),
            in_flight: 2,
            owed: 1,
            dead_letters: 0,
        }
    );
}

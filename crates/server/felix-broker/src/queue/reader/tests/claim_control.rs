//! A consumer extending its claim, nacking with a delay, and giving up on a
//! record, through the reader.

use super::*;

fn offsets(claimed: &[Claimed]) -> Vec<u64> {
    claimed.iter().map(|c| c.offset).collect()
}

async fn poll_for(
    fx: &Fixture,
    key: &GroupKey,
    now: Instant,
    visibility: Duration,
) -> Vec<Claimed> {
    fx.reader
        .poll_below(key, &fx.log, u64::MAX, 10, now, None, Some(visibility))
        .await
        .expect("poll")
}

#[tokio::test]
async fn a_poll_can_ask_for_its_own_visibility() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a"]).await;
    let base = Instant::now();

    assert_eq!(
        offsets(&poll_for(&fx, &key, base, Duration::from_secs(300)).await),
        [0]
    );
    // Past the reader's own thirty seconds, the claim still stands.
    assert!(poll_at(&fx, &key, base, 31).await.is_empty());
    assert_eq!(offsets(&poll_at(&fx, &key, base, 301).await), [0]);
}

#[tokio::test]
async fn an_extended_claim_outlasts_the_visibility_timeout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a"]).await;
    let base = Instant::now();
    let claimed = poll_at(&fx, &key, base, 0).await;

    fx.reader
        .extend(
            &key,
            0,
            claimed[0].attempts,
            Duration::from_secs(120),
            base + Duration::from_secs(25),
        )
        .await
        .expect("extend");

    assert!(poll_at(&fx, &key, base, 31).await.is_empty());
    assert_eq!(offsets(&poll_at(&fx, &key, base, 146).await), [0]);
}

#[tokio::test]
async fn extending_a_lapsed_claim_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a"]).await;
    let base = Instant::now();
    poll_at(&fx, &key, base, 0).await;
    // Handed to the next poll once it lapsed.
    assert_eq!(offsets(&poll_at(&fx, &key, base, 31).await), [0]);

    let err = fx
        .reader
        .extend(&key, 0, 1, VIS, base + Duration::from_secs(32))
        .await
        .expect_err("a lapsed claim was extended");
    assert!(matches!(err, BrokerError::GroupClaimLapsed { offset: 0 }));
}

/// **A seek voids standing claims, extensions included.** Extending one
/// afterwards would hold a record the group now owes.
#[tokio::test]
async fn a_claim_from_before_a_seek_cannot_be_extended_or_dead_lettered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "b"]).await;
    let base = Instant::now();
    assert_eq!(poll_at(&fx, &key, base, 0).await.len(), 2);

    fx.reader.seek(&key, 0, false).await.expect("seek");
    let tail = fx.log.tail_offset().await.expect("tail");
    fx.reader.inherit_below(&key, tail).await.expect("inherit");

    let now = base + Duration::from_secs(1);
    assert!(fx.reader.extend(&key, 0, 1, VIS, now).await.is_err());
    assert!(fx.reader.dead_letter(&key, 1).await.is_err());
    assert!(
        fx.reader
            .dead_lettered(&key)
            .await
            .expect("list")
            .is_empty()
    );
    assert_eq!(offsets(&poll_at(&fx, &key, base, 2).await), [0, 1]);
}

#[tokio::test]
async fn a_delayed_nack_is_redelivered_after_the_delay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a"]).await;
    let base = Instant::now();
    poll_at(&fx, &key, base, 0).await;

    fx.reader
        .nack_after(
            &key,
            0,
            None,
            Duration::from_secs(10),
            base + Duration::from_secs(1),
        )
        .await
        .expect("nack");

    assert!(poll_at(&fx, &key, base, 5).await.is_empty());
    let again = poll_at(&fx, &key, base, 12).await;
    assert_eq!(offsets(&again), [0]);
    assert_eq!(again[0].attempts, 2);
}

/// **A late nack cannot take a redelivery from whoever now holds it.** The
/// first delivery's claim lapsed and the record went out again; that first
/// consumer's delayed nack names its own delivery and is refused, so the
/// second claim stands and the record is not parked behind the delay.
#[tokio::test]
async fn a_nack_naming_a_lapsed_delivery_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a"]).await;
    let base = Instant::now();
    let first = poll_at(&fx, &key, base, 0).await;
    let second = poll_at(&fx, &key, base, 31).await;
    assert_eq!(offsets(&second), [0]);
    assert_eq!((first[0].attempts, second[0].attempts), (1, 2));

    let now = base + Duration::from_secs(32);
    for delay in [Duration::from_secs(600), Duration::ZERO] {
        let err = fx
            .reader
            .nack_after(&key, 0, Some(first[0].attempts), delay, now)
            .await
            .expect_err("a stale nack was taken");
        assert!(matches!(err, BrokerError::GroupClaimLapsed { offset: 0 }));
    }

    // The second delivery still holds its claim, and can extend and nack it.
    assert!(poll_at(&fx, &key, base, 33).await.is_empty());
    fx.reader
        .extend(&key, 0, second[0].attempts, VIS, now)
        .await
        .expect("the current claim was disturbed");
    fx.reader
        .nack_after(&key, 0, Some(second[0].attempts), Duration::ZERO, now)
        .await
        .expect("nack the current delivery");
    let third = poll_at(&fx, &key, base, 34).await;
    assert_eq!(offsets(&third), [0]);
    assert_eq!(third[0].attempts, 3);
}

/// A delayed nack is not a claim, so naming the same delivery twice is
/// refused the second time rather than pushing the delay out again.
#[tokio::test]
async fn a_delivery_can_be_nacked_only_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a"]).await;
    let base = Instant::now();
    let claimed = poll_at(&fx, &key, base, 0).await;
    let now = base + Duration::from_secs(1);
    let attempts = Some(claimed[0].attempts);

    fx.reader
        .nack_after(&key, 0, attempts, Duration::from_secs(10), now)
        .await
        .expect("nack");
    let err = fx
        .reader
        .nack_after(&key, 0, attempts, Duration::from_secs(600), now)
        .await
        .expect_err("a second nack moved the delay");
    assert!(matches!(err, BrokerError::GroupClaimLapsed { offset: 0 }));
    assert_eq!(offsets(&poll_at(&fx, &key, base, 12).await), [0]);
}

#[tokio::test]
async fn a_consumer_dead_letter_is_listed_finished_and_redrivable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["poison", "fine"]).await;
    let base = Instant::now();
    assert_eq!(poll_at(&fx, &key, base, 0).await.len(), 2);

    assert!(fx.reader.dead_letter(&key, 0).await.expect("dead letter"));
    fx.reader.ack(&key, 1).await.expect("ack");

    assert_eq!(fx.reader.dead_lettered(&key).await.expect("list"), vec![0]);
    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(2));
    assert!(poll_at(&fx, &key, base, 31).await.is_empty());

    assert!(fx.reader.redrive(&key, 0).await.expect("redrive"));
    let again = poll_at(&fx, &key, base, 32).await;
    assert_eq!(payloads(&again), ["poison"]);
    assert_eq!(again[0].attempts, 1, "a redrive starts the count again");
}

/// A record someone already finished is not listed as given up on.
#[tokio::test]
async fn dead_lettering_a_finished_record_writes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a"]).await;
    let base = Instant::now();
    poll_at(&fx, &key, base, 0).await;
    fx.reader.ack(&key, 0).await.expect("ack");

    assert!(!fx.reader.dead_letter(&key, 0).await.expect("dead letter"));
    assert!(
        fx.reader
            .dead_lettered(&key)
            .await
            .expect("list")
            .is_empty()
    );
}

#[tokio::test]
async fn an_offset_never_handed_out_cannot_be_dead_lettered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a"]).await;

    let err = fx
        .reader
        .dead_letter(&key, 0)
        .await
        .expect_err("dead-lettered a record nobody received");
    assert!(matches!(err, BrokerError::GroupOffsetNotHandedOut { .. }));
}

/// **A claim that outlasts the idle time keeps its tracker.** Dropping it
/// would rebuild the group from the cursor and hand the record out while
/// the consumer still holds it.
#[tokio::test]
async fn a_tracker_with_a_standing_claim_is_not_evicted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a"]).await;
    let base = Instant::now();
    let hours = Duration::from_secs(3 * 3600);
    poll_for(&fx, &key, base, hours).await;

    assert_eq!(fx.reader.evict_idle(base + Duration::from_secs(3600)), 0);
    assert!(
        poll_at(&fx, &key, base, 3601).await.is_empty(),
        "the record went out again while its claim stood"
    );
    // Gone once the claim has lapsed.
    assert_eq!(fx.reader.evict_idle(base + hours * 2), 1);
}

/// A dead letter above the cursor stays settled when a new leader rebuilds
/// the group before the cursor advances over it.
#[tokio::test]
async fn a_dead_letter_above_the_cursor_is_not_redelivered_after_a_leader_change() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "poisoned"]).await;
    let base = Instant::now();
    assert_eq!(offsets(&poll_at(&fx, &key, base, 0).await), [0, 1]);
    assert!(fx.reader.dead_letter(&key, 1).await.expect("dead letter"));
    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), None);

    let successor = GroupReader::new(
        Arc::clone(&fx.reader.cursors),
        Arc::clone(&fx.reader.dead_letters),
        VIS,
        MANY,
    );
    let polled = successor
        .poll(&key, &fx.log, 10, base + Duration::from_secs(1))
        .await
        .expect("poll");
    assert_eq!(offsets(&polled), [0], "the dead letter was delivered again");
    successor.ack(&key, 0).await.expect("ack");
    assert_eq!(successor.committed(&key).await.expect("cursor"), Some(2));
}

/// A seek back over a dead letter delivers it again, whether or not the
/// tracker is rebuilt before the next poll.
#[tokio::test]
async fn a_seek_back_over_a_dead_letter_delivers_it_again_after_a_leader_change() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    let records: Vec<String> = (0..10).map(|i| format!("r{i}")).collect();
    let records: Vec<&str> = records.iter().map(String::as_str).collect();
    publish(&fx.log, &records).await;
    let base = Instant::now();
    assert_eq!(poll_at(&fx, &key, base, 0).await.len(), 10);
    assert!(fx.reader.dead_letter(&key, 5).await.expect("dead letter"));
    for offset in (0..10).filter(|&offset| offset != 5) {
        fx.reader.ack(&key, offset).await.expect("ack");
    }
    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(10));

    fx.reader.seek(&key, 0, false).await.expect("seek");

    let successor = GroupReader::new(
        Arc::clone(&fx.reader.cursors),
        Arc::clone(&fx.reader.dead_letters),
        VIS,
        MANY,
    );
    let polled = successor
        .poll(&key, &fx.log, 10, base + Duration::from_secs(1))
        .await
        .expect("poll");
    assert_eq!(offsets(&polled), (0..10).collect::<Vec<_>>());
    // The marker is gone, not just skipped over.
    assert_eq!(
        fx.reader.dead_letters.list(&key).await.expect("list"),
        [0u64; 0]
    );
}

/// Ten records consumed, offset 5 dead-lettered and the rest acked: the
/// cursor at 10 with one dead letter below it.
async fn consumed_with_a_dead_letter(fx: &Fixture, key: &GroupKey) -> Instant {
    let records: Vec<String> = (0..10).map(|i| format!("r{i}")).collect();
    let records: Vec<&str> = records.iter().map(String::as_str).collect();
    publish(&fx.log, &records).await;
    let base = Instant::now();
    assert_eq!(poll_at(fx, key, base, 0).await.len(), 10);
    assert!(fx.reader.dead_letter(key, 5).await.expect("dead letter"));
    for offset in (0..10).filter(|&offset| offset != 5) {
        fx.reader.ack(key, offset).await.expect("ack");
    }
    assert_eq!(fx.reader.committed(key).await.expect("cursor"), Some(10));
    base
}

/// The broker after a crash: same disk, nothing in memory.
fn restarted(fx: &Fixture) -> GroupReader {
    GroupReader::new(
        Arc::clone(&fx.reader.cursors),
        Arc::clone(&fx.reader.dead_letters),
        VIS,
        MANY,
    )
}

/// A seek that crashes after beginning its clear and before its cursor is
/// written clears nothing: the dead letter is still listed and redrivable,
/// and the group resumes where it was.
#[tokio::test]
async fn a_seek_that_crashes_before_its_cursor_clears_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    let base = consumed_with_a_dead_letter(&fx, &key).await;
    fx.reader
        .dead_letters
        .begin_clear(&key, 0)
        .await
        .expect("begin")
        .expect("something to clear");
    fx.reader.dead_letter_mark(&key).await.expect("sync");

    let restarted = restarted(&fx);
    assert_eq!(restarted.committed(&key).await.expect("cursor"), Some(10));
    assert_eq!(restarted.dead_lettered(&key).await.expect("list"), [5]);
    // A commit after the restart passes the marker; the clear is still void.
    publish(&fx.log, &["r10"]).await;
    let polled = restarted
        .poll(&key, &fx.log, 10, base + Duration::from_secs(1))
        .await
        .expect("poll");
    assert_eq!(offsets(&polled), [10]);
    restarted.ack(&key, 10).await.expect("ack");
    let again = self::restarted(&fx);
    assert_eq!(again.dead_lettered(&key).await.expect("list"), [5]);
    assert!(again.redrive(&key, 5).await.expect("redrive"));
}

/// A seek that crashes after its cursor is written and before its clear
/// finishes is finished on restart: the dead letter is gone and delivered
/// again.
#[tokio::test]
async fn a_seek_that_crashes_after_its_cursor_is_finished_on_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    let base = consumed_with_a_dead_letter(&fx, &key).await;
    let clear = fx
        .reader
        .dead_letters
        .begin_clear(&key, 0)
        .await
        .expect("begin")
        .expect("something to clear");
    let requires = fx
        .reader
        .dead_letters
        .clear_mark(&key, clear)
        .await
        .expect("mark");
    fx.reader
        .cursors
        .place_locked(
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            &key.group,
            0,
            requires,
        )
        .await
        .expect("cursor");

    let restarted = restarted(&fx);
    assert_eq!(
        restarted.dead_lettered(&key).await.expect("list"),
        [0u64; 0]
    );
    let polled = restarted
        .poll(&key, &fx.log, 10, base + Duration::from_secs(1))
        .await
        .expect("poll");
    assert_eq!(offsets(&polled), (0..10).collect::<Vec<_>>());
}

/// A dead letter from a tracker a seek has retired is not recorded: it
/// would land above the new cursor, after the seek's clear, and the next
/// rebuild would settle it unread.
#[tokio::test]
async fn a_retired_tracker_records_no_dead_letter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "b"]).await;
    assert_eq!(poll_at(&fx, &key, Instant::now(), 0).await.len(), 2);
    let (held, locked) = fx.reader.current(&key).await.expect("tracker");
    drop(locked);
    fx.reader.seek(&key, 0, false).await.expect("seek");

    assert!(
        !fx.reader
            .record_dead_letter(&key, &held, 1)
            .await
            .expect("record")
    );
    assert_eq!(
        fx.reader.dead_lettered(&key).await.expect("list"),
        [0u64; 0]
    );
}

/// A build that ignores clear markers takes over after a seek's marker and
/// before its cursor, consumes past the marker and records another dead
/// letter. Its cursor needs the log past the marker but is not the seek's,
/// so the clear is voided: deleting would lose dead letters below it.
#[tokio::test]
async fn a_cursor_from_a_build_without_clears_voids_the_clear() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    consumed_with_a_dead_letter(&fx, &key).await;
    fx.reader
        .dead_letters
        .begin_clear(&key, 0)
        .await
        .expect("begin")
        .expect("something to clear");
    // The older build: dead letter 12, then a commit at 15 that passes it.
    fx.reader
        .dead_letters
        .record(&key, 12)
        .await
        .expect("record");
    let requires = fx.reader.dead_letter_mark(&key).await.expect("sync");
    fx.reader
        .cursors
        .commit_unless(
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            &key.group,
            15,
            &AtomicBool::new(false),
            requires,
        )
        .await
        .expect("commit");

    let restarted = restarted(&fx);
    assert_eq!(restarted.dead_lettered(&key).await.expect("list"), [5, 12]);
    assert_eq!(restarted.committed(&key).await.expect("cursor"), Some(15));
    assert!(
        fx.reader
            .dead_letters
            .pending_clears(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await
            .expect("pending")
            .is_empty()
    );
}

/// Until the fleet finalizes cursor dependencies, a rebuild does not settle
/// a dead letter above the cursor: it is delivered again, as before.
#[tokio::test]
async fn without_cursor_dependencies_a_rebuild_settles_no_dead_letter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    fx.reader.cursors.record_dependencies_when(|| false);
    let key = key();
    publish(&fx.log, &["a", "poisoned"]).await;
    let base = Instant::now();
    assert_eq!(offsets(&poll_at(&fx, &key, base, 0).await), [0, 1]);
    assert!(fx.reader.dead_letter(&key, 1).await.expect("dead letter"));

    let polled = restarted(&fx)
        .poll(&key, &fx.log, 10, base + Duration::from_secs(1))
        .await
        .expect("poll");
    assert_eq!(offsets(&polled), [0, 1]);
}

/// Until the fleet finalizes cursor dependencies, a seek clears no dead
/// letters and writes no marker.
#[tokio::test]
async fn without_cursor_dependencies_a_seek_keeps_dead_letters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    fx.reader.cursors.record_dependencies_when(|| false);
    let key = key();
    consumed_with_a_dead_letter(&fx, &key).await;

    fx.reader.seek(&key, 0, false).await.expect("seek");

    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(0));
    assert_eq!(fx.reader.dead_lettered(&key).await.expect("list"), [5]);
    assert!(
        fx.reader
            .dead_letters
            .pending_clears(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await
            .expect("pending")
            .is_empty()
    );
}

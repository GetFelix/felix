//! A group reading a real log, with a real cursor underneath it.

use bytes::Bytes;
use felix_storage::log::{FsyncMode, LogConfig};

use super::*;
use crate::durable::{DurableStorage, StreamLog};
use crate::queue::DeadLetters;
use crate::queue::tracker::MAX_CLAIM;

const T: &str = "t1";
const NS: &str = "ns";
const S: &str = "orders";
const G: &str = "workers";
const VIS: Duration = Duration::from_secs(30);
/// High enough that the cases below never reach it; the bound has its own tests.
const MANY: u32 = 1_000;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 64 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn key() -> GroupKey {
    GroupKey {
        tenant_id: T.to_string(),
        namespace: NS.to_string(),
        stream: S.to_string(),
        shard: 0,
        group: G.to_string(),
    }
}

/// A shard log and a group reader over the same directory, the way the broker
/// arranges them: streams under one root, group cursors under another.
struct Fixture {
    log: StreamLog,
    reader: GroupReader,
}

fn open(dir: &std::path::Path) -> Fixture {
    open_with_attempts(dir, MANY)
}

fn open_with_attempts(dir: &std::path::Path, max_attempts: u32) -> Fixture {
    let storage = DurableStorage::open(dir.join("streams"), config()).expect("storage");
    let log = storage.open_stream(T, NS, S, 0).expect("stream log");
    let cursors = Arc::new(ConsumerGroups::open(dir.join("groups"), config()).expect("cursors"));
    let dead = Arc::new(DeadLetters::open(dir.join("dead"), config()).expect("dead letters"));
    Fixture {
        log,
        reader: GroupReader::new(cursors, dead, VIS, max_attempts),
    }
}

async fn publish(log: &StreamLog, payloads: &[&str]) {
    let owned: Vec<Bytes> = payloads
        .iter()
        .map(|p| Bytes::copy_from_slice(p.as_bytes()))
        .collect();
    log.append(&owned).await.expect("append");
}

fn payloads(claimed: &[Claimed]) -> Vec<String> {
    claimed
        .iter()
        .map(|c| String::from_utf8(c.payload.to_vec()).expect("utf8"))
        .collect()
}

#[tokio::test]
async fn a_group_reads_from_the_beginning_of_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b", "c"]).await;

    let claimed = fx
        .reader
        .poll(&key(), &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    assert_eq!(payloads(&claimed), vec!["a", "b", "c"]);
    assert_eq!(claimed[0].offset, 0);
}

/// A group reads no further than the commit point it is given: past it a
/// record can still be lost at failover and its offset reused.
#[tokio::test]
async fn a_group_reads_only_below_the_commit_point() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b", "c"]).await;
    let now = Instant::now();

    let first = fx
        .reader
        .poll_below(&key(), &fx.log, 1, 10, now, None)
        .await
        .expect("poll");
    assert_eq!(payloads(&first), vec!["a"]);
    let nothing = fx
        .reader
        .poll_below(&key(), &fx.log, 1, 10, now, None)
        .await
        .expect("poll");
    assert!(nothing.is_empty(), "handed out past the commit point");

    let rest = fx
        .reader
        .poll_below(&key(), &fx.log, 3, 10, now, None)
        .await
        .expect("poll");
    assert_eq!(payloads(&rest), vec!["b", "c"]);
}

#[tokio::test]
async fn an_empty_log_yields_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());

    let claimed = fx
        .reader
        .poll(&key(), &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    assert!(claimed.is_empty());
}

/// **The queue property, over a real log.** Two polls do not hand out the same
/// record while the first claim stands.
#[tokio::test]
async fn a_second_poll_does_not_repeat_an_outstanding_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b", "c", "d"]).await;
    let now = Instant::now();

    let first = fx.reader.poll(&key(), &fx.log, 2, now).await.expect("poll");
    let second = fx.reader.poll(&key(), &fx.log, 2, now).await.expect("poll");

    assert_eq!(payloads(&first), vec!["a", "b"]);
    assert_eq!(payloads(&second), vec!["c", "d"]);
}

/// The cursor is written only when a contiguous run closes, so an
/// acknowledgement above a gap leaves nothing on disk to resume from.
#[tokio::test]
async fn the_cursor_is_written_only_when_a_run_closes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b", "c"]).await;
    let key = key();
    fx.reader
        .poll(&key, &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    fx.reader.ack(&key, 1).await.expect("ack");
    assert_eq!(
        fx.reader.committed(&key).await.expect("committed"),
        None,
        "the cursor moved past an offset nobody finished",
    );

    fx.reader.ack(&key, 0).await.expect("ack");
    assert_eq!(fx.reader.committed(&key).await.expect("committed"), Some(2));
}

/// **The slice's headline.** Work finished before a restart is not handed out
/// again after one; work still outstanding is.
#[tokio::test]
async fn a_restart_resumes_from_the_cursor_and_redelivers_the_rest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key();

    {
        let fx = open(dir.path());
        publish(&fx.log, &["a", "b", "c", "d"]).await;
        let claimed = fx
            .reader
            .poll(&key, &fx.log, 10, Instant::now())
            .await
            .expect("poll");
        assert_eq!(claimed.len(), 4);
        // Finish the first two; leave the rest in flight.
        fx.reader.ack(&key, 0).await.expect("ack");
        fx.reader.ack(&key, 1).await.expect("ack");
    }

    // A new reader over the same directory: nothing in memory survives.
    let fx = open(dir.path());
    let after = fx
        .reader
        .poll(&key, &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    assert_eq!(
        payloads(&after),
        vec!["c", "d"],
        "the group either lost finished work or repeated it",
    );
}

/// A consumer that stops answering has its records handed to the next one.
#[tokio::test]
async fn a_lapsed_claim_is_delivered_to_the_next_poll() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b"]).await;
    let base = Instant::now();
    let key = key();

    let first = fx.reader.poll(&key, &fx.log, 10, base).await.expect("poll");
    assert_eq!(payloads(&first), vec!["a", "b"]);

    let again = fx
        .reader
        .poll(&key, &fx.log, 10, base + Duration::from_secs(31))
        .await
        .expect("poll");
    assert_eq!(payloads(&again), vec!["a", "b"]);
}

#[tokio::test]
async fn a_nacked_record_comes_back_on_the_next_poll() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b"]).await;
    let now = Instant::now();
    let key = key();

    fx.reader.poll(&key, &fx.log, 10, now).await.expect("poll");
    fx.reader.nack(&key, 0).await.expect("nack");

    let again = fx.reader.poll(&key, &fx.log, 10, now).await.expect("poll");
    assert_eq!(payloads(&again), vec!["a"]);
}

/// Two groups over one shard are independent: each sees every record.
#[tokio::test]
async fn two_groups_each_receive_every_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b"]).await;
    let now = Instant::now();

    let mut first = key();
    first.group = "one".to_string();
    let mut second = key();
    second.group = "two".to_string();

    let a = fx
        .reader
        .poll(&first, &fx.log, 10, now)
        .await
        .expect("poll");
    let b = fx
        .reader
        .poll(&second, &fx.log, 10, now)
        .await
        .expect("poll");

    assert_eq!(payloads(&a), vec!["a", "b"]);
    assert_eq!(payloads(&b), vec!["a", "b"], "a group stole another's work");
}

/// A record larger than the per-read budget is still delivered.
///
/// Records are fetched one at a time with a one-byte budget. That works only
/// because a read of a range that holds data never answers empty. If it ever
/// did, this record would be owed for ever and the group would stall here.
#[tokio::test]
async fn a_record_larger_than_the_read_budget_is_delivered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let big = "x".repeat(64 * 1024);
    publish(&fx.log, &["small", &big]).await;

    let claimed = fx
        .reader
        .poll(&key(), &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[1].payload.len(), big.len());
}

/// Every record reaches a consumer exactly once when every claim is answered.
#[tokio::test]
async fn every_record_is_delivered_once_when_all_are_acknowledged() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let sent: Vec<String> = (0..50).map(|i| format!("record-{i}")).collect();
    publish(
        &fx.log,
        &sent.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;

    let key = key();
    let mut seen = Vec::new();
    let now = Instant::now();
    loop {
        let batch = fx.reader.poll(&key, &fx.log, 7, now).await.expect("poll");
        if batch.is_empty() {
            break;
        }
        for claimed in batch {
            seen.push(String::from_utf8(claimed.payload.to_vec()).expect("utf8"));
            fx.reader.ack(&key, claimed.offset).await.expect("ack");
        }
    }

    assert_eq!(seen, sent);
    assert_eq!(
        fx.reader.committed(&key).await.expect("committed"),
        Some(50)
    );
}

/// **A poison record does not stall the queue.** After the attempt bound the
/// group gives up on it, records it as a dead letter, and moves on to the work
/// behind it.
#[tokio::test]
async fn a_record_that_is_never_acknowledged_is_dead_lettered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open_with_attempts(dir.path(), 2);
    publish(&fx.log, &["poison", "good"]).await;
    let key = key();
    let base = Instant::now();

    // Two deliveries of both, each abandoned.
    for round in 0..2 {
        let claimed = fx
            .reader
            .poll(&key, &fx.log, 10, base + Duration::from_secs(round * 31))
            .await
            .expect("poll");
        assert_eq!(claimed.len(), 2, "round {round}");
    }

    // The third poll gives up on both and moves the cursor past them.
    let after = fx
        .reader
        .poll(&key, &fx.log, 10, base + Duration::from_secs(3 * 31))
        .await
        .expect("poll");
    assert!(after.is_empty());
    assert_eq!(
        fx.reader.dead_lettered(&key).await.expect("dead letters"),
        vec![0, 1],
    );
    assert_eq!(fx.reader.committed(&key).await.expect("committed"), Some(2));
}

/// The record itself is still in the log at the offset that was recorded, so a
/// dead letter is a pointer rather than a copy — nothing is duplicated, and
/// nothing is lost.
#[tokio::test]
async fn a_dead_lettered_record_is_still_readable_from_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open_with_attempts(dir.path(), 1);
    publish(&fx.log, &["poison"]).await;
    let key = key();
    let base = Instant::now();

    fx.reader.poll(&key, &fx.log, 10, base).await.expect("poll");
    fx.reader
        .poll(&key, &fx.log, 10, base + Duration::from_secs(31))
        .await
        .expect("poll");

    let dead = fx.reader.dead_lettered(&key).await.expect("dead letters");
    assert_eq!(dead, vec![0]);

    let records = fx.log.read_from(dead[0], 1).await.expect("read");
    assert_eq!(records[0].payload.as_ref(), b"poison");
}

/// A consumer is told how many times a record has been delivered, so it can
/// treat a retry differently from a first attempt.
#[tokio::test]
async fn a_redelivered_record_reports_its_attempt_number() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["work"]).await;
    let base = Instant::now();

    let first = fx
        .reader
        .poll(&key(), &fx.log, 10, base)
        .await
        .expect("poll");
    assert_eq!(first[0].attempts, 1);

    let again = fx
        .reader
        .poll(&key(), &fx.log, 10, base + Duration::from_secs(31))
        .await
        .expect("poll");
    assert_eq!(again[0].attempts, 2);
}

#[tokio::test]
async fn a_reset_shard_resumes_from_a_cursor_moved_elsewhere() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b", "c"]).await;
    let now = Instant::now();
    let claimed = fx
        .reader
        .poll(&key(), &fx.log, 10, now)
        .await
        .expect("poll");
    assert_eq!(payloads(&claimed), ["a", "b", "c"]);
    fx.reader.ack(&key(), 0).await.expect("ack");

    // Another leader finished the rest while this one was not serving, and
    // the cursor it wrote came back with the shard.
    fx.reader
        .cursors
        .commit(T, NS, S, 0, G, 3)
        .await
        .expect("commit");

    let lapsed = now + VIS * 2;
    fx.reader.reset_shard(T, NS, S, 0).await;
    let again = fx
        .reader
        .poll(&key(), &fx.log, 10, lapsed)
        .await
        .expect("poll");
    assert!(again.is_empty(), "handed out {:?} again", payloads(&again));
}

#[tokio::test]
async fn resetting_one_shard_leaves_another_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b"]).await;
    let now = Instant::now();
    fx.reader.poll(&key(), &fx.log, 1, now).await.expect("poll");

    fx.reader.reset_shard(T, NS, S, 1).await;
    // Still claimed: the tracker for shard 0 was not touched.
    let next = fx
        .reader
        .poll(&key(), &fx.log, 10, now)
        .await
        .expect("poll");
    assert_eq!(payloads(&next), ["b"]);
}

/// Poll `fx` at `secs` after `base`, with room for everything.
async fn poll_at(fx: &Fixture, key: &GroupKey, base: Instant, secs: u64) -> Vec<Claimed> {
    fx.reader
        .poll(key, &fx.log, 10, base + Duration::from_secs(secs))
        .await
        .expect("poll")
}

/// Give up on offset 0 of a one-record log (attempt bound 1).
async fn dead_letter_the_only_record(fx: &Fixture, key: &GroupKey, base: Instant) {
    publish(&fx.log, &["poison"]).await;
    assert_eq!(poll_at(fx, key, base, 0).await.len(), 1);
    assert!(poll_at(fx, key, base, 31).await.is_empty());
    assert_eq!(fx.reader.dead_lettered(key).await.expect("list"), vec![0]);
    assert_eq!(fx.reader.committed(key).await.expect("cursor"), Some(1));
}

/// **A redrive survives losing the leader.** Once the operator is told it
/// worked, the record is owed until someone finishes it: a new leader, which
/// rebuilds the group from disk, must hand it out even though the cursor has
/// passed it and it is no longer listed as dead.
#[tokio::test]
async fn a_redriven_record_is_still_owed_after_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key();
    let base = Instant::now();
    {
        let fx = open_with_attempts(dir.path(), 1);
        dead_letter_the_only_record(&fx, &key, base).await;
        assert!(fx.reader.redrive(&key, 0).await.expect("redrive"));
    }

    let fx = open_with_attempts(dir.path(), 1);
    let again = poll_at(&fx, &key, base, 62).await;
    assert_eq!(
        again.iter().map(|c| c.offset).collect::<Vec<_>>(),
        vec![0],
        "the redriven record was lost with the leader",
    );
    assert_eq!(again[0].attempts, 1, "its attempts did not start over");
    assert!(
        fx.reader
            .dead_lettered(&key)
            .await
            .expect("list")
            .is_empty(),
        "a redriven record is listed as given up on",
    );
}

/// Once it is finished it stays finished: the redrive record is cleared, so
/// the next leader does not hand it out yet again.
#[tokio::test]
async fn a_finished_redrive_is_not_redelivered_after_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key();
    let base = Instant::now();
    {
        let fx = open_with_attempts(dir.path(), 1);
        dead_letter_the_only_record(&fx, &key, base).await;
        assert!(fx.reader.redrive(&key, 0).await.expect("redrive"));
        assert_eq!(poll_at(&fx, &key, base, 62).await.len(), 1);
        fx.reader.ack(&key, 0).await.expect("ack");
    }

    let fx = open_with_attempts(dir.path(), 1);
    assert!(
        poll_at(&fx, &key, base, 93).await.is_empty(),
        "a finished redrive came back",
    );
}

/// A redriven record that fails again goes back on the list, and is neither
/// owed nor listed twice.
#[tokio::test]
async fn a_redriven_record_that_fails_again_is_dead_again() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open_with_attempts(dir.path(), 1);
    let key = key();
    let base = Instant::now();
    dead_letter_the_only_record(&fx, &key, base).await;

    assert!(fx.reader.redrive(&key, 0).await.expect("redrive"));
    assert!(
        !fx.reader.redrive(&key, 0).await.expect("second redrive"),
        "a record already back in the queue was redriven twice",
    );
    assert!(
        !fx.reader.discard(&key, 0).await.expect("discard"),
        "discarding a redriven record forgot a record that is owed",
    );
    assert_eq!(poll_at(&fx, &key, base, 62).await.len(), 1);
    assert!(poll_at(&fx, &key, base, 93).await.is_empty());

    assert_eq!(fx.reader.dead_lettered(&key).await.expect("list"), vec![0]);
    assert!(poll_at(&fx, &key, base, 124).await.is_empty());
    // Dead again, so it can be redriven again.
    assert!(fx.reader.redrive(&key, 0).await.expect("redrive"));
}

/// An acknowledgement at or past the tail names a record that does not exist
/// yet. Taking it would finish that record before it is written, and the group
/// would skip it when it arrives.
#[tokio::test]
async fn an_ack_for_an_offset_never_handed_out_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a"]).await;
    assert_eq!(poll_at(&fx, &key, Instant::now(), 0).await.len(), 1);

    for offset in [1, u64::MAX] {
        assert!(
            matches!(
                fx.reader.ack(&key, offset).await,
                Err(BrokerError::GroupOffsetNotHandedOut { next: 1, .. })
            ),
            "ack of {offset} was accepted",
        );
        assert!(
            matches!(
                fx.reader.nack(&key, offset).await,
                Err(BrokerError::GroupOffsetNotHandedOut { next: 1, .. })
            ),
            "nack of {offset} was accepted",
        );
    }

    publish(&fx.log, &["b"]).await;
    fx.reader.ack(&key, 0).await.expect("ack");
    let next = poll_at(&fx, &key, Instant::now(), 0).await;
    assert_eq!(
        payloads(&next),
        vec!["b"],
        "a record acked early was skipped"
    );
}

/// **One failed dead-letter write does not stall the group.** The record is
/// owed again, still at its bound, so the next poll retries the write rather
/// than leaving it in no set at all with the cursor stuck below it.
#[tokio::test]
async fn a_failed_dead_letter_write_is_retried() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open_with_attempts(dir.path(), 1);
    let key = key();
    let base = Instant::now();
    publish(&fx.log, &["poison", "also"]).await;
    assert_eq!(poll_at(&fx, &key, base, 0).await.len(), 2);

    fx.reader
        .dead_letters()
        .fail_next_record
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(
        fx.reader
            .poll(&key, &fx.log, 10, base + Duration::from_secs(31))
            .await
            .is_err(),
        "the dead-letter write did not fail",
    );

    assert!(poll_at(&fx, &key, base, 62).await.is_empty());
    assert_eq!(
        fx.reader.dead_lettered(&key).await.expect("list"),
        vec![0, 1],
        "a record whose dead-letter write failed was never retried",
    );
    assert_eq!(fx.reader.committed(&key).await.expect("cursor"), Some(2));
}

/// A poll asking for more than the cap gets the cap, and the rest stay owed.
#[tokio::test]
async fn a_poll_hands_out_at_most_the_cap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    let many: Vec<String> = (0..MAX_CLAIM + 5).map(|i| i.to_string()).collect();
    let refs: Vec<&str> = many.iter().map(String::as_str).collect();
    publish(&fx.log, &refs).await;

    let first = fx
        .reader
        .poll(&key, &fx.log, usize::MAX, Instant::now())
        .await
        .expect("poll");
    assert_eq!(first.len(), MAX_CLAIM);
    let rest = fx
        .reader
        .poll(&key, &fx.log, usize::MAX, Instant::now())
        .await
        .expect("poll");
    assert_eq!(rest.len(), 5);
}

/// An idle group's tracker is dropped, and the group carries on from disk as if
/// nothing happened.
#[tokio::test]
async fn an_idle_tracker_is_evicted_and_rebuilt() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "b"]).await;
    let now = Instant::now();
    assert_eq!(poll_at(&fx, &key, now, 0).await.len(), 2);
    fx.reader.ack(&key, 0).await.expect("ack");
    fx.reader.ack(&key, 1).await.expect("ack");

    assert_eq!(fx.reader.evict_idle(now), 0, "a group in use was evicted");
    assert_eq!(fx.reader.evict_idle(now + Duration::from_secs(3600)), 1);
    assert_eq!(fx.reader.tracked(), 0);

    publish(&fx.log, &["c"]).await;
    let next = poll_at(&fx, &key, Instant::now(), 0).await;
    assert_eq!(
        payloads(&next),
        vec!["c"],
        "the rebuilt group repeated work"
    );
}

/// A group at its in-flight cap answers empty, and counts it, until an
/// acknowledgement frees room.
#[tokio::test]
async fn a_group_at_its_in_flight_cap_gets_nothing_more() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    fx.reader.set_max_in_flight(2);
    publish(&fx.log, &["a", "b", "c", "d"]).await;
    let now = Instant::now();

    let first = fx
        .reader
        .poll(&key(), &fx.log, 10, now)
        .await
        .expect("poll");
    assert_eq!(payloads(&first), vec!["a", "b"]);
    let full = fx
        .reader
        .poll(&key(), &fx.log, 10, now)
        .await
        .expect("poll");
    assert!(full.is_empty(), "handed out past the cap");
    assert_eq!(fx.reader.capped_polls(), 2);

    fx.reader.ack(&key(), 0).await.expect("ack");
    let freed = fx
        .reader
        .poll(&key(), &fx.log, 10, now)
        .await
        .expect("poll");
    assert_eq!(payloads(&freed), vec!["c"]);
}

/// A waiting poll is woken by what frees work in the group itself: an
/// acknowledgement (room under the cap), a hand-back, a redrive.
#[tokio::test]
async fn settling_a_claim_wakes_a_waiting_poll() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a", "b"]).await;
    let now = Instant::now();
    let changed = fx.reader.changed(&key());
    fx.reader
        .poll(&key(), &fx.log, 10, now)
        .await
        .expect("poll");

    for settle in ["nack", "ack"] {
        let mut woken = std::pin::pin!(changed.notified());
        woken.as_mut().enable();
        match settle {
            "nack" => fx.reader.nack(&key(), 0).await.expect("nack"),
            _ => fx.reader.ack(&key(), 1).await.expect("ack"),
        }
        tokio::time::timeout(Duration::from_secs(5), woken)
            .await
            .unwrap_or_else(|_| panic!("a {settle} did not wake the group"));
    }
}

/// The earliest standing claim bounds how long a waiting poll sleeps.
#[tokio::test]
async fn next_lapse_is_the_earliest_standing_claim() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    assert_eq!(fx.reader.next_lapse(&key()).await, None);
    publish(&fx.log, &["a", "b"]).await;
    let now = Instant::now();

    fx.reader.poll(&key(), &fx.log, 1, now).await.expect("poll");
    fx.reader
        .poll(&key(), &fx.log, 1, now + Duration::from_secs(5))
        .await
        .expect("poll");
    assert_eq!(fx.reader.next_lapse(&key()).await, Some(now + VIS));

    fx.reader.ack(&key(), 0).await.expect("ack");
    assert_eq!(
        fx.reader.next_lapse(&key()).await,
        Some(now + Duration::from_secs(5) + VIS)
    );
}

/// A poll waiting on the group keeps it from being evicted as idle: dropping
/// it would leave the poll listening to a notifier nobody signals.
#[tokio::test]
async fn a_waited_on_group_is_not_evicted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let changed = fx.reader.changed(&key());
    let later = Instant::now() + fx.reader.idle_after() + Duration::from_secs(1);

    assert_eq!(fx.reader.evict_idle(later), 0);
    drop(changed);
    assert_eq!(fx.reader.evict_idle(later), 1);
}

/// Claims made before the tracker was rebuilt (a move back, a failover, an
/// eviction) are still the consumer's to settle, in any order, and a record
/// settled that way is not handed out again.
#[tokio::test]
async fn a_claim_from_before_a_new_term_is_still_settled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    let key = key();
    publish(&fx.log, &["a", "b", "c", "d"]).await;
    let first = fx
        .reader
        .poll(&key, &fx.log, 3, Instant::now())
        .await
        .expect("poll");
    assert_eq!(payloads(&first), ["a", "b", "c"]);
    fx.reader.reset_shard(T, NS, S, 0).await;
    // What the serving layer does before a settle.
    fx.reader.inherit_below(&key, 4).await.expect("inherit");

    // Out of order: 2 is held until 1 closes the run.
    fx.reader
        .ack(&key, 2)
        .await
        .expect("ack of a pre-reset claim");
    fx.reader
        .nack(&key, 0)
        .await
        .expect("nack of a pre-reset claim");
    fx.reader
        .ack(&key, 1)
        .await
        .expect("ack of a pre-reset claim");

    let next = poll_at(&fx, &key, Instant::now(), 0).await;
    assert_eq!(payloads(&next), ["a", "d"], "a settled record came back");
    fx.reader.ack(&key, 0).await.expect("ack");
    assert_eq!(fx.reader.committed(&key).await.expect("committed"), Some(3));

    // Written after the tracker was rebuilt: nobody before it had a claim.
    publish(&fx.log, &["e"]).await;
    assert!(matches!(
        fx.reader.ack(&key, 4).await,
        Err(BrokerError::GroupOffsetNotHandedOut { .. })
    ));
}

/// A generation-start record holds an offset but is no one's work. A group
/// is never handed it, each record keeps its own offset, and the cursor
/// closes over it once the records around it are finished.
#[tokio::test]
async fn a_group_steps_over_a_generation_start_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a"]).await;
    let marker = fx.log.append_generation_start(3).await.expect("marker");
    assert_eq!(marker, 1);
    publish(&fx.log, &["b"]).await;
    let key = key();

    let claimed = fx
        .reader
        .poll(&key, &fx.log, 10, Instant::now())
        .await
        .expect("poll");
    let seen: Vec<(u64, String)> = claimed
        .iter()
        .map(|c| {
            (
                c.offset,
                String::from_utf8(c.payload.to_vec()).expect("utf8"),
            )
        })
        .collect();
    assert_eq!(seen, vec![(0, "a".to_string()), (2, "b".to_string())]);

    fx.reader.ack(&key, 0).await.expect("ack");
    fx.reader.ack(&key, 2).await.expect("ack");
    assert_eq!(fx.reader.committed(&key).await.expect("committed"), Some(3));
    let nothing = fx
        .reader
        .poll(&key, &fx.log, 10, Instant::now())
        .await
        .expect("poll");
    assert!(nothing.is_empty());
}

/// With the generation start last, the group is not stalled owing it.
#[tokio::test]
async fn a_trailing_generation_start_does_not_stall_a_group() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a"]).await;
    fx.log.append_generation_start(3).await.expect("marker");
    let key = key();

    let claimed = fx
        .reader
        .poll(&key, &fx.log, 10, Instant::now())
        .await
        .expect("poll");
    assert_eq!(payloads(&claimed), vec!["a"]);
    fx.reader.ack(&key, 0).await.expect("ack");
    assert_eq!(fx.reader.committed(&key).await.expect("committed"), Some(2));
}

/// **A record says how many offsets below it were settled without delivery.**
/// Generation-start records are not a client's, so the record after a run of
/// them reports the run, and a consumer can tell the hole will not fill. The
/// run is reported again after a poll that ended on it.
#[tokio::test]
async fn a_record_after_generation_starts_reports_them_as_skipped() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    publish(&fx.log, &["a"]).await;
    fx.log.append_generation_start(2).await.expect("start");
    fx.log.append_generation_start(3).await.expect("start");
    publish(&fx.log, &["b"]).await;

    let claimed = fx
        .reader
        .poll(&key(), &fx.log, 10, Instant::now())
        .await
        .expect("poll");

    assert_eq!(payloads(&claimed), vec!["a", "b"]);
    let skipped: Vec<(u64, u64)> = claimed
        .iter()
        .map(|c| (c.offset, c.skipped_before))
        .collect();
    assert_eq!(skipped, vec![(0, 0), (3, 2)]);
}

/// A poll that settles a run and stops short of the record after it leaves
/// the count for the poll that delivers that record.
#[tokio::test]
async fn the_skip_count_survives_into_the_next_poll() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fx = open(dir.path());
    fx.log.append_generation_start(2).await.expect("start");
    publish(&fx.log, &["a"]).await;
    let now = Instant::now();

    let first = fx.reader.poll(&key(), &fx.log, 1, now).await.expect("poll");
    assert!(
        first.is_empty(),
        "the one offset claimed was a generation start"
    );
    let second = fx.reader.poll(&key(), &fx.log, 1, now).await.expect("poll");
    assert_eq!(
        second
            .iter()
            .map(|c| (c.offset, c.skipped_before))
            .collect::<Vec<_>>(),
        vec![(1, 1)]
    );
}

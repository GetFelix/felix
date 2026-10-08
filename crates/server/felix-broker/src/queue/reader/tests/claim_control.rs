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

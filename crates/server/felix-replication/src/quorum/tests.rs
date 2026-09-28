//! Waiting for a majority, and the ways a wait ends other than success.
use std::time::Duration;

use super::*;

const QUICK: Duration = Duration::from_millis(200);

fn key(stream: &str) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard: 0,
        kind: crate::ShardKind::Stream,
    }
}

/// A mark already past the batch returns at once rather than waiting for the
/// next change — a publish that arrives after its records replicated must not
/// wait for an unrelated one to move the mark.
#[tokio::test]
async fn a_mark_already_past_the_batch_returns_immediately() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 10);

    assert_eq!(
        marks.wait_for(&key("orders"), 4, 10, QUICK).await,
        QuorumWait::Reached,
    );
}

/// The wait ends when the mark reaches the batch, not before.
#[tokio::test]
async fn a_wait_ends_when_the_majority_reaches_the_batch() {
    let marks = std::sync::Arc::new(QuorumMarks::new());
    marks.publish(&key("orders"), 4, 0);

    let waiting = {
        let marks = std::sync::Arc::clone(&marks);
        tokio::spawn(async move { marks.wait_for(&key("orders"), 4, 5, QUICK).await })
    };

    // Short of the batch: not enough.
    marks.publish(&key("orders"), 4, 4);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished(), "the wait ended one record short");

    marks.publish(&key("orders"), 4, 5);
    assert_eq!(waiting.await.expect("join"), QuorumWait::Reached);
}

/// **A majority that never arrives times out rather than hanging.** A client
/// waiting forever is worse than one told the broker cannot vouch for the write.
#[tokio::test]
async fn a_majority_that_never_arrives_times_out() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 0);

    assert_eq!(
        marks
            .wait_for(&key("orders"), 4, 5, Duration::from_millis(50))
            .await,
        QuorumWait::TimedOut,
    );
}

/// **An untracked shard is not a quorum.** If this broker is not tracking the
/// shard it is not leading it, and it cannot promise a majority for a write.
#[tokio::test]
async fn a_shard_this_broker_does_not_lead_is_not_a_quorum() {
    let marks = QuorumMarks::new();

    assert_eq!(
        marks.wait_for(&key("orders"), 4, 1, QUICK).await,
        QuorumWait::NotLeading,
    );
}

/// **An older generation's mark does not satisfy a newer generation's wait.**
/// The replica set may be different, so what a majority held under the previous
/// leadership says nothing about this one.
#[tokio::test]
async fn a_wait_at_a_newer_generation_ignores_the_old_mark() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 100);

    assert_eq!(
        marks.wait_for(&key("orders"), 5, 10, QUICK).await,
        QuorumWait::NotLeading,
    );
}

/// A generation change restarts the mark rather than carrying it forward.
#[tokio::test]
async fn a_new_generation_restarts_the_mark() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 100);
    marks.publish(&key("orders"), 5, 0);

    assert_eq!(
        marks
            .wait_for(&key("orders"), 5, 10, Duration::from_millis(50))
            .await,
        QuorumWait::TimedOut,
        "the old generation's mark satisfied the new generation",
    );
}

/// **Losing the shard ends the wait rather than running it out.** A publish
/// held for its full timeout after leadership moved is a client kept waiting
/// for an answer that can no longer come.
#[tokio::test]
async fn losing_the_shard_ends_a_wait_in_progress() {
    let marks = std::sync::Arc::new(QuorumMarks::new());
    marks.publish(&key("orders"), 4, 0);

    let waiting = {
        let marks = std::sync::Arc::clone(&marks);
        tokio::spawn(async move {
            marks
                .wait_for(&key("orders"), 4, 5, Duration::from_secs(30))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    marks.forget(&key("orders"));

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the wait should end when the shard is released")
            .expect("join"),
        QuorumWait::NotLeading,
    );
}

/// The mark only goes forward within a generation. A pass that saw less than
/// the last one saw a follower mid-answer, not a record becoming un-stored.
#[tokio::test]
async fn the_mark_does_not_go_backwards_within_a_generation() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 10);
    marks.publish(&key("orders"), 4, 3);

    assert_eq!(
        marks.wait_for(&key("orders"), 4, 10, QUICK).await,
        QuorumWait::Reached,
        "the mark went backwards and un-acknowledged a batch",
    );
}

/// Shards are tracked independently: one shard's majority says nothing about
/// another's.
#[tokio::test]
async fn shards_do_not_share_a_mark() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 100);
    marks.publish(&key("payments"), 4, 0);

    assert_eq!(
        marks
            .wait_for(&key("payments"), 4, 10, Duration::from_millis(50))
            .await,
        QuorumWait::TimedOut,
    );
}

/// Shards this broker no longer leads are forgotten in one pass.
#[tokio::test]
async fn retaining_the_live_shards_forgets_the_rest() {
    let marks = QuorumMarks::new();
    marks.publish(&key("orders"), 4, 10);
    marks.publish(&key("payments"), 4, 10);

    marks.retain(&[key("orders")]);

    assert_eq!(
        marks.wait_for(&key("orders"), 4, 10, QUICK).await,
        QuorumWait::Reached,
    );
    assert_eq!(
        marks.wait_for(&key("payments"), 4, 10, QUICK).await,
        QuorumWait::NotLeading,
    );
}

/// A mark for a generation this broker leads but has not published one for
/// yet is waited for, not read as a lost leadership.
#[tokio::test]
async fn a_wait_at_a_generation_with_no_mark_yet_waits_for_it() {
    let marks = std::sync::Arc::new(QuorumMarks::new());
    let waiting = {
        let marks = std::sync::Arc::clone(&marks);
        tokio::spawn(async move {
            marks
                .wait_while_leading(&key("orders"), || Some(5), 10, Duration::from_secs(5))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !waiting.is_finished(),
        "the wait ended with no mark to end it"
    );

    marks.publish(&key("orders"), 5, 10);
    assert_eq!(waiting.await.expect("join"), QuorumWait::Reached);
}

/// **A new generation under the same leader carries a wait over.** Staging a
/// move's destination starts one, and a write taken just before is on the
/// log the new replica set is shipped from, so the new mark covers it.
#[tokio::test]
async fn a_wait_follows_a_new_generation_under_the_same_leader() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let marks = std::sync::Arc::new(QuorumMarks::new());
    let generation = std::sync::Arc::new(AtomicU64::new(4));
    marks.publish(&key("orders"), 4, 0);
    let waiting = {
        let (marks, generation) = (
            std::sync::Arc::clone(&marks),
            std::sync::Arc::clone(&generation),
        );
        tokio::spawn(async move {
            marks
                .wait_while_leading(
                    &key("orders"),
                    || Some(generation.load(Ordering::SeqCst)),
                    5,
                    Duration::from_secs(5),
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    generation.store(5, Ordering::SeqCst);
    marks.publish(&key("orders"), 5, 0);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !waiting.is_finished(),
        "the new generation's empty mark ended the wait"
    );

    marks.publish(&key("orders"), 5, 5);
    assert_eq!(waiting.await.expect("join"), QuorumWait::Reached);
}

/// Losing the shard while no mark exists still ends the wait promptly rather
/// than running out its timeout.
#[tokio::test]
async fn losing_the_shard_before_its_first_mark_ends_the_wait() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let marks = std::sync::Arc::new(QuorumMarks::new());
    let leading = std::sync::Arc::new(AtomicBool::new(true));
    let waiting = {
        let (marks, leading) = (
            std::sync::Arc::clone(&marks),
            std::sync::Arc::clone(&leading),
        );
        tokio::spawn(async move {
            marks
                .wait_while_leading(
                    &key("orders"),
                    || leading.load(Ordering::SeqCst).then_some(5),
                    10,
                    Duration::from_secs(30),
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    leading.store(false, Ordering::SeqCst);

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the wait should end when the shard is released")
            .expect("join"),
        QuorumWait::NotLeading,
    );
}

/// A leader that wrote a start record counts nothing until a majority holds
/// it, then everything up to what the majority holds.
#[test]
fn only_a_majority_past_the_start_record_counts() {
    assert_eq!(counted_offset(3, Some(3)), 0);
    assert_eq!(counted_offset(2, Some(3)), 0);
    assert_eq!(counted_offset(4, Some(3)), 4);
    assert_eq!(counted_offset(3, None), 3);
}

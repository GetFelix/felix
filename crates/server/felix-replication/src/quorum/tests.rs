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

fn follower(node: &str, confirmed: u64) -> FollowerCursor {
    let mut cursor = FollowerCursor::new(node, "127.0.0.1:1".parse().unwrap(), confirmed);
    cursor.confirmed = confirmed;
    cursor
}

/// A leader that has taken a newer leader's fence counts only its followers:
/// what it writes from then on is in no fence's answer, so counting itself
/// would let it acknowledge on one follower the fence never reached.
#[test]
fn a_deposed_leader_does_not_count_itself() {
    let followers = [follower("b", 5), follower("c", 0)];
    assert_eq!(held_at_generation(9, true, &followers, None), 5);
    assert_eq!(held_at_generation(9, false, &followers, None), 0);
    // Two followers are still a majority without it.
    let followers = [follower("b", 5), follower("c", 3)];
    assert_eq!(held_at_generation(9, false, &followers, None), 3);
}

/// A follower counts up to its own answer at this generation, not to where
/// shipping resumes: a resume point is the follower's claim about a log
/// nobody compared. An answer from before the follower took a newer fence
/// still counts, which is safe because the fence's answer carries it; a
/// follower that refused this leader since is halted and counts for nothing.
#[test]
fn an_answer_from_before_a_newer_fence_still_counts() {
    let mut resumed = follower("b", 2);
    resumed.next_offset = 8;
    assert_eq!(held_at_generation(9, true, &[resumed], None), 2);

    let mut fenced = follower("c", 6);
    assert_eq!(held_at_generation(9, true, &[fenced.clone()], None), 6);
    fenced.halted = Some(crate::Halt::Fenced);
    assert_eq!(held_at_generation(9, true, &[fenced], None), 0);
}

/// A learner still copying is left out of the majority and its size, as
/// under the report.
#[test]
fn a_learner_does_not_count_toward_follower_acks() {
    let followers = [follower("b", 4), follower("d", 9)];
    assert_eq!(held_at_generation(9, true, &followers, Some("d")), 4);
    // Two voters, the leader and b: without the leader there is no majority.
    assert_eq!(held_at_generation(9, false, &followers, Some("d")), 0);
}

/// Once the followers decide a shard's mark it releases without the lease
/// for the rest of that generation, and a new generation starts over.
#[test]
fn a_mark_the_followers_decided_is_remembered_for_its_generation() {
    let marks = QuorumMarks::new();
    let shard = key("orders");
    marks.publish(&shard, 1, 3);
    assert!(!marks.decided_by_followers(&shard, 1));
    marks.publish_by_followers(&shard, 1, 4);
    assert!(marks.decided_by_followers(&shard, 1));
    marks.publish(&shard, 1, 5);
    assert!(marks.decided_by_followers(&shard, 1));
    assert_eq!(marks.offset(&shard, 1), Some(5));
    marks.publish(&shard, 2, 1);
    assert!(!marks.decided_by_followers(&shard, 2));
    assert!(!marks.decided_by_followers(&shard, 1));
}

/// Follower acks need `majority_ack` and `generation_start` both finalized.
#[test]
fn follower_acks_need_both_fleet_features() {
    use felix_common::fleet::{FleetGate, GENERATION_START, MAJORITY_ACK};
    let names = [GENERATION_START.name(), MAJORITY_ACK.name()];
    let fleet = Arc::new(FleetGate::new(names));
    let marks = QuorumMarks::with_fleet(Arc::clone(&fleet));
    fleet.observe([MAJORITY_ACK.name()]);
    assert!(!marks.acks_by_followers());
    fleet.observe([GENERATION_START.name()]);
    assert!(marks.acks_by_followers());
}

struct AlwaysConfirms;

#[async_trait::async_trait]
impl LeadershipCheck for AlwaysConfirms {
    async fn confirm(&self, _key: &ShardKey, _generation: u64) -> Result<(), QuorumError> {
        Ok(())
    }
}

/// Reads leave the lease only once the fleet finalized `lease_free_reads`
/// with what follower acks need, and never when the operator kept them on it.
#[test]
fn reads_by_round_need_the_fleet_and_no_lease_opt_in() {
    use felix_common::fleet::{FleetGate, GENERATION_START, LEASE_FREE_READS, MAJORITY_ACK};
    let names = [
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        LEASE_FREE_READS.name(),
    ];
    let fleet = Arc::new(FleetGate::new(names));
    let marks = QuorumMarks::with_fleet(Arc::clone(&fleet));
    marks.set_read_check(Arc::new(AlwaysConfirms), false);
    fleet.observe([LEASE_FREE_READS.name()]);
    assert!(marks.reads_by_round().is_none(), "follower acks are not on");
    fleet.observe([GENERATION_START.name(), MAJORITY_ACK.name()]);
    assert!(marks.reads_by_round().is_some());

    let kept = QuorumMarks::with_fleet(Arc::clone(&fleet));
    kept.set_read_check(Arc::new(AlwaysConfirms), true);
    assert!(kept.reads_by_round().is_none(), "the lease was opted into");
    assert!(
        QuorumMarks::with_fleet(fleet).reads_by_round().is_none(),
        "no round to confirm with"
    );
}

/// A shard this broker leads at generation 3, with no lease.
struct Unleased;

impl ShardServing for Unleased {
    fn replicated(&self, _key: &ShardKey) -> bool {
        true
    }

    fn generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn lease_valid(&self) -> bool {
        false
    }

    fn record_ack_refusal(&self) {}
}

/// Readers keep the committed mark past a lapsed lease once the fleet reads
/// by round and the stream's mark was decided by its followers; a mark the
/// report decided still needs the lease, and so does a fleet on the lease.
#[test]
fn readers_keep_the_mark_without_the_lease_where_followers_decide_it() {
    use felix_broker::ReadBound;
    use felix_common::fleet::{FleetGate, GENERATION_START, LEASE_FREE_READS, MAJORITY_ACK};
    let names = [
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        LEASE_FREE_READS.name(),
    ];
    let fleet = Arc::new(FleetGate::new(names));
    fleet.observe(names);
    let stream = ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: crate::ShardKind::Stream,
    };

    let marks = QuorumMarks::with_fleet(Arc::clone(&fleet));
    marks.set_read_check(Arc::new(AlwaysConfirms), false);
    marks.publish(&stream, 3, 5);
    assert_eq!(
        committed_bound(&stream, &marks, &Unleased),
        ReadBound::Refused,
        "the report decided this mark"
    );
    marks.publish_by_followers(&stream, 3, 7);
    assert_eq!(
        committed_bound(&stream, &marks, &Unleased),
        ReadBound::Committed(7)
    );

    let kept = QuorumMarks::with_fleet(fleet);
    kept.set_read_check(Arc::new(AlwaysConfirms), true);
    kept.publish_by_followers(&stream, 3, 7);
    assert_eq!(
        committed_bound(&stream, &kept, &Unleased),
        ReadBound::Refused,
        "the operator kept reads on the lease"
    );
}

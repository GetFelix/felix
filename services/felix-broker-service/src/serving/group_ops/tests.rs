//! The write fence on consumer-group operations.
//!
//! Admission here is the ownership check, which reads the servable set; the
//! lifecycle closes the fence before that set catches up. Each test takes a
//! record while the shard is served, lets a move close the fence, and shows
//! every group write is then refused -- group state moves with the shard, so
//! a write landing after the fence would be left behind on the old leader.
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::*;
use crate::config::BrokerConfig;
use crate::serving::quic::ClusterContext;
use crate::serving::quic::handlers::publish::build_publish_context;
use crate::test_support::leader::{self, DURABLE, Leader, NAMESPACE, TENANT};

const GROUP: &str = "workers";

fn context(leader: &Leader) -> PublishContext {
    build_publish_context(
        Arc::clone(&leader.broker),
        &BrokerConfig::default(),
        ClusterContext {
            ingress: Some(Arc::clone(&leader.ingress)),
            ..ClusterContext::default()
        },
    )
}

/// A leader with two records on the queue and the first one claimed.
async fn claimed_one() -> (Leader, PublishContext) {
    let leader = Leader::start().await;
    leader
        .broker
        .publish_batch(
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            &[Bytes::from_static(b"first"), Bytes::from_static(b"second")],
        )
        .await
        .expect("publish");
    let publish_ctx = context(&leader);
    let claimed = poll(
        &leader.broker,
        &publish_ctx,
        None,
        TENANT,
        NAMESPACE,
        DURABLE,
        0,
        GROUP,
        1,
        Duration::ZERO,
        None,
    )
    .await
    .expect("served before the move");
    assert_eq!(claimed.len(), 1);
    (leader, publish_ctx)
}

async fn committed(leader: &Leader) -> Option<u64> {
    leader
        .broker
        .group_reader()
        .expect("groups")
        .committed(&group_key(TENANT, NAMESPACE, DURABLE, 0, GROUP))
        .await
        .expect("committed")
}

#[tokio::test]
async fn an_ack_after_the_fence_is_refused() {
    let (mut leader, publish_ctx) = claimed_one().await;
    leader.fence_move(&leader::stream_key(DURABLE));

    for finish in [true, false] {
        assert!(
            settle(
                &leader.broker,
                &publish_ctx,
                None,
                TENANT,
                NAMESPACE,
                DURABLE,
                0,
                GROUP,
                0,
                finish,
            )
            .await
            .is_err(),
            "a settle (finish: {finish}) landed after the fence closed"
        );
    }
    assert_eq!(committed(&leader).await, None, "the cursor moved");
}

/// Group acks commit to the shard like a publish does, so a broker whose
/// lease lapsed refuses them rather than moving a cursor a new leader owns.
#[tokio::test]
async fn an_ack_after_the_lease_lapses_is_refused() {
    let (leader, publish_ctx) = claimed_one().await;
    let lease = leader.hold_lease();
    lease.surrender();

    for finish in [true, false] {
        let refused = settle(
            &leader.broker,
            &publish_ctx,
            None,
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            GROUP,
            0,
            finish,
        )
        .await
        .expect_err("a settle landed without a lease");
        assert_eq!(
            refused.code(),
            &felix_wire::ErrorCode::ShardUnavailable,
            "finish: {finish}"
        );
    }
    assert_eq!(committed(&leader).await, None, "the cursor moved");

    lease.renew();
    settle(
        &leader.broker,
        &publish_ctx,
        None,
        TENANT,
        NAMESPACE,
        DURABLE,
        0,
        GROUP,
        0,
        true,
    )
    .await
    .expect("renewed");
    assert_eq!(committed(&leader).await, Some(1), "renewed, the ack lands");
}

/// **A claim made before the shard moved here is still settled.** The
/// tracker is in memory and does not move with the shard, so the new leader
/// never saw the claim; every claim its predecessor made is below the tail it
/// first saw, and the ack lands instead of being refused mid-move.
#[tokio::test]
async fn a_claim_from_before_the_move_is_still_settled() {
    let (leader, publish_ctx) = claimed_one().await;
    // What a move back, a failover or an eviction does to the tracker.
    leader
        .broker
        .group_reader()
        .expect("groups")
        .reset_shard(TENANT, NAMESPACE, DURABLE, 0)
        .await;
    settle(
        &leader.broker,
        &publish_ctx,
        None,
        TENANT,
        NAMESPACE,
        DURABLE,
        0,
        GROUP,
        0,
        true,
    )
    .await
    .expect("the pre-move claim is settled");
    assert_eq!(committed(&leader).await, Some(1));
}

/// **An ack for a record newer than the tracker is a retryable
/// `stale_claim`.** Offset 2 was written after the tracker was built and has
/// not been polled, so nobody holds a claim on it; the consumer is told so
/// and the record will come round. Past the tail nothing was ever handed
/// out, so that stays a bad request.
#[tokio::test]
async fn an_ack_for_a_record_newer_than_the_tracker_is_stale_not_invalid() {
    let (leader, publish_ctx) = claimed_one().await;
    leader
        .broker
        .publish_batch(
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            &[Bytes::from_static(b"third")],
        )
        .await
        .expect("publish");
    for finish in [true, false] {
        for (offset, code, retry) in [
            (
                2,
                felix_wire::ErrorCode::StaleClaim,
                felix_wire::RetryClass::Retry,
            ),
            (
                7,
                felix_wire::ErrorCode::InvalidRequest,
                felix_wire::RetryClass::Fatal,
            ),
        ] {
            let refused = settle(
                &leader.broker,
                &publish_ctx,
                None,
                TENANT,
                NAMESPACE,
                DURABLE,
                0,
                GROUP,
                offset,
                finish,
            )
            .await
            .expect_err("settled an offset never handed out here");
            assert_eq!(
                (refused.code(), refused.retry()),
                (&code, retry),
                "offset {offset}, finish: {finish}"
            );
        }
    }
    assert_eq!(committed(&leader).await, None, "the cursor moved");
}

/// **A group on a `Quorum` stream reads only to the quorum mark.** A record
/// past it can be lost at failover and its offset reused; a group that had
/// consumed it would skip whatever the next leader writes there.
#[tokio::test]
async fn a_quorum_group_poll_stops_at_the_quorum_mark() {
    let leader = Leader::start().await;
    leader
        .broker
        .publish_batch(
            TENANT,
            NAMESPACE,
            leader::QUORUM,
            0,
            &[
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ],
        )
        .await
        .expect("publish");
    let marks = Arc::new(felix_replication::quorum::QuorumMarks::new());
    let publish_ctx = build_publish_context(
        Arc::clone(&leader.broker),
        &BrokerConfig::default(),
        ClusterContext {
            ingress: Some(Arc::clone(&leader.ingress)),
            marks: Some(Arc::clone(&marks)),
            ..ClusterContext::default()
        },
    );
    let take = || async {
        poll(
            &leader.broker,
            &publish_ctx,
            None,
            TENANT,
            NAMESPACE,
            leader::QUORUM,
            0,
            GROUP,
            10,
            Duration::ZERO,
            None,
        )
        .await
        .expect("poll")
        .into_iter()
        .map(|record| record.offset)
        .collect::<Vec<_>>()
    };

    assert!(take().await.is_empty(), "no majority holds anything yet");
    marks.publish(&leader::stream_key(leader::QUORUM), leader::GENERATION, 2);
    assert_eq!(take().await, vec![0, 1]);
    assert!(take().await.is_empty(), "handed out past the mark");
    marks.publish(&leader::stream_key(leader::QUORUM), leader::GENERATION, 3);
    assert_eq!(take().await, vec![2]);
}

#[tokio::test]
async fn a_poll_after_the_fence_is_refused() {
    let (mut leader, publish_ctx) = claimed_one().await;
    leader.fence_move(&leader::stream_key(DURABLE));

    assert!(
        poll(
            &leader.broker,
            &publish_ctx,
            None,
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            GROUP,
            1,
            Duration::ZERO,
            None,
        )
        .await
        .is_err(),
        "a poll claimed a record after the fence closed"
    );
}

#[tokio::test]
async fn a_dead_letter_change_after_the_fence_is_refused() {
    let (mut leader, publish_ctx) = claimed_one().await;
    let broker = Arc::clone(&leader.broker);
    let reader = broker.group_reader().expect("groups");
    let key = group_key(TENANT, NAMESPACE, DURABLE, 0, GROUP);
    reader
        .dead_letters()
        .record(&key, 0)
        .await
        .expect("dead-letter");
    leader.fence_move(&leader::stream_key(DURABLE));

    for redrive in [true, false] {
        let refused = manage_dead_letter(
            &leader.broker,
            &publish_ctx,
            None,
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            GROUP,
            0,
            redrive,
        )
        .await;
        assert!(
            refused.is_err(),
            "a dead-letter change (redrive: {redrive}) landed after the fence closed"
        );
    }
    assert_eq!(
        reader.dead_lettered(&key).await.expect("list"),
        vec![0],
        "the dead letter was touched"
    );
}

/// A poll waiting for work when the shard stops serving here answers empty
/// rather than with an error: it claimed nothing, and the consumer's next poll
/// is held until the move cuts over.
#[tokio::test]
async fn a_waiting_poll_that_loses_its_shard_answers_empty() {
    let (mut leader, publish_ctx) = claimed_one().await;
    // Take the other record, so the next poll has to wait.
    let second = poll(
        &leader.broker,
        &publish_ctx,
        None,
        TENANT,
        NAMESPACE,
        DURABLE,
        0,
        GROUP,
        1,
        Duration::ZERO,
        None,
    )
    .await
    .expect("poll");
    assert_eq!(second.len(), 1);

    let waiting = tokio::spawn({
        let broker = Arc::clone(&leader.broker);
        let publish_ctx = publish_ctx.clone();
        async move {
            poll(
                &broker,
                &publish_ctx,
                None,
                TENANT,
                NAMESPACE,
                DURABLE,
                0,
                GROUP,
                1,
                Duration::from_secs(5),
                None,
            )
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    leader.fence_move(&leader::stream_key(DURABLE));
    let answered = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .expect("the poll kept waiting on a shard it lost")
        .expect("poll task");
    assert_eq!(answered.expect("an empty answer, not an error"), Vec::new());
}

/// Longer than any test waits, so a poll that answers was woken rather than
/// finding the work on a recheck.
const NEVER: Duration = Duration::from_secs(3600);

/// A poll waiting for records, with no recheck to fall back on.
fn waiting_poll(
    leader: &Leader,
    publish_ctx: &PublishContext,
) -> tokio::task::JoinHandle<Result<Vec<GroupRecord>, ClientError>> {
    let broker = Arc::clone(&leader.broker);
    let publish_ctx = publish_ctx.clone();
    tokio::spawn(async move {
        poll_rechecking(
            &broker,
            &publish_ctx,
            None,
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            GROUP,
            10,
            Duration::from_secs(60),
            NEVER,
            None,
        )
        .await
    })
}

async fn answered(
    waiting: tokio::task::JoinHandle<Result<Vec<GroupRecord>, ClientError>>,
    why: &str,
) -> Vec<u64> {
    tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .unwrap_or_else(|_| panic!("the poll was not woken by {why}"))
        .expect("poll task")
        .expect("poll")
        .iter()
        .map(|record| record.offset)
        .collect()
}

/// A waiting poll is woken by the shard's append, not by looking again later.
#[tokio::test]
async fn a_waiting_poll_is_woken_by_a_publish() {
    let leader = Leader::start().await;
    let publish_ctx = context(&leader);
    let waiting = waiting_poll(&leader, &publish_ctx);
    tokio::time::sleep(Duration::from_millis(50)).await;

    leader
        .broker
        .publish_batch(
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            &[Bytes::from_static(b"late")],
        )
        .await
        .expect("publish");
    assert_eq!(answered(waiting, "a publish").await, vec![0]);
}

/// A record handed back is owed at once, and a poll already waiting gets it.
#[tokio::test]
async fn a_waiting_poll_is_woken_by_a_hand_back() {
    let (leader, publish_ctx) = claimed_one().await;
    let rest = poll(
        &leader.broker,
        &publish_ctx,
        None,
        TENANT,
        NAMESPACE,
        DURABLE,
        0,
        GROUP,
        10,
        Duration::ZERO,
        None,
    )
    .await
    .expect("poll");
    assert_eq!(rest.len(), 1);
    let waiting = waiting_poll(&leader, &publish_ctx);
    tokio::time::sleep(Duration::from_millis(50)).await;

    settle(
        &leader.broker,
        &publish_ctx,
        None,
        TENANT,
        NAMESPACE,
        DURABLE,
        0,
        GROUP,
        0,
        false,
    )
    .await
    .expect("nack");
    assert_eq!(answered(waiting, "a hand-back").await, vec![0]);
}

/// A poll held back by the in-flight cap is woken when an acknowledgement
/// makes room.
#[tokio::test]
async fn a_poll_at_the_cap_is_woken_by_an_ack() {
    let (leader, publish_ctx) = claimed_one().await;
    leader
        .broker
        .group_reader()
        .expect("groups")
        .set_max_in_flight(1);
    let waiting = waiting_poll(&leader, &publish_ctx);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished(), "handed out past the cap");

    settle(
        &leader.broker,
        &publish_ctx,
        None,
        TENANT,
        NAMESPACE,
        DURABLE,
        0,
        GROUP,
        0,
        true,
    )
    .await
    .expect("ack");
    assert_eq!(answered(waiting, "an ack").await, vec![1]);
}

/// **A member that restarts under its name gets its records back at once.**
/// The first process claimed a record and died; the next, polling as the
/// same member with `reclaim`, is handed that record rather than the newer one
/// behind it, without waiting out the visibility timeout.
#[tokio::test]
async fn a_restarted_member_reclaims_what_it_held() {
    let leader = Leader::start().await;
    leader
        .broker
        .publish_batch(
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            &[Bytes::from_static(b"first"), Bytes::from_static(b"second")],
        )
        .await
        .expect("publish");
    let publish_ctx = context(&leader);
    let as_member = |reclaim| felix_broker::GroupConsumer {
        id: "snapshotter".to_string(),
        reclaim,
    };
    let poll_one = |member: felix_broker::GroupConsumer| {
        let broker = Arc::clone(&leader.broker);
        let publish_ctx = publish_ctx.clone();
        async move {
            poll(
                &broker,
                &publish_ctx,
                None,
                TENANT,
                NAMESPACE,
                DURABLE,
                0,
                GROUP,
                1,
                Duration::ZERO,
                Some(&member),
            )
            .await
            .expect("poll")
        }
    };

    let held = poll_one(as_member(false)).await;
    assert_eq!(held[0].payload.as_ref(), b"first");

    let back = poll_one(as_member(true)).await;
    assert_eq!(back[0].offset, held[0].offset);
    assert_eq!(back[0].attempts, 2);
}

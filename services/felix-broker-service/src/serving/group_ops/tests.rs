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
    let marks = Arc::new(crate::replication::quorum::QuorumMarks::new());
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

//! The write fence around a head trim.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::*;
use crate::config::BrokerConfig;
use crate::serving::quic::ClusterContext;
use crate::serving::quic::handlers::publish::build_publish_context;
use crate::test_support::leader::{self, Leader, NAMESPACE, QUORUM, TENANT};

/// A committed mark that stays at the two records published, as if no
/// follower ever acknowledged the trim.
#[derive(Debug)]
struct StuckMark;

impl felix_broker::ReadBounds for StuckMark {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> felix_broker::ReadBound {
        felix_broker::ReadBound::Committed(2)
    }
}

/// **A drain is not held up by a trim waiting on its commit.** The trim's
/// guard covers the record taking its place in the log; the wait for the
/// committed mark runs after it drops. Here no follower ever acknowledges, so
/// the trim waits out its whole timeout while the shard is moved.
#[tokio::test]
async fn a_drain_is_not_held_up_by_a_trim_waiting_on_commit() {
    let mut leader = Leader::start().await;
    leader.broker.allow_head_trims_when(|| true);
    leader.broker.set_read_bounds(Arc::new(StuckMark));
    leader
        .broker
        .publish_batch(
            TENANT,
            NAMESPACE,
            QUORUM,
            0,
            &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        )
        .await
        .expect("publish");
    let publish_ctx = build_publish_context(
        Arc::clone(&leader.broker),
        &BrokerConfig::default(),
        ClusterContext {
            engine: std::sync::Arc::new(crate::shards::routing::StreamEngine::new(Some(
                Arc::clone(&leader.ingress),
            ))),
            ..ClusterContext::default()
        },
    );
    let broker = Arc::clone(&leader.broker);
    let trim = tokio::spawn(async move {
        trim_at(
            &broker,
            &publish_ctx,
            TENANT,
            NAMESPACE,
            QUORUM,
            0,
            1,
            2,
            Bytes::new(),
        )
        .await
    });
    let log = leader
        .broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, QUORUM, 0)
        .await
        .expect("log");
    // The trim record is in the log: the trim is now waiting on the mark.
    tokio::time::timeout(Duration::from_secs(10), async {
        while log.tail_offset().await.expect("tail") < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the trim record lands");
    // Let it get past the claim and into the wait for the mark.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let key = leader::stream_key(QUORUM);
    leader.fence_move(&key);
    // No yield between the drain and the check: the trim must still be
    // waiting when the shard is already quiet.
    tokio::time::timeout(Duration::from_secs(5), leader.ingress.fence().quiesce(&key))
        .await
        .expect("the shard drains while the trim waits on its commit");
    assert!(!trim.is_finished(), "the drain waited for the trim");
    trim.abort();
}

fn quorum_ctx(leader: &Leader) -> PublishContext {
    build_publish_context(
        Arc::clone(&leader.broker),
        &BrokerConfig {
            publish_quorum_timeout_ms: 300,
            ..BrokerConfig::default()
        },
        ClusterContext {
            engine: std::sync::Arc::new(crate::shards::routing::StreamEngine::new(Some(
                Arc::clone(&leader.ingress),
            ))),
            ..ClusterContext::default()
        },
    )
}

/// **A state read confirms leadership on the lease**, and a lapsed lease
/// refuses it.
#[tokio::test]
async fn a_state_read_confirms_leadership_through_the_engine() {
    let leader = Leader::start().await;
    let publish_ctx = quorum_ctx(&leader);
    confirm_read_leader(&publish_ctx, &leader::stream_key(QUORUM))
        .await
        .expect("the lease holds");
    leader.hold_lease().surrender();
    confirm_read_leader(&publish_ctx, &leader::stream_key(QUORUM))
        .await
        .expect_err("the lease lapsed");
}

/// **A stalled Raft trim is answered as the hybrid answers one**: the
/// hybrid's `unacknowledged` trim timeout at the offset it was proposed at,
/// not the quorum timeout its engine reports.
#[test]
fn a_raft_trim_that_does_not_commit_in_time_is_the_hybrids_trim_timeout() {
    let stalled = TrimFailed::Engine(CommitError::TimedOut {
        what: "publish",
        timeout: Duration::from_secs(30),
    });
    let NotWritten::Refused(raft) = raft_trim_failed(stalled, 7) else {
        panic!("a timeout is a refusal");
    };
    let timed_out = BrokerError::TrimTimedOut { offset: 7 };
    let hybrid = ClientError::from_broker(&timed_out, timed_out.to_string());
    assert_eq!(raft.code(), &felix_wire::ErrorCode::Unacknowledged);
    assert_eq!(raft, hybrid, "the hybrid's answer");
}

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
            engine: std::sync::Arc::new(crate::shards::routing::StreamEngine::new(
                None,
                Some(Arc::clone(&leader.ingress)),
            )),
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

fn quorum_ctx(
    leader: &Leader,
    marks: &Arc<felix_replication::quorum::QuorumMarks>,
) -> PublishContext {
    build_publish_context(
        Arc::clone(&leader.broker),
        &BrokerConfig {
            publish_quorum_timeout_ms: 300,
            ..BrokerConfig::default()
        },
        ClusterContext {
            engine: std::sync::Arc::new(crate::shards::routing::StreamEngine::new(
                Some(Arc::clone(marks)),
                Some(Arc::clone(&leader.ingress)),
            )),
            ..ClusterContext::default()
        },
    )
}

async fn publish_a(
    leader: &Leader,
    publish_ctx: &PublishContext,
    expected_offset: u64,
) -> Result<u64, NotWritten> {
    publish_at(
        &leader.broker,
        publish_ctx,
        TENANT,
        NAMESPACE,
        QUORUM,
        None,
        Some(0),
        vec![Bytes::from_static(b"a")],
        expected_offset,
        None,
    )
    .await
}

/// **A conditional publish waits for the mark at the generation it was
/// written at, for the configured time.** A mark at another generation does
/// not release it, and one at its own does.
#[tokio::test]
async fn a_conditional_publish_waits_for_the_mark_at_its_generation() {
    let leader = Leader::start().await;
    let marks = Arc::new(felix_replication::quorum::QuorumMarks::new());
    let publish_ctx = quorum_ctx(&leader, &marks);
    let key = leader::stream_key(QUORUM);

    marks.publish(&key, leader::GENERATION + 1, 1_000);
    let started = std::time::Instant::now();
    publish_a(&leader, &publish_ctx, 0)
        .await
        .expect_err("a mark at another generation released the write");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "waited past the timeout"
    );

    marks.publish(&key, leader::GENERATION, 1_000);
    publish_a(&leader, &publish_ctx, 1)
        .await
        .expect("the mark at its own generation releases it");
}

/// With no mark the wait ends at the configured timeout and says so.
#[tokio::test]
async fn a_conditional_publish_without_a_mark_times_out_at_the_configured_wait() {
    let leader = Leader::start().await;
    let marks = Arc::new(felix_replication::quorum::QuorumMarks::new());
    let publish_ctx = quorum_ctx(&leader, &marks);

    let started = std::time::Instant::now();
    let err = publish_a(&leader, &publish_ctx, 0)
        .await
        .expect_err("no mark");
    let waited = started.elapsed();
    let NotWritten::Refused(err) = err else {
        panic!("expected a refusal");
    };
    assert_eq!(err.code(), &felix_wire::ErrorCode::QuorumTimeout, "{err:?}");
    assert!(
        waited >= Duration::from_millis(300),
        "ended early after {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(3),
        "ended late after {waited:?}"
    );
}

/// A round that refuses every read, as one would for a newer leader.
struct Refuses;

#[async_trait::async_trait]
impl felix_replication::quorum::LeadershipCheck for Refuses {
    async fn confirm(
        &self,
        _key: &ShardKey,
        _generation: u64,
    ) -> Result<(), felix_replication::quorum::QuorumError> {
        Err(felix_replication::quorum::QuorumError::LeadershipLost {
            what: "read",
            detail: "a newer leader fenced the majority",
        })
    }
}

/// **A state read confirms leadership through the engine's marks and
/// router.** In a fleet that reads by round, the round decides a replicated
/// shard and the lease an unreplicated one; on the lease, a lapsed lease
/// refuses the read.
#[tokio::test]
async fn a_state_read_confirms_leadership_through_the_engine() {
    use felix_common::fleet::{FleetGate, GENERATION_START, LEASE_FREE_READS, MAJORITY_ACK};
    let leader = Leader::start().await;
    let names = [
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        LEASE_FREE_READS.name(),
    ];
    let fleet = Arc::new(FleetGate::new(names));
    fleet.observe(names);
    let by_round = Arc::new(felix_replication::quorum::QuorumMarks::with_fleet(fleet));
    by_round.set_read_check(Arc::new(Refuses), false);
    let publish_ctx = quorum_ctx(&leader, &by_round);
    confirm_read_leader(&publish_ctx, &leader::stream_key(QUORUM))
        .await
        .expect_err("the round refuses");
    confirm_read_leader(&publish_ctx, &leader::stream_key(leader::DURABLE))
        .await
        .expect("no replicas to ask: the lease decides");

    let by_lease = Arc::new(felix_replication::quorum::QuorumMarks::new());
    let publish_ctx = quorum_ctx(&leader, &by_lease);
    confirm_read_leader(&publish_ctx, &leader::stream_key(QUORUM))
        .await
        .expect("the lease holds");
    leader.hold_lease().surrender();
    confirm_read_leader(&publish_ctx, &leader::stream_key(QUORUM))
        .await
        .expect_err("the lease lapsed");
}

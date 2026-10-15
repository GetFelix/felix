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

    for finish in [
        Settle::Ack,
        Settle::Nack {
            delay: Duration::ZERO,
            attempts: None,
        },
        Settle::Nack {
            delay: Duration::from_secs(5),
            attempts: None,
        },
        Settle::DeadLetter,
    ] {
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
            "a settle ({finish:?}) landed after the fence closed"
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

    for finish in [
        Settle::Ack,
        Settle::Nack {
            delay: Duration::ZERO,
            attempts: None,
        },
        Settle::Nack {
            delay: Duration::from_secs(5),
            attempts: None,
        },
        Settle::DeadLetter,
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
            0,
            finish,
        )
        .await
        .expect_err("a settle landed without a lease");
        assert_eq!(
            refused.code(),
            &felix_wire::ErrorCode::ShardUnavailable,
            "{finish:?}"
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
        Settle::Ack,
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
        Settle::Ack,
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
    for finish in [
        Settle::Ack,
        Settle::Nack {
            delay: Duration::ZERO,
            attempts: None,
        },
        Settle::Nack {
            delay: Duration::from_secs(5),
            attempts: None,
        },
        Settle::DeadLetter,
    ] {
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
                "offset {offset}, {finish:?}"
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
        Settle::Nack {
            delay: Duration::ZERO,
            attempts: None,
        },
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
        Settle::Ack,
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
    let as_member = |connection, reclaim| {
        felix_broker::GroupConsumer::new("", "snapshotter", connection, reclaim)
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
                None,
            )
            .await
            .expect("poll")
        }
    };

    let held = poll_one(as_member(1, false)).await;
    assert_eq!(held[0].payload.as_ref(), b"first");

    let back = poll_one(as_member(2, true)).await;
    assert_eq!(back[0].offset, held[0].offset);
    assert_eq!(back[0].attempts, 2);
}

async fn seek_to(
    leader: &Leader,
    publish_ctx: &PublishContext,
    start: StartPosition,
    if_new: bool,
) -> Result<felix_broker::Seek, ClientError> {
    seek(
        &leader.broker,
        publish_ctx,
        None,
        TENANT,
        NAMESPACE,
        DURABLE,
        0,
        GROUP,
        start,
        if_new,
    )
    .await
}

/// A seek or a delete writes group state, so the fence refuses it like an
/// ack.
#[tokio::test]
async fn a_seek_or_delete_after_the_fence_is_refused() {
    let (mut leader, publish_ctx) = claimed_one().await;
    leader.fence_move(&leader::stream_key(DURABLE));

    assert!(
        seek_to(&leader, &publish_ctx, StartPosition::Latest, false)
            .await
            .is_err(),
        "a seek landed after the fence closed"
    );
    assert!(
        delete(
            &leader.broker,
            &publish_ctx,
            None,
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            GROUP,
        )
        .await
        .is_err(),
        "a delete landed after the fence closed"
    );
    assert_eq!(committed(&leader).await, None, "the cursor moved");
}

/// `earliest` and `latest` are the ends of what the shard holds, and an
/// offset outside them is refused rather than clamped.
#[tokio::test]
async fn a_seek_lands_inside_the_log() {
    let (leader, publish_ctx) = claimed_one().await;

    let latest = seek_to(&leader, &publish_ctx, StartPosition::Latest, false)
        .await
        .expect("latest");
    assert_eq!(latest.offset, 2);
    let earliest = seek_to(&leader, &publish_ctx, StartPosition::Earliest, false)
        .await
        .expect("earliest");
    assert_eq!(earliest.offset, 0);
    let past = seek_to(&leader, &publish_ctx, StartPosition::Offset(3), false)
        .await
        .expect_err("past the tail");
    assert_eq!(past.code(), &felix_wire::ErrorCode::InvalidRequest);
    assert_eq!(committed(&leader).await, Some(0));
}

#[tokio::test]
async fn describe_reports_the_tail_and_the_claims() {
    let (leader, publish_ctx) = claimed_one().await;

    let (snapshot, tail) = describe(
        &leader.broker,
        &publish_ctx,
        TENANT,
        NAMESPACE,
        DURABLE,
        0,
        GROUP,
    )
    .await
    .expect("describe");
    assert_eq!(tail, 2);
    assert_eq!(snapshot.committed, None);
    assert_eq!(snapshot.in_flight, 1);
}

/// Counts the rounds a group write confirms leadership with; every one
/// succeeds.
#[derive(Default)]
struct Confirms(std::sync::atomic::AtomicUsize);

#[async_trait::async_trait]
impl felix_replication::quorum::LeadershipCheck for Confirms {
    async fn confirm(
        &self,
        _key: &ShardKey,
        _generation: u64,
    ) -> Result<(), felix_replication::quorum::QuorumError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

/// A follower that stores every batch it is sent.
struct Accepting;

impl felix_replication::peer::PeerRequester for Accepting {
    async fn request(
        &self,
        _node_id: &str,
        _addr: std::net::SocketAddr,
        message: felix_wire::internal::InternalMessage,
    ) -> Result<felix_wire::internal::InternalMessage, felix_replication::peer::PeerError> {
        use felix_wire::internal::{InternalMessage, ReplicateOk};
        let (InternalMessage::ReplicateRecords(batch)
        | InternalMessage::ReplicateCacheRecords(batch)
        | InternalMessage::ReplicateCounterRecords(batch)) = message
        else {
            panic!("the driver sent {:?}", message.kind());
        };
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

/// A restarted `Quorum` leader in a fleet that reads by round, with two
/// records published and its fleet gate not yet told anything.
struct ByRound {
    leader: Leader,
    fleet: Arc<felix_common::fleet::FleetGate>,
    marks: Arc<felix_replication::quorum::QuorumMarks>,
    confirms: Arc<Confirms>,
    publish_ctx: PublishContext,
}

impl ByRound {
    async fn start() -> Self {
        let leader = Leader::start().await;
        leader
            .broker
            .publish_batch(
                TENANT,
                NAMESPACE,
                leader::QUORUM,
                0,
                &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            )
            .await
            .expect("publish");
        let fleet = Arc::new(felix_common::fleet::FleetGate::for_this_build());
        let marks = Arc::new(felix_replication::quorum::QuorumMarks::with_fleet(
            Arc::clone(&fleet),
        ));
        let confirms = Arc::new(Confirms::default());
        marks.set_read_check(Arc::clone(&confirms) as _, false);
        marks.publish(&leader::stream_key(leader::QUORUM), leader::GENERATION, 2);
        let publish_ctx = build_publish_context(
            Arc::clone(&leader.broker),
            &BrokerConfig::default(),
            ClusterContext {
                ingress: Some(Arc::clone(&leader.ingress)),
                marks: Some(Arc::clone(&marks)),
                ..ClusterContext::default()
            },
        );
        Self {
            leader,
            fleet,
            marks,
            confirms,
            publish_ctx,
        }
    }

    /// What registration does: take the fleet's answer, which reads by round.
    fn register(&self) {
        use felix_common::fleet::{GENERATION_START, LEASE_FREE_READS, MAJORITY_ACK};
        self.fleet.restart(
            [GENERATION_START, MAJORITY_ACK, LEASE_FREE_READS]
                .iter()
                .map(|feature| feature.name()),
        );
    }

    /// One replication pass against this leader's real fence.
    async fn pass(&self) {
        use std::collections::HashMap;
        felix_replication::driver::replicate_once_with(
            &Accepting,
            &self.leader.broker,
            &self.leader.router,
            self.leader.ingress.fence().as_ref(),
            &felix_replication::promotion::NoGate,
            &self.marks,
            None,
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut HashMap::new(),
            &felix_replication::Rebuilds::disabled(),
            &felix_replication::MoveThrottle::unlimited(),
        )
        .await;
    }

    async fn take(&self) -> Result<Vec<GroupRecord>, ClientError> {
        poll(
            &self.leader.broker,
            &self.publish_ctx,
            None,
            TENANT,
            NAMESPACE,
            leader::QUORUM,
            0,
            GROUP,
            1,
            Duration::ZERO,
            None,
            None,
        )
        .await
    }
}

/// A restarted leader holds a lease before its first replication pass, while
/// placement may still promote a follower on the report its previous process
/// left. In a fleet that reads by round, group state on the lease alone would
/// then be acknowledged and lost, so it waits for the pass.
#[tokio::test]
async fn group_state_waits_for_the_first_pass_in_a_fleet_that_reads_by_round() {
    use felix_replication::driver::WriteFence;

    let shard = ByRound::start().await;
    shard.register();
    let _lease = shard.leader.hold_lease();
    let key = leader::stream_key(leader::QUORUM);

    let refused = shard
        .take()
        .await
        .expect_err("claimed on the lease before the first pass");
    assert_eq!(refused.code(), &felix_wire::ErrorCode::ShardUnavailable);

    // A pass that left the shard on the lease: the lease serves it again.
    WriteFence::passed(
        shard.leader.ingress.fence().as_ref(),
        &key,
        leader::GENERATION,
    );
    assert_eq!(shard.take().await.expect("claimed on the lease").len(), 1);
}

/// **A pass made before registration answered decides nothing.** Replication
/// starts before the broker registers, so its first pass can see an empty
/// fleet and leave the shard on the lease. Once registration adopts a fleet
/// that reads by round and the lease is granted, that pass must not let group
/// writes through on the lease: placement may already be promoting on the
/// previous process's report. Only a pass after adoption does.
#[tokio::test]
async fn a_pass_before_registration_does_not_open_group_writes_to_the_lease() {
    let shard = ByRound::start().await;
    shard.pass().await;
    shard.register();
    let _lease = shard.leader.hold_lease();

    let refused = shard
        .take()
        .await
        .expect_err("claimed on the lease after a pass that saw no fleet");
    assert_eq!(refused.code(), &felix_wire::ErrorCode::ShardUnavailable);

    shard.pass().await;
    assert_eq!(shard.take().await.expect("claimed after a pass").len(), 1);
}

/// **After a pass that reads by round, group writes confirm by a round and
/// need no lease.**
#[tokio::test]
async fn a_by_round_pass_confirms_group_writes_by_a_round() {
    let shard = ByRound::start().await;
    shard.register();
    let lease = shard.leader.hold_lease();
    shard.pass().await;
    lease.surrender();

    assert_eq!(
        shard
            .take()
            .await
            .expect("claimed by a round without the lease")
            .len(),
        1
    );
    assert!(
        shard.confirms.0.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "the write was not confirmed by a round",
    );
}
